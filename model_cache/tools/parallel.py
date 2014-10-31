# -*- coding: utf-8 -*-

import os, glob, time, math, itertools
import multiprocessing
import shelve
from etl_utils import process_notifier, cpickle_cache, cached_property, set_default_value
from termcolor import cprint

def pn(msg): cprint(msg, 'blue')

class Datasource(object):
    def __init__(self, datasource):
        self.datasource = datasource
        self.post_hook()
        self.id_func = None

    def post_hook(self): pass
    def __len__(self): raise NotImplemented
    def __iter__(self): raise NotImplemented

    def reconnect_after_fork(self): return self

class DictLikeDatasource(Datasource):
    def post_hook(self):
        if "reconnect" in dir(self.datasource):
            self.datasource.reconnect()

    def __len__(self): return len(self.datasource)
    def __iter__(self):
        for k1, v1 in self.datasource.iteritems():
            yield k1, v1

    reconnect_after_fork = post_hook

class ListLikeDatasource(Datasource):
    def post_hook(self):
        # compact with mongodb
        if 'collection' not in dir(self.datasource):
            try:
                self.datasource = self.datasource.find()
            except:
                None

    def __len__(self):
        result = set_default_value([ \
                    lambda : self.datasource.count(), \
                    lambda : len(self.datasource), \
                    ], u"self.datasource is invalid!")
        return result

    def __iter__(self):
        for v1 in self.datasource:
            yield unicode(self.id_func(v1)), v1

    reconnect_after_fork = post_hook

class PickleFile(object):
    """ 序列化文件相关 """
    def __init__(self, offset, io_prefix, cpu_prefix):
        self.offset     = offset
        self.io_prefix  = io_prefix
        self.cpu_prefix = cpu_prefix
        self.done       = False

    def is_exists(self, _type='io'):
        f1 = getattr(self, (_type + '_name'))()
        return os.path.exists(f1)

    def io_name(self): return self.io_prefix + unicode(self.offset)
    def cpu_name(self): return self.cpu_prefix + unicode(self.offset)
    def __repr__(self): return "<offset:%s, done:%s>" % (self.offset, self.done)

class FileQueue(list):
    """ 分片文件队列 """
    def __init__(self, max_size, chunk_size, process_count, offset, file_lambda):
        super(FileQueue, self).__init__()
        for chunk1 in itertools.count(0, chunk_size):
            if (chunk1 - max_size) >= 0: break
            if (chunk1 / chunk_size) % process_count != offset: continue
            self.append(file_lambda(chunk1))
        self.todo_list = list(self)

    def has_todo(self):
        self.todo_list = filter(lambda f1: not f1.done, self)
        return bool(self.todo_list)

class ActiveChildrenManagement(object):
    """ 多进程管理 是否结束 """
    def __init__(self):
        self.seconds = 1

    def still(self):
        self.seconds = len(multiprocessing.active_children())
        return bool(self.seconds)

class ParallelData(object):
    """
    Input:    Datasource

    => multiprocessing <=

    Output:   shelve, model_cache, ...

    流程图
    1. 划分好进程数
    2. fork 1个进程读取IO，并按进程数分片
    3. fork进程数，按总数和取模知道那些分片归自己处理，包括IO没完成的
    4. 这样23同时处理。

    同时也解决skip+limit划分中途出错问题。

    每个进程的CPU占用率取决于这些并行任务的CPU计算相对于IO读写的运行时间比例。
    """

    @classmethod
    def process(cls, datasource, datasource_type, cache_filename, **attrs):
        attrs['datasource']     = {
                "list" : ListLikeDatasource,
                "dict" : DictLikeDatasource,
            }[datasource_type](datasource)
        attrs['cache_filename'] = cache_filename

        ps = ParallelData(attrs)
        if (len(ps.datasource) - ps.result_len) > ps.offset: ps.recache()

        if ps.result_len == 0: os.system("rm -f %s" % ps.cache_filename)

        return ps.result

    def __init__(self, params):
        default_params = {"process_count"     : None,
                          "chunk_size"        : 1000,
                          "merge_size"        : 10000,
                          "offset"            : 10,

                          "output_lambda"     : None, # lambda items: None
                          "output_len_lambda" : None,

                          "cache_filename"    : None,
                          "item_func"         : lambda item1 : item1,
                          "id_func"           : lambda record: record['_id'],
                         }

        setattr(self, "datasource", params['datasource'])

        for k1 in default_params:
            default_v1 = default_params.get(k1, False)
            setattr(self, k1, params.get(k1, default_v1))
        self.datasource.id_func = params.get('id_func', default_params['id_func'])

        if isinstance(self.cache_filename, str):
            self.cache_filename = unicode(self.cache_filename, "UTF-8")
        assert isinstance(self.cache_filename, unicode)
        self.cache_basename = os.path.basename(self.cache_filename).split(".")[0]

        if (self.output_lambda is None) and (self.output_len_lambda is None):
            self.output_len_lambda = lambda : len(self.result)

        self.process_count = self.process_count or (multiprocessing.cpu_count()-2)
        self.scope_count   = len(self.datasource)

        fix_offset = lambda num : ( num / self.chunk_size + 1 ) * self.chunk_size
        fixed_scope_count  = fix_offset(self.scope_count)
        self.scope_limit   = fix_offset(fixed_scope_count / self.process_count)

        if self.output_lambda:
            self.result = None
            self.result_len = self.output_len_lambda()
        else:
            self.result = self.connection
            self.result_len = len(self.result)

    @cached_property
    def connection(self):
        return shelve.open(self.cache_filename, flag='c', writeback=False)

    def recache(self):
        # compact with shelve module generate "dat, dir, bak" three postfix files
        io_prefix  = self.cache_filename +  '.io.'
        io_regexp  = io_prefix +  '[0-9]*'
        cpu_prefix = self.cache_filename + '.cpu.'
        cpu_regexp = cpu_prefix + '[0-9]*'

        os.system("cd %s" % os.path.dirname(self.cache_filename))

        # A.1. 缓存IO
        def cache__io():
            self.datasource.reconnect_after_fork()

            pn("[%s cache__io] begin total ..." % self.cache_basename)
            def persistent(filename, current_items):
                cpickle_cache(filename, lambda : current_items)
                return []

            # A.1.1 如果全部缓存了，就不处理了
            if (len(self.datasource) / self.chunk_size) + 1 == len(glob.glob(io_regexp)):
                pn("[%s cache__io] end total ..." % self.cache_basename)
                return False

            # A.1.2 否则还是重新处理一遍
            current_items = []
            idx = 0
            for k1, v1 in self.datasource:
                current_items.append([k1, v1])
                if len(current_items) >= self.chunk_size:
                    cache_path = io_prefix + unicode(idx)
                    os.system("rm -f %s" % cache_path)
                    current_items = persistent(cache_path, current_items)
                    idx += self.chunk_size
            if current_items: persistent(io_prefix + unicode(idx), current_items)
            pn("[%s cache__io] end total ..." % self.cache_basename)
        multiprocessing.Process(target=cache__io).start()

        # A.2. 在IO基础上缓存CPU
        def cache__cpu(cpu_offset):
            fq = FileQueue(self.scope_count, self.chunk_size, self.process_count, cpu_offset, \
                           lambda chunk1 : PickleFile(chunk1, io_prefix, cpu_prefix))
            while_step = 0
            while fq.has_todo():
                while_step += 1
                pn("[%s cache__cpu:%s] todo_list := %s, while_step := %s" % (self.cache_basename, cpu_offset, fq.todo_list, while_step))
                for f1 in fq.todo_list:
                    if not f1.is_exists('io'): continue
                    if f1.is_exists('cpu'):
                        f1.done = True
                        continue
                    try:
                        io_items = cpickle_cache(f1.io_name(), lambda : not_exist)
                        cpu_items = [[i1[0], self.item_func(i1[1])] for i1 in io_items]
                        cpickle_cache(f1.cpu_name(), lambda : cpu_items)
                        f1.done = True
                    except: # 在IO进程中还没有写完这个文件
                        print "Maybe IO error happened ..."
                        continue
                time.sleep(1)
        for cpu_offset in xrange(self.process_count):
            multiprocessing.Process(target=cache__cpu, args=(cpu_offset,)).start()

        # B. 在前面基础上合并全部
        # Check if extract from original is finished.
        acm = ActiveChildrenManagement()
        while acm.still(): time.sleep(acm.seconds)

        def write(tmp_items):
            if self.output_lambda:
                self.output_lambda([i1[1] for i1 in tmp_items])
            else:
                for item_id, item1 in process_notifier(tmp_items):
                    self.result[item_id] = item1
                self.result.sync()
            return []

        print "\n"*5, "begin merge ..."
        tmp_items = []
        fs = sorted(glob.glob(cpu_regexp), key=lambda f1: int(f1.split("/")[-1].split(".")[-1]))
        for f1 in fs:
            chunk = cpickle_cache(f1, lambda: not_exist)
            tmp_items.extend(chunk)
            if len(tmp_items) >= self.merge_size:
                tmp_items = write(tmp_items)
        tmp_items = write(tmp_items)

        # update cache result len
        self.result_len = self.output_len_lambda()
