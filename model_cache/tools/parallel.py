# -*- coding: utf-8 -*-

import os, glob, time, math, itertools
import multiprocessing
import shelve
from etl_utils import process_notifier, cpickle_cache, cached_property
from termcolor import cprint

def pn(msg): cprint(msg, 'blue')

class DataSource(object):
    def __init__(self, datasource):
        self.datasource = datasource
        self.post_hook()

    def post_hook(self): pass
    def __len__(self): raise NotImplemented
    def __iter__(self): raise NotImplemented

class ModelCacheDataSource(DataSource):

    def post_hook(self):
        assert 'datadict' in dir(self.datasource), u"datasource should be a ModelCache"

    def __len__(self): return len(self.datasource)
    def __iter__(self):
        for k1, v1 in self.datasource.iteritems():
            yield k1, v1

    @cached_property
    def item_ids(self):
        # TODO 也许可以优化为iter，但是不取出来无法对键进行分割
        # 现在的问题是keys浪费内存，特别是百千万级别时

        # multiple process can't share the same file instance which forked from the same parent process
        if self.datasource.original.storage_type != 'memory': self.datasource.reconnect()

        return self.datasource.keys()

class MongodbDataSource(DataSource):
    def post_hook(self):
        if 'collection' not in dir(self.datasource):
            self.datasource = self.datasource.find()

    def __len__(self): return self.datasource.count()
    def __iter__(self):
        for v1 in self.datasource:
            yield unicode(v1.get('_id', '')), v1

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
    Input:    DataSource

    => multiprocessing <=

    Output:   shelve, model_cache, ...
    """

    @classmethod
    def process(cls, datasource, datasource_type, cache_filename, item_func, **attrs):
        attrs['datasource']     = {
                "mongodb" : MongodbDataSource,
                "model_cache" : ModelCacheDataSource
            }[datasource_type](datasource)

        attrs['cache_filename'] = cache_filename
        attrs['item_func']      = item_func

        ps = ParallelData(attrs)
        if (len(ps.datasource) - ps.result_len) > ps.offset: ps.recache()

        return ps.result

    def __init__(self, params):
        first_params = "datasource cache_filename item_func".split(" ")
        second_params = {"process_count" : None,
                         "chunk_size"    : 1000,
                         "merge_size"    : 10000,
                         "offset"        : 10,
                         "output_lambda" : None,
                         }

        for k1 in first_params: setattr(self, k1, params[k1])
        for k1 in second_params:
            default_v1 = second_params.get(k1, False)
            setattr(self, k1, params.get(k1, default_v1))

        if isinstance(self.cache_filename, str):
            self.cache_filename = unicode(self.cache_filename, "UTF-8")
        assert isinstance(self.cache_filename, unicode)

        self.process_count = self.process_count or (multiprocessing.cpu_count()-2)
        self.scope_count   = len(self.datasource)

        fix_offset = lambda num : ( num / self.chunk_size + 1 ) * self.chunk_size
        fixed_scope_count  = fix_offset(self.scope_count)
        self.scope_limit   = fix_offset(fixed_scope_count / self.process_count)

        self.result = None
        if not self.output_lambda: self.result = self.connection
        self.result_len = len(self.result or {})
        if self.result_len == 0: os.system("rm -f %s" % self.cache_filename)

    @cached_property
    def connection(self):
        return shelve.open(self.cache_filename, flag='c', writeback=False)

    def recache(self):
        # compact with shelve module generate "dat, dir, bak" three postfix files
        io_prefix  = self.cache_filename +  '.io.'
        io_regexp  = io_prefix +  '[0-9]*'
        cpu_prefix = self.cache_filename + '.cpu.'
        cpu_regexp = cpu_prefix + '[0-9]*'


        # A.1. 缓存IO
        def cache__io():
            def persistent(filename, current_items):
                cpickle_cache(filename, lambda : current_items)
                return []
            current_items = []
            idx = 0
            for k1, v1 in self.datasource:
                current_items.append([k1, v1])
                if len(current_items) >= self.chunk_size:
                    current_items = persistent(io_prefix + unicode(idx*self.chunk_size), current_items)
                    idx += 1
            current_items = persistent(io_prefix + unicode(idx), current_items)
        pn("[cache__io] total ...")
        multiprocessing.Process(target=cache__io).start()

        # A.2. 在IO基础上缓存CPU
        def cache__cpu(cpu_offset):
            fq = FileQueue(self.scope_count, self.chunk_size, self.process_count, cpu_offset, \
                           lambda chunk1 : PickleFile(chunk1, io_prefix, cpu_prefix))
            fq.has_todo()
            while fq.has_todo():
                pn("[cache__cpu] %s ... %s" % (cpu_offset, fq.todo_list))
                for f1 in fq.todo_list:
                    if not f1.is_exists(): continue
                    try:
                        io_items = cpickle_cache(f1.io_name(), lambda : None)
                        cpu_items = [[i1[0], self.item_func(i1[1])] for i1 in io_items]
                        cpickle_cache(cpu_prefix + unicode(f1.offset), lambda : cpu_items)
                        f1.done = True
                    except: # 在IO进程中还没有写完这个文件
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
        for f1 in glob.glob(cpu_regexp):
            chunk = cpickle_cache(f1, lambda: True)
            tmp_items.extend(chunk)
            if len(tmp_items) >= self.merge_size:
                tmp_items = write(tmp_items)
            tmp_items = write(tmp_items)
