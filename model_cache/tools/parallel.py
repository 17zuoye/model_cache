# -*- coding: utf-8 -*-

import os, glob, time, math
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
    def scope_range(self, from_idx, to_idx): raise NotImplemented
    def convert_iter_to_item(self, iter1): raise NotImplemented
    def convert_iter_to_item_id(self, iter1): raise NotImplemented

class ModelCacheDataSource(DataSource):

    def post_hook(self):
        assert 'datadict' in dir(self.datasource), u"datasource should be a ModelCache"

    def __len__(self): return len(self.datasource)

    def scope_range(self, from_idx, to_idx):
        # NOTE 不知道这里 datasource[item_id1] 随机读写效率如何，虽然 item_ids 其实是磁盘顺序的
        # 实际跑数据结果表示还是比Sqlite顺序读还快。这里dbm是17K/s, sqlite是14K/s，仅供参考。
        return self.item_ids[from_idx:to_idx]

    def convert_iter_to_item(self, iter1):    return self.datasource[iter1]
    def convert_iter_to_item_id(self, iter1): return iter1

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
            self.datasource = self.datasource.find({})

    def __len__(self): return self.datasource.count()

    def scope_range(self, from_idx, to_idx):
        return self.datasource.skip(from_idx).limit(to_idx-from_idx)

    def convert_iter_to_item(self, iter1):    return iter1
    def convert_iter_to_item_id(self, iter1): return unicode(iter1.get('_id', u""))

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
            setattr(self, k1, getattr(second_params, k1, default_v1))

        if isinstance(self.cache_filename, str): self.cache_filename = unicode(self.cache_filename, "UTF-8")
        assert isinstance(self.cache_filename, unicode)

        self.process_count = self.process_count or (multiprocessing.cpu_count()-2)
        self.scope_count   = len(self.datasource)

        fix_offset = lambda num : ( num / self.chunk_size + 1 ) * self.chunk_size
        fixed_scope_count  = fix_offset(self.scope_count)
        self.scope_limit   = fix_offset(fixed_scope_count / self.process_count)

        self.result = self.connnection()
        self.result_len = len(self.result)
        if self.result_len == 0: os.system("rm -f %s" % self.cache_filename)

    def connnection(self): return shelve.open(self.cache_filename, flag='c', writeback=False)

    def recache(self):
        items_cPickles = lambda : sorted( \
                            glob.glob(self.cache_filename + '.*'), \
                            key=lambda f1: int(f1.split("/")[-1].split(".")[-1]) # sort by chunk steps
                        )

        def process__load_items_func(from_idx, to_idx):

            while (from_idx < to_idx):
                def load_items_func():
                    return [ \
                            [ \
                                self.datasource.convert_iter_to_item_id(iter1), \
                                self.item_func(self.datasource.convert_iter_to_item(iter1)), \
                            ] \
                                for iter1 in process_notifier(self.datasource.scope_range(from_idx, from_idx+self.chunk_size))]
                filename = self.cache_filename + u'.' + unicode(from_idx)
                if not os.path.exists(filename): cpickle_cache(filename, load_items_func)
                from_idx += self.chunk_size

        # 检查所有items是否都存在
        if len(items_cPickles()) < math.ceil(self.scope_count / float(self.chunk_size)):
            pn("[begin parallel process items] ...")
            for idx in xrange(self.process_count):
                from_idx = idx * self.scope_limit
                to_idx   = (idx + 1) * self.scope_limit - 1
                if to_idx > self.scope_count: to_idx = self.scope_count
                pn("[multiprocessing] range %i - %i " % (from_idx, to_idx))
                multiprocessing.Process(target=process__load_items_func, \
                                        args=tuple((from_idx, to_idx,))).start()

        # Check if extract from original is finished.
        sleep_sec = lambda : len(multiprocessing.active_children())
        while sleep_sec() > 0: time.sleep(sleep_sec())

        if not self.output_lambda: self.result = self.connnection()
        def write(tmp_items):
            if self.output_lambda:
                self.output_lambda(tmp_items)
            else:
                for item_id, item1 in process_notifier(tmp_items):
                    self.result[item_id] = item1
                self.result.sync()

            return []

        print "\n"*5, "begin merge ..."
        tmp_items = []
        for pickle_filename in items_cPickles():
            chunk = cpickle_cache(pickle_filename, lambda: True)
            tmp_items.extend(chunk)
            if len(tmp_items) >= self.merge_size:
                tmp_items = write(tmp_items)
            tmp_items = write(tmp_items)
