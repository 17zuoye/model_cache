# -*- coding: utf-8 -*-

import os, glob, time, math
import multiprocessing
import shelve
from etl_utils import process_notifier, cpickle_cache
from termcolor import cprint

def pn(msg): cprint(msg, 'blue')

class ParallelShelve(object):
    """
    Input:    ModelCache

    => multiprocessing <=

    Output:   shelve
    """

    @classmethod
    def process(cls, model_cache, cache_filename, item_func, **attrs):
        attrs['model_cache']    = model_cache
        attrs['cache_filename'] = cache_filename
        attrs['item_func']      = item_func

        ps = ParallelShelve(attrs)
        if (len(ps.model_cache) - len(ps.result)) > ps.offset: ps.recache()

        return ps.result

    def __init__(self, params):
        first_params = "model_cache cache_filename item_func".split(" ")
        second_params = {"process_count" : None,
                         "chunk_size"    : 1000,
                         "merge_size"    : 10000,
                         "offset"        : 10,}

        for k1 in first_params: setattr(self, k1, params[k1])
        for k1 in second_params:
            default_v1 = second_params.get(k1, False)
            setattr(self, k1, getattr(second_params, k1, default_v1))

        if isinstance(self.cache_filename, str): self.cache_filename = unicode(self.cache_filename, "UTF-8")
        assert isinstance(self.cache_filename, unicode)

        self.process_count = self.process_count or (multiprocessing.cpu_count()-2)
        self.scope_count   = len(self.model_cache)

        fix_offset = lambda num : ( num / self.chunk_size + 1 ) * self.chunk_size
        fixed_scope_count  = fix_offset(self.scope_count)
        self.scope_limit   = fix_offset(fixed_scope_count / self.process_count)

        assert 'datadict' in dir(self.model_cache), u"model_cache should be a ModelCache"

        self.result = self.connnection()
        if len(self.result) == 0: os.system("rm -f %s" % self.cache_filename)

    def connnection(self): return shelve.open(self.cache_filename, writeback=False)

    def recache(self):
        items_cPickles = lambda : sorted( \
                            glob.glob(self.cache_filename + '.*'), \
                            key=lambda f1: int(f1.split("/")[-1].split(".")[-1]) # sort by chunk steps
                        )

        # TODO 也许可以优化为iter，但是不取出来无法对键进行分割
        # 现在的问题是keys浪费内存，特别是百千万级别时
        item_ids = self.model_cache.keys()

        def process__load_items_func(item_ids, from_idx, to_idx):
            # multiple process can't share the same file instance which forked from the same parent process
            if self.model_cache.original.storage_type != 'memory': self.model_cache.reconnect()

            while (from_idx < to_idx):
                def load_items_func():
                    # NOTE 不知道这里 model_cache[item_id1] 随机读写效率如何，虽然 item_ids 其实是磁盘顺序的
                    return [[item_id1, self.item_func(self.model_cache[item_id1]),] \
                                for item_id1 in process_notifier(item_ids[from_idx:(from_idx+self.chunk_size)])]
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
                                        args=tuple((item_ids, from_idx, to_idx,))).start()

        # Check if extract from original is finished.

        sleep_sec = lambda : len(multiprocessing.active_children())
        while sleep_sec() > 0: time.sleep(sleep_sec())

        self.result = self.connnection()
        def write(tmp_items):
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
