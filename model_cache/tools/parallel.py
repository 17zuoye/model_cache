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
        if model_cache.original.storage_type != 'memory': model_cache.reconnect()

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
        self.scope_limit   = (self.scope_count / self.process_count) + 1

        assert 'datadict' in dir(self.model_cache), u"model_cache should be a ModelCache"

        self.result = shelve.open(self.cache_filename, writeback=False)

    def recache(self):
        items_cPickles = lambda : sorted( \
                            glob.glob(self.cache_filename + '.*'), \
                            key=lambda f1: int(f1.split("/")[-1].split(".")[-1]) # sort by chunk steps
                        )

        # TODO 也许可以优化为iter，但是不取出来无法对键进行分割
        # 现在的问题是keys浪费内存，特别是百千万级别时
        item_ids = self.model_cache.keys()

        def process__load_items_func(item_ids, from_idx, to_idx):
            new_idx = from_idx / self.chunk_size * self.chunk_size
            if new_idx < from_idx: new_idx += self.chunk_size # 不要替代前一个去执行
            while (new_idx < to_idx):
                def load_items_func():
                    items = []
                    for item_id1 in item_ids[new_idx:(new_idx+self.chunk_size)]:
                        # NOTE 不知道这里 model_cache[item_id1] 随机读写效率如何，虽然 item_ids 其实是磁盘顺序的
                        f1 = self.item_func(self.model_cache[item_id1])
                        if not f1.item_content: continue # 过滤内容长度为0的item
                        items.append(f1)
                    return items
                filename = self.cache_filename + u'.' + unicode(new_idx)
                if not os.path.exists(filename): cpickle_cache(filename, load_items_func)
                new_idx += self.chunk_size

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
        sleep_count = 0
        while multiprocessing.active_children():
            time.sleep(sleep_count % 5)
            sleep_count += 1

        def write(tmp_items):
            for item1 in process_notifier(tmp_items):
                self.result[item1.item_id] = item1
            self.result.sync()
            tmp_items = []

        tmp_items = []
        for pickle_filename in items_cPickles():
            chunk = cpickle_cache(pickle_filename, lambda: True)
            tmp_items.extend(chunk)
            if len(tmp_items) >= self.merge_size:
                write(tmp_items)
            write(tmp_items)
