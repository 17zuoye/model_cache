# -*- coding: utf-8 -*-

import os

from .store.sqlite import ModelCacheStoreSqlite
from .store.redis  import ModelCacheStoreRedis

class ModelCache(object):

    cache_dir     = None

    datadict_type = ["sqlite", "redis"][0]
    datadict      = None

    def has_item_id(self, record):
        """ Detect if there is an item_id, which should be already wrote to database """
        raise NotImplemented

    @classmethod
    def init_datadict(cls):
        class_name = repr(cls).split("'")[1].split(".")[-1]
        dbpath = os.path.join(cls.cache_dir, class_name + ".db")

        cls.datadict = {
            "sqlite" : ModelCacheStoreSqlite,
            "redis"  : ModelCacheStoreRedis,
        }[cls.datadict_type](dbpath)
        print "[ModelCache] Init at %s" % dbpath

        return cls.datadict

    @classmethod
    def build_indexes(cls, items=[]):
        cls.init_datadict()

        # items 必定是list, 经过cPickle反序列化回来的
        cls.datadict.build_indexes(items)

    @classmethod
    def find(cls, object_id):
        object_id = str(object_id)

        if cls.datadict.has_key(object_id):
            return cls.datadict[object_id]
        else:
            print "跳过 [%s] 这个被过滤的Question, 因为它的['enabled']或['verify']['verified']是False" % object_id
            return None

    @classmethod
    def remove(cls, object_id):
        object_id = str(object_id)
        if cls.datadict.has_key(object_id):
            del cls.datadict[object_id]

    @classmethod
    def count(cls): return len(cls.datadict)
