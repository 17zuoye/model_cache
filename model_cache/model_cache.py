# -*- coding: utf-8 -*-

import os
import json
from .storage import *
from .load_data import LoadData

class ModelCache(LoadData):

    cache_dir     = None

    datadict_type = ["memory", "sqlite", "redis"][1]
    datadict      = None

    def __init__(self, record):
        self.load_data(record)

        assert self.item_id, "self.item_id should be assign in self.load_data function!"
        assert type(self.item_content) in [str, unicode], "self.item_content should be assign in self.load_data function!"

    def load_data(self, record):
        """
        extract data.
        e.g. self.item_id, self.item_content, etc...
        """
        raise NotImplemented

    def dump_record(self, record):
        return json.dumps(record)

    def has_item_id(self, record):
        """ Detect if there is an item_id, which should be already wrote to database """
        raise NotImplemented

    @classmethod
    def init_datadict(cls):
        assert cls.cache_dir, u"cache_dir should be seted."

        class_name = repr(cls).split("'")[1].split(".")[-1]
        dbpath = os.path.join(cls.cache_dir, class_name + ".db")

        cls.datadict = {
            "memory" : ModelCacheStoreMemory,
            "sqlite" : ModelCacheStoreSqlite,
            "redis"  : ModelCacheStoreRedis,
        }[cls.datadict_type](dbpath)
        print "[ModelCache] Init at %s" % dbpath

        return cls.datadict

    @classmethod
    def build_indexes(cls, items=[]):
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

    @classmethod
    def filter_deleted(cls, record):
        return False
