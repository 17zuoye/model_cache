# -*- coding: utf-8 -*-

import os
import json
from .storage import *
from .load_data import LoadData
from etl_utils import process_notifier

class ModelCache(LoadData):
    valid_storage_types = ("memory", "sqlite", "redis")

    @classmethod
    def config(cls, original_model, **kwargs):
        # assert original_model's behavior
        process_notifier(original_model)

        # setup args
        default_kwargs = {
                    'cache_dir'      : None,
                    'storage_type'   : None,
                    'percentage'     : 0.9999,
                    'filter_lambda'  : lambda item1: False,
                    'read_id_lambda' : lambda item1: str(item1['_id']),
                }
        for k1, v1 in kwargs.iteritems():
            if k1 in default_kwargs:
                default_kwargs[k1] = v1

        # validate storage
        assert default_kwargs['storage_type'] in ModelCache.valid_storage_types
        if (default_kwargs['cache_dir'] is None) and (default_kwargs['storage_type'] != "memory"):
            raise Exception(u"`cache_dir` should not be None when storage_type is not memory.")

        # decorate class
        def _model_cache_decorator(decorated_class):
            # ensure decorated_class's methods will overwrite ModelCache's.
            class _model_cache(decorated_class, ModelCacheClass):
                class OriginalClass(): pass
                original = OriginalClass()
                for k1, v1 in default_kwargs.iteritems():
                    setattr(original, k1, v1)
                    del k1; del v1
                original.model   = original_model
            _model_cache.__name__ = decorated_class.__name__
            return _model_cache
        return _model_cache_decorator


class ModelCacheClass(object):

    def __init__(self, record={}):
        self.load_data(record)

        assert self.item_id, "self.item_id should be assign in self.load_data function!"
        assert type(self.item_content) in [str, unicode], \
                "self.item_content should be assign in self.load_data function!"

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
        """ TODO 也许datadict可以作为属性，如果失效就重新生成，参考cache_property """
        class_name = repr(cls).split("'")[1].split(".")[-1]
        dbpath = None
        if cls.original.cache_dir:
            dbpath = os.path.join(cls.original.cache_dir, class_name + ".db")

        cls.datadict = {
            "memory" : ModelCacheStoreMemory,
            "sqlite" : ModelCacheStoreSqlite,
            "redis"  : ModelCacheStoreRedis,
        }[cls.original.storage_type](dbpath)
        print "[ModelCache] Init at %s" % dbpath

        return cls.datadict

    @classmethod
    def build_indexes(cls, items=[]):
        # items 必定是list, 经过cPickle反序列化回来的
        cls.datadict.build_indexes(items)

    @classmethod
    def find(cls, object_id):
        return cls.datadict.datadict.get(str(object_id), None)

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
