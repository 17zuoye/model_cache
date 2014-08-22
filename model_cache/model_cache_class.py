# -*- coding: utf-8 -*-

import json
from .storage import *

class ModelCacheClass(object):

    def __init__(self, record={}):
        """ You should overwrite `init__before` and `init__after`, \
            instead of this `__init__` function.

            `__init__after` this name style will conflict with default \
            python object functions.
            """
        self.init__before(record)

        self.init__load_data(record)

        assert self.item_id, "self.item_id should be assigned in self.load_data function!"
        assert isinstance(self.item_content, unicode), \
                "self.item_content should be assigned as unicode in self.load_data function!"

        self.init__after(record)

    def init__before(self, record): pass

    def init__load_data(self, record):
        """
        extract data.
        e.g. self.item_id, self.item_content, etc...
        """
        raise NotImplemented

    def init__after(self, record): pass

    def dump_record(self, record):
        return json.dumps(record)


    @classmethod
    def connect(cls): cls.reconnect(is_reconnect=False)

    @classmethod
    def reconnect(cls, is_reconnect=True):
        cls.datadict = {
            "memory" : ModelCacheStoreMemory,
            "sqlite" : ModelCacheStoreSqlite,
            "redis"  : ModelCacheStoreRedis,
        }[cls.original.storage_type](cls.dbpath)

        msg = 'Reconnect' if is_reconnect else 'Init'
        print "[ModelCache] %s at %s" % (msg, cls.dbpath or '[memory]')

    @classmethod
    def pull_data(cls):
        print; print "[LOAD] %s [INTO] %s" % (cls.original.model.__module__, cls.__module__)

        original_model_count = set_default_value([ \
                lambda : cls.original.model.count(), \
                lambda : len(cls.original.model), \
                ], u"original.model is invalid!")

        if len(cls) / float(original_model_count) < cls.original.percentage:
            print "[load ids cache] ..."
            cls.datadict.sync() # sync first
            ids_cache = {item_id1 : True for item_id1, item1 in process_notifier(cls.datadict.datadict)}

            items = []
            for e1 in process_notifier(cls.original.model):
                if cls.original.read_id_lambda(e1) in ids_cache: continue
                if cls.original.filter_lambda(e1): continue

                items.append(cls(e1))

                if len(items) > 10000:
                    cls.feed_data(items)
                    items = []
            cls.feed_data(items)
            del ids_cache

    @classmethod
    def feed_data(cls, items=[]):
        # items 必定是list, 经过cPickle反序列化回来的
        """ 也许reopen在build_indexes解决sqlite close等问题 """
        for i1 in items:
            # Fix InterfaceError: Error binding parameter 0 - probably unsupported type
            assert isinstance(i1.item_id, (str, unicode, int,)), ("not " + repr(i1.item_id))

            cls.datadict[i1.item_id] = i1

        cls.datadict.sync()
