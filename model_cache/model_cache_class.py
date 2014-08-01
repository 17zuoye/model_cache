# -*- coding: utf-8 -*-

import os
import json
from .storage import *
from etl_utils import process_notifier

class ModelCacheClass(object):

    def __init__(self, record={}):
        """ You should overwrite `init__before` and `init__after`, \
            instead of this `__init__` function.

            `__init__after` this name style will conflict with default \
            python object functions.
            """
        self.init__before(record)

        self.init__load_data(record)

        assert self.item_id, "self.item_id should be assign in self.load_data function!"
        assert type(self.item_content) in [str, unicode], \
                "self.item_content should be assign in self.load_data function!"

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

    def has_item_id(self, record):
        """ Detect if there is an item_id, which should be already wrote to database """
        raise NotImplemented

    @classmethod
    def build_indexes(cls, items=[]):
        # items 必定是list, 经过cPickle反序列化回来的
        """ 也许reopen在build_indexes解决sqlite close等问题 """
        cls.datadict.build_indexes(items)
        cls.datadict.sync()

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

    @classmethod
    def pull_data(cls):
        print; print "LOAD %s INTO %s" % (cls.original.model.__module__, cls.__module__)

        if cls.count() / float(cls.original.model.count()) < cls.original.percentage:
            print "[load ids cache] ..."
            ids_cache = {str(i1.item_id) : True for i1 in process_notifier(cls.datadict.datadict)}

            items = []
            for e1 in process_notifier(cls.original.model):
                if cls.original.read_id_lambda(e1) in ids_cache: continue
                if cls.original.filter_lambda(e1): continue

                items.append(cls(e1))

                if len(items) > 10000:
                    cls.build_indexes(items)
                    items = []
            cls.build_indexes(items)
            del ids_cache
