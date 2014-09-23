# -*- coding: utf-8 -*-

import json
from .storage import *
from .tools.parallel import ParallelData

class ModelCacheClass(object):

    def __init__(self, record={}):
        """ You should overwrite `init__before` and `init__after`, \
            instead of this `__init__` function.

            `__init__after` this name style will conflict with default \
            python object functions.
            """
        self.init__before(record)

        self.init__load_data(record)

        assert self.item_id is not None, "self.item_id should be assigned in self.load_data function!"
        assert isinstance(self.item_content, unicode), \
                ("self.item_content should be assigned as unicode in self.load_data function! %s" \
                    % repr(self.item_content))

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
        cls.datadict = valid_storages[cls.original.storage_type](cls.dbpath)

        msg = 'Reconnect' if is_reconnect else 'Init'
        print "[ModelCache] %s at %s" % (msg, cls.dbpath or '[memory]')

    @classmethod
    def pull_data(cls):
        print; print "[LOAD] %s [INTO] %s" % (cls.original.model.__module__, cls.__module__)

        # default is list, so mongodb, mysql can be compacted.
        ParallelData.process(cls.original.model, 'list', cls.dbpath, \
                output_lambda=lambda items: cls.feed_data(items), \
                output_len_lambda=lambda : len(cls), \
                id_func=cls.original.read_id_lambda,
                )

    @classmethod
    def feed_data(cls, items=[]):
        # items 必定是list, 经过cPickle反序列化回来的
        """ 也许reopen在build_indexes解决sqlite close等问题 """
        for i1 in items:
            if not isinstance(i1, cls): i1 = cls(i1)
            # Fix InterfaceError: Error binding parameter 0 - probably unsupported type
            assert isinstance(i1.item_id, (str, unicode, int,)), ("not " + repr(i1.item_id))
            cls.datadict[i1.item_id] = i1
        cls.datadict.sync()
