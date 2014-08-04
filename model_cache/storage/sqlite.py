# -*- coding: utf-8 -*-

from ._base import ModelCacheStore

class ModelCacheStoreSqlite(ModelCacheStore):
    """ BTree查找实现 """

    def __init__(self, name):
        from sqlitedict import SqliteDict
        self.datadict = SqliteDict(name)

    def sync(self): return self.datadict.commit() # instead of #sync
