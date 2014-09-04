# -*- coding: utf-8 -*-

from ._base import ModelCacheStore

class ModelCacheStoreShelve(ModelCacheStore):
    """ Hash查找实现 """

    def __init__(self, name):
        import shelve
        self.datadict = shelve.open(name)

    def sync(self): return self.datadict.sync()
