# -*- coding: utf-8 -*-

from ._base import ModelCacheStore

class ModelCacheStoreMemory(ModelCacheStore):
    """ 内存Hash查找实现 """

    def __init__(self, name=None):
        self.datadict = {}

    def sync(self): return True
    def __getitem__(self, k1): return self.datadict.get(k1, None)
