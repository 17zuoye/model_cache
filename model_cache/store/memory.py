# -*- coding: utf-8 -*-

from .base import ModelCacheStore

class ModelCacheStoreMemory(ModelCacheStore):
    """ 内存Hash查找实现 """

    def __init__(self, name):
        self.datadict = {}

    def sync(self): return True
    def __getitem__(self, k1): return self.datadict.get(k1, None)
