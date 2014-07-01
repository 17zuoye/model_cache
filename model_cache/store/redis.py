# -*- coding: utf-8 -*-

from .base import ModelCacheStore

class ModelCacheStoreRedis(ModelCacheStore):
    """ Hash查找实现 """

# TODO itervalues 会取出所有键，然后反序列化，所以占内存后慢了
        #result = iter(self.redis.hvals(self.key))
        #return (self._unpickle(v) for v in result)
    def __init__(self, name):
        from redis_collections.dicts import Dict as RedisDict
        self.datadict = RedisDict(key=name)

    def sync(self): return True
    def __getitem__(self, k1): return self.datadict.get(k1)
