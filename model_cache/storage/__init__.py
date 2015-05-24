# -*- coding: utf-8 -*-

__all__ = ["valid_storages", "dict_attrs"]

from ._base import dict_attrs
from .memory_cache_store_memory import ModelCacheStoreMemory
from .memory_cache_store_sqlite import ModelCacheStoreSqlite
from .memory_cache_store_redis import ModelCacheStoreRedis
from .memory_cache_store_shelve import ModelCacheStoreShelve


valid_storages = {
    "memory": ModelCacheStoreMemory,
    "sqlite": ModelCacheStoreSqlite,
    "redis": ModelCacheStoreRedis,
    "shelve": ModelCacheStoreShelve,
}
