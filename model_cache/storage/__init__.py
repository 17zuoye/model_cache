from etl_utils import process_notifier, set_default_value
from ._base import *
from .memory import ModelCacheStoreMemory
from .sqlite import ModelCacheStoreSqlite
from .redis  import ModelCacheStoreRedis
from .shelve import ModelCacheStoreShelve


valid_storages = {
            "memory" : ModelCacheStoreMemory,
            "sqlite" : ModelCacheStoreSqlite,
            "redis"  : ModelCacheStoreRedis,
            "shelve" : ModelCacheStoreShelve,
        }
