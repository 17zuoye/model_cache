Model Cache
======================
[![Build Status](https://img.shields.io/travis/17zuoye/model_cache/master.svg?style=flat)](https://travis-ci.org/17zuoye/model_cache)
[![Coverage Status](https://coveralls.io/repos/17zuoye/model_cache/badge.svg)](https://coveralls.io/r/17zuoye/model_cache)
[![Health](https://landscape.io/github/17zuoye/model_cache/master/landscape.svg?style=flat)](https://landscape.io/github/17zuoye/model_cache/master)
[![Download](https://img.shields.io/pypi/dm/model_cache.svg?style=flat)](https://pypi.python.org/pypi/model_cache)
[![License](https://img.shields.io/pypi/l/model_cache.svg?style=flat)](https://pypi.python.org/pypi/model_cache)


Cache data in `{ item_id => item_content }` format, supported storages
are memory, shelve, sqlite and redis.



Example
------------------------

## 1. ModelCache

```python
from model_cache import ModelCache

default_kwargs = {
                    'cache_dir'      : os.getenv("ModelCacheDir"), # the default.

                    # available storage_types are ['memory', 'sqlite', 'shelve', 'redis']. default
                    # is 'shelve', which is faster than sqlite
                    'storage_type'   : 'shelve',

                    'filter_lambda'  : lambda item1: False,
                    'read_id_lambda' : lambda item1: str(item1['_id']),
                    'included_class' : object,
                }
@ModelCache.connect(mongo_query, default_kwargs)
class Foobar:
    def init__load_data(self, record)
        self.item_id      = process1(record) # must should be string or unicode like object.
        self.item_content = process1(record)

Foobar.pull_data() # Use ParallelData.process API

len(Foobar) # get total count
Foobar_a = Foobar[Foobar_a_id] # fetch Foobar_a
del Foobar[Foobar_a_id] # remove Foobar_a
Foobar[Foobar_b_id] = Foobar_b # remove Foobar_b
Foobar.feed_data([Foobar_c, Foobar_d, ...]) # save and persist data

Foobar.reconnect() # reopen persistent file descriptor if original is gone
```

## 2. ParallelData

```python
from model_cache import ParallelData


result = ParallelData.process(original_model_data,
                              'list', # or 'dict'
                              dbpath,
                              item_func=process,
                              id_func=lambda record: record['id'],
                             )

```


License
------------------------
MIT. David Chen @ 17zuoye.
