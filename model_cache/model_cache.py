# -*- coding: utf-8 -*-

import os
from .storage import *
from .model_cache_class import ModelCacheClass

valid_storage_types = ("memory", "sqlite", "redis")

class ModelCache():
    @classmethod
    def connect(cls, original_model, **kwargs):
        # assert original_model's behavior
        process_notifier(original_model)

        # setup args
        default_kwargs = {
                    'cache_dir'      : os.getenv("ModelCacheDir"),
                    'storage_type'   : 'sqlite',
                    'percentage'     : 0.9999,
                    'filter_lambda'  : lambda item1: False,
                    'read_id_lambda' : lambda item1: str(item1['_id']),
                    'included_class' : object,
                }
        for k1, v1 in kwargs.iteritems():
            if k1 in default_kwargs:
                default_kwargs[k1] = v1

        # validate storage
        assert default_kwargs['storage_type'] in valid_storage_types
        if (default_kwargs['cache_dir'] is None) and (default_kwargs['storage_type'] != "memory"):
            raise Exception(u"`cache_dir` should not be None when storage_type is not memory.")

        cache_dir = default_kwargs['cache_dir']
        del default_kwargs['cache_dir']

        # decorate class
        def _model_cache_decorator(decorated_class):
            # 1. included_class should not overwrite ModelCacheClass's important methods,
            #    include `__init__`, `init__before`, `init__after`.
            # 2. ensure decorated_class's methods will overwrite ModelCache's.

            for k1 in ['init__before', 'init__after']:
                if k1 in dir(default_kwargs['included_class']):
                    setattr(ModelCacheClass, k1, getattr(default_kwargs['included_class'], k1))

            class _model_cache(decorated_class, ModelCacheClass, default_kwargs['included_class']):
                class OriginalClass(): pass # so we can setattr here.
                original = OriginalClass()
                for k1, v1 in default_kwargs.iteritems():
                    setattr(original, k1, v1)
                    del k1; del v1
                original.model   = original_model

                # Thx http://stackoverflow.com/questions/4932438/how-to-create-a-custom-string-representation-for-a-class-object/4932473#4932473
                class MetaClass(type):
                    __metaclass__ = Forwardable
                    _ = def_delegators('datadict', dict_attrs)

                    def __repr__(self):
                        is_total_len_enough = len(self) > 5
                        while is_total_len_enough and (len(self.first_five_items) < 5):
                            for item_id1, item1 in self.iteritems():
                                self.first_five_items.append(item1)
                                if len(self.first_five_items) == 5: break

                        dots = ", ......" if is_total_len_enough else ""
                        return (u"<%s has %i items:[%s%s]>" % \
                                        (self.__name__, len(self), \
                                        ", ".join([str(item1.item_id) for item1 in self.first_five_items]), \
                                        dots, )).encode("UTF-8")
                __metaclass__ = MetaClass

                @classmethod
                def pickle_path(cls, name):
                    return cls.cache_dir + "/" + name + ".cPickle"

            _model_cache.__name__   = decorated_class.__name__
            _model_cache.__module__ = decorated_class.__module__ # so can pickle :)

            _model_cache.first_five_items = []

            _model_cache.cache_dir  = os.path.join(cache_dir or u"", _model_cache.__name__)
            if default_kwargs['storage_type'] != 'memory':
                if not os.path.isdir(_model_cache.cache_dir): os.makedirs(_model_cache.cache_dir)
            _model_cache.dbpath = None
            if _model_cache.cache_dir:
                _model_cache.dbpath = os.path.join(_model_cache.cache_dir, _model_cache.__name__ + ".db")

            _model_cache.connect()

            return _model_cache
        return _model_cache_decorator
