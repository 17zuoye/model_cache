# -*- coding: utf-8 -*-

from .model_cache_class import *

valid_storage_types = ("memory", "sqlite", "redis")

class ModelCache():
    @classmethod
    def connect(cls, original_model, **kwargs):
        # assert original_model's behavior
        process_notifier(original_model)

        # setup args
        default_kwargs = {
                    'cache_dir'      : None,
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

        # decorate class
        def _model_cache_decorator(decorated_class):
            # ensure decorated_class's methods will overwrite ModelCache's.
            class _model_cache(decorated_class, default_kwargs['included_class'], ModelCacheClass):
                class OriginalClass(): pass # so we can setattr here.
                original = OriginalClass()
                for k1, v1 in default_kwargs.iteritems():
                    setattr(original, k1, v1)
                    del k1; del v1
                original.model   = original_model

                # Thx http://stackoverflow.com/questions/4932438/how-to-create-a-custom-string-representation-for-a-class-object/4932473#4932473
                class MetaClass(type):
                    def __getitem__(self, k1, v1=None):
                        return self.datadict[str(k1)] or v1

                    def __setitem__(self, k1, v1):
                        self.datadict[str(k1)] = v1

                    def __contains__(self, k1):
                        return str(k1) in self.datadict

                    def __delitem__(self, k1):
                        del self.datadict[k1]

                    def __len__(self): return len(self.datadict)

                __metaclass__ = MetaClass

            _model_cache.__name__ = decorated_class.__name__
            _model_cache.__module__ = decorated_class.__module__ # so can pickle :)

            _model_cache.connect()

            return _model_cache
        return _model_cache_decorator
