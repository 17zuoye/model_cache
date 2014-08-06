# -*- coding: utf-8 -*-

from forwardable import Forwardable, def_delegators

class ModelCacheStore(object):
    """ 抽取后的Model快速访问，含item_id, 和一系列特征 """

    def __init__(self, name) : raise NotImplemented
    def sync(self)           : raise NotImplemented

    __metaclass__ = Forwardable
    _ = def_delegators('datadict', ('__getitem__', '__setitem__', '__delitem__', \
                                    '__iter__', '__len__', '__contains__', \
                                    'has_key', 'get', 'pop', 'popitem', \
                                    'keys', 'values', \
                                    'items', 'iteritems', 'iterkeys', 'itervalues', ))
