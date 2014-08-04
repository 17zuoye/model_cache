# -*- coding: utf-8 -*-

class ModelCacheStore(object):
    """ 抽取后的Model快速访问，含item_id, 和一系列特征 """

    def __init__(self, name) : raise NotImplemented
    def sync(self)           : raise NotImplemented

    def build_indexes(self, items):
        for i1 in items:
            self.datadict[str(i1.item_id)] = i1
        self.sync()

    def items(self)     : return [i1[1] for i1 in self.datadict.items()]
    def itervalues(self) : return self.datadict.itervalues()
    def iteritems(self) : return self.datadict.iteritems()

    def __len__(self)   : return len(self.datadict)
    def has_key(self, k1): return k1 in self.datadict # 兼容没有 has_key
    def __delitem__(self, i1): del self.datadict[i1]

    def __getitem__(self, k1, v1=None):
        return self.datadict.get(str(k1), v1)

    def __contains__(self, k1): return str(k1) in self.datadict
