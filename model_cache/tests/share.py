# -*- coding: utf-8 -*-

from model_cache import ModelCache


class OriginalModel(list):

    @classmethod
    def fake(cls, num):
        result = cls([{'id': idx1, 'content': 'content_' + str(idx1)}
                      for idx1 in xrange(num)])
        setattr(result, '__module__', 'original_model')
        return result

    def count(self):
        return len(self)


class IncludedClass(object):

    def im_include(self):
        return 1

    def init__after(self, record):
        self.overwrite_init__after = True


def make_a_foobar(data):
    attrs = {
        'read_id_lambda': lambda item1: item1['id'],
        'storage_type': 'memory',
        'included_class': IncludedClass,
    }

    @ModelCache.connect(data, **attrs)
    class Foobar():
        inc = 0

        def init__load_data(self, record):
            if record:
                self.item_id = record['id']
                self.item_content = unicode(record['content'])
            else:
                Foobar.inc += 1
                self.item_id = str(Foobar.inc)
                self.item_content = unicode(self.item_id)
    return Foobar
