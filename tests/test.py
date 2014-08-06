# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from model_cache import ModelCache

class OriginalModel(list): pass


class IncludedClass(object):
    def im_include(self): return 1


def generate_test_model_cache(data):
    attrs = {
              'read_id_lambda' : lambda item1: item1['id'],
              'storage_type'   : 'memory',
              'included_class' : IncludedClass,
             }

    @ModelCache.connect(data, **attrs)
    class Foobar():
        inc = 0

        def init__load_data(self, record):
            Foobar.inc += 1
            self.item_id = str(Foobar.inc)
            self.item_content = unicode(self.item_id)
    return Foobar




class TestModelCache(unittest.TestCase):

    def test_import(self):
        Foobar = generate_test_model_cache({})

        self.assertTrue("datadict" in Foobar.__dict__)
        self.assertEqual(len(Foobar), 0)

        f1 = Foobar()
        f2 = Foobar()
        f3 = Foobar()

        Foobar.feed_data([f1, f2, f3])
        self.assertEqual(len(Foobar), 3)
        self.assertEqual(Foobar['3'], f3)

        del Foobar['1']
        self.assertEqual(len(Foobar), 2)

    def test_load_from(self):
        total = 100000

        original_model_data = OriginalModel([ \
                {'id': idx1, 'content': 'content_' + str(idx1)} \
                    for idx1 in xrange(total)])
        setattr(original_model_data, '__module__', 'original_model')

        Foobar = generate_test_model_cache(original_model_data)

        repr(Foobar) # when 0 items
        Foobar.pull_data()
        self.assertEqual(len(Foobar), total)
        print repr(Foobar)
        #import pdb; pdb.set_trace()

    def test_included_class(self):
        Foobar = generate_test_model_cache({})
        self.assertEqual(Foobar().im_include(), 1)

if __name__ == '__main__': unittest.main()
