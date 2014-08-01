# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from model_cache import ModelCache

class OriginalModel(list):
    def __init__(self, list1):
        super(OriginalModel, self).__init__(list1)
        self.__module__ = None

    def count(self): return len(self)




def generate_test_model_cache(data):
    attrs = {
              'read_id_lambda' : lambda item1: item1['id'],
              'storage_type'   : 'memory',
             }

    @ModelCache.config(data, **attrs)
    class Foobar(object):
        inc = 0

        def load_data(self, record):
            Foobar.inc += 1
            self.item_id = Foobar.inc
            self.item_content = unicode(self.item_id)
    return Foobar




class TestModelCache(unittest.TestCase):

    def test_import(self):
        Foobar = generate_test_model_cache({})
        Foobar.init_datadict()

        self.assertTrue("datadict" in Foobar.__dict__)
        self.assertEqual(Foobar.count(), 0)

        f1 = Foobar()
        f2 = Foobar()
        f3 = Foobar()

        Foobar.build_indexes([f1, f2, f3])
        self.assertEqual(Foobar.count(), 3)
        self.assertTrue(Foobar.find(3), f3)

        Foobar.remove(1)
        self.assertEqual(Foobar.count(), 2)

    def test_load_from(self):
        original_model_data = OriginalModel([ \
                {'id': idx1, 'content': 'content_' + str(idx1)} for idx1 in xrange(100000)])
        setattr(original_model_data, '__module__', 'original_model')

        Foobar = generate_test_model_cache(original_model_data)
        Foobar.init_datadict()
        #import pdb; pdb.set_trace()

        #Foobar.load_from(original_model_data)

if __name__ == '__main__': unittest.main()
