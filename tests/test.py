# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from model_cache import ModelCache

class Foobar(ModelCache):
    datadict_type = "memory"
    cache_dir     = "tmp"

    inc = 0

    def load_data(self, record):
        Foobar.inc += 1
        self.item_id = Foobar.inc
        self.item_content = unicode(self.item_id)

class TestModelCache(unittest.TestCase):
    def test_import(self):
        Foobar.init_datadict()

        self.assertTrue("datadict" in Foobar.__dict__)
        self.assertEqual(Foobar.count(), 0)

        f1 = Foobar({})
        f2 = Foobar({})
        f3 = Foobar({})

        Foobar.build_indexes([f1, f2, f3])
        self.assertEqual(Foobar.count(), 3)
        self.assertTrue(Foobar.find(3), f3)

        Foobar.remove(1)
        self.assertEqual(Foobar.count(), 2)

if __name__ == '__main__': unittest.main()
