# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from model_cache.tests.share import make_a_foobar, OriginalModel


class TestModelCache(unittest.TestCase):

    def test_import(self):
        Foobar = make_a_foobar({})

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
        os.system("rm -rf Foobar")
        os.system("mkdir  Foobar")
        import time
        time.sleep(1)

        total = 100000
        original_model_data = OriginalModel.fake(total)
        Foobar = make_a_foobar(original_model_data)

        repr(Foobar)  # when 0 items
        Foobar.pull_data()
        self.assertEqual(len(Foobar), total)
        print repr(Foobar)
        os.system("rm -rf Foobar")

    def test_included_class(self):
        Foobar = make_a_foobar({})
        self.assertEqual(Foobar().im_include(), 1)
        self.assertTrue('overwrite_init__after' in dir(Foobar()))

if __name__ == '__main__':
    unittest.main()
