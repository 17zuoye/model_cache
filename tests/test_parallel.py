# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from tests.setup import *

from model_cache.tools.parallel import ParallelShelve

dbpath = "tests/parallel.db"
total = 3000
original_model_data = OriginalModel.fake(total)
setattr(original_model_data, '__module__', 'original_model')
Foobar = generate_test_model_cache(original_model_data)
class Foobar(Foobar): pass # defined as a class, to pickle

os.system("rm -f %s*" % dbpath)

class TestTools(unittest.TestCase):

    def test_len(self):
        Foobar.pull_data()

        repr(Foobar) # preload first_five_items
        random_item_id = Foobar.first_five_items[0].item_id

        result = ParallelShelve.process(Foobar, dbpath, lambda item1: item1)

        self.assertEqual(len(result), len(Foobar))
        self.assertEqual(result[random_item_id].item_content, Foobar[random_item_id].item_content)
        #import pdb; pdb.set_trace()

if __name__ == '__main__': unittest.main()
