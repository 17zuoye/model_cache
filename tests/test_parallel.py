# -*- coding: utf-8 -*-

import os, sys, time
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from tests.setup import *

from model_cache.tools.parallel import ParallelData

dbpath = "tests/parallel.db"
total = 12000
original_model_data = OriginalModel.fake(total)

os.system("rm -f %s*" % dbpath)

class TestTools(unittest.TestCase):

    def test_len(self):
        def process(item1): time.sleep(0.002); return item1
        result = ParallelData.process(original_model_data,
                                      'list', # or 'dict'
                                      dbpath,
                                      item_func=process,
                                      id_func=lambda record: record['id'],
                                     )

        #import pdb; pdb.set_trace()
        self.assertEqual(len(result), len(original_model_data))

        random_item_id = unicode(original_model_data[3]['id'])
        random_item    = original_model_data[3]['content']
        self.assertEqual(result[random_item_id]['content'], random_item)

if __name__ == '__main__': unittest.main()
