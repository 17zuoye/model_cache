# -*- coding: utf-8 -*-

import os
import sys
import time
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from model_cache.tools.parallel import ParallelData
from model_cache.tests.share import OriginalModel

dbpath = "tests/parallel.db"
total = 12000
original_model_data = OriginalModel.fake(total)


def clean_cache():
    os.system("rm -f %s*" % dbpath)
    os.system("rm -f *.cpu.*")
    os.system("rm -f *.io.*")
    os.system("rm -f %s*" % dbpath)


class TestTools(unittest.TestCase):

    def test_len(self):
        clean_cache()

        def process(item1):
            time.sleep(0.002)
            return item1
        result = ParallelData.process(original_model_data,
                                      'list',  # or 'dict'
                                      cache_filename=dbpath,
                                      item_func=process,
                                      id_func=lambda record: record['id'],)

        self.assertEqual(len(result), len(original_model_data))

        random_item_id = unicode(original_model_data[3]['id'])
        random_item    = original_model_data[3]['content']
        self.assertEqual(result[random_item_id]['content'], random_item)
        clean_cache()

if __name__ == '__main__':
    unittest.main()
