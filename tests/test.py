# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

class TestModelCache(unittest.TestCase):
    def test_import(self):
        from model_cache import ModelCache

if __name__ == '__main__': unittest.main()
