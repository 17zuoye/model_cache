#!/usr/bin/env bash

for file1 in test.py test_parallel.py
do
  echo "[test] $file1"
  python tests/$file1
done

rm -rf build dist model_cache-info *.cpu.* *.io.* tests/parallel.db # clean tmp files
