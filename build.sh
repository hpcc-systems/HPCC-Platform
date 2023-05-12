#!/bin/bash

plugins=("CASSANDRAEMBED" "COUCHBASEEMBED" "ECLBLAS" "H3" "JAVAEMBED" "KAFKA" "MEMCACHED" "MYSQLEMBED" "NLP" "REDIS" "SQLITE3EMBED" "SQS" "PLATFORM")

for plugin in "${plugins[@]}"; do
  rm -f ./build/CMakeCache.txt
  rm -rf ./build/CMakeFiles
  cmake -S . -B ./build -G Ninja -D$plugin=ON -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF
  cmake --build ./build --parallel --target package
done
