#!/bin/bash

dir_path=$(dirname $(realpath $0))
cd $dir_path

# Build PMDK
pmdk/build.sh


# Build bench executable
g++ -o bench bench.cpp pmdk/pipe.cpp -pthread -lpmemobj -std=c++17 # TODO: Corundum은 왜 -O2 사용?
