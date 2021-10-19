#!/bin/bash

dir_path=$(dirname $(realpath $0))

cd $dir_path
wget https://github.com/pmem/pmdk/archive/1.11.0.tar.gz && \
    tar -xzvf 1.11.0.tar.gz && rm -f 1.11.0.tar.gz && cd pmdk-1.11.0 && \
    make -j$(nproc) && $su make install

cd $dir_path
wget https://github.com/pmem/libpmemobj-cpp/archive/1.11.tar.gz && \
    tar -xzvf 1.11.tar.gz && rm -f 1.11.tar.gz && cd libpmemobj-cpp-1.11 && \
    mkdir -p build && cd build && cmake -D CMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc) && $su make install

$su ldconfig
