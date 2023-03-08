#!/bin/sh
cd mnemosyne-gcc

cd usermode/library/pmalloc/include/alps
mkdir build
cd build
cmake .. -DTARGET_ARCH_MEM=CC-NUMA -DCMAKE_BUILD_TYPE=Release
make

cd ../../../../../

scons --build-bench=stamp-kozy
scons --build-bench=memcached

