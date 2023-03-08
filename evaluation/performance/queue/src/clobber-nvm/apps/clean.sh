#!/bin/bash
rm -rf /mnt/pmem0/*
cd bptree
make clean
cd ../hashmap
make clean
cd ../rbtree
make clean
cd ../skiplist
make clean
cd ../memcached
make clean
cd ../stamp/vacation
make clean
cd ../yada
make clean
cd ../../
cd runtime
rm *.o
cd ..

