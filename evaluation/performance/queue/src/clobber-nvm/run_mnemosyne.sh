#!/bin/sh
cd mnemosyne-gcc/usermode

./run_vacation.sh
mv vacation.csv ../../vacation_mnemosyne.csv

./run_memcache.sh
mv memcached.csv ../../memcached_mnemosyne.csv

cd ../../

