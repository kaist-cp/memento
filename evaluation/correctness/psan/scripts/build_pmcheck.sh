#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/../
BUILD=$DIR_BASE/build
mkdir -p $BUILD
cd $BUILD

mode=$1
branch=""
if [ "$mode" == "yashme" ]; then
    branch=pmrace # https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/setup.sh#L12
elif [ "$mode" == "psan" ]; then
    branch=psan # https://github.com/uci-plrg/psan-vagrant/blob/master/data/setup.sh#L12
else
    echo "Invalid mode: $mode"
    exit 0
fi

rm -rf pmcheck*
git clone https://github.com/uci-plrg/jaaru.git
mv jaaru pmcheck
cd pmcheck/
git checkout $branch
make clean 
make -j
echo $mode > $BUILD/pmcheck.config
