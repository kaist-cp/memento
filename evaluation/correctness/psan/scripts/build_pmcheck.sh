#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/../
BUILD=$DIR_BASE/build
mkdir -p $BUILD
cd $BUILD

function build() {
    tool=$1
    branch=""
    if [ "$tool" == "yashme" ]; then
        branch=pmrace # https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/setup.sh#L12
        # echo yashme
    elif [ "$tool" == "psan" ]; then
        branch=psan # https://github.com/uci-plrg/psan-vagrant/blob/master/data/setup.sh#L12
        # echo psan
    else
        echo "Invalid mode: $tool (possible tool: yashme, psan)"
        exit 0
    fi
    git clone https://github.com/uci-plrg/jaaru.git
    # mv jaaru pmcheck_$tool
    # cd pmcheck_$tool/
    mv jaaru pmcheck
    cd pmcheck
    git checkout $branch
    make -j
    cd ..
}

# build psan
build yashme



