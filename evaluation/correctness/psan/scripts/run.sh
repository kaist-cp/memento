#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))/..
BUILD=$DIR_BASE/build
OUT=$DIR_BASE/out
PMCHECK=$BUILD/pmcheck/bin
RUSTSTD=/home/ubuntu/.rustup/toolchains/nightly-2022-05-26-x86_64-unknown-linux-gnu/lib
mkdir -p $OUT
OUT_LOG=/$OUT/debug.log

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo "[$time] $msg" >> $OUT_LOG
}

cd $BUILD
TARGET=$1
MODE=$(cat pmcheck.config)

# Build
if [ "${MODE}" == "yashme" ]; then
    # Yashme (https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/pmdk-races.sh)
    OPT="-v -p -y -x100"
elif [ "${MODE}" == "psan" ]; then
    # PSan (https://github.com/uci-plrg/psan-vagrant/blob/master/data/pmdk-bugs.sh)
    OPT="-v -p -o2 -x100"
else
    echo "invalid mode: $MODE (possible mode: yashme, psan)"
    exit
fi
echo "[Run] target: $TARGET, mode: $MODE, option: $OPT"
dmsg "[Run] target: $TARGET, mode: $MODE, option: $OPT"

export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD
export PMCheck="-d/mnt/pmem0/test/$TARGET/$TARGET.pool_valid $OPT"
rm -rf PMCheckOutput*
rm -rf /mnt/pmem0/*
ulimit -s 82920000
mkdir -p $OUT/psan
mkdir -p $OUT/psan/$MODE
RUST_MIN_STACK=100000000 ./psan $TARGET 2>&1>$OUT/psan/$MODE/$TARGET.log
echo "[Finish] target: $TARGET, mode: $MODE, option: $OPT"
dmsg "[Finish] target: $TARGET, mode: $MODE, option: $OPT"
