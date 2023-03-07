#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))
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
TOOL=$2
MODE=$3

# Set
if [ "${TOOL}" == "yashme" ]; then
    # Yashme (https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/pmdk-races.sh)
    OPT="-y" # -v, -p for debugging
elif [ "${TOOL}" == "psan" ]; then
    # PSan (https://github.com/uci-plrg/psan-vagrant/blob/master/data/pmdk-bugs.sh)
    OPT="-o2"
else
    echo "invalid TOOL: $TOOL (possible TOOL: yashme, psan)"
    exit
fi
if [ "${MODE}" == "model" ]; then
    OPT="$OPT"
elif [ "${MODE}" == "random" ]; then
    OPT="$OPT -x1000"
else
    echo "invalid mode: $MODE (possible mode: model, random)"
    exit
fi
echo "[Run] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
dmsg "[Run] target: $TARGET, TOOL: $TOOL, (option: $OPT)"

# Run
export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD
export PMCheck="-d/mnt/pmem0/test/$TARGET/$TARGET.pool_valid $OPT"
rm -rf PMCheckOutput*
rm -rf /mnt/pmem0/*
ulimit -s 82920000
mkdir -p $OUT/psan
mkdir -p $OUT/psan/$TOOL
# RUST_MIN_STACK=100000000 ./psan $TARGET 2>&1>$OUT/psan/$TOOL/$TARGET.log
RUST_MIN_STACK=100000000 ./psan $TARGET
echo "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
dmsg "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
