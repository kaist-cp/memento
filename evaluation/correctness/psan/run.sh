#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))
BUILD=$DIR_BASE/build
OUT=$DIR_BASE/out
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
case $1 in
  checkpoint|detectable_cas|queue_O0|queue_O1|queue_O2|queue_comb|treiber_stack|list|clevel)
    ;;
  *)
    echo "$1 is not a valid test."
    exit
    ;;
esac
if [ "${TOOL}" == "yashme" ]; then
    # Yashme (https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/pmdk-races.sh)
    PMCHECK=$BUILD/pmcheck_yashme/bin
    OPT="-y" # -v, -p for debugging
elif [ "${TOOL}" == "psan" ]; then
    # PSan (https://github.com/uci-plrg/psan-vagrant/blob/master/data/pmdk-bugs.sh)
    PMCHECK=$BUILD/pmcheck_psan/bin
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
mkdir -p $OUT/$TOOL
RUST_MIN_STACK=100000000 ./test_mmt_$TOOL $TARGET 2>&1>>$OUT/$TOOL/$TARGET.log
# RUST_MIN_STACK=100000000 ./test_mmt_$TOOL $TARGET 
# 2>&1 | tee -a the_log_file
echo "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
dmsg "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
