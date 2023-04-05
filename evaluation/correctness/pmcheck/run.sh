#!/bin/bash

set -e

DIR_BASE=$(dirname $(realpath $0))
BUILD=$DIR_BASE/.build
OUT=$DIR_BASE/out
RUSTUP_PATH=$(rustc --print sysroot)
RUSTSTD="${RUSTUP_PATH}/lib"
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
pool_postfix=$(date +%s%3N)
pool_id=${TARGET}_${pool_postfix}

# Set
case $1 in
  simple|checkpoint|detectable_cas|queue_O0|queue_O1|queue_O2|queue_comb|treiber_stack|list|clevel)
    ;;
  *)
    echo "$1 is not a valid test."
    exit
    ;;
esac
if [ "${TOOL}" == "yashme" ]; then
    # Yashme (https://github.com/uci-plrg/pmrace-vagrant/blob/master/data/pmdk-races.sh)
    PMCHECK=$BUILD/pmcheck_psan/bin
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
echo "[Run] target: $TARGET, TOOL: $TOOL, (option: $OPT, pool: $pool_id)"
dmsg "[Run] target: $TARGET, TOOL: $TOOL, (option: $OPT, pool: $pool_id)"

# Run
export LD_LIBRARY_PATH=$PMCHECK:$RUSTSTD
export PMCheck="-d/mnt/pmem0/test/$pool_id/$pool_id.pool_valid $OPT"
ulimit -s 82920000
mkdir -p $OUT/$TOOL
# RUST_MIN_STACK=100000000 ./test_mmt_$TOOL $TARGET 2>&1>>$OUT/$TOOL/$TARGET.log
RUST_MIN_STACK=100000000 ./test_mmt_$TOOL $TARGET $pool_postfix
# 2>&1 | tee -a the_log_file
rm -rf PMCheckOutput*
rm -rf /mnt/pmem0/test/$pool_id
echo "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
dmsg "[Finish] target: $TARGET, TOOL: $TOOL, (option: $OPT)"
