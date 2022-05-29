#!/bin/bash

# Test Config
PMEM_PATH="/mnt/pmem0"
FEATURE="default"
# TARGETS=("clevel" "elim_stack" "exchanger" "queue_comb" "queue_general" "queue_lp" "queue" "soft_hash" "soft_list" "stack" "treiber_stack")
TARGETS=("queue" "queue_general" "queue_lp")    # Test target
CNT_NORMAL=3                                    # Number of normal test
CNT_CRASH=10                                    # Number of crash test

# DRAM Setting
arg=$1
if [ "$arg" == "no_persist" ]; then
    PMEM_PATH="$SCRIPT_DIR/../../test"
    FEATURE="no_persist"
fi

# Initialize
set -e
SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out_fullcrash"
rm -rf $OUT_PATH/debug.out
rm -rf $OUT_PATH/queue_*.out
mkdir -p $OUT_PATH
cargo clean
cargo build --tests --release --features=$FEATURE
rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$1"
    echo "[$time] $msg" >> $OUT_PATH/debug.out
    echo "[$time] $msg" >> $OUT_PATH/$target.out
}

function init() {
    target=$1
    dmsg "initialze $target"

    # create new pool
    rm -rf $PMEM_PATH/*
    RUST_MIN_STACK=100737418200 POOL_EXECUTE=0 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out
}

function run() {
    target=$1
    dmsg "run $target"
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out
}

function run_bg() {
    target=$1
    dmsg "run_bg $target"
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out &
}

# Run test
for target in ${TARGETS[@]}; do
    # Test normal run.
    avgtest=0 # average test time
    for i in $(seq 1 $CNT_NORMAL); do
        # initlaize
        dmsg "normal run $target $i/$CNT_NORMAL"
        init $target

        # run
        start=$(date +%s%N)
        run $target
        end=$(date +%s%N)

        # calculate average test time
        avgtest=$(($avgtest+$(($end-$start))))
    done
    avgtest=$(($avgtest/$CNT_NORMAL))
    dmsg "avgtest: $avgtest ns"

    # Test full crash and recovery run.
    crash_max=$avgtest # maximum crash time
    dmsg "maximum crash time=$crash_max ns"
    for i in $(seq 1 $CNT_CRASH); do
        dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ crash-recovery test $target $i/$CNT_CRASH ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
        init $target

        # run
        dmsg "-------------------------- crash run ------------------------------"
        start=$(date +%s%N)
        run_bg $target

        # crash
        crashtime=$(((RANDOM * RANDOM * RANDOM) % $crash_max))
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # kill after random crash time
            if [ $elapsed -gt $crashtime ]; then
                kill -9 %1 || true
                wait %1 || true
                dmsg "crash after $elapsed ns"
                break
            fi
        done

        # recovery run
        dmsg "-------------------------- recovery run ---------------------------"
        run $target
        dmsg "ok"
        dmsg "⎿_________________________________________________________________⏌"
    done

    # # TODO: Test thread-crash
    # for i=0; i<CNT_CRASH; i++ {
    #    프로세스 p1이 프로세스 p0의 내부 특정 스레드만 죽일 수는 없어보임.
    #    p0의 내부에서 thread-crash를 일으킬 스레드를 만들어야할듯
    # }
done
