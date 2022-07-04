#!/bin/bash

# Test Config
PMEM_PATH="/mnt/pmem0"
FEATURE="default"
# TARGETS=("clevel" "elim_stack" "exchanger" "queue_comb" "queue_general" "queue_lp" "queue" "soft_hash" "soft_list" "stack" "treiber_stack")
# TARGETS=("queue" "queue_general" "queue_lp")    # Test target
TARGETS=("queue_general")    # Test target
CNT_NORMAL=1                                   # Number of normal test
CNT_CRASH=100                                    # Number of crash test

# Initialize
set -e
SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out_threadcrash"
rm -rf $OUT_PATH
mkdir -p $OUT_PATH
cargo clean

# Use original std
# cargo build --tests --release --features=simulate_tcrash
# rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d

# Use Customized std
cargo +nightly-2022-05-26 build --tests --release --features=simulate_tcrash -Z build-std --target=x86_64-unknown-linux-gnu
rm -f $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-*.d

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
    # RUST_MIN_STACK=100737418200 POOL_EXECUTE=0 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out
    RUST_MIN_STACK=100737418200 POOL_EXECUTE=0 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* ds::$target::test --nocapture &>> $OUT_PATH/$target.out
}

function run() {
    target=$1
    dmsg "run $target"

    # RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* ds::$target::test --nocapture &>> $OUT_PATH/$target.out
}

function run_bg() {
    target=$1
    dmsg "run_bg $target"

    # RUST_BACKTRACE=0 RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture >> $OUT_PATH/$target.out &
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* ds::$target::test --nocapture &>> $OUT_PATH/$target.out &
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
        # run $target
    done
    avgtest=$(($avgtest/$CNT_NORMAL))
    dmsg "avgtest: $avgtest ns"

    # Test thread crash and recovery run.
    crash_min=$(($avgtest / 3))        # minimum crash time
    crash_max=$avgtest # maximum crash time
    dmsg "minimum crash time=$crash_min ns"
    dmsg "maximum crash time=$crash_max ns"
    for i in $(seq 1 $CNT_CRASH); do
        dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test $target $i/$CNT_CRASH ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
        init $target

        # run
        dmsg "-------------------------- crash run ------------------------------"
        start=$(date +%s%N)
        run_bg $target
        pid_bg=$!

        # thread crash
        # TODO: many times?
        crashtime=$(shuf -i $crash_min-$crash_max -n 1)
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # kill random thread after random crash time
            if [ $elapsed -gt $crashtime ]; then
                $SCRIPT_DIR/tgkill -10 $pid_bg $pid_bg || true
                dmsg "kill random thread after $elapsed ns"
                break
            fi
        done

        # wait until finish
        dmsg "wait $pid_bg"
        wait $pid_bg
        dmsg "ok"
        dmsg "⎿_________________________________________________________________⏌"
    done
done
