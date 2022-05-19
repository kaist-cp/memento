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
OUT_PATH="$SCRIPT_DIR/out"
rm -rf $OUT_PATH
mkdir -p $OUT_PATH
cargo clean
cargo build --tests --release --features=$FEATURE
# rm -f $SCRIPT_DIR/../../target/release/deps/memento-*.d

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$1"
    echo "[$time] $msg" >> $OUT_PATH/debug.out
    echo "[$time] $msg" >> $OUT_PATH/$target.out
}

function clear() {
    rm -rf $PMEM_PATH/*
}

function run() {
    target=$1
    RUST_MIN_STACK=1007374182 numactl --cpunodebind=0 --membind=0 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture >> $OUT_PATH/$target.out
    # RUST_MIN_STACK=1007374182 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test -- --nocapture >> $OUT_PATH/$target.out
}

function run_bg() {
    target=$1
    RUST_MIN_STACK=1007374182 numactl --cpunodebind=0 --membind=0 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture >> $OUT_PATH/$target.out &
    # RUST_MIN_STACK=1007374182 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test -- --nocapture & >> $OUT_PATH/$target.out
}

# Run test
for target in ${TARGETS[@]}; do
    # Test normal run.
    avgtest=0                 # Average test time
    mintest=$((1000 * 10**9)) # Minimum test time
    for i in $(seq 1 $CNT_NORMAL); do
        # initlaize
        dmsg "normal run $target $i/$CNT_NORMAL"
        clear

        # run
        start=$(date +%s%N)
        run $target
        end=$(date +%s%N)

        # calculate elpased time
        elapsed=$(($end-$start))
        avgtest=$(($avgtest+$elapsed))
        if [ $mintest -gt $elapsed ]; then
            mintest=$elapsed
        fi
    done
    avgtest=$(($avgtest/$CNT_NORMAL))
    dmsg "mintest: $mintest ns, avgtest: $avgtest ns"

    # Test full-crash and recovery run.
    crash_min=$(($avgtest/4))  # Minimum crash time (to guarantee crash after finishing pool creation)
    crash_max=$mintest         # Maximum crash time
    dmsg "crash_min: $crash_min ns, crash_max: $crash_max ns"
    for i in $(seq 1 $CNT_CRASH); do
        # initialze
        dmsg "crash run $target $i/$CNT_CRASH"
        clear

        # run
        start=$(date +%s%N)
        run_bg $target
        bg_id=$!

        # crash
        crashtime=$(((RANDOM * RANDOM * RANDOM) % ($crash_max-$crash_min) + $crash_min))
        dmsg "crash time=${crashtime} ns"
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # kill after random crash time
            if [ $elapsed -gt $crashtime ]; then
                kill -9 $bg_id || true
                wait $bg_id || true
                dmsg "crash after $elapsed ns"
                break
            fi
        done

        # recovery run
        dmsg "recovery run $target $i/$CNT_CRASH"
        run $target
    done

    # # TODO: Test thread-crash
    # for i=0; i<CNT_CRASH; i++ {
    #    프로세스 p1이 프로세스 p0의 내부 특정 스레드만 죽일 수는 없어보임.
    #    p0의 내부에서 thread-crash를 일으킬 스레드를 만들어야할듯
    # }
done
