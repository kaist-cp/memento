#!/bin/bash

# Test Config
PMEM_PATH="/mnt/pmem0"
target=$1
CNT_NORMAL=10    # Number of normal test
CNT_CRASH=10000   # Number of crash test

# Initialize
SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out"

function pmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$1"
    echo "[$time] $msg" >> $OUT_PATH/${target}_progress.out
}

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$1"
    echo "[$time] $msg" >> $OUT_PATH/$target.out
}

function run() {
    target=$1
    dmsg "run $target"

    rm -rf $PMEM_PATH/test/$target/*
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture &>> $OUT_PATH/$target.out
}

function run_bg() {
    target=$1
    dmsg "run_bg $target"

    rm -rf $PMEM_PATH/test/$target/*
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test --nocapture &>> $OUT_PATH/$target.out &
}

# Test normal run.
avgtest=0 # average test time
for i in $(seq 1 $CNT_NORMAL); do
    dmsg "normal run $target $i/$CNT_NORMAL"

    # run
    start=$(date +%s%N)
    run $target
    end=$(date +%s%N)

    # calculate average test time
    avgtest=$(($avgtest+$(($end-$start))))
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
    start=$(date +%s%N)
    run_bg $target
    pid_bg=$!

    # thread crash
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

    ext=$?
    if [ $ext -eq 0 ]; then
        dmsg "ok"
        pmsg "[${i}th test] success"
    else
        dmsg "fails with exit code $ext"
        pmsg "[${i}th test] fails with exit code $ext"
        kill -9 $pid_bg || true
    fi
    dmsg "⎿___________________________________________________________________________⏌"
done
