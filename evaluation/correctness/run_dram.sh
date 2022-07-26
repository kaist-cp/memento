#!/bin/bash

# Test Config
# PMEM_PATH="/mnt/pmem0"
PMEM_PATH="/home/drzix/Projects/memento/"
target=$1
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

function run_bg() {
    target=$1
    dmsg "run $target"

    rm -rf $PMEM_PATH/test/$target/*
    RUST_BACKTRACE=1 RUST_MIN_STACK=10737418200 $SCRIPT_DIR/../../target/release/deps/memento-* $target::test --nocapture &>> $OUT_PATH/$target.out &
}

# Test thread crash and recovery run.
for i in $(seq 1 $CNT_CRASH); do
    dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test $target $i/$CNT_CRASH ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
    start=$(date +%s%N)
    run_bg $target
    pid_bg=$!

    limit=$((40 * 10**9))
    while ps | grep $pid_bg > /dev/null; do
        current=$(date +%s%N)
        elapsed=$(($current-$start))
        if [ $elapsed -gt $limit ]; then
            kill -9 $pid_bg || true
            dmsg "kill $pid_bg because it has been running for over 100 seconds."
            break
        fi
    done

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
