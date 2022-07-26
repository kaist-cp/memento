#!/bin/bash

# Test Config
PMEM_PATH="/mnt/pmem0"
COMMIT=$(git log -1 --format="%h")
target=$1
CNT_BUGS=30     # Number of saving bugs

# Initialize
nr_bug=0
SCRIPT_DIR=`dirname $(realpath "$0")`
OUT_PATH="$SCRIPT_DIR/out_${COMMIT}/${target}"
out_bug_path=$OUT_PATH/bug${nr_bug}
mkdir -p $PMEM_PATH/test
mkdir -p $OUT_PATH

OUT_LOG=$OUT_PATH/log.out
OUT_PROGRESS=$OUT_PATH/progress.out

function pmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$msg"
    echo "[$time] $msg" >> $OUT_PROGRESS
}

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$msg"
    echo "[$time] $msg" >> $log_tmp
}

function run_bg() {
    target=$1
    dmsg "run $target"

    rm -rf $PMEM_PATH/test/$target/*
    RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 $SCRIPT_DIR/../../target/release/deps/memento-* $target::test --nocapture &>> $log_tmp &
}

# Test thread crash and recovery run.
i=0
while true; do
    i=$(($i+1))
    log_tmp="$(mktemp)"
    dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test $target $i ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
    start=$(date +%s%N)
    run_bg $target
    pid_bg=$!

    limit=$((100 * 10**9))
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
        pmsg "[${i}th test] success"
    else
        dmsg "fails with exit code $ext"
        pmsg "[${i}th test] fails with exit code $ext"
        kill -9 $pid_bg || true

        # Save bug pool and logs
        mkdir -p $out_bug_path
        cp -r $PMEM_PATH/test/$target/*.pool* $out_bug_path
        cp $log_tmp $out_bug_path/info.txt

        # Update output path of bug
        nr_bug=$(($nr_bug+1))
        if [ $nr_bug -eq $CNT_BUGS ]; then
            exit
        fi
        out_bug_path=$OUT_PATH/bug${nr_bug}
    fi
    dmsg "⎿___________________________________________________________________________⏌"
    cat $log_tmp >> $OUT_LOG
done
