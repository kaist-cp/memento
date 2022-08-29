#!/bin/bash
target=$1
feature=$2
SCRIPT_DIR=`dirname $(realpath "$0")`

# Test Config
if [ "$feature" == "no_persist" ]; then
    PMEM_PATH="$SCRIPT_DIR/../../" # memento crate path
else
    PMEM_PATH="/mnt/pmem0"
fi
COMMIT=$(git log -1 --format="%h")
BUG_LIMIT=30     # Limitation of the number of saving pool file when a bug occurs
TIMEOUT=10

# Initialize
trap "exit;" SIGINT SIGTERM

bug_cnt=0
OUT_PATH="$SCRIPT_DIR/out_${COMMIT}/${target}"
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

function run() {
    target=$1
    dmsg "run $target"

    rm -rf $PMEM_PATH/test/$target/*
    if [ "$feature" == "no_persist" ]; then
        RUST_BACKTRACE=1 RUST_MIN_STACK=10737418200 timeout $TIMEOUT $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* $target::test --nocapture &>> $log_tmp
    else
        RUST_BACKTRACE=1 RUST_MIN_STACK=100737418200 numactl --cpunodebind=0 --membind=0 timeout $TIMEOUT $SCRIPT_DIR/../../target/release/deps/memento-* $target::test --nocapture &>> $log_tmp
    fi

}

# Test thread crash and recovery run.
i=0
while true; do
    i=$(($i+1))
    log_tmp="$(mktemp)"
    dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test $target $i ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
    run $target
    ext=$?
    if [ $ext -eq 0 ]; then
        pmsg "[${i}th test] success"
    else
        dmsg "fails with exit code $ext"
        pmsg "[${i}th test] fails with exit code $ext"

        # Save bug pool and logs
        out_bug_path=$OUT_PATH/bug${bug_cnt}_exit${ext}
        mkdir -p $out_bug_path
        cp -r $PMEM_PATH/test/$target/*.pool* $out_bug_path
        cp $log_tmp $out_bug_path/info.txt

        # Update output path of bug
        bug_cnt=$(($bug_cnt+1))
        if [ $bug_cnt -eq $BUG_LIMIT ]; then
            exit
        fi
    fi
    dmsg "⎿___________________________________________________________________________⏌"
    cat $log_tmp >> $OUT_LOG
done
