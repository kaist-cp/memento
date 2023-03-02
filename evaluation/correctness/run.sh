#!/bin/bash
target=$1
cfg=$(cat config)
SCRIPT_DIR=`dirname $(realpath "$0")`

# Test Config
if [ "$cfg" == "no-persist" ]; then
    PMEM_PATH="$SCRIPT_DIR/../../" # memento crate path
else
    PMEM_PATH="/mnt/pmem0"
fi
COMMIT=$(git log -1 --format="%h")
BUG_LIMIT=30     # Limitation of the number of saving pool file when a bug occurs
TIMEOUT=10
RETRY_LIMIT=10   # Limiations of the number of retrying the timeout.

# Initialize
trap "exit;" SIGINT SIGTERM

OUT_PATH="$SCRIPT_DIR/out_${COMMIT}/${target}"
mkdir -p $PMEM_PATH/test
mkdir -p $OUT_PATH

OUT_LOG=$OUT_PATH/log.out
OUT_PROGRESS=$OUT_PATH/progress.out

# Print message to "out/{target}/progress.out"
function pmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$msg"
    echo "[$time] $msg" >> $OUT_PROGRESS
}

# Print message to:
# - "out/{target}/log.out": Full log
# - "out/{target}/{bugnum}_log.out": Corrosponding log for that {bugnum}
function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$msg"
    echo "[$time] $msg" >> $log_tmp
}

function clear() {
    target=$1
    dmsg "clear $target"
    rm -rf $PMEM_PATH/test/$target/*
}

function run() {
    target=$1
    dmsg "run $target"
    if [ "$cfg" == "no-persist" ]; then
        RUST_BACKTRACE=1 RUST_MIN_STACK=2000000000 timeout $TIMEOUT $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* $target::test --nocapture &>> $log_tmp
    else
        RUST_BACKTRACE=1 RUST_MIN_STACK=2000000000 numactl --cpunodebind=0 --membind=0 timeout $TIMEOUT $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* $target::test --nocapture &>> $log_tmp
    fi

}

# Test thread crash and recovery run.
bug_cnt=0
i=0
# {i}th test
while true; do
    i=$(($i+1))
    try=0
    log_tmp="$(mktemp)"
    clear $target

    # {try}th try of {i}th test
    while true; do
        dmsg "⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test $target $i (try: $try) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋"
        run $target
        ext=$?
        if [ $ext -eq 0 ]; then
            pmsg "[${i}th test] success (try: $try)"
            break
        # Retry if the timeout occurs. (exit code=124)
        elif [[ $ext -eq 124 && $try -ne $RETRY_LIMIT ]]; then
            dmsg "fails with exit code $ext. Retry it. (try: $try)"
            pmsg "[${i}th test] fails with exit code $ext. Retry it. (try: $try)"
            try=$(($try+1))
        else
            dmsg "fails with exit code $ext (try: $try)"
            pmsg "[${i}th test] fails with exit code $ext (try: $try)"

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
            break
        fi
        dmsg "⎿___________________________________________________________________________⏌"
    done

    cat $log_tmp >> $OUT_LOG
done
