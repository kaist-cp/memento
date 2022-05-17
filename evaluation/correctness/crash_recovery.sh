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
    RUST_MIN_STACK=1007374182 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture >> $OUT_PATH/$target.out
    # RUST_MIN_STACK=1007374182 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test -- --nocapture >> $OUT_PATH/$target.out
}

function run_bg() {
    target=$1
    RUST_MIN_STACK=1007374182 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture >> $OUT_PATH/$target.out &
    # RUST_MIN_STACK=1007374182 $SCRIPT_DIR/../../target/release/deps/memento-* ds::$target::test -- --nocapture & >> $OUT_PATH/$target.out
}

# Run test
for target in ${TARGETS[@]}; do
    avgtime=0 # Test 완료하는 데 걸리는 시간. crash-recovery 테스트시 이 시간 내에 crash 일으켜야함

    # Test normal run.
    for i in $(seq 1 $CNT_NORMAL); do
        # initlaize
        dmsg "normal run $target $i/$CNT_NORMAL"
        clear

        # run
        start=$(date +%s%N)
        run $target
        end=$(date +%s%N)

        # calculate elpased time
        avgtime=$(($avgtime+$(($end-$start))))

        # re-execute
        run $target
    done

    avgtime=$(($avgtime/$CNT_NORMAL))
    dmsg "avgtime: $avgtime ns"

    # Test full-crash and recovery run.
    for i in $(seq 1 $CNT_CRASH); do
        # initialze
        dmsg "crash run $target $i/$CNT_CRASH"
        clear

        # execute
        start=$(date +%s%N)
        run_bg $target

        # crash
        min=$((2 * 10**9))  # 최소 3초 이후에 crash (pool create은 끝난 다음에 crash해야함) TODO: 3초가 적절한가?
        ktime=$(((RANDOM * RANDOM * RANDOM) % ($avgtime-$min) + $min))
        dmsg "ktime=${ktime} ns"
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # 랜덤시간 이후 kill
            if [ $elapsed -gt $ktime ]; then
                kill -9 %1
                dmsg "kill after $elapsed ns"
                break
            fi
        done

        # re-execute
        sleep 5
        dmsg "recovery run $target $i/$CNT_CRASH"
        run $target
    done

    # # TODO: Test thread-crash
    # for i=0; i<CNT_CRASH; i++ {
    #    프로세스 p1이 프로세스 p0의 내부 특정 스레드만 죽일 수는 없어보임.
    #    p0의 내부에서 thread-crash를 일으킬 스레드를 만들어야할듯
    # }
done
