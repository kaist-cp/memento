#!/bin/bash

PMEM_PATH="/mnt/pmem0"
FEATURE="default"
TARGETS=("queue_general")   # Test target
CNT_NORMAL=1                        # Number of normal test
CNT_CRASH=1                         # Number of crash test

set -e
arg=$1
if [ "$arg" == "no_persist" ]; then
    FEATURE="no_persist"
    PMEM_PATH="./test"
fi

cd ../../
cargo clean
cargo build --tests --release --features=$FEATURE
# rm -f ./target/release/deps/memento-*.d

function clear() {
    rm -rf $PMEM_PATH/*
}

function run() {
    target=$1
    # RUST_MIN_STACK=1007374182 ./target/release/deps/memento-* ds::$target::test -- --nocapture
    RUST_MIN_STACK=1007374182 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture
}

function run_bg() {
    target=$1
    # RUST_MIN_STACK=1007374182 ./target/release/deps/memento-* ds::$target::test -- --nocapture &
    RUST_MIN_STACK=1007374182 cargo test --release --features=$FEATURE ds::$target::test -- --nocapture &
}


# Run test
for target in ${TARGETS[@]}; do
    avgtime=0 # Test 완료하는 데 걸리는 시간. crash-recovery 테스트시 이 시간 내에 crash 일으켜야함

    # Test normal run.
    for i in $(seq 1 $CNT_NORMAL); do
        # initlaize
        echo -e "normal run $target $i/$CNT_NORMAL";
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
    echo -e "avgtime: $avgtime\n"

    # Test full-crash and recovery run.
    for i in $(seq 1 $CNT_CRASH); do
        # initialze
        echo -e "crash run $target $i/$CNT_CRASH";
        clear

        # execute
        start=$(date +%s%N)
        run_bg $target

        # crash
        min=$((15 * 10**8))  # 최소 1.5초 이후에 crash (pool create은 끝난 다음에 crash해야함) TODO: 1.5초가 적절한가?
        ktime=$((RANDOM % ($avgtime-$min) + $min))
        echo "ktime=${ktime}ns"
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # 랜덤시간 이후 kill
            if [ $elapsed -gt $ktime ]; then
                echo "kill after $elapsed ns"
                kill %1
                break
            fi
        done

        # re-execute
        echo "re-execute"
        run $target
    done

    # # TODO: test thread-crash recovery. COUNT번 랜덤 crash, 복구후 이어서 끝낸 뒤 assert
    # for i=0; i<CNT_CRASH; i++ {
    #     # NOTE 프로세스 p1이 프로세스 p0의 내부 특정 스레드만 죽일 수는 없어보임. p0의 내부에서 thread-crash를 일으킬 스레드를 만들어야할듯

    #     clear
    #     ...
    # }
done
