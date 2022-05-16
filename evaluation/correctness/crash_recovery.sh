#!/bin/bash
# script for test normal run and crash/recovery run

# TODO:
# ./crash_recovery.sh => PMEM 버전 테스트
# ./crash_recovery.sh no_persist => DRAM 버전 테스트


set -e
cd ../../
cargo clean
cargo build --tests --release --features no_persist # TODO: PMEM_version

TARGETS=("queue" "queue_general") # Test Target
CNT_NORMAL=3
CNT_CRASH=100

function clear() {
    # DRAM version
    rm -rf test
    mkdir test

    # TODO: PMEM version
}

function run() {
    target=$1

    # DRAM version
    RUST_MIN_STACK=1007374182 ./target/release/deps/memento-* ds::$target::test -- --nocapture > crash_recovery_out.txt

    # TODO: queue는 이렇게 해야 queue_lp, queue_general, queue_..과 한꺼번에 실행되지 않고 얘만 타겟할 수 있음
    # RUST_MIN_STACK=1007374182 ./target/release/deps/memento-* ds::queue::test -- --nocapture

    # TODO: PMEM version
    # RUST_MIN_STACK=1007374182 cargo test --release $target -- --nocapture
}

function run_bg() {
    target=$1

    # DRAM version
    RUST_MIN_STACK=1007374182 ./target/release/deps/memento-* ds::$target::test -- --nocapture > crash_recovery_out.txt &

    # TODO: PMEM version
    # RUST_MIN_STACK=1007374182 cargo test --release $target -- --nocapture
}


# Run test
for target in ${TARGETS[@]}; do
    avgtime=0 # Test 완료하는 데 걸리는 시간. crash-recovery 테스트시 이 시간 내에 crash 일으켜야함

    # TODO: test normal run. 끝까지 완주 후 assert
    for i in $(seq 1 $CNT_NORMAL); do
        # initlaize
        echo -e "normal run $target $i/$CNT_NORMAL";
        clear

        # run
        start=$(date +%s%N)
        run $target
        # RUST_MIN_STACK=1007374182 cargo test --release --features no_persist $target -- --nocapture
        echo $!
        echo $!

        end=$(date +%s%N)

        # calculate elpased time
        avgtime=$(($avgtime+$(($end-$start)))) # TODO: 시간 정확하지 않음
        echo $!

        # re-execute
        run $target
    done

    avgtime=$(($avgtime/$CNT_NORMAL))
    echo -e "avgtime: $avgtime\n"

    # TODO: test full-crash and recovery. COUNT번 랜덤 crash, 복구후 이어서 끝낸 뒤 assert
    for i in $(seq 1 $CNT_CRASH); do
        # initialze
        echo -e "crash run $target $i/$CNT_CRASH";
        clear

        # execute
        start=$(date +%s%N)
        run_bg $target
        echo "pid=$!"

        # crash
        crash_time=$((RANDOM % ($avgtime-1500000000) + 1500000000)) # 최소 1.5초 이후에 crash (최소 pool create은 끝난 다음에 crash해야함..)
        echo "crash_time=${crash_time}ns"
        while true; do
            current=$(date +%s%N)
            elapsed=$(($current-$start))

            # crash_time 이후 kill(pid)
            if [ $elapsed -gt $crash_time ]; then
                echo "kill $!"
                kill -9 $!
                break
            fi
        done

        # re-execute: recover and keep operation, assert result
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
