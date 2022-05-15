#!/bin/bash
# script for test normal run and crash/recovery run

# TODO: 
# ./crash_recovery.sh => PMEM 버전 테스트
# ./crash_recovery.sh no_persist => DRAM 버전 테스트 

set -e

TARGETS=("queue_general" "queue_lp") # Test Target
CNT_NORMAL=1
CNT_CRASH=1

function clear() {
    # DRAM version
    rm -rf test
    mkdir test
    
    # TODO: PMEM version
}

function run() {
    target=$1

    # DRAM version
    RUST_MIN_STACK=1007374182 cargo test --release --features no_persist $target -- --nocapture

    # TODO: PMEM version
    # RUST_MIN_STACK=1007374182 cargo test --release $target -- --nocapture
}

# Build
cargo build --features no_persist # TODO: PMEM_version

# Run test
for target in ${TARGETS[@]}; do
    avgtime=0 # Test 완료하는 데 걸리는 시간. crash-recovery 테스트시 이 시간 내에 crash 일으켜야함

    # TODO: test normal run. 끝까지 완주 후 assert
    for i in $(seq 1 $CNT_NORMAL); do 
        echo -e "normal run $target $i/$CNT_NORMAL"; 
        clear

        start=$(date +%s%N)
        run $target
        end=$(date +%s%N)

        avgtime=$(($avgtime+$(($end-$start)))) # TODO: 시간 정확하지 않음
    done

    avgtime=$(($avgtime/$CNT_NORMAL))
    echo -e "avgtime: $avgtime\n"

    # # TODO: test full-crash and recovery. COUNT번 랜덤 crash, 복구후 이어서 끝낸 뒤 assert
    # for i=0; i<CNT_CRASH; i++ {
    #     # execute
    #     clear

    #     start=$(date +%s%N)
    #     pid = run_background(cargo test --release $target)

    #     # crash
    #     crash_time = $((RANDOM % $avgtime ))
    #     while true {
    #         current=$(date +%s%N)
    #         elapsed = $(($current-$start))

    #         # crash_time 이후 kill(pid) 
    #         if elapsed >= crash_time {
    #             kill(pid)
    #             break
    #         }
    #     }

    #     # re-execute: recover and keep operation, assert result
    #     run(cargo test --release $target)
    # }

    # # TODO: test thread-crash recovery. COUNT번 랜덤 crash, 복구후 이어서 끝낸 뒤 assert
    # for i=0; i<CNT_CRASH; i++ {
    #     # NOTE 프로세스 p1이 프로세스 p0의 내부 특정 스레드만 죽일 수는 없어보임. p0의 내부에서 thread-crash를 일으킬 스레드를 만들어야할듯

    #     clear
    #     ...
    # }
done
