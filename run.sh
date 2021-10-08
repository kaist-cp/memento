#!/bin/bash

function show_cfg() {
    echo "<Configurations>"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "Test cnt per bench: ${TEST_CNT}"
    echo "Duration per test: ${TEST_DUR}(s)"
    echo ""
}

function bench() {
    target=$1
    kind=$2
    poolname=${target}.pool
    poolpath=$PMEM_PATH/$poolname
    echo "< Running performance benchmark through using thread 1~${MAX_THREADS} (target: ${target}, bench kind: ${kind}) >"
    for t in $( seq 1 $MAX_THREADS )
    do
        rm -f $poolpath
        $dir_path/target/release/examples/bench -f $poolpath -a $target -k $kind -t $t -c $TEST_CNT -d $TEST_DUR
    done
    echo "done."
    echo ""
}

# 1. Setup
PMEM_PATH=/mnt/pmem0  # PMEM_PATH에 풀 파일을 생성하여 사용
MAX_THREADS=32        # 1~MAX_THREADS까지 스레드 수를 달리하며 처리율 계산
TEST_CNT=5            # 한 bench당 테스트 횟수
TEST_DUR=10           # 한 테스트당 지속시간

time=$(date +%Y)$(date +%m)$(date +%d)$(date +%H)$(date +%M)
dir_path=$(dirname $(realpath $0))
rm -rf ${PMEM_PATH}*.pool # 기존 풀 파일 제거
show_cfg

# 2. Benchmarking queue performance
bench our_queue prob50
bench durable_queue prob50
bench log_queue prob50
bench our_queue pair
bench durable_queue pair
bench log_queue pair

# 3. Benchmarking pipe performance
# TODO: bench our_pipe pipe
# TODO: bench corundum_pipe pipe
# TODO: bench pmdk_pipe pipe (examples/bench_impl/pmdk/pmdk_pipe.cpp를 테스트)

# 4. Print result
echo "Entire benchmarking was done! see result on \".out/\""
