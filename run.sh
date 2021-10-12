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
    out=$3
    poolname=${target}.pool
    poolpath=$PMEM_PATH/$poolname
    echo "< Running performance benchmark through using thread 1~${MAX_THREADS} (target: ${target}, bench kind: ${kind}) >"
    
    # 스레드 `t`개를 사용할 때의 처리율 계산
    for t in $( seq 1 $MAX_THREADS )
    do
        # `TEST_CNT`번 반복
        for ((var=1; var<=$TEST_CNT; var++));
        do
            echo "test $var/$TEST_CNT...";
            rm -f $poolpath
            $dir_path/target/release/examples/bench -f $poolpath -a $target -k $kind -t $t -d $TEST_DUR
        done
    done
    echo "done."
    echo ""
}

# 1. Setup
PMEM_PATH=./pmem   # PMEM_PATH에 풀 파일을 생성하여 사용
MAX_THREADS=4        # 1~MAX_THREADS까지 스레드 수를 달리하며 처리율 계산
TEST_CNT=3            # 한 bench당 테스트 횟수 
TEST_DUR=1           # 한 테스트당 지속시간

time=$(date +%Y)$(date +%m)$(date +%d)$(date +%H)$(date +%M)
dir_path=$(dirname $(realpath $0))
rm -rf ${PMEM_PATH}*.pool # 기존 풀 파일 제거
show_cfg

# 2. Benchmarking queue performance
bench our_queue prob50 out/queue.csv
bench durable_queue prob50 out/queue.csv
bench log_queue prob50 out/queue.csv
bench our_queue pair out/queue.csv
bench durable_queue pair out/queue.csv
bench log_queue pair out/queue.csv

# 3. Benchmarking pipe performance
# TODO: bench our_pipe pipe
# TODO: bench corundum_pipe pipe
# TODO: bench pmdk_pipe pipe (examples/bench_impl/pmdk/pmdk_pipe.cpp를 테스트)

# 4. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
