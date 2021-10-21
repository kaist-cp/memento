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
            if [ "pmdk_pipe" == "${target}" ]; then
                $dir_path/target/release/bench_cpp $poolpath $target $kind $t $TEST_DUR $out
            else
                $dir_path/target/release/bench -f $poolpath -a $target -k $kind -t $t -d $TEST_DUR -o $out
            fi
        done
    done
    echo "done."
    echo ""
}

set -e

if [ $# -ne 1 ] ; then
    echo "Usage: run.sh <pmempath>"
    exit 0
fi

# 1. Setup
PMEM_PATH=$1   # PMEM_PATH에 풀 파일을 생성하여 사용
MAX_THREADS=32        # 1~MAX_THREADS까지 스레드 수를 달리하며 처리율 계산
TEST_CNT=5            # 한 bench당 테스트 횟수
TEST_DUR=10           # 한 테스트당 지속시간

time=$(date +%Y)$(date +%m)$(date +%d)$(date +%H)$(date +%M)
dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH
mkdir -p $out_path
rm -rf ${PMEM_PATH}*.pool # 기존 풀 파일 제거
show_cfg

# 2. Benchmarking queue performance
# bench our_queue prob50 $out_path/queue.csv
# bench durable_queue prob50 $out_path/queue.csv
# bench log_queue prob50 $out_path/queue.csv
# bench dss_queue prob50 $out_path/queue.csv
bench our_queue pair $out_path/queue.csv
bench durable_queue pair $out_path/queue.csv
bench log_queue pair $out_path/queue.csv
bench dss_queue pair $out_path/queue.csv

# 3. Benchmarking pipe performance
bench our_pipe pipe $out_path/pipe.csv
bench crndm_pipe pipe $out_path/pipe.csv
bench pmdk_pipe pipe $out_path/pipe.csv

# 4. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
