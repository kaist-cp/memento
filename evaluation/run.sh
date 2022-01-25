#!/bin/bash

git_hash=$(git log -1 --format="%h")
git_date=$(git log -1 --date=format:'%Y%m%d' --format=%cd)

function show_cfg() {
    echo "<Configurations>"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "Max threads: ${MAX_THREADS}"
    echo "Test count: ${TEST_CNT}"
    echo "Test duration: ${TEST_DUR}s"

    let total_dur=$TEST_CNT*$TEST_DUR*$MAX_THREADS/60
    echo "테스트 총 소요시간: obj 수 * 약 ${total_dur}m (thread * count * duration)"
    echo "git hash: $git_hash"
    echo "git date: $git_date"
    echo ""
}

function bench() {
    target=$1
    kind=$2
    thread=$3

    outpath=$out_path/${target}_${git_hash}_${git_date}.csv
    poolpath=$PMEM_PATH/${target}.pool

    rm -f $poolpath*
    if [ "${target}" == "pmdk_pipe" ] || [ "${target}" == "pmdk_queue" ]; then
        # pinning NUMA node 0
        numactl --cpunodebind=0 --membind=0 $dir_path/target/release/bench_cpp $poolpath $target $kind $t $TEST_DUR $outpath
    else
        numactl --cpunodebind=0 --membind=0 $dir_path/target/release/bench -f $poolpath -a $target -k $kind -t $thread -d $TEST_DUR -o $outpath
    fi
}

function benches() {
    target=$1
    kind=$2
    echo "< Running performance benchmark through using thread 1~${MAX_THREADS} (target: ${target}, bench kind: ${kind}) >"
    # 스레드 `t`개를 사용할 때의 처리율 계산
    for t in $( seq 1 $MAX_THREADS )
    do
        # `TEST_CNT`번 반복
        for ((var=1; var<=$TEST_CNT; var++));
        do
            echo "test $var/$TEST_CNT...";
            bench $target $kind $t
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

dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH
mkdir -p $out_path
rm -rf ${PMEM_PATH}/*.pool* # 기존 풀 파일 제거
show_cfg

# 2. Benchmarking queue performance
for kind in pair prob50; do
    benches memento_queue $kind
    benches memento_queue_general $kind
    # benches memento_pipe_queue $kind
    benches durable_queue $kind
    benches log_queue $kind
    benches dss_queue $kind
    benches pmdk_queue $kind
    benches crndm_queue $kind
done

# 3. Benchmarking pipe performance
# for kind in pipe; do
#     benches memento_pipe $kind
#     benches crndm_pipe $kind
#     benches pmdk_pipe $kind
# done

# 4. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
