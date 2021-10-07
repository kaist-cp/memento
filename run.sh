#!/bin/bash

function show_cfg() {
    echo "<Test Configurations>"
    echo "[path]"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "output path: ${out_path}"
    echo "[test size for each bench]"
    echo "테스트당 수행시간: ${DURATION}초"
    echo -e "테스트 횟수: ${COUNT}회\n"
}

function bench() {
    target=$1
    kind=$2
    poolname=${target}_${kind}.pool
    outname=${target}_${kind}.out
    poolpath=$PMEM_PATH/$poolname
    echo "Running performance benchmark (target: ${target}, bench kind: ${kind})"
    echo -e "Duration: ${DURATION}, Count: ${COUNT}, Kind: ${kind}\n" > $out_path/$outname
    $dir_path/target/release/examples/bench $poolpath $DURATION $COUNT $target $kind >> $out_path/$outname
}

# 1. Setup
## test parameters
PMEM_PATH=$1        # 이곳에 pool 파일을 생성하여 테스트 (e.g. `/mnt/pmem0`)
DURATION=$2         # 테스트당 수행시간
COUNT=$3            # 테스트 횟수
## variable
time=$(date +%Y)$(date +%m)$(date +%d)$(date +%H)$(date +%M)
dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out/$time
## 잡일
show_cfg
rm -rf ${PMEM_PATH}*.pool # 기존 풀 파일 제거
if [ ! -d $dir_path/out/ ]; then
    mkdir $dir_path/out/
fi
if [ ! -d $out_path ]; then
    mkdir $out_path
fi

# 2. Benchmarking queue performance
bench our_queue prob50
bench friedman_durable_queue prob50
bench friedman_log_queue prob50
bench our_queue pair
bench friedman_durable_queue pair
bench friedman_log_queue pair

# 3. Benchmarking pipe performance
# TODO: bench our_pipe
# TODO: bench corundum_pipe
# TODO: bench pmdk_pipe (examples/bench_impl/pmdk/pmdk_pipe.cpp를 테스트)

# 4. Print result
echo "Test finished! see ${out_path}"
