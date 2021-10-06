#!/bin/bash

# Test Parameters
PMEM_PATH=$1        # e.g. j02서버에서는 /mnt/pmem0
DURATION=$2         # 테스트 한 번은 n초동안 지속
COUNT=$3            # n번 테스트
ENQ_PROBABILITY=$4  # n% 확률로 enq, 100-n% 확률로 deq
# TODO?: num_init_node = 1000000 # 초기 노드 수

# Setup
time=$(date +%Y)$(date +%m)$(date +%d)$(date +%H)$(date +%M)
dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out-$time
if [ ! -d $out_path ]; then
    mkdir $out_path
fi

function info() {
    echo "[Test Configurations]: $0"
    echo "Pmem path: ${PMEM_PATH}"
    echo "Duration: ${DURATION}"
    echo "Count: ${COUNT}"
    echo -e "Enqueue probability: ${ENQ_PROBABILITY}\n"
}

function test() {
    target=$1
    poolname=$target.pool
    outname=$target.out
    poolpath=$PMEM_PATH/$poolname
    echo "Running performance test (${target}: throughput)"
    echo -e "Duration: ${DURATION}, Count: ${COUNT}, Enq_probability: ${ENQ_PROBABILITY}\n" > $out_path/$outname
    $dir_path/../target/release/examples/eval $poolpath $target $DURATION $COUNT $ENQ_PROBABILITY >> $out_path/$outname
}

sudo rm -rf $PMEM_PATH*.pool # 기존 풀 파일 제거
info

# Test queue performance
test our_queue
test friedman_durable_queue
test friedman_log_queue

# Test pipe performance
# TODO: test our_pipe
# TODO: test pmdk_pipe (eval/pmdk/pmdk_pipe.cpp를 테스트)
# TODO: test corundum_pipe

# Print result
echo "Test finished! see ${out_path}"