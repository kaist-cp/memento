#!/bin/bash

function show_cfg() {
    echo "<Test Configurations>"
    echo "[path]"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "output path: ${out_path}"

    echo "[test size]"
    echo "Number of test: ${COUNT}"
    echo "Duration per test: ${DURATION}"

    echo "[test argument]"
    echo "Enqueue probability: ${ENQ_PROBABILITY}"
    echo ""
}

function test() {
    target=$1
    poolname=$target.pool
    outname=$target.out
    poolpath=$PMEM_PATH/$poolname
    echo "Running performance test (${target})"
    echo -e "Duration: ${DURATION}, Count: ${COUNT}, Enq_probability: ${ENQ_PROBABILITY}\n" > $out_path/$outname
    $dir_path/target/release/examples/bench $poolpath $target $DURATION $COUNT $ENQ_PROBABILITY >> $out_path/$outname
}

# 1. Setup
## test parameters
PMEM_PATH=$1        # 이곳에 pool 파일을 생성하여 테스트 (e.g. j02 서버에서는 `/mnt/pmem0`)
DURATION=$2         # 테스트당 지속시간
COUNT=$3            # 테스트 횟수
ENQ_PROBABILITY=$4  # n% 확률로 enq, 100-n% 확률로 deq
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

# 2. Test queue performance
test our_queue
test friedman_durable_queue
test friedman_log_queue

# 3. Test pipe performance
# TODO: test our_pipe
# TODO: test corundum_pipe
# TODO: test pmdk_pipe (examples/bench_impl/pmdk/pmdk_pipe.cpp를 테스트)

# 4. Print result
echo "Test finished! see ${out_path}"