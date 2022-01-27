#!/bin/bash

# TODO: 이 파일은 프로젝트에 필요없음. 간단한 테스트 용도

function test_single() {
    target=$1
    kind=$2
    thread=$3
    duration=$4
    init=$5

    rm -rf /mnt/pmem0/*
    mkdir -p out
    if [ "${target}" == "pmdk_pipe" ] || [ "${target}" == "pmdk_queue" ]; then
        numactl --cpunodebind=0 --membind=0 ./target/release/bench_cpp /mnt/pmem0/$target $target $kind $thread $duration $init out/my_run.csv
    else
        numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/$target -a $target -k $kind -t $thread -d $duration -i $init -o out/my_run.csv
    fi
}

targets=("memento_queue" "memento_queue_general" "durable_queue" "log_queue" "dss_queue")
kind="pair"
duration=5
init=0 # 초기 노드 수

for target in ${targets[@]}; do
    for thread in 1 4 8 12 32; do
        # echo "$target"
        test_single $target $kind $thread $duration $init
    done
done
