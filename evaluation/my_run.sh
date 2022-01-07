#!/bin/bash

# TODO: 이 파일은 프로젝트에 필요없음. 간단한 테스트 용도

target="memento_queue_general"
kind="pair"
thread=12
duration=10

rm -rf /mnt/pmem0/*
mkdir -p out
if [ "${target}" == "pmdk_pipe" ] || [ "${target}" == "pmdk_queue" ]; then
    numactl --cpunodebind=0 --membind=0 ./target/release/bench_cpp /mnt/pmem0/$target $target $kind $thread $duration out/my_run.csv
else
    numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/$target -a $target -k $kind -t $thread -d $duration -o out/my_run.csv
fi