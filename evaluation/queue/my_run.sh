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
    if [ "${target}" == "pmdk_queue" ]; then
        numactl --cpunodebind=0 --membind=0 ./target/release/bench_cpp /mnt/pmem0/$target $target $kind $thread $duration $init out/my_run.csv
    else
        numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/$target -a $target -k $kind -t $thread -d $duration -i $init -o out/my_run.csv
    fi
}

# targets=("memento_queue" "memento_queue_lp" "memento_queue_general" "durable_queue" "log_queue" "dss_queue" "crndm_queue" "pmdk_queue")
# targets=("memento_queue" "memento_queue_lp" "memento_queue_general" "memento_queue_comb" "durable_queue" "log_queue" "dss_queue" "pbcomb_queue")
# targets=("pbcomb_queue")
# targets=("memento_queue" "memento_queue_general" "memento_queue_lp" "durable_queue" "log_queue")
# targets=("durable_queue" "log_queue")
# targets=("memento_queue_comb")
# targets=("memento_queue_comb" "pbcomb_queue_full_detectable")
targets=("memento_queue_comb" "pbcomb_queue")
# kind="prob100"
duration=5

for kind in pair prob20 prob50 prob80; do
# for kind in pair; do
    if [ $kind == pair ]; then
        init_nodes=0
    elif [ $kind == prob100 ]; then
        init_nodes=0
    else
        init_nodes=10000000
    fi

    for target in ${targets[@]}; do
        # for thread in 1 8 16 48; do
        for thread in 1 8 16 48; do
            # echo "$target"
            test_single $target $kind $thread $duration $init
        done
    done
done
