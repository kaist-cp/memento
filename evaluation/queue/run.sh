#!/bin/bash

git_hash=$(git log -1 --format="%h")

function show_cfg() {
    echo "<Configurations>"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "Test count: ${TEST_CNT}"
    echo "Test duration: ${TEST_DUR}s"
    echo ""
}

function bench() {
    target=$1
    kind=$2
    thread=$3
    init_nodes=$4

    outpath=$out_path/${target}_${git_hash}.csv
    poolpath=$PMEM_PATH/${target}.pool

    rm -f $poolpath*
    if [ "${target}" == "pmdk_queue" ] || [ "${target}" == "crndm_queue" ] || [ "${target}" == "clobber_queue" ] ; then
        if [ "${kind}" != "pair" ] && [[ $t -gt 32 ]]; then
            echo "skip test because it takes too long time to initialze queue.";
            return
        fi
    fi

    if [ "${target}" == "pmdk_queue" ]; then
        # pinning NUMA node 0
        numactl --cpunodebind=0 --membind=0 $dir_path/target/release/bench_cpp $poolpath $target $kind $thread $TEST_DUR $init_nodes $outpath
    elif [ "${target}" == "clobber_queue" ]; then
        PMEM_IS_PMEM_FORCE=1 numactl --cpunodebind=0 --membind=0 $dir_path/src/clobber-nvm/apps/queue/benchmark-clobber -k $kind -t $thread -d 8 -s $TEST_DUR -i $init_nodes -o $outpath
    else
        numactl --cpunodebind=0 --membind=0 $dir_path/target/release/bench -f $poolpath -a $target -k $kind -t $thread -d $TEST_DUR -i $init_nodes -o $outpath
    fi
}

function benches() {
    target=$1
    kind=$2
    init_nodes=$3
    for t in ${THREADS[@]}; do
        echo "< Running performance benchmark using $t threads (target: ${target}, workload: ${kind}, init nodes: ${init_nodes}) >"
        for ((var=1; var<=$TEST_CNT; var++)); do
            echo "test $var/$TEST_CNT...";
            bench $target $kind $t $init_nodes
        done
        echo ""
    done
    echo "done."
    echo ""
}

# set -e # TODO: Clobber-NVM 종종 터지는 요인 해결되면 주석해제 `../context/context.c:108: ThreadContext *my_context(): Assertion `myContext != NULL' failed.`

# 1. Setup
PMEM_PATH=/mnt/pmem0
THREADS=(1 2 3 4 5 6 7 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)
TEST_CNT=5            # test cnt per 1 bench
TEST_DUR=10           # test duration

dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH
mkdir -p $out_path
rm -rf ${PMEM_PATH}/*.pool*
show_cfg

# 2. Benchmarking queue performance
for kind in pair prob20 prob50 prob80; do
    if [ $kind == pair ]; then
        init_nodes=0
    else
        init_nodes=10000000
    fi
    benches memento_queue $kind $init_nodes
    benches memento_queue_lp $kind $init_nodes
    benches memento_queue_general $kind $init_nodes
    benches memento_queue_comb $kind $init_nodes
    benches durable_queue $kind $init_nodes
    benches log_queue $kind $init_nodes
    benches dss_queue $kind $init_nodes
    benches pbcomb_queue $kind $init_nodes
    benches pmdk_queue $kind $init_nodes
    benches crndm_queue $kind $init_nodes
    benches clobber_queue $kind $init_nodes
done

# 3. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
