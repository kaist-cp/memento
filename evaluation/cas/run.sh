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
    thread=$2

    outpath=$out_path/${target}_${git_hash}.csv
    poolpath=$PMEM_PATH/${target}.pool

    rm -f $poolpath*
    numactl --cpunodebind=0 --membind=0 $dir_path/target/release/cas_bench -f $poolpath -a $target -t $thread -d $TEST_DUR -o $outpath
}

function benches() {
    target=$1
    echo "< Running performance benchmark through using thread 1~${MAX_THREADS} (target: ${target}) >"
    for t in ${THREADS[@]}; do
        for ((var=1; var<=$TEST_CNT; var++)); do
            echo "test $var/$TEST_CNT...";
            bench $target $t
        done
    done
    echo "done."
    echo ""
}

set -e

# 1. Setup
PMEM_PATH=/mnt/pmem0
THREADS=(1 2 3 4 5 6 7 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)
TEST_CNT=1            # test cnt per 1 bench
TEST_DUR=5           # test duration

dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH
mkdir -p $out_path
rm -rf ${PMEM_PATH}/*.pool*
show_cfg

# 2. Benchmarking cas performance
benches cas
benches mcas
benches pcas

# 3. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
