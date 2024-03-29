#!/bin/bash

git_hash=$(git log -1 --format="%h")

function show_cfg() {
    echo "<Configurations>"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "Test count: ${TEST_CNT}"
    echo "Test duration: ${TEST_DUR}s"
    echo "Total time: $((${TEST_DUR}*${TEST_CNT}*${#THREADS[@]}*${#CONTENTIONS[@]}*${#DS[@]}))s" # duration * count * # threads * # contentions * # DSs
    echo ""
}

function bench() {
    target=$1
    thread=$2
    contention=$3

    outpath=$out_path/${target}_contention${contention}_${git_hash}.csv
    poolpath=$PMEM_PATH/eval_cas/${target}.pool

    RUST_MIN_STACK=5073741824 numactl --cpunodebind=0 --membind=0 $dir_path/target/release/cas_bench -f $poolpath -a $target -t $thread -c $contention -d $TEST_DUR -o $outpath
}

function benches() {
    target=$1
    echo "< Bench ${target} >"
    for c in ${CONTENTIONS[@]}; do
        for t in ${THREADS[@]}; do
            for ((var=1; var<=$TEST_CNT; var++)); do
                echo "test $var/$TEST_CNT...";
                bench $target $t $c
            done
        done
    done
    echo "done."
    echo ""
}

set -e

# 1. Setup
PMEM_PATH=/mnt/pmem0
THREADS=(1 2 3 4 5 6 7 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)
CONTENTIONS=(1 1000 1000000)
TEST_CNT=5            # test cnt per 1 bench
TEST_DUR=10           # test duration
DS=("mcas" "pmwcas" "nrlcas")

dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH/eval_cas
mkdir -p $out_path
show_cfg

# 2. Benchmarking cas performance
for ds in ${DS[@]}; do
    benches $ds
done

# 3. Plot and finish
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
