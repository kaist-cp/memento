#!/bin/bash

git_hash=$(git log -1 --format="%h")

function show_cfg() {
    echo "<Configurations>"
    echo "PMEM path: $(realpath ${PMEM_PATH})"
    echo "Test count: ${TEST_CNT}"
    echo "Test duration: ${TEST_DUR}s"
    echo "Total time: $((${TEST_DUR}*${TEST_CNT}*${#THREADS[@]}*${#KEY_RANGES[@]}*${#DS[@]}*${#KINDS[@]}))s" # duration * count * # threads * # key_ranges * # DSs * KIND
    echo ""
}

function bench() {
    target=$1
    thread=$2
    key_range=$3
    insert_rt=$4
    delete_rt=$5
    read_rt=$6

    outpath=$out_path/${target}_${git_hash}.csv
    poolpath=$PMEM_PATH/eval_list/${target}.pool
    numactl --cpunodebind=0 --membind=0 $dir_path/target/release/bench -f $poolpath -a $target -t $thread -d $TEST_DUR -k $key_range --insert-ratio $insert_rt --delete-ratio $delete_rt --read-ratio $read_rt -o $outpath
}

function benches() {
    target=$1
    key_range=$2
    insert_rt=$3
    delete_rt=$4
    read_rt=$5
    for thread in ${THREADS[@]}; do
        for ((var=1; var<=$TEST_CNT; var++)); do
            echo "test $var/$TEST_CNT...";
            bench $target $thread $key_range $insert_rt $delete_rt $read_rt
        done
        echo ""
    done
    echo "done."
    echo ""
}

# 1. Setup
PMEM_PATH=/mnt/pmem0
THREADS=(1 2 3 4 5 6 7 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)
TEST_DUR=10
TEST_CNT=5
KEY_RANGES=(20 100 500 2000)
DS=("list-mmt" "tracking" "capusles")
KINDS=("read" "update")

dir_path=$(dirname $(realpath $0))
out_path=$dir_path/out
mkdir -p $PMEM_PATH/eval_list
mkdir -p $out_path
show_cfg

# 2. Benchmarking list performance

for key_range in ${KEY_RANGES[@]}; do
    ### Read & Update intensive for tracking
    (cd src/tracking; ./figures_compile.sh $key_range)
    (cd src/tracking; ./figures_run.sh $key_range $TEST_DUR $TEST_CNT)

    ### Read intensive for mmt
    insert_rt=0.15
    delete_rt=0.15
    read_rt=0.7
    benches list-mmt $key_range $insert_rt $delete_rt $read_rt

    ### Update intensive for mmt
    insert_rt=0.35
    delete_rt=0.35
    read_rt=0.3
    benches list-mmt $key_range $insert_rt $delete_rt $read_rt
done

# 3. Plot and finish
(cd src/tracking; python3 figures_plot.py) # Gathering tracking and capusles data
python3 plot.py
echo "Entire benchmarking was done! see result on \".out/\""
