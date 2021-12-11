#!/bin/bash

rm -rf /mnt/pmem0/pibench*

BIN="bin"
OUT="out"
OUT_DEBUG=./$OUT/debug.out

mkdir -p out

function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo "[$time] $msg" >> $OUT_DEBUG
}

function bench() {
    target=$1   # possible arg: CCEH, Level, Dash, PCLHT, SOFT, clevel
    workload=$2 # possible arg: insert, pos_search, ...
    mode=$3     # possible arg: THROUGHPUT, LOAD_FACTOR, RESIZE, LATENCY (대소문자 중요!!)
    dist=$4     # possible arg: UNIFORM, SELFSIMILAR, ZIPFIAN
    thread=$5

    # output 설정
    out_dir=./$OUT/$mode/$dist/$workload
    mkdir -p $out_dir
    out=$out_dir/$target.out
    echo "out: $out"

    # # clevel 제외한 나머지는 libvmmalloc과 함께 잘 동작하기 위해 더미 폴더 만들어줌
    # rm -rf /mnt/pmem0/pibench*
    # if [ "$target" != "clevel" ]; then
    #     mkdir /mnt/pmem0/pibench
    # fi

    # workload에 맞게 파라미터 설정
    HASH_SIZE=16777216      # Initial capacity of hash table (TODO: SOFT는 init capacity 0으로 하고 다른 설정 필요)
    OP=200000000            # Load, Run phase 각가에서 실행시킬 op 수
    SKIP_LOAD=false         # Load phase를 skip할지 여부
    READ_RT=0               # Run phase에 실행시킬 op 중 몇 %를 read로 할건가
    INSERT_RT=1             # Run phase에 실행시킬 op 중 몇 %를 insert로 할건가
    REMOVE_RT=0             # Run phase에 실행시킬 op 중 몇 %를 remove로 할건가
    NEGATIVE_RT=0           # Run phase에 실행시킬 read 중 몇 %를 negative search로 할건가
    DISTRIBUTION=$dist      # Key distribution

    if [ "${workload}" == "insert" ]; then
        # Load 0M, Run 200M, Insert 100%
        SKIP_LOAD=true
    elif [ "${workload}" == "pos_search" ]; then
        # Load 200M, Run 200M, Read 100%, Negative 0%
        READ_RT=1
        INSERT_RT=0
        REMOVE_RT=0
    elif [ "${workload}" == "neg_search" ]; then
        # Load 200M, Run 200M, Read 100%, Negative 100%
        READ_RT=1
        INSERT_RT=0
        REMOVE_RT=0
        NEGATIVE_RT=1
    elif [ "${workload}" == "delete" ]; then
        # Load 200M, Run 200M, Delete 100%
        READ_RT=0
        INSERT_RT=0
        REMOVE_RT=1
    elif [ "${workload}" == "write_heavy" ]; then
        # Load 200M, Run 200M, Insert 80%, Read 20%
        READ_RT=0.2
        INSERT_RT=0.8
        REMOVE_RT=0
    elif [ "${workload}" == "balanced" ]; then
        # Load 200M, Run 200M, Insert 50%, Read 50%
        READ_RT=0.5
        INSERT_RT=0.5
        REMOVE_RT=0
    elif [ "${workload}" == "read_heavy" ]; then
        # Load 200M, Run 200M, Insert 20%, Read 80%
        READ_RT=0.8
        INSERT_RT=0.2
        REMOVE_RT=0
    elif [ "${workload}" == "dummy" ]; then
        # 빨리 끝나는 더미 테스트. 일단 돌려지는지 확인하는 데 유용
        HASH_SIZE=1
        OP=1
    else
        echo "invalid workload"
        exit
    fi

    # 맞춘 파라미터로 실행
    echo "start target: $target, workload: $workload, mode: $mode, dist: $dist, thread: $thread"
    dmsg  "start target: $target, workload: $workload, mode: $mode, dist: $dist, thread: $thread"
    # NOTE: NUMA node 0에 pinning하여 테스트하려면 아래처럼 실행해야함
    # numactl --cpunodebind=0 --membind=0 sudo ./$BIN/PiBench ...
    ./$BIN/PiBench ./$BIN/$target.so \
        -S $HASH_SIZE \
        -p $OP \
        --skip_load=$SKIP_LOAD \
        -r $READ_RT -i $INSERT_RT -d $REMOVE_RT \
        -N $NEGATIVE_RT \
        -M $mode --distribution $DISTRIBUTION \
        -t $thread \
        >> $out

    # 정상 종료되지 않은 것 기록
    ext=$?
    if [ $ext -ne 0 ]; then
        dmsg "exit with code $ext! (target: $target, workload: $workload, mode: $mode, dist: $dist, thread: $thread)"
    fi
    echo -e "\n\n" >> $out
}

function bench_all() {
    workload=$1 # possible arg: insert, pos_search, ...
    mode=$2     # possible arg: THROUGHPUT, LOAD_FACTOR, RESIZE, LATENCY (대소문자 중요!!)
    dist=$3     # possible arg: UNIFORM, SELFSIMILAR, ZIPFIAN

    for THREAD in 1 4 8 16 24 32 48 64; do
        # LATENCY 측정은 32 스레드로만 한 번 하고 끝냄
        if [ "$mode" == "LATENCY" ]; then
            THREAD=32
        fi

        bench clevel_rust $workload $mode $dist $THREAD
        # bench clevel $workload $mode $dist $THREAD
        # bench CCEH $workload $mode $dist $THREAD
        # bench Level $workload $mode $dist $THREAD
        # # bench Dash $workload $mode $dist $THREAD # (TODO: compile)
        # bench PCLHT $workload $mode $dist $THREAD
        # # bench SOFT $workload $mode $dist $THREAD # (TODO: 필요하면 추가, 추가시 init capacity 확인 필요)

        # LATENCY 측정은 32 스레드로만 한 번 하고 끝냄
        if [ "$mode" == "LATENCY" ]; then
            break
        fi
    done
}

dmsg "start run.sh"

# Fig 4, 5. Throughput
dmsg "start throughput with uniform distribution."
bench_all insert THROUGHPUT UNIFORM
bench_all pos_search THROUGHPUT UNIFORM
bench_all neg_search THROUGHPUT UNIFORM
bench_all delete THROUGHPUT UNIFORM
bench_all write_heavy THROUGHPUT UNIFORM
bench_all balanced THROUGHPUT UNIFORM
bench_all read_heavy THROUGHPUT UNIFORM
dmsg "throughput with uniform distribution was done."
dmsg "start throughput with self-similar distribution."
bench_all insert THROUGHPUT SELFSIMILAR
bench_all pos_search THROUGHPUT SELFSIMILAR
bench_all neg_search THROUGHPUT SELFSIMILAR
bench_all delete THROUGHPUT SELFSIMILAR
bench_all write_heavy THROUGHPUT SELFSIMILAR
bench_all balanced THROUGHPUT SELFSIMILAR
bench_all read_heavy THROUGHPUT SELFSIMILAR
dmsg "throughput with self-similar distribution was done."
dmsg "all throughput was done."

# # Fig 7. Latency
dmsg "start latency with uniform distribution."
bench_all insert LATENCY UNIFORM
bench_all pos_search LATENCY UNIFORM
bench_all neg_search LATENCY UNIFORM
bench_all delete LATENCY UNIFORM
dmsg "latency with uniform distribution was done."
dmsg "all latency was done."

# plot
dmsg "plotting.."
python3 plot.py

dmsg "all work is done!"
