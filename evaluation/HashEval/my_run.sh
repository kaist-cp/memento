#!/bin/bash

BIN="bin"
target="clevel_rust"

HASH_SIZE=16777216      # Initial capacity of hash table
OP=200000000            # Load, Run phase 각가에서 실행시킬 op 수
SKIP_LOAD=true          # Load phase를 skip할지 여부
READ_RT=0               # Run phase에 실행시킬 op 중 몇 %를 read로 할건가
INSERT_RT=1             # Run phase에 실행시킬 op 중 몇 %를 insert로 할건가
REMOVE_RT=0             # Run phase에 실행시킬 op 중 몇 %를 remove로 할건가
NEGATIVE_RT=0           # Run phase에 실행시킬 read 중 몇 %를 negative search로 할건가
MODE="THROUGHPUT"       # Evaluation mode (possbile arg: THROUGHPUT, LATENCY, LOAD_FACTOR)
DISTRIBUTION="UNIFORM"  # Key distribution (possible arg: UNIFORM, SELFSIMILAR, ZIPFIAN)
THREAD=8

# NUMA node 0에 pinning하여 테스트하려면 아래처럼 실행해야함
# numactl --cpunodebind=0 --membind=0 sudo ./$BIN/PiBench ...
./$BIN/PiBench ./$BIN/$target.so \
    -S $HASH_SIZE \
    -p $OP \
    --skip_load=$SKIP_LOAD \
    -r $READ_RT -i $INSERT_RT -d $REMOVE_RT \
    -N $NEGATIVE_RT \
    -M $MODE --distribution $DISTRIBUTION \
    -t $THREAD \
    # >> out/debug.out

