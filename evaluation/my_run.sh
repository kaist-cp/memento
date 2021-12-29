#!/bin/bash

# TODO: 이 파일은 프로젝트에 필요없음. 간단한 테스트 용도

rm -rf /mnt/pmem0/*

TARGET="memento_queue"
mkdir -p out

numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/q -a $TARGET -k pair -t 12 -d 5 -o out/my_run.csv
numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/q -a $TARGET -k pair -t 12 -d 5 -o out/my_run.csv
