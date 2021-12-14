#!/bin/bash

# TODO: 이 파일은 프로젝트에 필요없음. 간단한 테스트 용도

numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/q -a memento_queue -k pair -t 12 -d 5 -o q.out
numactl --cpunodebind=0 --membind=0 ./target/release/bench -f /mnt/pmem0/q -a memento_queue -k pair -t 12 -d 5 -o q.out
