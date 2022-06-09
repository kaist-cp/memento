To run skiplist on a specific library:

```
make clean
make benchmark-<LIB>
rm -rf /mnt/pmem0/*
PMEM_IS_PMEM_FORCE=1 ./benchmark-<LIB> -t <THREAD> -r -d <DATA-SIZE> -w a -f ../../traces
```

## Test clobber

```
make clean
make benchmark-clobber
rm -rf /mnt/pmem0/*
PMEM_IS_PMEM_FORCE=1 ./benchmark-clobber -t <THREAD> -r -d <DATA-SIZE> -w a -s <DURATION(seconds)> -f ../../traces
```

### 실험결과

result

```
rm -rf /mnt/pmem0/* && PMEM_IS_PMEM_FORCE=1 ./benchmark-clobber -t 16 -r -d 8 -w a -s 5 -f ../../traces

Thread count:    16
Value size:      8
Run time:        5003951823 (5003.95 ms)
[main] Total Ops = 547214
Run throughput:  109356 Ops/sec
```

result when pinning NUMA node 0

```
rm -rf /mnt/pmem0/* && PMEM_IS_PMEM_FORCE=1 numactl --cpunodebind=0 --membind=0 ./benchmark-clobber -t 16 -r -d 8 -w a -s 5 -f ../../traces

Thread count:    16
Value size:      8
Run time:        5002813372 (5002.81 ms)
[main] Total Ops = 560846
Run throughput:  112106 Ops/sec
```

## Test PMDK

```
make clean
make benchmark-undo
rm -rf /mnt/pmem0/*
PMEM_IS_PMEM_FORCE=1 ./benchmark-undo -t 1 -r -d <DATA-SIZE> -w a -s <DURATION(seconds)> -f ../../traces
```

### 실험결과

result

```
rm -rf /mnt/pmem0/* && PMEM_IS_PMEM_FORCE=1 ./benchmark-undo -t 16 -r -d 8 -w a -s 5 -f ../../traces

Thread count:    16
Value size:      8
Run time:        5004332824 (5004.33 ms)
[main] Total Ops = 502594
Run throughput:  100431 Ops/sec
```

result when pinning NUMA node 0

```
rm -rf /mnt/pmem0/* && PMEM_IS_PMEM_FORCE=1 numactl --cpunodebind=0 --membind=0 ./benchmark-undo -t 16 -r -d 8 -w a -s 5 -f ../../traces

Thread count:    16
Value size:      8
Run time:        5002934900 (5002.93 ms)
[main] Total Ops = 571562
Run throughput:  114245 Ops/sec
```

# TODO

Clobber-NVM queue 성능이 ResCPT의 실험결과대로 전혀 안나온다... 뭐가 다른거지?

- Q) PM 설정: ResCPT는 PM을 system-ram모드로 했다함. system-ram 모드가 뭔가?
- Q) 실험방법: enq/deq 1:1 ratio로 했다는 데 pair로 한건지 50% enq or deq로 한건지.. 그리고 몇초 돌린거?
- Q) ResCPT가 측정한 queue 성능은 기존 논문에서 측정한 queue 성능과 비슷한가?
    - Friedman queue:
        - ResCPT는 Montage의 소스 코드 썼다함
        - Montage의 fig에 있는 friedman queue랑 성능 안맞는 것 같은데... 흠
    - Quadra/Trinity: ResCPT는 좀 변형해서 측정해서 비교 불가
        - ResCPT는 Quadra queue를 썼다는데, Quadra/Trinity가 적용한 Flat Combining는 넘 사기(Fig2 참고)라 이거 안하고 그냥 lock쓰는 Quadra queue로 비교했다함


참고: Clobber-NVM의 list 성능은 Clobber-NVM fig 결과 유사하게 나옴

ResCPT랑 똑같이 friedman queue 성능 측정해보자 (Motnage에 구현된 friedman queue 측정)

