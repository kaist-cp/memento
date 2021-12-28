
## Memento hash evaluation

### 비교방법

[Persistent Memory Hash Indexes: An Experimental Evaluation](http://vldb.org/pvldb/vol14/p785-chen.pdf)의 [repo](https://github.com/HNUSystemsLab/HashEvaluation)를 그대로 사용

- 비교군: Memento clevel, clevel, level, CCEH, P-CLHT, Dash (TODO?: SOFT)
- 실험종류: throughput, latency, load factor
- 실험 파라미터: key distribution (uniform or self-similar)

### Build

```
make -j
```

### Run

##### To run a single benchamrk,

```
./bin/PiBench ./bin/<TARGET>.so \
    -S <HASH_SIZE> \
    -p <OP> \
    --skip_load=<SKIP_LOAD> \
    -r <READ_RT> -i <INSERT_RT> -d <REMOVE_RT> \
    -N <NEGATIVE_RT> \
    -M <MODE> --distribution <DISTRIBUTION> \
    -t <THREAD> \
```

- NUMA node 0에 pinning하려면 커맨드 앞에 `numactl --cpunodebind=0 --membind=0` 를 붙여야함
- 입력
    - `<TARGET>`: 측정 대상 (possible arg: CCEH, Level, Dash, PCLHT, clevel, clevel_rust)
    - `<HASH_SIZE>`: Initial capacity of hash table (uint)
    - `<OP>`: Load, Run phase 각가에서 실행시킬 op 수 (uint)
    - `<SKIP_LOAD>`: Load phase를 skip할지 여부 (bool)
        - e.g. skip 안하면 Load phase에선 op 수만큼 insert함
    - `<READ_RT>`: Run phase에 실행시킬 op 중 몇 %를 read로 할건가 (float `x` where 0 <= x <= 1)
    - `<INSERT_RT>`: Run phase에 실행시킬 op 중 몇 %를 insert로 할건가 (float `x` where 0 <= x <= 1)
    - `<REMOVE_RT>`: Run phase에 실행시킬 op 중 몇 %를 remove로 할건가 (float `x` where 0 <= x <= 1)
    - `<NEGATIVE_RT>`Run phase에 실행시킬 read 중 몇 %를 negative search로 할건가 (float `x` where 0 <= x <= 1)
    - `<MODE>`: Evaluation mode (possbile arg: THROUGHPUT, LATENCY, LOAD_FACTOR)
    - `<DISTRIBUTION>`: Key distribution (possible arg: UNIFORM, SELFSIMILAR, ZIPFIAN)
    - `<THREAD>`: number of threads (uint)
- paper의 실험에 사용한 입력은 [run.sh](./run.sh) 참고
- 결과: 터미널에 찍힘

example: `clevel_rust`에 32 스레드로 1000번 insert 했을 때의 처리율 측정

```bash
./bin/PiBench ./bin/clevel_rust.so \
    -S 0 \
    -p 1000 \
    --skip_load=true \
    -r 0 -i 1 -d 0 \
    -N 0 \
    -M THROUGHPUT --distribution UNIFORM \
    -t 32 \
``` 

TODO: clevel, clevel-rust 제외한 나머지는 더미 폴더 필요한 거 어떻게 적지? (run.sh 참고)

##### To run the entire benchamrk,

```
run.sh
```

- 결과: out/ 폴더에 저장

TODO?: pmempath 경로 받기 (e.g. `/mnt/pmem0/`)