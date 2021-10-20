# Performance Evaluation of Compositional Construction of Failure-Safe Persistent Objects

evaluation을 위한 실험은 (1) queue 처리율 비교, (2) pipe 처리율 비교로 이루어짐
- queue 비교대상 및 실험종류
    - 비교대상: 우리의 queue, friedman's durable queue, friedman's log queue, dss queue
    - 실험종류:
        1. pair: 각 스레드가 { enq; deq; }를 반복
        2. prob: 각 스레드가 { 50% 확률로 enq 혹은 deq }를 반복
- pipe 비교대상 및 실험종류
    - 비교대상: 우리의 pipe, PMDK의 pipe, Corundum의 pipe
    - 실험종류:
        1. pipe: 각 스레드가 서로 다른 queue A, B에 대해 { A.deq; B.enq; B.deq; A.enq }를 반복
## Build

```bash
build.sh
```

persist instruction (e.g. `clflush`) 없이 돌려보고 싶다면,
```
build.sh no_persist
```
TODO: 현재 PMDK만 no_persist 구현 안됨. PMDK도 no_persist 같은 옵션 있는지 확인하고 있다면 `build.sh`에 적용

## Run a single benchmark

```
./target/release/bench -f <poolpath> -a <target> -k <bench kind> -t <threads> -d <test-dur> -o <output>
```
- `<target>`: our_queue, durable_queue, log_queue, dss_queue, our_pipe, pmdk_pipe, crndm_pipe
- `<bench kind>`
    - `<target>`이 queue일 때: pair, prob50 (prob30, prob10과 같이 enq 확률 조정 가능)
    - `<target>`이 pipe일 때: pipe

example:
```bash
./target/release/bench -f /mnt/pmem0/our_queue.pool -a our_queue -k prob50 -t 16 -d 10 -o ./out/queue.csv
```
`우리 큐`에 `16`개 스레드로 `{ 50% enq or deq }`를 반복할 때의 처리율 측정
- 풀 파일: `/mnt/pmem0/our_queue.pool`을 새로 생성하여 사용
- 처리율 측정방법: `10`초동안 op을 반복 실행한 후 평균 op/s를 계산
- 결과: `./out/queue.csv`

TODO: cpp bench executable도 사용방법 설명

## Run the entire benchmark
```bash
run.sh <pmempath>
```
모든 (`<target>`, `<bench kind>`, `<threads=1~32>`) 쌍에 대하여 처리율 측정
- 풀 파일: `{pmempath}/{target}.pool`을 새로 생성하여 사용
- 처리율 측정방법: single bench를 10초씩 `5`번 반복한 후 평균 M op/s를 계산
- 결과:
    - raw: `./out/{obj}.csv` (obj: queue, pipe)
    - graph: `./out/{obj}-{bench kind}.png`
