# Memento Evaluation

Memento로 만든 Queue, Pipe, Hash table의 성능을 기 제안된 자료구조와 비교

- **TODO**: Queue, Hash는 확정. Pipe, Stack, Lock은 할건가?
- **TODO**: Pipe 한다면 (1) Corundum pipe 컴파일, (2) PMDK pipe를 위한 c++ bench binary 재확인

## Queue, Pipe

### 비교방법

- queue
    - 비교군: Memento queue, Memento pipe-queue, Friedman's durable queue, Friedman's log queue, DSS queue
    - 실험종류
        1. pair: 각 스레드가 { enq; deq; }를 반복했을 때의 처리율 비교
        2. prob50: 각 스레드가 { 50% 확률로 enq 혹은 deq }를 반복했을 때의 처리율 비교
- pipe
    - 비교군: Memento pipe, PMDK pipe, Corundum pipe
    - 실험종류
        1. pipe: 각 스레드가 서로 다른 queue A, B에 대해 { A.deq; B.enq; B.deq; A.enq }를 반복했을 때의 처리율 비교

### Build

```bash
build.sh
```

persist instruction (e.g. `clflush`) 없이 돌려보고 싶다면,
```
build.sh no_persist
```

TODO: 현재 PMDK만 no_persist 구현 안됨. PMDK도 no_persist 같은 옵션 있는지 확인하고 있다면 `build.sh`에 적용

### Run

##### To run a single benchmark,

```
./target/release/bench -f <poolpath> -a <target> -k <bench kind> -t <threads> -d <test-dur> -o <output>
```
- `<target>`:
    - queue: `memento_queue`, `memento_pipe_queue`, `durable_queue`, `log_queue`, `dss_queue`
    - pipe: `memento_pipe`, `pmdk_pipe`, `crndm_pipe`
- `<bench kind>`
    - `<target>`이 queue일 때: `pair`, `prob50` (`prob30`, `prob10`과 같이 enq 확률 조정 가능)
    - `<target>`이 pipe일 때: `pipe`

example:
```bash
./target/release/bench -f /mnt/pmem0/memento_queue.pool -a memento_queue -k pair -t 16 -d 5 -o ./out/queue.csv
```
`16`개 스레드로 `memento queue`에 `{enq; deq;}`를 `5초` 동안 반복 실행했을 때의 처리율(M op/s) 측정
- 결과: `./out/queue.csv`
- 풀 파일: `/mnt/pmem0/memento_queue.pool`을 새로 생성하여 사용

##### To run the entire benchmark,

```bash
run.sh <pmempath>
```
모든 (`<target>`, `<bench kind>`, `<threads=1~32>`) 쌍에 대하여, single bench를 5초씩 10번 반복하여 평균 처리율 계산
- 결과:
    - raw: `./out/{obj}.csv` (obj: queue, pipe)
    - graph: `./out/{obj}-{bench kind}.png`
- 풀 파일: 매 single bench 마다 `{pmempath}/{target}.pool`을 새로 생성하여 사용

## Hash

See the documentation [here](./HashEval)
