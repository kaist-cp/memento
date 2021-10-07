# Compositional Construction of Failure-Safe Persistent Objects

## Resource

- [Drive](https://drive.google.com/drive/u/1/folders/1DHXS67QvUaZGUWluOwqcFV-V7wh5YkBb)
- [Proposal](https://docs.google.com/document/d/1lEQc1tiZ5nVnXoYXy262q7kIYw_sRTN4NNbPMGMItO4/edit?usp=sharing)
- [Design](https://docs.google.com/document/d/147tqRFIaAN1PeYG6KBrdjk5esPZ0dtY-R9yzQVYxCXc/edit?usp=sharing)

## Performance Evaluation

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
### Build

```bash
build.sh
```

### Run a single benchmark
```
./target/release/examples/bench <pmem-path> <target> <bench kind> <threads> <test-dur> <test-cnt>
```
- `<target>`: our_queue, durable_queue, log_queue, dss_queue, our_pipe, pmdk_pipe, crndm_pipe
- `<bench kind>`
    - `<target>`이 queue일 때: pair, prob50 (prob30, prob10과 같이 enq 확률 조정 가능)
    - `<target>`이 pipe일 때: pipe

example:
```bash
./target/release/examples/bench /mnt/pmem0 our_queue prob50 16 /mnt/pmem0 5 10
```
`우리 큐`에 `16`개 스레드로 `{ 50% enq or deq }`를 반복할 때의 처리율 측정
- 처리율 측정에는 `/mnt/pmem0`에 생성한 풀 파일을 사용하고, `5`초씩 `10`번 측정하여 평균을 냄
- 결과는 `./out/{실행시킨 시간}`에 저장

### Run the entire benchmark
```bash
run.sh /mnt/pmem0 5 10
```
모든 (`<target>`, `<bench kind>`, `<threads=1~32>`) 쌍에 대하여 처리율 측정
- 각 쌍의 처리율 측정에는 `/mnt/pmem0`에 생성한 풀 파일을 사용하고, `5`초씩 `10`번 측정하여 평균을 냄
- TODO: 결과 요약은 ~~ 여기에 저장
