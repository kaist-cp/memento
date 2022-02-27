# Queue Performance Evaluation

Memento로 만든 Queue의 성능을 기 제안된 Queue와 비교

### Summary

Assume you mount PMEM at `/mnt/pmem0/`

```bash
./build.sh
./run.sh
```

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

```bash
./target/release/bench -f <filepath> -a <target> -k <bench kind> -t <threads> -d <test-dur> -o <output>
```
- To pinning NUMA node 0, you should attach `numactl --cpunodebind=0 --membind=0` at the front of the command.
- `<target>`: `mmt`, `mmt_lp`, `durable_queue`, `log_queue`, `dss_queue`
- `<bench kind>`:
    - `<pair>`:  { enq; deq; }를 반복했을 때의 처리율
    - `<prob{n}>`: { n% 확률로 enq 혹은 deq }를 반복했을 때의 처리율



example:

`16`개 스레드로 `memento queue`에 `{enq; deq;}`를 `5초` 동안 반복 실행했을 때의 처리율(M op/s)을 측정하고 싶다면,

```bash
./target/release/bench -f /mnt/pmem0/mmt.pool -a mmt -k pair -t 16 -d 5 -o ./out/memento_queue.csv
```

This

- 결과: `./out/memento_queue.csv`
- 풀 파일: `/mnt/pmem0/memento_queue.pool`을 새로 생성하여 사용

##### To run the entire benchmark,

```bash
run.sh <pmempath>
```
모든 (`<target>`, `<bench kind>`, `<threads=1~32>`) 쌍에 대한 처리율 측정 (NUMA node 0에 pinning)
- 결과:
    - raw: `./out/{target}.csv`
    - graph: `./out/{obj}-{bench kind}.png` (obj: queue, pipe)
- 각 쌍의 처리율: single bench로 처리율 측정을 10번 반복한 후 평균 처리율 계산
- 풀 파일: 매 singe bench마다 `{pmempath}/{target}.pool`을 새로 생성하여 사용
