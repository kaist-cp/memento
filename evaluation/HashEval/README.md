
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
my_run.sh
```

- 실험 세팅: my_run.sh 보면 바꿀수있음
- 실험 결과: 터미널에 찍힘

TODO: 스크립트 input으로 비교군, 실험종류, 실험 파라미터 택할 수 있게 하기

##### To run the entire benchamrk,

```
run.sh
```

- 실험 세팅: VDSL의 실험 똑같이 전부
- 실험 결과: out/ 폴더에 저장

TODO?: pmempath 경로 받기 (e.g. `/mnt/pmem0/`)

