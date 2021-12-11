## Build

```
make -j
```

## Test

- Full test: 현재는 clevel-rust만 테스트하게 되어있음. Full test 돌리려면 run.sh에서 비교군 테스트 주석 해제해야함
- NUMA: 현재는 NUMA pinning 안하게 세팅되어있음. 하나에 pinning하려면 스크립트 안의 주석처럼 실행 커맨드 앞에 `numactl --cpunodebind=0 --membind=0`를 붙여야함

Test single

- 실험 세팅: my_run.sh 보면 바꿀수있음
- 실험 결과: 터미널에 찍힘

```
my_run.sh
```

Test overall

- 실험 세팅: VDSL의 실험 똑같이 전부
- 실험 결과: out/ 폴더에 저장

```
run.sh
```
