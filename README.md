# Compositional Construction of Failure-Safe Persistent Objects

## Resource

- [Drive](https://drive.google.com/drive/u/1/folders/1DHXS67QvUaZGUWluOwqcFV-V7wh5YkBb)
- [Proposal](https://docs.google.com/document/d/1lEQc1tiZ5nVnXoYXy262q7kIYw_sRTN4NNbPMGMItO4/edit?usp=sharing)
- [Design](https://docs.google.com/document/d/147tqRFIaAN1PeYG6KBrdjk5esPZ0dtY-R9yzQVYxCXc/edit?usp=sharing)
- [PMEM setup guideline](./document/pmem_setup.md)

## Performance Evaluation
### Build
```bash
build.sh
```

### Run
```bash
# { /mnt/pmem0 }의 풀 파일로 { 5 }초씩 { 10 }번 테스트 진행. enq 확률은 { 50 }%
run.sh /mnt/pmem0 5 10 50
```
TODO: 현재는 enq-deq pair 테스트를 하려면 enq 확률을 65535로 넣어줘야함. 이렇게 말고 우아하게 구현하기