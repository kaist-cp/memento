# Compositional Construction of Failure-Safe Persistent Objects

## Resource

- [Drive](https://drive.google.com/drive/u/1/folders/1DHXS67QvUaZGUWluOwqcFV-V7wh5YkBb)
- [Proposal](https://docs.google.com/document/d/1lEQc1tiZ5nVnXoYXy262q7kIYw_sRTN4NNbPMGMItO4/edit?usp=sharing)
- [Design](https://docs.google.com/document/d/147tqRFIaAN1PeYG6KBrdjk5esPZ0dtY-R9yzQVYxCXc/edit?usp=sharing)

## Performance Evaluation
### Build
```bash
build.sh
```

### Run
`/mnt/pmem0`에 생성한 풀 파일로 `5`초씩 `10`번 테스트 진행
```bash
run.sh /mnt/pmem0 5 10
```