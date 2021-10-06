TODO(seungmin.jeon)
- implement DSS queue
- refactoring
    - testing enq-deq pair
    - readme
    - parsing argument: use clap crate or struct(?: ask chunmyung.park) crate?
    - ...

## Note
`/mnt/pmem0` 에서 작업하려면 sudo 권한 필요
```bash
sudo -i
```

## Performance Evaluation
Build
```bash
build.sh
```

Run
```bash
# /mnt/pmem0의 풀 파일로 5초씩 10번 테스트 진행. enq 확률은 50% 
run.sh /mnt/pmem0 5 10 50
```
