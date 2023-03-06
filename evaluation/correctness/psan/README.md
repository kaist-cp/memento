
TODO: 
- README, script 정리
- ext/pmdk-rs 추가?: pmemobj_direct가 rust crate pmemobj_sys의 API로 노출 돼야함.

# Build libmemento.a using PMCPass llvm instrumentation

```
./scripts/build_pmcpass.sh
./scripts/build_memento.sh
```

# Build executable file with PMCheck

```
./scripts/build_pmcheck.sh <mode>
./scripts/build_exe.sh
```

where mode: `yashme`, `psan`



# Run

```sh
./scripts/run.sh <target>
```

where target: `checkpoint`, `detectable_cas`, `queue_O0`, TODO

