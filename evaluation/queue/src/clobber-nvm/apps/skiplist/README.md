To run skiplist on a specific library:

```
make clean
make benchmark-<LIB>
rm -rf /mnt/pmem0/*
PMEM_IS_PMEM_FORCE=1 ./benchmark-<LIB> -t <THREAD> -r -d <DATA-SIZE> -w a -f ../../traces
```
