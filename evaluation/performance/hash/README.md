
# Performance Evaluation of Hash

We used the same benchmark as [Persistent Memory Hash Indexes: An Experimental Evaluation (VLDB '21)](http://vldb.org/pvldb/vol14/p785-chen.pdf) to evaluate our hash. Each implementation of comparison targets exists in [`./hash/`](./hash/)

## Run the entire benchmark

```bash
ulimit -s 8192000
./build.sh
./run.sh  # This may take about 30 hours
```

This creates raw txt that containing measuring result and plots under `./out/`.

## Run a single benchmark,

You can run a single benchamrk with PiBench executable,

```bash
ulimit -s 8192000
./build.sh
cd bin
./PiBench [lib.so] [args...]
```

where
- `[lib.so]`: `clevel.so`, `clevel_rust.so` (Clevel-mmt at paper)
- `[args...]`: please see [Persistent Memory Hash Indexes repo](https://github.com/HNUSystemsLab/HashEvaluation#run-with-pibench).

For example, following command measure the search throughput of `clevel_rust` when using 32 threads with uniform distribution.

```bash
./bin/PiBench ./bin/clevel_rust.so \
    -S 16777216 \       # initial capacity
    -p 200000000 \      # number of operations
    -r 1 -i 0 -d 0 \    # read 100%, insert 0%, delete 0%
    -M THROUGHPUT --distribution UNIFORM \
    -t 32 \
```



