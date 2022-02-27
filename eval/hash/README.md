
# Hash Evaluation

We used the same benchmark and comparison target as [Persistent Memory Hash Indexes: An Experimental Evaluation](http://vldb.org/pvldb/vol14/p785-chen.pdf) to evaluate our hash. Each implementation of comparion targets exists in [`./hash/`](./hash/)

## Run the entire benchmark


```
ulimit -s 8192000
./build.sh
./run.sh
```

This creates raw txt that containing measuring result and plots under `./out/`.

## Run a single benchmark,

You can run a single benchamrk with PiBench executable likes following command.

```
ulimit -s 8192000
./build.sh
cd bin
./PiBench [lib.so] [args...]
```

where
- `[lib.so]`: `CCEH(.so)`, `Level`, `Dash`, `PCLHT`, `clevel`, `clevel_rust`, `SOFT`, `SOFT_rust`
- `[args...]`: please see [Persistent Memory Hash Indexes repo](https://github.com/HNUSystemsLab/HashEvaluation#run-with-pibench).

For example, following command measure the search throughput of `clevel_mmt` when using 32 threads with uniform distribution.

```bash
./bin/PiBench ./bin/clevel_mmt.so \
    -S 16777216 \       # initial capacity
    -p 200000000 \      # number of operations
    -r 1 -i 0 -d 0 \    # read 100%, insert 0%, delete 0%
    -M THROUGHPUT --distribution UNIFORM \
    -t 32 \
```



