# Performance Evaluation of List

We evaluate the performance of memento-based list compared to other detectable list. Each implementation of comparion targets exists in [`./src/`](./src). To evaluate the performance of detectable list based on `Tracking`, `Capsule`, `Casule-Opt`, we use the implementations published by [Detectable Recovery of Lock-Free Data Structures (PPoPP '22)](https://dl.acm.org/doi/pdf/10.1145/3503221.3508444) authors.

## Run the entire benchmark

```bash
./build.sh
./run.sh
```

This creates CSV data and plots under `./out/`.

## Run a single benchmark

### List-mmt

You can run a single benchamrk for list-mmt,

```bash
./build.sh
./target/release/bench -f <filepath> -a list-mmt  -t <threads> -k <key-range> --insert-ratio <insert-ratio> --delete-ratio <delete-ratio> --read-ratio <read-ratio> -o <outpath>
```

For example, following command measure the throughput of `list-mmt` with read-intensive workload, when using `16` threads and `500` key ranges.

```bash
./target/release/bench -f /mnt/pmem0/list-mmt.pool -a list-mmt -t 16 -k 500 --insert-ratio 0.15 --delete-ratio 0.15 --read-ratio 0.7 -o ./out/list-mmt.csv
```

- This creates raw CSV data under `./out/list-mmt.csv`.
- To pinning NUMA node 0, you should attach `numactl --cpunodebind=0 --membind=0` at the front of the command.


For detailed usage,

```
./target/release/bench -h
```

### Tracking, Capsules, Capsules-Opt

Please refer to https://github.com/ConcurrentDistributedLab/Tracking.
