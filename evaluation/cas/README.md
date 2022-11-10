# Performance Evaluation of CAS

We evaluate the performance of CASes with our benchmark. Each implementation of comparion targets exists in [`./src/`](./src)

## Run the entire benchmark

```bash
./build.sh
./run.sh
```

This creates CSV data and plots under `./out/`.

## Run a single benchmark

You can run a single benchamrk,

```bash
./build.sh
./target/release/cas_bench -f <filepath> -a <target> -c <locations> -t <threads> -o <output>
```

where
- `target`: mcas (CAS-mmt at paper), pmwcas, nrlcas
- `locations`: number of locations

For example, following command measure the throughput and memory usage of `mcas` when using `1000` locations and `16` threads.

```bash
./target/release/cas_bench -f /mnt/pmem0/mcas.pool -a mcas -c 1000 -t 16 -o ./out/cas-mmt.csv
```

- This creates raw CSV data under `./out/cas-mmt.csv`.
- To pinning NUMA node 0, you should attach `numactl --cpunodebind=0 --membind=0` at the front of the command.

For detailed usage information,

```
./target/release/cas_bench -h
```
