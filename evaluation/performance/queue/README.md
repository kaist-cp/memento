# Performance Evaluation of Queue

We evaluate the performance of memento-based queues and other queues. Each implementation of comparison targets exists in [`./src/`](./src)

## Run the entire benchmark

```bash
./build.sh
./run.sh  # This may take more than 14 hours
```

This creates CSV data and plots under `./out/`.

## Run a single benchmark

You can run a single benchamrk,

```bash
./build.sh
./target/release/bench -f <filepath> -a <target> -k <kind> -t <threads> -i <init_nodes> -o <output>
```

where
- `target`: memento_queue (***MSQ-mmt-O2*** in the paper), memento_queue_lp (***MSQ-mmt-O1*** in the paper), memento_queue_general (***MSQ-mmt-O0*** in the paper), memento_queue_comb (***CombQ-mmt*** in the paper), durable_queue, log_queue, dss_queue, pbcomb_queue, crndm_queue
- `kind`: pair (enq-deq pair), prob{n} (n% probability enq or 100-n% deq)

For example, following command measure the throughput of `memento_queue` with `pair` workload, when using `16` threads.

```bash
./target/release/bench -f /mnt/pmem0/mmt.pool -a memento_queue -k pair -t 16 -i 0 -o ./out/mmt.csv
```

- This creates raw CSV data under `./out/mmt.csv`.
- To pinning NUMA node 0, you should attach `numactl --cpunodebind=0 --membind=0` at the front of the command.


For detailed usage information,

```
./target/release/bench -h
```

### Benchmarking PMDK and Clobber-NVM queue

To run a single benchmark for PMDK and Clobber-NVM queues, you should use separate executables with the following commands.

PMDK queue:

```bash
./build.sh
./target/release/bench_cpp <filepath> <target> <kind> <threads> <duration> <init_nodes> <output> # <target> should be "pmdk_queue"
```

Clobber-NVM queue:

```bash
./build.sh
PMEM_IS_PMEM_FORCE=1 ./src/clobber-nvm/apps/queue/benchmark-clobber -k <kind> -t <threads> -d 8 -s <duration> -i <init_nodes> -o <output>
```


