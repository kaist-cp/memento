# Queue Evaluation

We evaluate the performance of queues with our own benchmark. Each implementation of comparion targets exists in [`./src/`](./src)

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
./target/release/bench -f <filepath> -a <target> -k <kind> -t <threads> -o <output>
```

where
- `target`: memento_queue (MSQ-mmt-vol at paper), memento_queue_lp (MSQ-mmt-indel at paper), memento_queue_general (MSQ-mmt at paper), memento_queue_comb (CombQ-mmt at paper), durable_queue, log_queue, dss_queue, pbcomb_queue, crndm_queue
- `kind`: pair (enq-deq pair), prob{n} (n% probability enq or 100-n% deq)

For example, following command measure the throughput of `mmt` queue with `pair` workload, when using `16` threads.

```bash
./target/release/bench -f /mnt/pmem0/mmt.pool -a memento_queue -k pair -t 16 -o ./out/mmt.csv
```

- This creates raw CSV data under `./out/mmt.csv`.
- To pinning NUMA node 0, you should attach `numactl --cpunodebind=0 --membind=0` at the front of the command.


For detailed usage information,

```
./target/release/bench -h
```

### Benchmarking PMDK queue

We implement separate benchmark to evaluate PMDK queue since we could't import PMDK library to our Rust based benchmark. You can run a single benchmark for PMDK queue with following command.

```bash
./build.sh
./target/release/bench_cpp <filepath> <target> <kind> <threads> <duration> <init_nodes> <output>
```

Usage is same with above, but `target` only for "pmdk_queue"
