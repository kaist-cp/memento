# Memento: A Framework for Detectable Recoverability in Persistent Memory


This is the artifact for the following [paper](https://drive.google.com/file/d/1LaXKo1w3fHF2-ra7EA5d24ucCx1kRE3Q):

Kyeongmin Cho, Seungmin Jeon, Azalea Raad, Jeehoon Kang.
*Memento: A Framework for Detectable Recoverability in Persistent Memory.* **PLDI 2023.**

## Contributions (paper §1)

- In §2, we describe how to design programs that are deterministically replayed after a crash. We
do so using two primitive operations, detectably recoverable checkpoint and CAS, by composing
them with usual control constructs such as sequential composition, conditionals, and loops.
- In §3, we design a core language for persistent programming and its associated type system for
deterministic replay, and prove that well-typed programs are detectably recoverable.
- In §4, we present an implementation of our core language in the Intel-x86 Optane DCPMM
architecture. Our construction is not tightly coupled with Intel-x86, and we believe that our
implementation can be straightforwardly adapted to other PM architectures.
- In §5, we adapt several volatile, lock-free data structures (DSs) to satisfy our type system, automatically deriving
detectable, persistent lock-free DSs. These include a detectable, persistent linked-list [Harris
2001](https://timharris.uk/papers/2001-disc.pdf), Treiber stack [Treiber 1986](https://dominoweb.draco.res.ibm.com/58319a2ed2b1078985257003004617ef.html), Michael-Scott queue [Michael and Scott 1996](https://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf), a combining
queue, and Clevel hash table [Chen et al. 2020](https://www.usenix.org/conference/atc20/presentation/chen). In doing so, we capture the optimizations of
hand-tuned persistent lock-free DSs with additional primitives and type derivation rules (§B
and §C), and support safe memory reclamation even in the presence of crashes (§D).
- In §6, we evaluate the detectability and performance of our CAS and automatically derived
persistent DSs. They recover from random thread crashes in stress tests (§6.1); and perform
comparably with the existing persistent DSs with and without detectability (§6.2).


## Artifacts

- Implementation of the Memento framework and its primitives (§4 : [src/pmem/](src/pmem/) and [src/ploc/](src/ploc/))
- Implementation of several detectably persistent DSs based on Memento (§5 : [src/ds/](src/ds/))
- Evaluation programs (correctness and performance) (§7 : [evaluation/](evaluation/))
- Full result data of benchmark (§7 : `evaluation_data/` in [Zenodo](https://doi.org/10.5281/zenodo.7811928))


## Getting Started Guide

You can either reuse a pre-built docker image `memento-image.tar` from our [Zenodo](https://doi.org/10.5281/zenodo.7811928) archive or manually build the framework.

### Requirements

- Ubuntu 20.04 or later
- Intel® Optane™ Persistent Memory 100 Series (mounted at `/mnt/pmem0`).
  + In case that a persistent memory is not mounted, you can still perform a *limited* evaluation on DRAM.

### Option 1: Running on Docker (Loading Docker Image)

You can reuse a pre-built docker image by loading `memento-image.tar`:

```sh
docker load -i memento-image.tar
docker run -it -v /mnt/pmem0:/mnt/pmem0 --cap-add=SYS_NICE memento  # Assuming persistent memory is mounted at /mnt/pmem0
```

Here, `-v /mnt/pmem0:/mnt/pmem0` option is *conditionally* required to share the mounted persistent memory area with the container for the *full* evaluation. Also, `--cap-add=SYS_NICE` option is needed to evalute performance by unifying all used cores into a single numa node.

You can re-build a docker image by `docker build -t memento .`. (It may take more than 30 minutes.)

### Option 2: Running on host

#### Dependencies

- [Rust](https://www.rust-lang.org/)

  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

- Additional dependencies for evaluation:

  ```sh
  apt install build-essential clang python3-pip numactl \
    libpmemobj-dev libvmem-dev libgflags-dev \
    libpmemobj1 libpmemobj-cpp-dev \
    libatomic1 libnuma1 libvmmalloc1 libvmem1 libpmem1
  pip3 install --user pandas matplotlib gitpython
  ```

#### Build

To build our framework including detectable operations, DSs and SMR libraries:
```sh
git submodule update --init --recursive
(cd ext/pmdk-rs; git apply ../pmdk-rs.patch)
cargo build --release
```

If persistent memory is *not* mounted on your machine, add a feature flag with `no_persist` as follows:
```sh
cargo build --release --features no_persist
```


## Step-by-Step Instructions

This artifact aims to achieve the following goals:

- G1: Locating our framework's core concepts (§4,5,B,D) in the development
- G2: Reproducing the detectability evaluation (§6.1)
- G3: Reproducing the performance evaluation (§6.2)

### G1: Locating our framework's core concepts (§4,5,B,D) in the development

- [src/ploc/](src/ploc/): persistent memory (PM) infrastructure and primitive operations (§4, §B)
- [src/ds/](src/ds/): Memento-based persistent, detectable DSs supporting exactly-once semantics (§5)
- [crossbeam-persistency/](crossbeam-persistency/): safe memory reclamation scheme (§D)

#### PM Infrastructure (§4.1)

- [src/pmem/ll.rs](src/pmem/ll.rs): Low-level PM instructions (§4.1)
- [src/pmem/pool.rs](src/pmem/pool.rs): PM pool manager and **crash handler** (§4.1)

#### Primitive Operations (§4, §B)

- [src/ploc/common.rs](src/ploc/common.rs): Timestamp calibration (§4.1) and Checkpoint (§4.2)
- [src/ploc/detectable_cas.rs](src/ploc/detectable_cas.rs): Atomic Pointer Location supporting Detectable CAS (§4.3)
- [src/ploc/insert_delete.rs](src/ploc/insert_delete.rs): Insertion and Deletion (§B in Appendix)

#### Concurrent Data Structures (§5)

- [src/ds/comb.rs](src/ds/comb.rs): A Memento-based detectable combining operation. We convert the original PBComb to one using mementos to support multi-time detectability. (**Comb-mmt**)
- [src/ds/list.rs](src/ds/list.rs): A Memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris' ordered linked list. (**List-mmt**)
- [src/ds/treiber_stack.rs](src/ds/treiber_stack.rs): A Memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber's stack. (**TreiberS-mmt**)
- [src/ds/queue_general.rs](src/ds/queue_general.rs): A Memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O0**)
- [src/ds/queue_lp.rs](src/ds/queue_lp.rs): A Memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits DS-specific invariant for issuing less flushes when loading shared pointer. (**MSQ-mmt-O1**)
- [src/ds/queue_comb.rs](src/ds/queue_comb.rs): A Memento-based combining queue that uses `Combining` operation. (**CombQ-mmt**)
- [src/ds/clevel.rs](src/ds/clevel.rs): A Memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (**Clevel-mmt**)
- [src/ds/queue.rs](src/ds/queue.rs): A Memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O2**)

#### Safe Memory Reclamation (§D)

- [crossbeam-persistency/crossbeam-epoch/src/guard.rs](crossbeam-persistency/crossbeam-epoch/src/guard.rs): "Flushing Location before Retirement"
- [crossbeam-persistency/crossbeam-epoch/src/internal.rs](crossbeam-persistency/crossbeam-epoch/src/internal.rs): "Allowing Double Retirement"


### G2: Reproducing the detectability evaluation (§6.1)

#### Thread Crash Test

We evaluate the detectability in case of thread crashes by randomly crashing an arbitrary thread while running the integration test. To crash a specific thread, we use the tgkill system call to send the SIGUSR1 signal to the thread and let its signal handler abort its execution.

##### Install

```bash
cd evaluation/correctness/tcrash
./build.sh # specially build for the thread crash test
```

##### Run

You can test each DS with the following command:

```bash
./run.sh [tested DS]
```

where `tested DS` should be replaced with one of supported tests (listed below).
For example, the following command is to infinitely check that the test of ***MSQ-mmt-O0*** in the paper always pass in case of an unexpected thread crash:

```bash
./run.sh queue_general
```

Then the output is printed out like below:

```
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 1 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 1] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 2 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 2] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 3 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
[Test 3] success
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 4 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
^C
```

It also creates a short progress log and a full test log under `./out`.

If a bug exists (just for an example), the output is like below:

```
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 1 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
./run.sh: line 51: 855011 Aborted                 RUST_BACKTRACE=1 RUST_MIN_STACK=2000000000 numactl --cpunodebind=0 --membind=0 timeout $TIMEOUT $SCRIPT_DIR/../../target/x86_64-unknown-linux-gnu/release/deps/memento-* $target::test --nocapture &>> $log_tmp
fails with exit code 134
[Test 1] fails with exit code 134
clear queue_general
⎾⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺ thread crash-recovery test queue_general 2 (retry: 0) ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⏋
run queue_general
^C
```

It then generates a bug directory consisting of a text file containg specific error log (`info.txt`) and a PM pool files (`queue_general.pool_*`) of the buggy execution so that we can debug the DS using it.

For each primitive and DS, we observe *no* test failures for 1M runs with thread crashes.

##### Supported tests

###### For primitives

- `checkpoint`
- `detectable_cas`

###### For data structures

- `queue_general`: ***MSQ-mmt-O0*** (in the paper)
- `queue_lp`: ***MSQ-mmt-O1***
- `queue`: ***MSQ-mmt-O2***
- `queue_comb` ***CombQ-mmt***
- `treiber_stack`: ***TreiberS-mmt***
- `list`: ***List-mmt***
- `clevel`: ***Clevel-mmt***


#### Persistency Bug Finding Test (Yashme/PSan)

We evaluate the correctness of our primitives and DSs using existing bug finding tools, [Yashme](https://plrg.ics.uci.edu/yashme/) and [PSan](https://plrg.ics.uci.edu/psan/). They are finding persistent bugs such as persistency race, missing flushes based on model checking framework [Jaaru](https://plrg.ics.uci.edu/jaaru/).

##### Install

```bash
cd evaluation/correctness/pmcheck
./scripts/build_pmcpass.sh # may take more than 10 minutes to build LLVM
./build.sh
```

##### Run

You can test each DS with the following command:

```bash
./run.sh [tested DS] [tool] [mode]
```

where
- `tested DS` should be replaced with one of supported tests (listed below).
- `tool`: `yashme` or `psan`
- `mode`: `random` or `model` (random testing mode or model checking mode, respectively)

For example, the following command is to test the **MSQ-mmt-O0** using **PSan** with random mode:

```bash
./run.sh queue_O0 psan random
```

Then the output is printed out like below:

```
Jaaru
Copyright (c) 2021 Regents of the University of California. All rights reserved.
Written by Hamed Gorjiara, Brian Demsky, Peizhao Ou, Brian Norris, and Weiyu Luo

Execution 1 at sequence number 198
nextCrashPoint = 83987	max execution seqeuence number: 88289
nextCrashPoint = 2876	max execution seqeuence number: 4161
Execution 2 at sequence number 4161
nextCrashPoint = 1106	max execution seqeuence number: 4171
nextCrashPoint = 1583	max execution seqeuence number: 4181
Execution 3 at sequence number 4181
nextCrashPoint = 3756	max execution seqeuence number: 4166
nextCrashPoint = 31	max execution seqeuence number: 4176
Execution 4 at sequence number 4176
nextCrashPoint = 2400	max execution seqeuence number: 4181

...

******* Model-checking complete: *******
Number of complete, bug-free executions: 10
Number of buggy executions: 0
Total executions: 10
```

For each primitive and DS, we observe *no* buggy executions for 1K runs with random mode.

##### Supported tests

###### For primitives

- `checkpoint`
- `detectable_cas`

###### For data structures

- `queue_O0`: ***MSQ-mmt-O0*** (in the paper)
- `queue_O1`: ***MSQ-mmt-O1***
- `queue_O2`: ***MSQ-mmt-O2***
- `queue_comb` ***CombQ-mmt***
- `treiber_stack`: ***TreiberS-mmt***
- `list`: ***List-mmt***
- `clevel`: ***Clevel-mmt***



### G3: Reproducing the performance evaluation (§6.2)

#### Performance Evaluation of CAS

We evaluate the performance of CASes with our benchmark. Each implementation of comparison targets exists in [evaluation/performance/cas/src/](evaluation/performance/cas/src/).

##### Install

```bash
cd evaluation/performance/cas
./build.sh
```

##### Run the entire benchmark

```bash
./run.sh  # This may take about 3 hours
```

This creates CSV data and plots under `./out/`.

##### Run a single benchmark

You can run a single benchamrk,

```bash
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

```sh
./target/release/cas_bench -h
```

#### Performance Evaluation of List

We evaluate the performance of Memento-based list compared to other detectable list. Each implementation of comparison targets exists in [evaluation/performance/list/src/](evaluation/performance/list/src/). To evaluate the performance of detectable list based on `Tracking`, `Capsule`, `Casule-Opt`, we use the implementations published by [Detectable Recovery of Lock-Free Data Structures (PPoPP '22)](https://dl.acm.org/doi/pdf/10.1145/3503221.3508444) authors.

##### Install

```bash
cd evaluation/performance/list
./build.sh
```

##### Run the entire benchmark

```bash
./run.sh  # This may take about 7 hours
```

This creates CSV data and plots under `./out/`.

##### Run a single benchmark

###### List-mmt

You can run a single benchamrk for list-mmt,

```bash
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

###### Tracking, Capsules, Capsules-Opt

We refer to https://github.com/ConcurrentDistributedLab/Tracking.




#### Performance Evaluation of Queue

We evaluate the performance of Memento-based queues and other queues. Each implementation of comparison targets exists in [evaluation/performance/queue/src/](evaluation/performance/queue/src/).


##### Install

```bash
cd evaluation/performance/queue
./build.sh
```

##### Run the entire benchmark

```bash
./run.sh  # This may take more than 14 hours
```

This creates CSV data and plots under `./out/`.

##### Run a single benchmark

You can run a single benchamrk,

```bash
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

###### Benchmarking PMDK and Clobber-NVM queue

To run a single benchmark for PMDK and Clobber-NVM queues, you should use separate executables with the following commands.

PMDK queue:

```bash
./target/release/bench_cpp <filepath> <target> <kind> <threads> <duration> <init_nodes> <output> # <target> should be "pmdk_queue"
```

Clobber-NVM queue:

```bash
PMEM_IS_PMEM_FORCE=1 ./src/clobber-nvm/apps/queue/benchmark-clobber -k <kind> -t <threads> -d 8 -s <duration> -i <init_nodes> -o <output>
```


#### Performance Evaluation of Hash

We used the same benchmark as [Persistent Memory Hash Indexes: An Experimental Evaluation (VLDB '21)](http://vldb.org/pvldb/vol14/p785-chen.pdf) to evaluate our hash. Each implementation of comparison targets exists in [evaluation/performance/hash/hash/](evaluation/performance/hash/hash/).

##### Install

```bash
ulimit -s 8192000
cd evaluation/performance/hash
./build.sh
```

##### Run the entire benchmark

```bash
./run.sh  # This may take about 30 hours
```

This creates raw txt that containing measuring result and plots under `./out/`.

##### Run a single benchmark

You can run a single benchamrk with PiBench executable,

```bash
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

#### Use PMDK allocator

You can evaluate `clevel_rust` on top of the PMDK allocator (instead of Ralloc) by appending `pmdk` to the build command.

For example:

```bash
./build.sh pmdk # This builds clevel_rust on the top of PMDK allocator
```


