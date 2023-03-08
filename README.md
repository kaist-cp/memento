# A General Framework for Detectable, Persistent, Lock-Free Data Structures

This is the artifact for the following paper:

> A General Framework for Detectable, Persistent, Lock-Free Data Structures.
>
> Anonymous Author(s).
>
> PLDI 2023 (conditionally accepted, paper #90).


## Goal

This artifact aims to achieve the following goals:

- G1: Locating our framework's core concepts (§TODO) in the development
- G2: Preparing the evaluation (§6)
- G3: Reproducing the detectability evaluation (§6.1)
- G4: Reproducing the performance evaluation (§6.2)


## G1: Locating our framework's core concepts (§TODO) in the development

TODO: maybe an introduction?

### Persistent Memory (PM) Infrastructure (§4.1)

- `src/pmem/ll.rs`: Low-level instructions for **PM Access** (§4.1)
- `src/pmem/pool.rs`: A library that creates an environment (i.e., PM pool) and runs a memento-based program (**Crash Handler** described in §4.1)

### Primitive Operations (§4, §B)

- `src/ploc/common.rs`: Timestamp calibration (§4.1) and Checkpoint (§4.2)
- `src/ploc/detectable_cas.rs`: Atomic Pointer Location supporting Detectable CAS (§4.3)
- `src/ploc/insert_delete.rs`: Insertion and Deletion (§B in Appendix)

### Concurrent Data Structures (§5)

- `src/ds/comb.rs`: A memento-based detectable combining operation. We convert the original PBComb to one using mementos to support multi-time detectability. (**Comb-mmt** in the paper)
- `src/ds/list.rs`: A memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris' ordered linked list. (**List-mmt** in the paper)
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber's stack. (**TreiberS-mmt** in the paper)
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O0** in the paper)
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits data structure-specific invariant for issuing less flushes when loading shared pointer. (**MSQ-mmt-O1** in the paper)
- `src/ds/queue_comb.rs`: A memento-based combining queue that uses `Combining` operation. (**CombQ-mmt** in the paper)
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (**Clevel-mmt** in the paper)
- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O2** in the paper)

#### Safe Memory Reclamation (§D)

The directory `crossbeam-persistency/` contains implementations of safe memory reclamation. Specifically,

- `crossbeam-persistency/crossbeam-epoch/src/guard.rs`: "Flushing Location before Retirement"
- `crossbeam-persistency/crossbeam-epoch/src/internal.rs`: "Allowing Double Retirement"


## G2: Preparing the evaluation (§6)

### Requirements

- Ubuntu 20.04 or later
- Intel® Optane™ Persistent Memory 100 Series (mounted at `/mnt/pmem0`).
  + In case that a persistent memory is not mounted, you can still perform the evaluation on DRAM.


### Option 1: Running on Docker

```sh
docker build -t memento .
docker run -it -v /mnt/pmem0:/mnt/pmem0 --cap-add=SYS_NICE memento # persistent memory must be mounted at /mnt/pmem0
```

### Option 2: Running on host

#### Dependencies

- [Rust](https://www.rust-lang.org/)

  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

- Additional dependencies for evaluation:

  ```sh
  apt install build-essential python3-pip numactl \
    libpmemobj-dev libvmem-dev libgflags-dev \
    libpmemobj1 libpmemobj-cpp-dev \
    libatomic1 libnuma1 libvmmalloc1 libvmem1 libpmem1
  pip3 install --user pandas matplotlib gitpython
  ```

#### Build

```sh
cargo build --release
```

If persistent memory is *not* mounted on your machine, add a feature flag with `no_persist` as follows:

```sh
cargo build --release --features no_persist
```


## G3: Reproducing the detectability evaluation (§6.1)

TODO: move `evaluation/README.md` to here.

You can run *all* evaluation mentioned in the paper (§6).
See the `README.md` in the [evaluation](./evaluation) directory.

## G4: Reproducing the performance evaluation (§6.2)

TODO
