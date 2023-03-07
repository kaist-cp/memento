# A General Framework for Detectable, Persistent, Lock-Free Data Structures

This is the artifact for a paper, Anonymous, "A General Framework for Detectable, Persistent, Lock-Free Data Structures", PLDI 2023 (conditionally accepted). This artifact provides the Memento framework and evaluation programs/results mentioned in the paper.

## Installation

We assume you use **Ubuntu 20.04** or later.

### Running on Docker

```sh
docker build -t memento .
docker run -it -v /mnt/pmem0:/mnt/pmem0 --cap-add=SYS_NICE memento # peristent memory must be mounted at /mnt/pmem0
```

### Running on host

#### Requirements

- [Rust](https://www.rust-lang.org/)
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- For the evaluation purpose, some dependencies are additionally required:
  ```sh
  apt install build-essential python3-pip numactl \
    libpmemobj-dev libvmem-dev libgflags-dev \
    libpmemobj1 libpmemobj-cpp-dev \
    libatomic1 libnuma1 libvmmalloc1 libvmem1 libpmem1
  pip3 install --user pandas matplotlib gitpython
  ```

#### Build

To build our framework including detectable operations, data structures and SMR libraries:
```sh
cargo build --release
```

If persistent memory is *not* mounted on your machine, add a feature flag with `no_persist` as follows:
```sh
cargo build --release --features no_persist
```

## Evaluation

You can run *all* evaluation mentioned in the paper(§6).
See the `README.md` in the [evaluation](./evaluation) directory.

### Our Implementations

#### Primitive Operations

The directory `src/ploc/` contains memento-based detectable operations.

- `src/ploc/common.rs`: Implementation of timestamp calibration (corresponding to §4.1) and Checkpoint (corresponding to §4.2)
- `src/ploc/detectable_cas.rs`: Implementation of Atomic Pointer Location supporting Detectable CAS (corresponding to §4.3)
- `src/ploc/insert_delete.rs`: Implementation of Insertion and Deletion (corresponding to §B in Appendix)

#### Concurrent Data Structures (§5)

The directory `src/ds/` contains memento-based persistent data structures supporting exactly-once semantics using detectable operations.

- `src/ds/comb.rs`: A memento-based detectable combining operation. We convert original PBComb to one using mementos to support multi-time detectability. (***Comb-mmt*** in the paper)
- `src/ds/list.rs`: A memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris' ordered linked list. (***List-mmt*** in the paper)
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber stack. (***TreiberS-mmt*** in the paper)
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (***MSQ-mmt-O0*** in the paper)
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits DS-specific invariant for issuing less flushes when loading shared pointer. (***MSQ-mmt-O1*** in the paper)
- `src/ds/queue_comb.rs`: A memento-based combining queue that uses `Combining` operation. (***CombQ-mmt*** in the paper)
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (***Clevel-mmt*** in the paper)
- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (***MSQ-mmt-O2*** in the paper)

#### Safe Memory Reclamation (§D in appendix)

The directory `crossbeam-persistency/` contains implementations of safe memory reclamations. Specifically,

- `crossbeam-persistency/crossbeam-epoch/src/guard.rs`: Implementation of "Flushing Location before Retirement"
- `crossbeam-persistency/crossbeam-epoch/src/internal.rs`: Implementation of "Allowing Double Retirement"

#### Others (§4.1)

- `src/pmem/ll.rs`: Low-level instructions for ***PM Access*** (corresponding to §4.1)
- `src/pmem/pool.rs`: A library that creates an environment (i.e. PM pool) and runs a memento-based program. (corresponding to ***Crash Handler*** described in §4.1)
