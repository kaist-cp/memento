# A General Framework for Detectable, Persistent, Lock-Free Data Structures


This is the artifact for the following paper:

> A General Framework for Detectable, Persistent, Lock-Free Data Structures.
>
> Anonymous Author(s).
>
> PLDI 2023 (conditionally accepted, paper #90).


## Contributions (paper §1)

- In §2, we describe how to design programs that are deterministically replayed after a crash. We
do so using two primitive operations, detectably recoverable checkpoint and CAS, by composing
them with usual control constructs such as sequential composition, conditionals, and loops.
- In §3, we design a core language for persistent programming and its associated type system for
deterministic replay, and prove that well-typed programs are detectably recoverable.
- In §4, we present an implementation of our core language in the Intel-x86 Optane DCPMM
architecture. Our construction is not tightly coupled with Intel-x86, and we believe that our
implementation can be straightforwardly adapted to other PM architectures.
- In §5, we adapt several volatile, lock-free DSs to satisfy our type system, automatically deriving
detectable, persistent lock-free DSs. These include a detectable, persistent linked-list [Harris
2001], Treiber stack [Treiber 1986], Michael-Scott queue [Michael and Scott 1996], a combining
queue, and Clevel hash table [Chen et al. 2020]. In doing so, we capture the optimizations of
hand-tuned persistent lock-free DSs with additional primitives and type derivation rules (§B
and §C), and support safe memory reclamation even in the presence of crashes (§D).
- In §6, we evaluate the detectability and performance of our CAS and automatically derived
persistent DSs. They recover from random thread crashes in stress tests (§6.1); and perform
comparably with the existing persistent DSs with and without detectability (§6.2).


## Artifacts

- Implementation of the Memento framework and its primitives (§4 : `memento/`)
- Implementation of several detectably persistent data structures based on Memento (§5 : `memento/`)
- Evaluation programs (correctness and performance) (§7 : `memento/`)
- Full result data of benchmark (§7 : `evaluation_data/`)
- Appendix including full algorithm of CAS (§A), insert/delete operations (§B), advanced optimizations (§C), safe memory reclamation (§D), full evaluation results (§E), full core langue syntax, semantics and type system (§F, §G) and proof of detectability theorem (§H) (`appendix.pdf`)


## Getting Started Guide

You can either reuse a pre-built docker image `memento-image.tar` or manually build the framework.

### Requirements

- Ubuntu 20.04 or later
- Intel® Optane™ Persistent Memory 100 Series (mounted at `/mnt/pmem0`).
  + In case that a persistent memory is not mounted, you can still perform the *limited* evaluation on DRAM.

### Option 1: Running on Docker (Loading Docker Image)

You can reuse a pre-built docker image by loading `memento-image.tar.gz`:

```sh
docker load < memento-image.tar.gz
docker run -it -v /mnt/pmem0:/mnt/pmem0 --cap-add=SYS_NICE memento  # persistent memory must be mounted at /mnt/pmem0
```

Here, `-v /mnt/pmem0:/mnt/pmem0` option is required to share the mounted persistent memory area with the container. Also, `--cap-add=SYS_NICE` option is needed to evalute performance by unifying all used cores into a single numa node.

You can re-build a docker image by `docker build -t memento memento/`. (It may take more than 30 minutes.)

### Option 2: Running on host

#### Dependencies

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
git submodule update --init --recursive
cd ext/pmdk-rs; git apply ../pmdk-rs.patch 
cargo build --release
```

If persistent memory is *not* mounted on your machine, add a feature flag with `no_persist` as follows:
```sh
cargo build --release --features no_persist
```


## Step-by-Step Instructions

### Goal

This artifact aims to achieve the following goals:

- G1: Locating our framework's core concepts (§4,5,B,D) in the development
- G2: Reproducing the detectability evaluation (§6.1)
- G3: Reproducing the performance evaluation (§6.2)

### G1: Locating our framework's core concepts (§4,5,B,D) in the development

- `src/ploc/`: persistent memory (PM) infrastructure and primitive operations (§4, §B)
- `src/ds/`: memento-based persistent, detectable data structures supporting exactly-once semantics (§5)
- `crossbeam-persistency/`: safe memory reclamation scheme (§D)

#### PM Infrastructure (§4.1)

- `src/pmem/ll.rs`: Low-level PM instructions (§4.1)
- `src/pmem/pool.rs`: PM pool manager and **crash handler** (§4.1)

#### Primitive Operations (§4, §B)

- `src/ploc/common.rs`: Timestamp calibration (§4.1) and Checkpoint (§4.2)
- `src/ploc/detectable_cas.rs`: Atomic Pointer Location supporting Detectable CAS (§4.3)
- `src/ploc/insert_delete.rs`: Insertion and Deletion (§B in Appendix)

#### Concurrent Data Structures (§5)

- `src/ds/comb.rs`: A memento-based detectable combining operation. We convert the original PBComb to one using mementos to support multi-time detectability. (**Comb-mmt**)
- `src/ds/list.rs`: A memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris' ordered linked list. (**List-mmt**)
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber's stack. (**TreiberS-mmt**)
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O0**)
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits data structure-specific invariant for issuing less flushes when loading shared pointer. (**MSQ-mmt-O1**)
- `src/ds/queue_comb.rs`: A memento-based combining queue that uses `Combining` operation. (**CombQ-mmt**)
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (**Clevel-mmt**)
- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (**MSQ-mmt-O2**)

#### Safe Memory Reclamation (§D)

- `crossbeam-persistency/crossbeam-epoch/src/guard.rs`: "Flushing Location before Retirement"
- `crossbeam-persistency/crossbeam-epoch/src/internal.rs`: "Allowing Double Retirement"


### G2: Reproducing the detectability evaluation (§6.1)

See the README below:

- [Detecability evaluation](./evaluation/correctness/README.md)

### G3: Reproducing the performance evaluation (§6.2)

For each DS evaluation, see the corresponding README below:

- [Detectable CAS](./evaluation/performance/cas/README.md)
- [Detectable List](./evaluation/performance/list/README.md)
- [Detectable Queue](./evaluation/performance/queue/README.md)
- [Detectable Hash Table](./evaluation/performance/hash/README.md)
