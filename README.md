# Supplementary Materials of "A General Framework for Detectable, Persistent, Lock-Free Data Structures"

## Installation

We assume you use **Ubuntu 20.04**.

### Requirements

- [Rust](https://www.rust-lang.org/)
  ```
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

### Build

- To build our framework including detectable operations, data structures and SMR libraries:
  ```
  cargo build --release
  ```

### Our Implementations

#### Detectable Operations for Location

The directory `src/ploc/` contains memento-based detectable operations.

- `src/ploc/common.rs`: Implementation of Checkpoint (corresponding to section 4.2) and timestamp calibration (corresponding to section 4.1)
- `src/ploc/detectable_cas.rs`: Implementation of Atomic Pointer Location supporting Detectable CAS (corresponding to section 4.3)
- `src/ploc/insert_delete.rs`: Implementation of Insertion and Deletion (corresponding to section B in Appendix)

#### Data Structures

The directory `src/ds/` contains memento-based persistent data structures supporting exactly-once semantics using detectable operations.

- `src/ds/comb.rs`: A memento-based detectable combining operation. We convert original PBComb to one using mementos to support multi-time detectability. (`Comb-mmt` at the paper)
- `src/ds/list.rs`: A memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris’ ordered linked list. (`List-mmt` at the paper)
- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (`MSQ-mmt-O2` at the paper)
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits DS-specific invariant for issuing less flushes when loading shared pointer. (`MSQ-mmt-O1` at the paper)
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (`MSQ-mmt-O0` at the paper)
- `src/ds/exchanger.rs`: A memento-based lock-free exchanger that uses `Insert`, `Delete` and `Checkpoint`.
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber stack. (`TreiberS-mmt` at the paper)
- `src/ds/elim_stack.rs`: An elimination-backoff stack combining our memento-based treiber stack and exchanger.
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (`Clevel-mmt` at the paper)
- `src/ds/queue_comb.rs`: A memento-based combining queue that uses `Combining` operation. (`CombQ-mmt` at the paper)

#### Safe Memory Reclamation

- Implementation of 'deferred persist' (corresponding to section D in Appendix)
- Implementation of 'resumable critical section and de-duplicated retirement' (corresponding to section D in Appendix)

#### Others

- `src/pmem`: This directory contains several libraries to create an environment to run memento-based programs, including persistent memory pool, persistent poiner, low-level instructions and other existing libraries (e.g. Ralloc).
- `src/pepoch`: Implementation of atomic pointer for persistent memory pool and SMR. It also provide ways to tag on a pointer to support detectable operations.

## Evaluation

See the `README.md` in the [evaluation](./evaluation)
