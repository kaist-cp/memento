# Supplementary Materials of "Exactly-Once Semantics for Persistent Lock-Free Data Structures"

## Installation

We assume you use **Ubuntu 20.04**.

### Requirements

- [Rust](https://www.rust-lang.org/)
  ```
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

### Build

- TODO: Ralloc
  ```
  TODO: Ralloc
  ```
- To build our framework including detectable operations, data structures and SMR libraries:
  ```
  cargo build --release
  ```

### Our Implementations

#### Detectable Operations

The directory `src/ploc/` contains TODO

- `src/ploc/common.rs`: Implementation of Checkpoint (corresponding TODO) and timestamp calibration (corresponding TODO).
- `src/ploc/detectable_cas.rs`: Implementation of Atomic Pointer Location supporting Detectable CAS corresponding TODO.
- `src/ploc/insert_delete.rs`: Implementation of Insertion (corresponding TODO) and Deletion (corresponding TODO)

#### Data Structures

The directory `src/ds/` contains memento-based data structures supporting exactly-once semantics using detectable operations.

- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Micheal-Scott Queue (TODO: cite).
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist`(TODO: cite) technique rather than exploits DS-specific invariant for issuing less flushes when loading shared pointer.
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Micheal-Scott Queue.
- `src/ds/exchanger.rs`: A memento-based lock-free exchanger that uses `Insert`, `Delete` and `Checkpoint`.
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber stack.
- `src/ds/elim_stack.rs`: An elimination-backoff stack combining our memento-based treiber stack and exchanger.
- `src/ds/soft_list.rs` (and `src/ds/soft_hash.rs`): SOFT list (and hash table). We convert original SOFT list (and hash table, respectively.) (TODO: cite) to one using mementos.
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel (TODO: cite) to one using mementos.
- `src/ds/queue_pbcomb.rs`: A memento-based PBQueue which is a queue using combining technique. We convert original PBQueue (TODO: cite) to one using mementos.

#### Safe Memory Reclamation

TODO

#### Others

- `src/pepoch`: TODO
- `src/pmem`: TODO

## Performance Evaluation

See the `README.md` in the [evaluation](./evaluation)
