# Exactly-Once Semantics for Persistent Lock-Free Data Structures

## Installation

We assume you use **Ubuntu 20.04**.

### Requirements

- [Rust](https://www.rust-lang.org/)
  ```
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

### Build

- `cargo build`: build our framework including data structures.

### Our results

#### Detectable Operations

`src/ploc/` contains TODO

- `src/ploc/common.rs`: TODO corresponding TODO
- `src/ploc/detectable_cas.rs`: TODO corresponding TODO
- `src/ploc/insert_delete.rs`: TODO corresponding TODO

#### Data Structures

`src/ds/` contains persistent data structures supporting exactly-once semantics using *mementos*.

- `src/ds/queue.rs`: A persistent lock-free queue that uses TODO based on Micheal-Scott Queue
- `src/ds/queue_lp.rs`: A persistent lock-free queue that uses TODO based on Micheal-Scott Queue
- `src/ds/queue_general.rs`: A persistent lock-free queue that uses TODO based on Micheal-Scott Queue
- `src/ds/queue_pbcomb.rs`: A persistent queue that uses TODO
- `src/ds/exchanger.rs`: TODO
- `src/ds/treiber_stack.rs`: A persistent lock-free stack that uses TODO based on Treiber stack
- `src/ds/elim_stack.rs`: TODO
- `src/ds/soft_list.rs`: TODO
- `src/ds/soft_hash.rs`: TODO
- `src/ds/clevel.rs`: TODO

#### Safe Memory Reclamation

TODO

#### Others

- `src/pepoch`: TODO
- `src/pmem`: TODO

## Performance Evaluation

See the `README.md` in the [evaluation](./evaluation)
