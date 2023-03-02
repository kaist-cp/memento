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

#### Primitive Operations

The directory `src/ploc/` contains memento-based detectable operations.

- `src/ploc/common.rs`: Implementation of timestamp calibration (corresponding to §4.1) and Checkpoint (corresponding to §4.2)
- `src/ploc/detectable_cas.rs`: Implementation of Atomic Pointer Location supporting Detectable CAS (corresponding to §4.3)
- `src/ploc/insert_delete.rs`: Implementation of Insertion and Deletion (corresponding to §B in Appendix)

#### Concurrent Data Structures (§5)

The directory `src/ds/` contains memento-based persistent data structures supporting exactly-once semantics using detectable operations.

- `src/ds/comb.rs`: A memento-based detectable combining operation. We convert original PBComb to one using mementos to support multi-time detectability. (***Comb-mmt*** in the paper)
- `src/ds/list.rs`: A memento-based lock-free list that uses `DetectableCas` and `Checkpoint` based on Harris' ordered linked list. (***List-mmt*** in the paper)
- `src/ds/queue.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint` based on Michael-Scott Queue. (***MSQ-mmt-O2*** in the paper)
- `src/ds/queue_lp.rs`: A memento-based lock-free queue that uses `Insert`, `Delete` and `Checkpoint`. The difference from `queue.rs` is that this queue uses general `link-persist` technique rather than exploits DS-specific invariant for issuing less flushes when loading shared pointer. (***MSQ-mmt-O1*** in the paper)
- `src/ds/queue_general.rs`: A memento-based lock-free queue that uses `DetectableCas` and `Checkpoint` based on Michael-Scott Queue. (***MSQ-mmt-O0*** in the paper)
- `src/ds/exchanger.rs`: A memento-based lock-free exchanger that uses `Insert`, `Delete` and `Checkpoint`.
- `src/ds/treiber_stack.rs`: A memento-based lock-free stack that uses `DetectableCas` and `Checkpoint` based on Treiber stack. (***TreiberS-mmt*** in the paper)
- `src/ds/elim_stack.rs`: An elimination-backoff stack combining our memento-based treiber stack and exchanger.
- `src/ds/clevel.rs`: A memento-based Clevel extensible hash table. We convert original Clevel to one using mementos. (***Clevel-mmt*** in the paper)
- `src/ds/queue_comb.rs`: A memento-based combining queue that uses `Combining` operation. (***CombQ-mmt*** in the paper)

#### Safe Memory Reclamation (§D in appendix)

The directory `crossbeam-persistency` contains implementations of safe memory reclamations.

TODO: More specifically...

- Implementation of "deferred persist"
- Implementation of "resumable critical section and de-duplicated retirement"

#### Others (§4.1)

- `src/pmem/ll.rs`: Low-level instructions for ***PM Access*** (corresponding to §4.1)
- `src/pmem/pool.rs`: A library that creates an environment (i.e. PM pool) and runs a memento-based program. (corresponding to ***Crash Handler*** described in §4.1)

## Evaluation (§6)

See the `README.md` in the [evaluation](./evaluation)
