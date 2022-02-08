//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
//!
//! NOTE: This is not memento-based yet.

#![allow(warnings)] // TODO: remove

use crate::pepoch::PAtomic;
use crate::pmem::{Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;

const MAX_THREADS: usize = 32;

type Data = usize; // TODO: generic

/// function type of queue
#[derive(Debug)]
pub enum Func {
    /// enq
    ENQUEUE,

    /// deq
    DEQUEUE,
}

/// return value of queue function
#[derive(Debug, Clone)]
pub enum ReturnVal {
    /// return value of enq
    EnqRetVal(()),

    /// return value of deq
    DeqRetVal(PPtr<Node>), // TODO: PPtr 괜찮나? PShared가 더 맞는 표현인가?
}

#[derive(Debug)]
#[repr(align(64))]
struct RequestRec {
    func: Func,
    arg: usize,
    seq: usize,     // TODO: seq and activate are stored in the same memroy word
    activate: bool, // TODO: bit
}

// TODO: generic data
/// Node
#[derive(Debug)]
pub struct Node {
    data: Data,
    next: PAtomic<Node>, // TODO: load, store만 써야함. CAS는 쓸일 없음
}

/// State of Enqueue PBComb
#[derive(Debug)]
struct EStateRec {
    tail: PAtomic<Node>, // TODO: load, store만 써야함. CAS는 쓸일 없음
    return_val: [Option<ReturnVal>; MAX_THREADS], // TODO: type of return value
    deactivate: [bool; MAX_THREADS], // TODO: bit
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: PAtomic<Node>, // TODO: load, store만 써야함. CAS는 쓸일 없음
    return_val: [Option<ReturnVal>; MAX_THREADS], // TODO: type of return value
    deactivate: [bool; MAX_THREADS], // TODO: bit
}

/// Shared volatile variables
// TODO: lazy_static?
// static mut OLD_TAIL: *mut Node = null_mut(); // TODO: initially, &DUMMY
// static mut TO_PERSIST: Set<*mut Node>;  // TODO: initiallay, empty set

/// Shared volatile variables used by the PBQueueENQ instance of PBCOMB
static mut E_LOCK: usize = 0; // TODO: AtomicUsize

/// Shared volatile variables used by the PBQueueDEQ instance of PBCOMB
static mut D_LOCK: usize = 0; // TODO: AtomicUsize

/// TODO: doc
// TODO: 내부 필드 전부 cachepadded? -> 일단 이렇게 실험하고 성능 이상하다 싶으면 그때 cachepadded 해보기.
#[derive(Debug)]
pub struct QueuePBComb {
    /// Shared non-volatile variables
    dummy: Node, // TODO: initially, ..

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    e_request: [RequestRec; MAX_THREADS], // TODO: initially, ...
    e_state: [EStateRec; 2], // TODO: initially, ...
    e_index: usize,          // TODO: bit

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_request: [RequestRec; MAX_THREADS], // TODO: initially, ...
    d_state: [DStateRec; 2], // TODO: initially, ...
    d_index: usize,          // TODO: bit
}

impl Collectable for QueuePBComb {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl PDefault for QueuePBComb {
    fn pdefault(pool: &PoolHandle) -> Self {
        todo!("initialize")
    }
}

impl QueuePBComb {
    /// normal run
    ///
    /// enq or deq 실행
    pub fn PBQueue(&self, func: Func, arg: Data, seq: usize, tid: usize) -> ReturnVal {
        match func {
            Func::ENQUEUE => self.PBQueueEnq(arg, seq, tid),
            Func::DEQUEUE => self.PBQueueDnq(seq, tid),
        }
    }

    /// recovery run
    ///
    /// 최근 crash난 enq or deq를 재실행 (exactly-once)
    pub fn recover(&self, func: Func, arg: Data, seq: usize, tid: usize) -> ReturnVal {
        match func {
            Func::ENQUEUE => {
                // 1. check seq number and re-announce if request is not yet announced
                if self.e_request[tid].seq != seq {
                    return self.PBQueue(func, arg, seq, tid);
                }

                // 2. check activate and re-execute if request is not yet applied
                let e_state = &self.e_state[self.e_index];
                if self.e_request[tid].activate != e_state.deactivate[tid] {
                    return self.PerformEnqReq(tid); // TODO: arg
                }

                // 3. return value if request is already applied
                return e_state.return_val[tid].clone().unwrap();
            }
            Func::DEQUEUE => {
                // 1. check seq number and re-announce if request is not yet announced
                if self.d_request[tid].seq != seq {
                    return self.PBQueue(func, arg, seq, tid);
                }

                // 2. check activate and re-execute if request is not yet applied
                let d_state = &self.d_state[self.d_index];
                if self.d_request[tid].activate != d_state.deactivate[tid] {
                    return self.PerformDeqReq(tid); // TODO: arg
                }

                // 3. return value if request is already applied
                return d_state.return_val[tid].clone().unwrap();
            }
        }
    }
}

/// Enq
impl QueuePBComb {
    /// enqueue 요청 등록 후 실행 (thread-local)
    fn PBQueueEnq(&self, arg: Data, seq: usize, tid: usize) -> ReturnVal {
        todo!()
    }

    /// enqueue 요청 실행 (thread-local)
    fn PerformEnqReq(&self, tid: usize) -> ReturnVal {
        todo!()
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    // TODO: arg
    fn enqueue() {
        todo!()
    }
}

/// Deq
impl QueuePBComb {
    /// dequeue 요청 등록 후 실행 (thread-local)
    fn PBQueueDnq(&self, seq: usize, tid: usize) -> ReturnVal {
        todo!()
    }

    /// dequeue 요청 실행 (thread-local)
    fn PerformDeqReq(&self, tid: usize) -> ReturnVal {
        todo!()
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    // TODO: arg
    fn dequeue() {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::ds::queue_pbcomb::QueuePBComb;
    use crate::pmem::{Collectable, GarbageCollection, PoolHandle, RootObj};
    use crate::test_utils::tests::{run_test, TestRootObj};
    use crossbeam_epoch::Guard;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    #[derive(Default)]
    struct EnqDeq {
        seq: usize, // thread-local op seqeuence number. TODO: log queue였나? 구현보고 똑같이 구현
    }

    impl Collectable for EnqDeq {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<QueuePBComb> {
        fn run(&self, mmt: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            todo!()
        }
    }

    fn enq_deq() {
        const FILE_NAME: &str = "pbcomb_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<QueuePBComb>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
