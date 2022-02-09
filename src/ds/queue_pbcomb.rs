//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
//!
//! NOTE: This is not memento-based yet.
#![allow(warnings)] // TODO: remove

use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pepoch::PAtomic;
use crate::pmem::{persist_obj, Collectable, GarbageCollection, PPtr, PoolHandle};
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
    EnqRetVal(()), // TODO: ACK 표현?

    /// return value of deq
    DeqRetVal(Option<PPtr<Node>>),
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
    next: PPtr<Node>,
}

/// State of Enqueue PBComb
#[derive(Debug, Clone)]
struct EStateRec {
    tail: PPtr<Node>,
    return_val: [Option<ReturnVal>; MAX_THREADS], // TODO: type of return value
    deactivate: [bool; MAX_THREADS],              // TODO: bit
}

/// State of Dequeue PBComb
#[derive(Debug, Clone)]
struct DStateRec {
    head: PPtr<Node>,
    return_val: [Option<ReturnVal>; MAX_THREADS], // TODO: type of return value
    deactivate: [bool; MAX_THREADS],              // TODO: bit
}

/// Shared volatile variables
// TODO: 프로그램 시작시 dummy르 초기화해줘야함. 첫 시작은 pdefault에서 하면 될 것 같고, 이후엔? gc에서?
static mut OLD_TAIL: PPtr<Node> = PPtr::null(); // TODO: initially, &DUMMY

lazy_static::lazy_static! {
    // static mut TO_PERSIST: Set<*mut Node>;  // TODO: initiallay, empty set

    /// Used PBQueueENQ instance of PBCOMB
    static ref E_LOCK: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: AtomicUsize = AtomicUsize::new(0);
}

/// TODO: doc
// TODO: 내부 필드 전부 cachepadded? -> 일단 이렇게 실험하고 성능 이상하다 싶으면 그때 cachepadded 해보기.
#[derive(Debug)]
pub struct QueuePBComb {
    /// Shared non-volatile variables
    dummy: PPtr<Node>, // TODO: initially, ..

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    // TODO: enq하는데 deq의 variable을 쓰는 실수 주의
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
        // TODO: old_tail = dummy
        todo!("initialize")
    }
}

impl QueuePBComb {
    /// normal run
    ///
    /// enq or deq 실행
    pub fn PBQueue(&mut self, func: Func, arg: Data, seq: usize, tid: usize) -> ReturnVal {
        match func {
            Func::ENQUEUE => self.PBQueueEnq(arg, seq, tid),
            Func::DEQUEUE => self.PBQueueDnq(seq, tid),
        }
    }

    /// recovery run
    ///
    /// 최근 crash난 enq or deq를 재실행 (exactly-once)
    pub fn recover(&mut self, func: Func, arg: Data, seq: usize, tid: usize) -> ReturnVal {
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
    fn PBQueueEnq(&mut self, arg: Data, seq: usize, tid: usize) -> ReturnVal {
        // 요청 등록
        self.e_request[tid] = RequestRec {
            func: Func::ENQUEUE,
            arg,
            seq,
            activate: !self.e_request[tid].activate, // TODO: 1-activate?
        };

        // 실행
        self.PerformEnqReq(tid)
    }

    /// enqueue 요청 실행 (thread-local)
    fn PerformEnqReq(&mut self, tid: usize) -> ReturnVal {
        // enq combiner 결정
        loop {
            let lval = E_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if (lval % 2 == 0)
                && E_LOCK
                    .compare_exchange(lval, lval + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                break;
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            while lval == E_LOCK.load(Ordering::SeqCst) {}
            if self.e_request[tid].activate == self.e_state[self.e_index].deactivate[tid] {
                return self.e_state[self.e_index].return_val[tid].clone().unwrap();
            }
        }

        // enq combiner는 쌓인 enq 요청들을 수행
        let ind = 1 - self.e_index;
        self.e_state[ind] = self.e_state[self.e_index].clone(); // create a copy of current state
        unsafe { OLD_TAIL = self.e_state[ind].tail };

        for t in 0..MAX_THREADS {
            // if `t` thread has a request that is not yet applied
            let t_req = &self.e_request[t];
            if t_req.activate != self.e_state[ind].deactivate[t] {
                // TODO: add EState[ind].tail to `toPersist`
                self.enqueue(&self.e_state[ind].tail, t_req.arg);
                self.e_state[ind].return_val[t] = Some(ReturnVal::EnqRetVal(()));
                self.e_state[ind].deactivate[t] = t_req.activate;
            }
        }
        // TODO: add EState[ind].tail to `toPersist`
        // TODO: persist all in `toPersist`
        persist_obj(&self.e_request, false);
        persist_obj(&self.e_state[ind], true); // TODO: 논문에서 얘는 왜 state는 &붙여 pwb하고 위의 request는 &없이 pwb함?
        self.e_index = ind;
        persist_obj(&self.e_index, true);
        unsafe { OLD_TAIL = PPtr::null() };
        // TODO: make toPersist empty
        let _ = E_LOCK.fetch_add(1, Ordering::SeqCst);
        self.e_state[self.e_index].return_val[tid].clone().unwrap()
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    // TODO: arg
    fn enqueue(&self, tail: &PPtr<Node>, arg: Data) {
        todo!()
    }
}

/// Deq
impl QueuePBComb {
    /// dequeue 요청 등록 후 실행 (thread-local)
    fn PBQueueDnq(&mut self, seq: usize, tid: usize) -> ReturnVal {
        // 요청 등록
        self.d_request[tid] = RequestRec {
            func: Func::DEQUEUE,
            arg: 0,
            seq,
            activate: !self.d_request[tid].activate, // TODO: 1-activate?
        };

        // 실행
        self.PerformDeqReq(tid)
    }

    /// dequeue 요청 실행 (thread-local)
    fn PerformDeqReq(&mut self, tid: usize) -> ReturnVal {
        // deq combiner 결정
        loop {
            let lval = D_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if (lval % 2 == 0)
                && D_LOCK
                    .compare_exchange(lval, lval + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                break;
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            while lval == D_LOCK.load(Ordering::SeqCst) {}
            if self.d_request[tid].activate == self.d_state[self.d_index].deactivate[tid] {
                return self.d_state[self.d_index].return_val[tid].clone().unwrap();
            }
        }

        // deq combiner는 쌓인 deq 요청들을 수행
        let ind = 1 - self.d_index;
        self.d_state[ind] = self.d_state[self.d_index].clone(); // create a copy of current state

        for t in 0..MAX_THREADS {
            // if `t` thread has a request that is not yet applied
            let t_req = &self.d_request[t];
            if t_req.activate != self.d_state[ind].deactivate[t] {
                let ret_val;
                // 확실히 persist된 노드들만 deq 수행. OLD_TAIL부터는 현재 enq 중인거라 persist 보장되지 않음
                if unsafe { OLD_TAIL } != self.d_state[ind].head {
                    let node = self.dequeue(&self.d_state[ind].head);
                    ret_val = ReturnVal::DeqRetVal(Some(node));
                } else {
                    ret_val = ReturnVal::DeqRetVal(None);
                }
                self.d_state[ind].return_val[t] = Some(ret_val);
                self.d_state[ind].deactivate[t] = t_req.activate;
            }
        }
        persist_obj(&self.d_request, false);
        persist_obj(&self.d_state[ind], true); // TODO: 논문에서 얘는 왜 state는 &붙여 pwb하고 위의 request는 &없이 pwb함?
        self.d_index = ind;
        persist_obj(&self.d_index, true);
        let _ = D_LOCK.fetch_add(1, Ordering::SeqCst);
        self.d_state[self.d_index].return_val[tid].clone().unwrap()
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    fn dequeue(&self, head: &PPtr<Node>) -> PPtr<Node> {
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
