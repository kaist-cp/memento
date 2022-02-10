//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
//!
//! NOTE: This is not memento-based yet.
#![allow(warnings)] // TODO: remove

use crate::pmem::{persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;
use array_init::array_init;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

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
    DeqRetVal(PPtr<Node>),
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct RequestRec {
    func: Option<Func>,
    arg: usize,
    seq: usize,     // TODO: seq and activate are stored in the same memroy word
    activate: bool, // TODO: bit
}

/// Node
#[derive(Debug)]
pub struct Node {
    data: Data,
    next: PPtr<Node>,
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        if !s.next.is_null() {
            let next_ref = unsafe { s.next.deref_mut(pool) };
            Collectable::mark(next_ref, tid, gc);
        }
    }
}

/// State of Enqueue PBComb
#[derive(Debug, Clone)]
struct EStateRec {
    tail: PPtr<Node>,
    return_val: [Option<ReturnVal>; MAX_THREADS],
    deactivate: [bool; MAX_THREADS], // TODO: bit?
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        if !s.tail.is_null() {
            let tail_ref = unsafe { s.tail.deref_mut(pool) };
            Collectable::mark(tail_ref, tid, gc);
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug, Clone)]
struct DStateRec {
    head: PPtr<Node>,
    return_val: [Option<ReturnVal>; MAX_THREADS],
    deactivate: [bool; MAX_THREADS], // TODO: bit?
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        if !s.head.is_null() {
            let head_ref = unsafe { s.head.deref_mut(pool) };
            Collectable::mark(head_ref, tid, gc);
        }
    }
}

/// Shared volatile variables

lazy_static::lazy_static! {
    /// 현재 진행중인 enq combiner가 enq 시작한 지점. 여기서부턴 persist아직 보장되지 않았으니 deq해가면 안됨
    ///
    /// - 프로그램 시작시 dummy node를 가리키도록 초기화 (첫 시작은 pdefault에서, 이후엔 gc에서)
    /// - 노드를 직접 저장하지 않고 노드의 상대주소를 저장 (여기에 Atomic<PPtr<Node>>나 PAtomic<Node>는 이상함)
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// 현재 진행중인 enq combiner가 OLD_TAIL 이후에 enq한 노드들(의 상대주소)을 이 set에 모아뒀다가 나중에 한꺼번에 persist
    static ref TO_PERSIST: Mutex<HashSet<usize>> = Mutex::new(HashSet::new());

    /// Used by the PBQueueENQ instance of PBCOMB
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
    e_request: [RequestRec; MAX_THREADS],
    e_state: [EStateRec; 2],
    e_index: usize, // TODO: bit?

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_request: [RequestRec; MAX_THREADS],
    d_state: [DStateRec; 2],
    d_index: usize, // TODO: bit?
}

impl Collectable for QueuePBComb {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        assert!(s.dummy.is_null());
        Collectable::mark(unsafe { s.dummy.deref_mut(pool) }, tid, gc);

        for t in 0..MAX_THREADS {
            Collectable::filter(&mut s.e_state[t], tid, gc, pool);
            Collectable::filter(&mut s.d_state[t], tid, gc, pool);
        }

        // initialize global volatile variable manually
        OLD_TAIL.store(s.dummy.into_offset(), Ordering::SeqCst);
    }
}

impl PDefault for QueuePBComb {
    fn pdefault(pool: &PoolHandle) -> Self {
        let dummy = pool.alloc::<Node>();
        let dummy_ref = unsafe { dummy.deref_mut(pool) };
        dummy_ref.data = 0;
        dummy_ref.next = PPtr::null();

        // initialize global volatile variable manually
        OLD_TAIL.store(dummy.into_offset(), Ordering::SeqCst);

        Self {
            dummy,
            e_request: array_init(|_| RequestRec::default()),
            e_state: array_init(|_| EStateRec {
                tail: dummy,
                return_val: array_init(|_| None),
                deactivate: array_init(|_| false),
            }),
            e_index: 0,
            d_request: array_init(|_| RequestRec::default()),
            d_state: array_init(|_| DStateRec {
                head: dummy,
                return_val: array_init(|_| None),
                deactivate: array_init(|_| false),
            }),
            d_index: 0,
        }
    }
}

impl QueuePBComb {
    /// normal run
    ///
    /// enq or deq 실행
    pub fn PBQueue(
        &mut self,
        func: Func,
        arg: Data,
        seq: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> ReturnVal {
        match func {
            Func::ENQUEUE => self.PBQueueEnq(arg, seq, tid, pool),
            Func::DEQUEUE => self.PBQueueDnq(seq, tid, pool),
        }
    }

    /// recovery run
    ///
    /// 최근 crash난 enq or deq를 재실행 (exactly-once)
    pub fn recover(
        &mut self,
        func: Func,
        arg: Data,
        seq: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> ReturnVal {
        match func {
            Func::ENQUEUE => {
                // 1. check seq number and re-announce if request is not yet announced
                if self.e_request[tid].seq != seq {
                    return self.PBQueue(func, arg, seq, tid, pool);
                }

                // 2. check activate and re-execute if request is not yet applied
                let e_state = &self.e_state[self.e_index];
                if self.e_request[tid].activate != e_state.deactivate[tid] {
                    return self.PerformEnqReq(tid, pool); // TODO: arg
                }

                // 3. return value if request is already applied
                return e_state.return_val[tid].clone().unwrap();
            }
            Func::DEQUEUE => {
                // 1. check seq number and re-announce if request is not yet announced
                if self.d_request[tid].seq != seq {
                    return self.PBQueue(func, arg, seq, tid, pool);
                }

                // 2. check activate and re-execute if request is not yet applied
                let d_state = &self.d_state[self.d_index];
                if self.d_request[tid].activate != d_state.deactivate[tid] {
                    return self.PerformDeqReq(tid, pool); // TODO: arg
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
    fn PBQueueEnq(&mut self, arg: Data, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // 요청 등록
        self.e_request[tid] = RequestRec {
            func: Some(Func::ENQUEUE),
            arg,
            seq,
            activate: !self.e_request[tid].activate, // TODO: 1-activate?
        };

        // 실행
        self.PerformEnqReq(tid, pool)
    }

    /// enqueue 요청 실행 (thread-local)
    fn PerformEnqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // enq combiner 결정
        let lval = loop {
            let lval = E_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if (lval % 2 == 0)
                && E_LOCK
                    .compare_exchange(
                        lval,
                        lval.wrapping_add(1),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                break lval.wrapping_add(1);
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            // TODO: backoff
            while lval == E_LOCK.load(Ordering::SeqCst) {}
            if self.e_request[tid].activate == self.e_state[self.e_index].deactivate[tid] {
                return self.e_state[self.e_index].return_val[tid].clone().unwrap();
            }
        };

        // enq combiner는 쌓인 enq 요청들을 수행
        let ind = 1 - self.e_index;
        self.e_state[ind] = self.e_state[self.e_index].clone(); // create a copy of current state
        OLD_TAIL.store(self.e_state[ind].tail.into_offset(), Ordering::SeqCst);

        for q in 0..MAX_THREADS {
            // if `q` thread has a request that is not yet applied
            if self.e_request[q].activate != self.e_state[ind].deactivate[q] {
                let _ = TO_PERSIST
                    .lock()
                    .unwrap()
                    .insert(self.e_state[ind].tail.into_offset());
                Self::enqueue(&mut self.e_state[ind].tail, self.e_request[q].arg, pool);
                self.e_state[ind].return_val[q] = Some(ReturnVal::EnqRetVal(()));
                self.e_state[ind].deactivate[q] = self.e_request[q].activate;
            }
        }

        let _ = TO_PERSIST
            .lock()
            .unwrap()
            .insert(self.e_state[ind].tail.into_offset());
        // persist all in `TO_PERSIST`
        for i in TO_PERSIST.lock().unwrap().iter() {
            let node = PPtr::<Node>::from(*i);
            persist_obj(unsafe { node.deref(pool) }, false);
        }
        persist_obj(&self.e_request, false);
        persist_obj(&self.e_state[ind], false);
        sfence();
        self.e_index = ind;
        persist_obj(&self.e_index, false);
        sfence();
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        TO_PERSIST.lock().unwrap().clear(); // clear to_persist set
        E_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        self.e_state[self.e_index].return_val[tid].clone().unwrap()
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    fn enqueue(tail: &mut PPtr<Node>, arg: Data, pool: &PoolHandle) {
        let new_node = pool.alloc::<Node>();
        let new_node_ref = unsafe { new_node.deref_mut(pool) };
        new_node_ref.data = arg;
        new_node_ref.next = PPtr::null();

        let tail_ref = unsafe { tail.deref_mut(pool) };
        tail_ref.next = new_node;
        *tail = new_node;
    }
}

/// Deq
impl QueuePBComb {
    /// dequeue 요청 등록 후 실행 (thread-local)
    fn PBQueueDnq(&mut self, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // 요청 등록
        self.d_request[tid] = RequestRec {
            func: Some(Func::DEQUEUE),
            arg: 0,
            seq,
            activate: !self.d_request[tid].activate, // TODO: 1-activate?
        };

        // 실행
        self.PerformDeqReq(tid, pool)
    }

    /// dequeue 요청 실행 (thread-local)
    fn PerformDeqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // deq combiner 결정
        let lval = loop {
            let lval = D_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if (lval % 2 == 0)
                && D_LOCK
                    .compare_exchange(
                        lval,
                        lval.wrapping_add(1),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                break lval.wrapping_add(1);
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            while lval == D_LOCK.load(Ordering::SeqCst) {}
            if self.d_request[tid].activate == self.d_state[self.d_index].deactivate[tid] {
                return self.d_state[self.d_index].return_val[tid].clone().unwrap();
            }
        };

        // deq combiner는 쌓인 deq 요청들을 수행
        let ind = 1 - self.d_index;
        self.d_state[ind] = self.d_state[self.d_index].clone(); // create a copy of current state

        for q in 0..MAX_THREADS {
            // if `t` thread has a request that is not yet applied
            if self.d_request[q].activate != self.d_state[ind].deactivate[q] {
                let ret_val;
                // 확실히 persist된 노드들만 deq 수행. OLD_TAIL부터는 현재 enq 중인거라 persist 보장되지 않음
                if OLD_TAIL.load(Ordering::SeqCst) != self.d_state[ind].head.into_offset() {
                    let node = Self::dequeue(&mut self.d_state[ind].head, pool);
                    ret_val = ReturnVal::DeqRetVal(node);
                } else {
                    ret_val = ReturnVal::DeqRetVal(PPtr::null());
                }
                self.d_state[ind].return_val[q] = Some(ret_val);
                self.d_state[ind].deactivate[q] = self.d_request[q].activate;
            }
        }
        persist_obj(&self.d_request, false);
        persist_obj(&self.d_state[ind], false);
        sfence();
        self.d_index = ind;
        persist_obj(&self.d_index, false);
        sfence();
        D_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        self.d_state[self.d_index].return_val[tid].clone().unwrap()
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    fn dequeue(head: &mut PPtr<Node>, pool: &PoolHandle) -> PPtr<Node> {
        let head_ref = unsafe { head.deref(pool) };

        // NOTE: 얘네 구현은 데이터를 반환하질 않고 노드를 반환.
        // 노드를 dequeue해간 애는 다 썼다고 함부로 노드 free하면 안됨. queue의 sentinel 노드로 남아있을 수 있음. TODO: 그냥 data 반환해도 될 것같은데..왜지
        let ret = head_ref.next;
        if !ret.is_null() {
            *head = head_ref.next;
        }
        ret
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use crate::ds::queue_pbcomb::{Func, QueuePBComb, ReturnVal};
    use crate::pmem::{persist_obj, Collectable, GarbageCollection, PPtr, PoolHandle, RootObj};
    use crate::test_utils::tests::{run_test, TestRootObj, JOB_FINISHED, RESULTS};
    use crossbeam_epoch::Guard;

    const NR_THREAD: usize = 8;
    const COUNT: usize = 100_000;

    #[derive(Default)]
    struct EnqDeq {
        enq_seq: usize, // thread-local op seqeuence number. TODO: log queue였나? 구현보고 똑같이 구현
        deq_seq: usize, // thread-local op seqeuence number. TODO: log queue였나? 구현보고 똑같이 구현
    }

    impl Collectable for EnqDeq {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<QueuePBComb> {
        fn run(&self, mmt: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // Get &mut queue
            let queue =
                unsafe { (&self.obj as *const QueuePBComb as *mut QueuePBComb).as_mut() }.unwrap();

            match tid {
                // T0: 다른 스레드들의 실행결과를 확인
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let res = queue.PBQueue(Func::DEQUEUE, tid, 0, tid, pool);
                    if let ReturnVal::DeqRetVal(res) = res {
                        assert!(res.is_null());
                    } else {
                        panic!("func and return value must be of the same type");
                    }

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    assert!((1..NR_THREAD + 1)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T0이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let _ = queue.PBQueue(Func::ENQUEUE, tid, mmt.enq_seq, tid, pool);
                        mmt.enq_seq += 1;
                        persist_obj(mmt, true);

                        let res = queue.PBQueue(Func::DEQUEUE, tid, mmt.deq_seq, tid, pool);
                        mmt.deq_seq += 1;
                        persist_obj(mmt, true);

                        if let ReturnVal::DeqRetVal(res) = res {
                            // deq 결과를 실험결과에 전달
                            let v = unsafe { res.deref(pool) }.data;
                            let _ = RESULTS[v].fetch_add(1, Ordering::SeqCst);
                        }

                        // // deq 성공할 때까지 반복.
                        // // NOTE: 내 enq 완료를 마지막으로 새로운 enq combine이 진행중이라면, 내가 enq한 노드(OLD_TAIL)부터는 enq combine이 끝날때까지 deq되지 않음.
                        // loop {
                        //     let res = queue.PBQueue(Func::DEQUEUE, tid, mmt.deq_seq, tid, pool);
                        //     mmt.deq_seq += 1;
                        //     persist_obj(mmt, true);

                        //     if let ReturnVal::DeqRetVal(res) = res {
                        //         if res.is_null() {
                        //             continue;
                        //         }

                        //         // deq 결과를 실험결과에 전달
                        //         let v = unsafe { res.deref(pool) }.data;
                        //         let _ = RESULTS[v].fetch_add(1, Ordering::SeqCst);
                        //         break;
                        //     } else {
                        //         panic!("func and return value must be of the same type");
                        //     }
                        // }
                    }

                    // "나 끝났다"
                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "pbcomb_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<QueuePBComb>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
