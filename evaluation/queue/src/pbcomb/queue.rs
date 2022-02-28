//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
#![allow(non_snake_case)]
use array_init::array_init;
use core::sync::atomic::Ordering;
use crossbeam_epoch::{unprotected, Guard};
use crossbeam_utils::{Backoff, CachePadded};
use memento::pepoch::atomic::Pointer;
use memento::pepoch::{PAtomic, POwned};
use memento::ploc::{compose_aux_bit, decompose_aux_bit};
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{persist_obj, pool::*, sfence, PPtr};
use memento::PDefault;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tinyvec::tiny_vec;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

type Data = usize;

/// 사용할 스레드 수. combining시 이 스레드 수만큼만 op 순회
pub static mut NR_THREADS: usize = MAX_THREADS;

// Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
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
    DeqRetVal(PPtr<Node>),
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct RequestRec {
    func: Option<Func>,
    arg: usize,
    act_seq: AtomicUsize, // 1: activate, 63: sequence number
}

impl Collectable for RequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl RequestRec {
    // helper function to update activate bit and sequence number atomically
    #[inline]
    fn store_act_seq(&self, activate: bool, seq: usize) {
        self.act_seq
            .store(compose_aux_bit(activate as usize, seq), Ordering::SeqCst)
    }

    #[inline]
    fn load_seq(&self) -> usize {
        let (_, seq) = decompose_aux_bit(self.act_seq.load(Ordering::SeqCst));
        seq
    }

    #[inline]
    fn load_activate(&self) -> bool {
        let (act, _) = decompose_aux_bit(self.act_seq.load(Ordering::SeqCst));
        act != 0
    }
}

/// Node
#[derive(Debug)]
pub struct Node {
    data: Data,
    next: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let mut next = s.next.load(Ordering::SeqCst, unsafe { unprotected() });
        if !next.is_null() {
            let next_ref = unsafe { next.deref_mut(pool) };
            Collectable::mark(next_ref, tid, gc);
        }
    }
}

/// State of Enqueue PBComb
#[derive(Debug)]
struct EStateRec {
    tail: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [Option<ReturnVal>; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl Clone for EStateRec {
    fn clone(&self) -> Self {
        Self {
            tail: self.tail.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicBool::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let mut tail = s.tail.load(Ordering::SeqCst, unsafe { unprotected() });
        if !tail.is_null() {
            let tail_ref = unsafe { tail.deref_mut(pool) };
            Collectable::mark(tail_ref, tid, gc);
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: PAtomic<Node>, // NOTE: reordering 방지를 위한 atomic. CAS는 안씀
    return_val: [Option<ReturnVal>; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl Clone for DStateRec {
    fn clone(&self) -> Self {
        Self {
            head: self.head.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicBool::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let mut head = s.head.load(Ordering::SeqCst, unsafe { unprotected() });
        if !head.is_null() {
            let head_ref = unsafe { head.deref_mut(pool) };
            Collectable::mark(head_ref, tid, gc);
        }
    }
}

// Shared volatile variables
lazy_static::lazy_static! {
    /// 현재 진행중인 enq combiner가 enq 시작한 지점. 여기서부턴 persist아직 보장되지 않았으니 deq해가면 안됨
    ///
    /// - 프로그램 시작시 dummy node를 가리키도록 초기화 (첫 시작은 pdefault에서, 이후엔 gc에서)
    /// - 노드를 직접 저장하지 않고 노드의 상대주소를 저장 (여기에 Atomic<PPtr<Node>>나 PAtomic<Node>는 이상함)
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: AtomicUsize = AtomicUsize::new(0);
    static ref E_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0)); // TODO: 더 적절한 이름..

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: AtomicUsize = AtomicUsize::new(0);
    static ref D_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0));
}

/// TODO: doc
#[derive(Debug)]
pub struct PBCombQueue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    // NOTE: enq하는데 deq의 variable을 쓰는 실수 주의
    e_request: [RequestRec; MAX_THREADS + 1],
    e_state: [CachePadded<EStateRec>; 2],
    e_index: AtomicUsize,

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_request: [RequestRec; MAX_THREADS + 1],
    d_state: [CachePadded<DStateRec>; 2],
    d_index: AtomicUsize,
}

impl Collectable for PBCombQueue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        assert!(s.dummy.is_null());
        Collectable::mark(unsafe { s.dummy.deref_mut(pool) }, tid, gc);

        for t in 1..MAX_THREADS + 1 {
            Collectable::filter(&mut s.e_request[t], tid, gc, pool);
            Collectable::filter(&mut s.d_request[t], tid, gc, pool);
        }

        // initialize global volatile variable manually
        OLD_TAIL.store(s.dummy.into_offset(), Ordering::SeqCst);
    }
}

impl PDefault for PBCombQueue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let dummy = pool.alloc::<Node>();
        let dummy_ref = unsafe { dummy.deref_mut(pool) };
        dummy_ref.data = 0;
        dummy_ref.next = PAtomic::null();

        // initialize global volatile variable manually
        OLD_TAIL.store(dummy.into_offset(), Ordering::SeqCst);

        Self {
            dummy,
            e_request: array_init(|_| Default::default()),
            e_state: array_init(|_| {
                CachePadded::new(EStateRec {
                    tail: PAtomic::from(dummy),
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| AtomicBool::new(false)),
                })
            }),
            e_index: Default::default(),
            d_request: array_init(|_| Default::default()),
            d_state: array_init(|_| {
                CachePadded::new(DStateRec {
                    head: PAtomic::from(dummy),
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| AtomicBool::new(false)),
                })
            }),
            d_index: Default::default(),
        }
    }
}

impl PBCombQueue {
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
                if self.e_request[tid].load_seq() != seq {
                    return self.PBQueue(func, arg, seq, tid, pool);
                }

                // 2. check activate and re-execute if request is not yet applied
                let e_state = &self.e_state[self.e_index.load(Ordering::SeqCst)];
                if self.e_request[tid].load_activate()
                    != e_state.deactivate[tid].load(Ordering::SeqCst)
                {
                    return self.PerformEnqReq(tid, pool);
                }

                // 3. return value if request is already applied
                return e_state.return_val[tid].clone().unwrap();
            }
            Func::DEQUEUE => {
                // 1. check seq number and re-announce if request is not yet announced
                if self.d_request[tid].load_seq() != seq {
                    return self.PBQueue(func, arg, seq, tid, pool);
                }

                // 2. check activate and re-execute if request is not yet applied
                let d_state = &self.d_state[self.d_index.load(Ordering::SeqCst)];
                if self.d_request[tid].load_activate()
                    != d_state.deactivate[tid].load(Ordering::SeqCst)
                {
                    return self.PerformDeqReq(tid, pool);
                }

                // 3. return value if request is already applied
                return d_state.return_val[tid].clone().unwrap();
            }
        }
    }
}

/// Enq
impl PBCombQueue {
    /// enqueue 요청 등록 후 실행 (thread-local)
    fn PBQueueEnq(&mut self, arg: Data, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // 요청 등록
        self.e_request[tid].func = Some(Func::ENQUEUE);
        self.e_request[tid].arg = arg;
        self.e_request[tid].store_act_seq(!self.e_request[tid].load_activate(), seq);

        // 실행
        self.PerformEnqReq(tid, pool)
    }

    /// enqueue 요청 실행 (thread-local)
    fn PerformEnqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // enq combiner 결정
        let mut lval;
        loop {
            lval = E_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if lval % 2 == 0 {
                match E_LOCK.compare_exchange(
                    lval,
                    lval.wrapping_add(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        lval = lval.wrapping_add(1);
                        break;
                    }
                    Err(cur) => lval = cur,
                }
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            while lval == E_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }
            if self.e_request[tid].load_activate()
                == self.e_state[self.e_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // 자신의 op을 처리한 combiner가 끝날때까지 기다렸다가 결과 반환
                let deactivate_lval = E_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while !(deactivate_lval < E_LOCK.load(Ordering::SeqCst)) {
                    backoff.snooze();
                }

                return self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid]
                    .clone()
                    .unwrap();
            }
        }

        // enq combiner는 쌓인 enq 요청들을 수행
        let ind = 1 - self.e_index.load(Ordering::SeqCst);
        self.e_state[ind] = self.e_state[self.e_index.load(Ordering::SeqCst)].clone(); // create a copy of current state
        OLD_TAIL.store(
            self.e_state[ind]
                .tail
                .load(Ordering::SeqCst, unsafe { unprotected() })
                .into_usize(),
            Ordering::SeqCst,
        );

        // enq한 노드들(의 상대주소)을 여기에 모아뒀다가 나중에 한꺼번에 persist
        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `q` thread has a request that is not yet applied
            if self.e_request[q].load_activate()
                != self.e_state[ind].deactivate[q].load(Ordering::SeqCst)
            {
                // 현재 tail의 persist를 예약
                let tail_addr = self.e_state[ind]
                    .tail
                    .load(Ordering::SeqCst, unsafe { unprotected() })
                    .into_usize();
                match to_persist.binary_search(&tail_addr) {
                    Ok(_) => {} // 같은 주소 중복 persist 방지
                    Err(idx) => to_persist.insert(idx, tail_addr),
                }

                // enq
                Self::enqueue(&mut self.e_state[ind].tail, self.e_request[q].arg, pool);
                E_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                self.e_state[ind].return_val[q] = Some(ReturnVal::EnqRetVal(()));
                self.e_state[ind].deactivate[q]
                    .store(self.e_request[q].load_activate(), Ordering::SeqCst);
            }
        }
        let tail_addr = self.e_state[ind]
            .tail
            .load(Ordering::SeqCst, unsafe { unprotected() })
            .into_usize();
        match to_persist.binary_search(&tail_addr) {
            Ok(_) => {} // 같은 주소 중복 persist 방지
            Err(idx) => to_persist.insert(idx, tail_addr),
        }
        // persist all in `to_persist`
        while !to_persist.is_empty() {
            let node = PPtr::<Node>::from(to_persist.pop().unwrap());
            persist_obj(unsafe { node.deref(pool) }, false);
        }
        persist_obj(&self.e_request, false);
        persist_obj(&*self.e_state[ind], false);
        sfence();
        self.e_index.store(ind, Ordering::SeqCst);
        persist_obj(&self.e_index, false);
        sfence();
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        E_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        self.e_state[self.e_index.load(Ordering::SeqCst)].return_val[tid]
            .clone()
            .unwrap()
    }

    /// 실질적인 enqueue: tail 뒤에 새로운 노드 삽입하고 tail로 설정
    fn enqueue(tail: &PAtomic<Node>, arg: Data, pool: &PoolHandle) {
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(unsafe { unprotected() });
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, unprotected()).deref_mut(pool) };
        tail_ref.next.store(new_node, Ordering::SeqCst); // tail.next = new node
        tail.store(new_node, Ordering::SeqCst); // tail = new node
    }
}

/// Deq
impl PBCombQueue {
    /// dequeue 요청 등록 후 실행 (thread-local)
    fn PBQueueDnq(&mut self, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // 요청 등록
        self.d_request[tid].func = Some(Func::DEQUEUE);
        self.d_request[tid].store_act_seq(!self.d_request[tid].load_activate(), seq);

        // 실행
        self.PerformDeqReq(tid, pool)
    }

    /// dequeue 요청 실행 (thread-local)
    fn PerformDeqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // deq combiner 결정
        let mut lval;
        loop {
            lval = D_LOCK.load(Ordering::SeqCst);

            // lval이 홀수라면 이미 누가 lock잡고 combine 수행하고 있는 것.
            // lval이 짝수라면 내가 lock잡고 combiner 되기를 시도
            if lval % 2 == 0 {
                match D_LOCK.compare_exchange(
                    lval,
                    lval.wrapping_add(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        lval = lval.wrapping_add(1);
                        break;
                    }
                    Err(cur) => lval = cur,
                }
            }

            // non-comibner는 combiner가 lock 풀 때까지 busy waiting한 뒤, combiner가 준 결과만 받아감
            let backoff = Backoff::new();
            while lval == D_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }
            if self.d_request[tid].load_activate()
                == self.d_state[self.d_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // 자신의 op을 처리한 combiner가 끝날때까지 기다렸다가 결과 반환
                let deactivate_lval = D_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while !(deactivate_lval < D_LOCK.load(Ordering::SeqCst)) {
                    backoff.snooze();
                }

                return self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid]
                    .clone()
                    .unwrap();
            }
        }

        // deq combiner는 쌓인 deq 요청들을 수행
        let ind = 1 - self.d_index.load(Ordering::SeqCst);
        self.d_state[ind] = self.d_state[self.d_index.load(Ordering::SeqCst)].clone(); // create a copy of current state

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if self.d_request[q].load_activate()
                != self.d_state[ind].deactivate[q].load(Ordering::SeqCst)
            {
                let ret_val;
                // 확실히 persist된 노드들만 deq 수행. OLD_TAIL부터는 현재 enq 중인거라 persist 보장되지 않음
                if OLD_TAIL.load(Ordering::SeqCst)
                    != self.d_state[ind]
                        .head
                        .load(Ordering::SeqCst, unsafe { unprotected() })
                        .into_usize()
                {
                    let node = Self::dequeue(&self.d_state[ind].head, pool);
                    ret_val = ReturnVal::DeqRetVal(node);
                } else {
                    ret_val = ReturnVal::DeqRetVal(PPtr::null());
                }
                D_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                self.d_state[ind].return_val[q] = Some(ret_val);
                self.d_state[ind].deactivate[q]
                    .store(self.d_request[q].load_activate(), Ordering::SeqCst);
            }
        }
        persist_obj(&self.d_request, false);
        persist_obj(&*self.d_state[ind], false);
        sfence();
        self.d_index.store(ind, Ordering::SeqCst);
        persist_obj(&self.d_index, false);
        sfence();
        D_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid]
            .clone()
            .unwrap()
    }

    /// 실질적인 dequeue: head 한 칸 전진하고 old head를 반환
    fn dequeue(head: &PAtomic<Node>, pool: &PoolHandle) -> PPtr<Node> {
        let head_ref = unsafe { head.load(Ordering::SeqCst, unprotected()).deref(pool) };

        // NOTE: 얘네 구현은 데이터를 반환하질 않고 노드를 반환.
        // 노드를 dequeue해간 애는 다 썼다고 함부로 노드 free하면 안됨. queue의 sentinel 노드로 남아있을 수 있음. TODO: 그냥 data 반환해도 될 것같은데..왜지
        let ret = head_ref
            .next
            .load(Ordering::SeqCst, unsafe { unprotected() });
        if !ret.is_null() {
            head.store(ret, Ordering::SeqCst);
        }
        PPtr::from(ret.into_usize())
    }
}

impl TestQueue for PBCombQueue {
    type EnqInput = (usize, usize, &'static mut usize); // value, tid, sequence number
    type DeqInput = (usize, &'static mut usize); // tid, sequence number

    fn enqueue(&self, (value, tid, seq): Self::EnqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const PBCombQueue as *mut PBCombQueue).as_mut() }.unwrap();

        // enq
        let _ = queue.PBQueue(Func::ENQUEUE, value, *seq, tid, pool);
        *seq += 1;
        persist_obj(seq, true);
    }

    fn dequeue(&self, (tid, seq): Self::DeqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const PBCombQueue as *mut PBCombQueue).as_mut() }.unwrap();

        // deq
        let _ = queue.PBQueue(Func::DEQUEUE, 0, *seq, tid, pool);
        *seq += 1;
        persist_obj(seq, true);
    }
}

#[derive(Debug)]
pub struct TestPBCombQueue {
    queue: PBCombQueue,
}

impl Collectable for TestPBCombQueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestPBCombQueue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let mut queue = PBCombQueue::pdefault(pool);

        // 초기 노드 삽입
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            let _ = queue.PBQueue(Func::ENQUEUE, i, 0, 1, pool); // tid 1
        }
        Self { queue }
    }
}

impl TestNOps for TestPBCombQueue {}

#[derive(Debug, Default)]
pub struct TestPBCombQueueEnqDeq<const PAIR: bool> {
    enq_seq: CachePadded<usize>,
    deq_seq: CachePadded<usize>,
}

impl<const PAIR: bool> Collectable for TestPBCombQueueEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestPBCombQueueEnqDeq<PAIR>> for TestPBCombQueue {
    fn run(
        &self,
        mmt: &mut TestPBCombQueueEnqDeq<PAIR>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_seq =
                    unsafe { (&*mmt.enq_seq as *const _ as *mut usize).as_mut() }.unwrap();
                let deq_seq =
                    unsafe { (&*mmt.deq_seq as *const _ as *mut usize).as_mut() }.unwrap();
                let enq_input = (tid, tid, enq_seq); // `tid` 값을 enq. 특별한 이유는 없음
                let deq_input = (tid, deq_seq);

                if PAIR {
                    enq_deq_pair(q, enq_input, deq_input, guard, pool);
                } else {
                    enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
                }
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
