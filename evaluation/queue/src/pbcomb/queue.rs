//! Implementation of PBComb queue (The performance power of software combining in persistence, PPoPP '22)
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

const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
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
    next: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
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
    tail: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
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
    head: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
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
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: AtomicUsize = AtomicUsize::new(0);
    static ref E_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0));

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
    e_request: [CachePadded<RequestRec>; MAX_THREADS + 1],
    e_state: [CachePadded<EStateRec>; 2],
    e_index: AtomicUsize,

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_request: [CachePadded<RequestRec>; MAX_THREADS + 1],
    d_state: [CachePadded<DStateRec>; 2],
    d_index: AtomicUsize,
}

impl Collectable for PBCombQueue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        assert!(s.dummy.is_null());
        Collectable::mark(unsafe { s.dummy.deref_mut(pool) }, tid, gc);

        for t in 1..MAX_THREADS + 1 {
            Collectable::filter(&mut *s.e_request[t], tid, gc, pool);
            Collectable::filter(&mut *s.d_request[t], tid, gc, pool);
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
            e_request: array_init(|_| CachePadded::new(Default::default())),
            e_state: array_init(|_| {
                CachePadded::new(EStateRec {
                    tail: PAtomic::from(dummy),
                    return_val: array_init(|_| None),
                    deactivate: array_init(|_| AtomicBool::new(false)),
                })
            }),
            e_index: Default::default(),
            d_request: array_init(|_| CachePadded::new(Default::default())),
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
    /// Re-run enq or deq that crashed recently (exactly-once)
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
    fn PBQueueEnq(&mut self, arg: Data, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // request enq
        self.e_request[tid].func = Some(Func::ENQUEUE);
        self.e_request[tid].arg = arg;
        self.e_request[tid].store_act_seq(!self.e_request[tid].load_activate(), seq);

        // perform
        self.PerformEnqReq(tid, pool)
    }

    fn PerformEnqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // decide enq combiner
        let mut lval;
        loop {
            lval = E_LOCK.load(Ordering::SeqCst);

            // odd: someone already combining.
            // even: there is no comiber, so try to be combiner.
            if lval % 2 == 0 {
                match E_LOCK.compare_exchange(
                    lval,
                    lval.wrapping_add(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        lval = lval.wrapping_add(1);
                        break; // i am combiner
                    }
                    Err(cur) => lval = cur,
                }
            }

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == E_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }
            if self.e_request[tid].load_activate()
                == self.e_state[self.e_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // wait until the combiner that processed my op is finished
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

        // enq combiner executes the enq requests
        let ind = 1 - self.e_index.load(Ordering::SeqCst);
        self.e_state[ind] = self.e_state[self.e_index.load(Ordering::SeqCst)].clone(); // create a copy of current state
        OLD_TAIL.store(
            self.e_state[ind]
                .tail
                .load(Ordering::SeqCst, unsafe { unprotected() })
                .into_usize(),
            Ordering::SeqCst,
        );

        // collect the enqueued nodes here and persist them all at once
        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `q` thread has a request that is not yet applied
                if self.e_request[q].load_activate()
                    != self.e_state[ind].deactivate[q].load(Ordering::SeqCst)
                {
                    // reserve persist(current tail)
                    let tail_addr = self.e_state[ind]
                        .tail
                        .load(Ordering::SeqCst, unsafe { unprotected() })
                        .into_usize();
                    match to_persist.binary_search(&tail_addr) {
                        Ok(_) => {} // no duplicate
                        Err(idx) => to_persist.insert(idx, tail_addr),
                    }

                    // enq
                    Self::enqueue(&mut self.e_state[ind].tail, self.e_request[q].arg, pool);
                    E_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                    self.e_state[ind].return_val[q] = Some(ReturnVal::EnqRetVal(()));
                    self.e_state[ind].deactivate[q]
                        .store(self.e_request[q].load_activate(), Ordering::SeqCst);

                    // count
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }
        let tail_addr = self.e_state[ind]
            .tail
            .load(Ordering::SeqCst, unsafe { unprotected() })
            .into_usize();
        match to_persist.binary_search(&tail_addr) {
            Ok(_) => {} // no duplicate
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
    fn PBQueueDnq(&mut self, seq: usize, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // request deq
        self.d_request[tid].func = Some(Func::DEQUEUE);
        self.d_request[tid].store_act_seq(!self.d_request[tid].load_activate(), seq);

        // perform
        self.PerformDeqReq(tid, pool)
    }

    fn PerformDeqReq(&mut self, tid: usize, pool: &PoolHandle) -> ReturnVal {
        // decide deq combiner
        let mut lval;
        loop {
            lval = D_LOCK.load(Ordering::SeqCst);

            // odd: someone already combining.
            // even: there is no comiber, so try to be combiner.
            if lval % 2 == 0 {
                match D_LOCK.compare_exchange(
                    lval,
                    lval.wrapping_add(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        lval = lval.wrapping_add(1);
                        break; // i am combiner
                    }
                    Err(cur) => lval = cur,
                }
            }

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == D_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }
            if self.d_request[tid].load_activate()
                == self.d_state[self.d_index.load(Ordering::SeqCst)].deactivate[tid]
                    .load(Ordering::SeqCst)
            {
                // wait until the combiner that processed my op is finished
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

        // deq combiner executes the deq requests
        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if self.d_request[q].load_activate()
                != self.d_state[ind].deactivate[q].load(Ordering::SeqCst)
            {
                let ret_val;
                // only nodes that are persisted can be dequeued.
                // from `OLD_TAIL`, persist is not guaranteed as it is currently enqueud.
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

                    serve_reqs += 1;
            }
        }
            if serve_reqs == 0 {
                break;
            }
        }
        sfence();
        self.d_index.store(ind, Ordering::SeqCst);
        persist_obj(&self.d_index, false);
        sfence();
        D_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        self.d_state[self.d_index.load(Ordering::SeqCst)].return_val[tid]
            .clone()
            .unwrap()
    }

    fn dequeue(head: &PAtomic<Node>, pool: &PoolHandle) -> PPtr<Node> {
        let head_ref = unsafe { head.load(Ordering::SeqCst, unprotected()).deref(pool) };

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
                let enq_input = (tid, tid, enq_seq);
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
