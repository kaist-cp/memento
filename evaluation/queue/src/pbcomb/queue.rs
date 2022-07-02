//! Implementation of PBComb queue (The performance power of software combining in persistence, PPoPP '22)
#![allow(non_snake_case)]
use array_init::array_init;
use core::sync::atomic::Ordering;
use crossbeam_epoch::{unprotected, Guard};
use crossbeam_utils::{Backoff, CachePadded};
use memento::pepoch::atomic::Pointer;
use memento::pepoch::{PAtomic, PDestroyable, POwned};
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{mfence, persist_obj, pool::*, sfence, PPtr};
use memento::PDefault;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tinyvec::tiny_vec;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

use super::Data;

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
    DeqRetVal(Data),
}

impl ReturnVal {
    pub fn enq_retval(self) -> Option<()> {
        match self {
            ReturnVal::EnqRetVal(v) => Some(v),
            _ => None,
        }
    }

    pub fn deq_retval(self) -> Option<Data> {
        match self {
            ReturnVal::DeqRetVal(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct RequestRec {
    func: Option<Func>,
    arg: usize,
    activate: u32,
    valid: u32,
}

impl Collectable for RequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// Node
#[derive(Debug)]
#[repr(align(128))]
pub struct Node {
    data: Data,
    next: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// State of Enqueue PBComb
#[derive(Debug, Clone)]
struct EStateRec {
    tail: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
    return_val: [Option<ReturnVal>; MAX_THREADS + 1],
    deactivate: [bool; MAX_THREADS + 1],
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.tail, tid, gc, pool);
    }
}

#[derive(Debug)]
struct EThreadState {
    state: [PAtomic<EStateRec>; 2],
    index: AtomicUsize, // indicate what is consistent state
}

/// State of Dequeue PBComb
#[derive(Debug, Clone)]
struct DStateRec {
    head: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
    return_val: [Option<ReturnVal>; MAX_THREADS + 1],
    deactivate: [bool; MAX_THREADS + 1],
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.head, tid, gc, pool);
    }
}

#[derive(Debug)]
struct DThreadState {
    state: [PAtomic<DStateRec>; 2],
    index: AtomicUsize, // indicate what is consistent state
}

// Shared volatile variables
lazy_static::lazy_static! {
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
    static ref E_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
    static ref D_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
}

/// TODO: doc
#[derive(Debug)]
pub struct PBCombQueue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    // global
    e_request: [CachePadded<RequestRec>; MAX_THREADS + 1],
    e_state: CachePadded<PAtomic<EStateRec>>, // global state
    // per-thread
    e_thread_state: [CachePadded<EThreadState>; MAX_THREADS + 1],
    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    // global
    d_request: [CachePadded<RequestRec>; MAX_THREADS + 1],
    d_state: CachePadded<PAtomic<DStateRec>>,
    // per-thread
    d_thread_state: [CachePadded<DThreadState>; MAX_THREADS + 1],
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
            e_state: CachePadded::new(PAtomic::new(
                EStateRec {
                    tail: PAtomic::from(dummy),
                    return_val: array_init(|_| Default::default()),
                    deactivate: array_init(|_| Default::default()),
                },
                pool,
            )),
            e_thread_state: array_init(|_| {
                CachePadded::new(EThreadState {
                    state: array_init(|_| {
                        PAtomic::new(
                            EStateRec {
                                tail: PAtomic::from(dummy),
                                return_val: array_init(|_| Default::default()),
                                deactivate: array_init(|_| Default::default()),
                            },
                            pool,
                        )
                    }),
                    index: AtomicUsize::default(),
                })
            }),
            d_request: array_init(|_| CachePadded::new(Default::default())),
            d_state: CachePadded::new(PAtomic::new(
                DStateRec {
                    head: PAtomic::from(dummy),
                    return_val: array_init(|_| Default::default()),
                    deactivate: array_init(|_| Default::default()),
                },
                pool,
            )),
            d_thread_state: array_init(|_| {
                CachePadded::new(DThreadState {
                    state: array_init(|_| {
                        PAtomic::new(
                            DStateRec {
                                head: PAtomic::from(dummy),
                                return_val: array_init(|_| Default::default()),
                                deactivate: array_init(|_| Default::default()),
                            },
                            pool,
                        )
                    }),
                    index: AtomicUsize::default(),
                })
            }),
        }
    }
}

impl PBCombQueue {
    /// normal run
    pub fn PBQueue(
        &mut self,
        func: Func,
        arg: Data,
        seq: u32,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        match func {
            Func::ENQUEUE => self.PBQueueEnq(arg, seq, tid, guard, pool),
            Func::DEQUEUE => self.PBQueueDnq(seq, tid, guard, pool),
        }
    }

    /// recovery run
    ///
    /// Re-run enq or deq that crashed recently (exactly-once)
    pub fn recover(
        &mut self,
        func: Func,
        arg: Data,
        seq: u32,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // set OLD_TAIL if it is dummy node
        let ltail = unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) }
            .tail
            .load(Ordering::SeqCst, guard);
        let _ = OLD_TAIL.compare_exchange(
            self.dummy.into_offset(),
            ltail.into_usize(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        match func {
            Func::ENQUEUE => {
                self.e_request[tid].func = Some(func);
                self.e_request[tid].arg = arg;
                mfence();
                self.e_request[tid].activate = seq % 2;
                self.e_request[tid].valid = 1;
                mfence();

                // check activate and re-execute if request is not yet applied
                if unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) }.deactivate[tid]
                    != (seq % 2 == 1)
                {
                    return self.PerformEnqReq(tid, guard, pool);
                }

                // return value if request is already applied
                return unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) }
                    .return_val[tid]
                    .clone()
                    .unwrap();
            }
            Func::DEQUEUE => {
                self.d_request[tid].func = Some(func);
                self.d_request[tid].arg = arg;
                mfence();
                self.d_request[tid].activate = seq % 2;
                self.d_request[tid].valid = 1;
                mfence();

                // check activate and re-execute if request is not yet applied
                if unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) }.deactivate[tid]
                    != (seq % 2 == 1)
                {
                    return self.PerformDeqReq(tid, guard, pool);
                }

                // return value if request is already applied
                return unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) }
                    .return_val[tid]
                    .clone()
                    .unwrap();
            }
        }
    }
}

/// Enq
impl PBCombQueue {
    const EMPTY: usize = usize::MAX;

    fn PBQueueEnq(
        &mut self,
        arg: Data,
        _seq: u32,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // request enq

        self.e_request[tid].func = Some(Func::ENQUEUE);
        self.e_request[tid].arg = arg;
        mfence();
        self.e_request[tid].activate = 1 - self.e_request[tid].activate;
        if self.e_request[tid].valid == 0 {
            self.e_request[tid].valid = 1;
        }
        mfence();

        // perform
        self.PerformEnqReq(tid, guard, pool)
    }

    fn PerformEnqReq(&mut self, tid: usize, guard: &Guard, pool: &PoolHandle) -> ReturnVal {
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
                    Err(_) => continue,
                }
            }

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == E_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }
            let last_state = unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) };
            if self.e_request[tid].activate == last_state.deactivate[tid] as u32 {
                if E_LOCK_VALUE.load(Ordering::SeqCst) == lval {
                    return last_state.return_val[tid].clone().unwrap();
                }

                // wait until the combiner that processed my op is finished
                backoff.reset();
                while E_LOCK.load(Ordering::SeqCst) == lval + 2 {
                    backoff.snooze();
                }
                return last_state.return_val[tid].clone().unwrap();
            }
        }

        // enq combiner executes the enq requests
        let ind = self.e_thread_state[tid].index.load(Ordering::SeqCst);
        let mut new_state = self.e_thread_state[tid].state[ind].load(Ordering::SeqCst, guard);
        let new_state_ref = unsafe { new_state.deref_mut(pool) };
        *new_state_ref = unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) }.clone(); // create a copy of current state

        // collect the enqueued nodes here and persist them all at once
        let mut to_persist = tiny_vec!([usize; 1024]);

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `q` thread has a request that is not yet applied
                if self.e_request[q].activate != new_state_ref.deactivate[q] as u32
                    && self.e_request[q].valid == 1
                {
                    // reserve persist(current tail)
                    let tail_addr = new_state_ref
                        .tail
                        .load(Ordering::SeqCst, guard)
                        .into_usize();
                    match to_persist.binary_search(&tail_addr) {
                        Ok(_) => {} // no duplicate
                        Err(idx) => to_persist.insert(idx, tail_addr),
                    }

                    // enq
                    Self::enqueue(&new_state_ref.tail, self.e_request[q].arg, guard, pool);
                    new_state_ref.return_val[q] = Some(ReturnVal::EnqRetVal(()));
                    new_state_ref.deactivate[q] = self.e_request[q].activate == 1;

                    // count
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }

        // ``` final_persist_func
        let tail_addr = new_state_ref
            .tail
            .load(Ordering::SeqCst, guard)
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
        // ```

        persist_obj(new_state_ref, false);
        sfence();

        E_LOCK_VALUE.store(lval, Ordering::SeqCst);
        self.e_state.store(new_state, Ordering::SeqCst); // commit point

        persist_obj(&*self.e_state, false);
        sfence();

        // ``` after_persist_func
        OLD_TAIL.store(
            new_state_ref
                .tail
                .load(Ordering::SeqCst, guard)
                .into_usize(),
            Ordering::SeqCst,
        );
        // ```

        // Flip old/new of per-thread state.
        self.e_thread_state[tid]
            .index
            .store(1 - ind, Ordering::SeqCst);
        E_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        new_state_ref.return_val[tid].clone().unwrap()
    }

    fn enqueue(tail: &PAtomic<Node>, arg: Data, guard: &Guard, pool: &PoolHandle) {
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(guard);
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, guard).deref_mut(pool) };
        tail_ref.next.store(new_node, Ordering::SeqCst); // tail.next = new node
        tail.store(new_node, Ordering::SeqCst); // tail = new node
    }
}

/// Deq
impl PBCombQueue {
    fn PBQueueDnq(&mut self, _seq: u32, tid: usize, guard: &Guard, pool: &PoolHandle) -> ReturnVal {
        // request deq
        self.d_request[tid].func = Some(Func::DEQUEUE);
        self.d_request[tid].activate = 1 - self.d_request[tid].activate;
        mfence();
        if self.d_request[tid].valid == 0 {
            self.d_request[tid].valid = 1;
        }
        mfence();

        // perform
        self.PerformDeqReq(tid, guard, pool)
    }

    fn PerformDeqReq(&mut self, tid: usize, guard: &Guard, pool: &PoolHandle) -> ReturnVal {
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
                    Err(_) => continue,
                }
            }

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == D_LOCK.load(Ordering::SeqCst) {
                backoff.snooze();
            }

            let last_state = unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) };
            if self.d_request[tid].activate == last_state.deactivate[tid] as u32 {
                if D_LOCK_VALUE.load(Ordering::SeqCst) == lval {
                    return last_state.return_val[tid].clone().unwrap();
                }

                // wait until the combiner that processed my op is finished
                backoff.reset();
                while D_LOCK.load(Ordering::SeqCst) == lval + 2 {
                    backoff.snooze();
                }
                return last_state.return_val[tid].clone().unwrap();
            }
        }

        // deq combiner executes the deq requests
        let ind = self.d_thread_state[tid].index.load(Ordering::SeqCst);
        let mut new_state = self.d_thread_state[tid].state[ind].load(Ordering::SeqCst, guard);
        let new_state_ref = unsafe { new_state.deref_mut(pool) };
        *new_state_ref = unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) }.clone(); // create a copy of current state

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `t` thread has a request that is not yet applied
                if self.d_request[q].activate != new_state_ref.deactivate[q] as u32
                    && self.d_request[q].valid == 1
                {
                    let ret_val;
                    // only nodes that are persisted can be dequeued.
                    // from `OLD_TAIL`, persist is not guaranteed as it is currently enqueud.
                    if OLD_TAIL.load(Ordering::SeqCst)
                        != new_state_ref
                            .head
                            .load(Ordering::SeqCst, guard)
                            .into_usize()
                    {
                        let node = Self::dequeue(&new_state_ref.head, guard, pool);
                        ret_val = ReturnVal::DeqRetVal(node);
                    } else {
                        ret_val = ReturnVal::DeqRetVal(Self::EMPTY);
                    }
                    new_state_ref.return_val[q] = Some(ret_val);
                    new_state_ref.deactivate[q] = self.d_request[q].activate == 1;

                    // cnt
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }

        persist_obj(new_state_ref, false);
        sfence();

        D_LOCK_VALUE.store(lval, Ordering::SeqCst);
        self.d_state.store(new_state, Ordering::SeqCst);

        persist_obj(&*self.d_state, false);
        sfence();

        self.d_thread_state[tid]
            .index
            .store(1 - ind, Ordering::SeqCst);
        D_LOCK.store(lval.wrapping_add(1), Ordering::SeqCst);
        new_state_ref.return_val[tid].clone().unwrap()
    }

    fn dequeue(head: &PAtomic<Node>, guard: &Guard, pool: &PoolHandle) -> Data {
        let head_shared = head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head_shared.deref(pool) };

        let ret = head_ref.next.load(Ordering::SeqCst, guard);
        if !ret.is_null() {
            head.store(ret, Ordering::SeqCst);
            // NOTE: It should not be deallocated immediately as it may crash during deq combine.
            unsafe { guard.defer_pdestroy(head_shared) };
            return unsafe { ret.deref(pool) }.data;
        }
        Self::EMPTY
    }
}

impl TestQueue for PBCombQueue {
    type EnqInput = (usize, usize, &'static mut u32); // value, tid, sequence number
    type DeqInput = (usize, &'static mut u32, bool, &'static mut Option<usize>); // tid, sequence number, flag for full detectable, return value for full detectable

    fn enqueue(&self, (value, tid, seq): Self::EnqInput, guard: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const PBCombQueue as *mut PBCombQueue).as_mut() }.unwrap();

        // enq
        let _ = queue.PBQueue(Func::ENQUEUE, value, *seq, tid, guard, pool);
        *seq += 1;
        persist_obj(seq, true);
    }

    fn dequeue(&self, (tid, seq, fd, retval): Self::DeqInput, guard: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const PBCombQueue as *mut PBCombQueue).as_mut() }.unwrap();

        // deq
        let ret = queue.PBQueue(Func::DEQUEUE, 0, *seq, tid, guard, pool);
        *seq += 1;
        persist_obj(seq, true);

        // Backup retrun value if it is fully detectable.
        if fd {
            *retval = ret.deq_retval();
            persist_obj(retval, true);
        }
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
            let _ = queue.PBQueue(Func::ENQUEUE, i, 0, 1, unsafe { unprotected() }, pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestPBCombQueue {}

#[derive(Debug, Default)]
pub struct TestPBCombQueueEnqDeq<const PAIR: bool, const FD: bool> {
    enq_seq: CachePadded<u32>,
    deq_seq: CachePadded<u32>,
    deq_retval: CachePadded<Option<usize>>,
}

impl<const PAIR: bool, const FD: bool> Collectable for TestPBCombQueueEnqDeq<PAIR, FD> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool, const FD: bool> RootObj<TestPBCombQueueEnqDeq<PAIR, FD>>
    for TestPBCombQueue
{
    fn run(
        &self,
        mmt: &mut TestPBCombQueueEnqDeq<PAIR, FD>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_seq = unsafe { (&*mmt.enq_seq as *const _ as *mut u32).as_mut() }.unwrap();
                let deq_seq = unsafe { (&*mmt.deq_seq as *const _ as *mut u32).as_mut() }.unwrap();
                let deq_retval =
                    unsafe { (&*mmt.deq_retval as *const _ as *mut Option<usize>).as_mut() }
                        .unwrap();

                let enq_input = (tid, tid, enq_seq);
                let deq_input = (tid, deq_seq, FD, deq_retval);

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
