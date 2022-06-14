//! Detectable Combining queue
#![allow(non_snake_case)]
#![allow(warnings)]
use crate::ds::spin_lock_volatile::VSpinLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::Checkpoint;
use crate::pmem::{
    global_pool, persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle,
};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::ok_or;
use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use tinyvec::tiny_vec;

const MAX_THREADS: usize = 64;
type Data = usize;

const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

/// client for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    req: Checkpoint<PAtomic<RequestRec>>,
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.req, tid, gc, pool);
    }
}

/// client for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    req: Checkpoint<PAtomic<RequestRec>>,
    return_val: Checkpoint<Option<ReturnVal>>,
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.req, tid, gc, pool);
        Checkpoint::filter(&mut deq.return_val, tid, gc, pool);
    }
}

// Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
/// function type of queue
#[derive(Debug, Copy, Clone)]
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
    /// TODO: doc
    pub fn enq_retval(self) -> Option<()> {
        match self {
            ReturnVal::EnqRetVal(v) => Some(v),
            _ => None,
        }
    }
    /// TODO: doc
    pub fn deq_retval(self) -> Option<Data> {
        match self {
            ReturnVal::DeqRetVal(v) => Some(v),
            _ => None,
        }
    }
}

impl Collectable for ReturnVal {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // no-op
    }
}

#[derive(Debug, Default)]
struct RequestRec {
    func: Option<Func>,
    arg: usize,
    activate: usize,
}

impl Collectable for RequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// Node
#[derive(Debug)]
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
#[derive(Debug)]
struct EStateRec {
    tail: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
    return_val: [Option<ReturnVal>; MAX_THREADS + 1], // TODO: 실험 스레드 수만큼만 동적할당. 그래야 이 state를 persist할 때의 비용 낭비를 줄임
    deactivate: [AtomicUsize; MAX_THREADS + 1], // TODO: 실험 스레드 수만큼만 동적할당. 그래야 이 state를 persist할 때의 비용 낭비를 줄임
}

impl Clone for EStateRec {
    fn clone(&self) -> Self {
        Self {
            tail: self.tail.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicUsize::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
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
#[derive(Debug)]
struct DStateRec {
    head: PAtomic<Node>, // NOTE: Atomic type to restrict reordering. We use this likes plain pointer.
    return_val: [Option<ReturnVal>; MAX_THREADS + 1], // TODO: 실험 스레드 수만큼만 동적할당. 그래야 이 state를 persist할 때의 비용 낭비를 줄임
    deactivate: [AtomicUsize; MAX_THREADS + 1], // TODO: 실험 스레드 수만큼만 동적할당. 그래야 이 state를 persist할 때의 비용 낭비를 줄임
}

impl Clone for DStateRec {
    fn clone(&self) -> Self {
        Self {
            head: self.head.clone(),
            return_val: self.return_val.clone(),
            deactivate: array_init(|i| AtomicUsize::new(self.deactivate[i].load(Ordering::SeqCst))),
        }
    }
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
    static ref E_LOCK: CachePadded<VSpinLock> = CachePadded::new(VSpinLock::default());
    static ref E_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: CachePadded<VSpinLock> = CachePadded::new(VSpinLock::default());
    static ref D_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
}

/// Detectable Combining Queue
#[derive(Debug)]
pub struct Queue {
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

impl Collectable for Queue {
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

impl PDefault for Queue {
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

/// Enq
impl Queue {
    const EMPTY: usize = usize::MAX;

    /// enq
    /// TODO: 함수명 그냥 enqueue로 바꾸기
    pub fn PBQueueEnq<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // make new request
        let req = POwned::new(
            RequestRec {
                func: Some(Func::ENQUEUE),
                arg,
                activate: self.e_request[tid].activate + 1,
            },
            pool,
        );
        persist_obj(unsafe { req.deref(pool) }, true);
        let my_req = ok_or!(
            enq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                let dup_req = e.new.load(Ordering::Relaxed, guard);
                drop(dup_req.into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // register request
        let my_req_ref = unsafe { my_req.deref(pool) };
        self.e_request[tid].func = my_req_ref.func;
        self.e_request[tid].arg = my_req_ref.arg;
        sfence();
        self.e_request[tid].activate = my_req_ref.activate;
        sfence();

        // perform
        self.PerformEnqReq::<REC>(tid, enq, guard, pool)
    }

    fn PerformEnqReq<const REC: bool>(
        &mut self,
        tid: usize,
        enq: &mut Enqueue,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // decide enq combiner
        let (lval, lockguard) = loop {
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => {
                    if lval % 2 == 0 {
                        continue; // fail but retry because there is no combiner
                    }
                    lval
                }
            };

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == E_LOCK.peek().0 {
                backoff.snooze();
            }

            let lastest_state = unsafe { self.e_state.load(Ordering::SeqCst, guard).deref(pool) };
            if self.e_request[tid].activate <= lastest_state.deactivate[tid].load(Ordering::SeqCst)
            {
                if E_LOCK_VALUE.load(Ordering::SeqCst) == lval {
                    // enq의 retval은 항상 unit이라 내꺼 따로 구분할 필요 없음
                    return lastest_state.return_val[tid].clone().unwrap();
                }

                // wait until the combiner that processed my op is finished
                backoff.reset();
                while E_LOCK.peek().0 == lval + 2 {
                    backoff.snooze();
                }
                return lastest_state.return_val[tid].clone().unwrap();
            }
        };

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
                let e_req_qid = &mut *self.e_request[q];
                if e_req_qid.activate > new_state_ref.deactivate[q].load(Ordering::SeqCst) {
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
                    Self::enqueue(&new_state_ref.tail, e_req_qid.arg, guard, pool);

                    new_state_ref.return_val[q] = Some(ReturnVal::EnqRetVal(()));
                    new_state_ref.deactivate[q].store(e_req_qid.activate, Ordering::SeqCst);

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
        self.e_state.store(new_state, Ordering::SeqCst); // global에 박기 (commit point)

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

        // per-thread state의 old/new 뒤집기. 위에서 global에 박고 이건 못한채 crash나도 괜찮다. combiner는 어차피 global을 copy해오고 시작함
        self.e_thread_state[tid]
            .index
            .store(1 - ind, Ordering::SeqCst);
        drop(lockguard);
        // enq의 retval은 항상 unit이라 내꺼 따로 구분할 필요 없음
        return new_state_ref.return_val[tid].clone().unwrap();
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
impl Queue {
    /// deq
    /// TODO: 함수명 그냥 dequeue로 바꾸기
    pub fn PBQueueDeq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // make new request
        let mut req = POwned::new(
            RequestRec {
                func: Some(Func::DEQUEUE),
                arg: tid,
                activate: self.d_request[tid].activate + 1,
            },
            pool,
        );
        let req_ref = unsafe { req.deref_mut(pool) };
        persist_obj(req_ref, true);
        let my_req = ok_or!(
            deq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                let dup_req = e.new.load(Ordering::Relaxed, guard);
                drop(dup_req.into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // register request
        let my_req_ref = unsafe { my_req.deref(pool) };
        self.d_request[tid].arg = my_req_ref.arg;
        self.d_request[tid].func = my_req_ref.func;
        sfence();
        self.d_request[tid].activate = my_req_ref.activate;
        sfence();

        // perform
        self.PerformDeqReq::<REC>(tid, deq, guard, pool)
    }

    fn PerformDeqReq<const REC: bool>(
        &mut self,
        tid: usize,
        deq: &mut Dequeue,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> ReturnVal {
        // decide deq combiner
        let (lval, lockguard) = loop {
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => {
                    if lval % 2 == 0 {
                        continue; // fail but retry because there is no combiner
                    }
                    lval
                }
            };

            // non-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while lval == D_LOCK.peek().0 {
                backoff.snooze();
            }

            // checkpoint deactivate of mmt-local request
            let lastest_state = unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) };
            if self.d_request[tid].activate <= lastest_state.deactivate[tid].load(Ordering::SeqCst)
            {
                if D_LOCK_VALUE.load(Ordering::SeqCst) == lval {
                    // checkpoint return value of mmt-local request
                    return ok_or!(
                        deq.return_val.checkpoint::<REC>(
                            lastest_state.return_val[tid].clone(),
                            tid,
                            pool
                        ),
                        e,
                        e.current
                    )
                    .unwrap();
                }

                // wait until the combiner that processed my op is finished
                backoff.reset();
                while D_LOCK.peek().0 == lval + 2 {
                    backoff.snooze();
                }
                // checkpoint return value of mmt-local request
                return ok_or!(
                    deq.return_val.checkpoint::<REC>(
                        lastest_state.return_val[tid].clone(),
                        tid,
                        pool
                    ),
                    e,
                    e.current
                )
                .unwrap();
            }
        };

        // deq combiner executes the deq requests
        let ind = self.d_thread_state[tid].index.load(Ordering::SeqCst);
        let mut new_state = self.d_thread_state[tid].state[ind].load(Ordering::SeqCst, guard);
        let new_state_ref = unsafe { new_state.deref_mut(pool) };
        *new_state_ref = unsafe { self.d_state.load(Ordering::SeqCst, guard).deref(pool) }.clone(); // create a copy of current state

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `q` thread has a request that is not yet applied
                let d_req_qid = &mut *self.d_request[q];
                if d_req_qid.activate > new_state_ref.deactivate[q].load(Ordering::SeqCst) {
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
                    new_state_ref.deactivate[q].store(d_req_qid.activate, Ordering::SeqCst);

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

        // checkpoint return value of mmt-local request
        let ret_val = ok_or!(
            deq.return_val
                .checkpoint::<REC>(new_state_ref.return_val[tid].clone(), tid, pool),
            e,
            e.current
        )
        .unwrap();
        drop(lockguard);
        return ret_val;
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

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use crate::pmem::{Collectable, GarbageCollection, PoolHandle, RootObj};
    use crate::test_utils::tests::{run_test, TestRootObj, JOB_FINISHED, RESULTS};
    use crossbeam_epoch::Guard;

    use super::{Dequeue, Enqueue, Queue};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    struct EnqDeq {
        enqs: [Enqueue; COUNT],
        deqs: [Dequeue; COUNT],
    }

    impl Default for EnqDeq {
        fn default() -> Self {
            Self {
                enqs: array_init::array_init(|_| Enqueue::default()),
                deqs: array_init::array_init(|_| Dequeue::default()),
            }
        }
    }

    impl Collectable for EnqDeq {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..COUNT {
                Enqueue::filter(&mut m.enqs[i], tid, gc, pool);
                Dequeue::filter(&mut m.deqs[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<Queue> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // Get &mut queue
            let queue = unsafe { (&self.obj as *const _ as *mut Queue).as_mut() }.unwrap();

            match tid {
                // T1: Check results of other threads
                1 => {
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::default();
                    let res = queue.PBQueueDeq::<true>(&mut tmp_deq, tid, guard, pool);
                    let v = res.deq_retval().unwrap();
                    println!("check last deq v={v}");
                    assert!(v == Queue::EMPTY);

                    // Check results
                    assert!(RESULTS[1].load(Ordering::SeqCst) == 0);
                    assert!((2..NR_THREAD + 2).all(|tid| {
                        println!(" RESULTS[{tid}] = {}", RESULTS[tid].load(Ordering::SeqCst));
                        RESULTS[tid].load(Ordering::SeqCst) == COUNT
                    }));
                }
                // other threads: { enq; deq; }
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let val = tid;
                        let _ =
                            queue.PBQueueEnq::<true>(val, &mut enq_deq.enqs[i], tid, guard, pool);

                        let res = queue.PBQueueDeq::<true>(&mut enq_deq.deqs[i], tid, guard, pool);
                        let v = res.deq_retval().unwrap();
                        // println!("deq v={v}");
                        assert!(v != Queue::EMPTY);

                        // send output of deq
                        let _ = RESULTS[v].fetch_add(1, Ordering::SeqCst);
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "combining_enq_deq.pool";
        const FILE_SIZE: usize = 32 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}
