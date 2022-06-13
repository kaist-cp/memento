//! Combining queue
//!
//! 저장용 (VLDB 제출시의 구현)
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
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use tinyvec::tiny_vec;

const MAX_THREADS: usize = 64;
type Data = usize;

const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

type EnqRetVal = ();
type DeqRetVal = Option<Data>;

/// client for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    req: Checkpoint<PAtomic<EnqRequestRec>>,
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.req, tid, gc, pool);
    }
}

/// client for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    req: Checkpoint<PAtomic<DeqRequestRec>>,
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.req, tid, gc, pool);
    }
}

#[derive(Debug, Default)]
struct EnqRequestRec {
    arg: PAtomic<Node>,
    retval: Option<EnqRetVal>,
    deactivate: AtomicBool,
}

impl Collectable for EnqRequestRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut s.arg, tid, gc, pool);
    }
}

#[derive(Debug, Default)]
struct DeqRequestRec {
    retval: Option<DeqRetVal>,
    deactivate: AtomicBool,
}

impl Collectable for DeqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// Node
#[derive(Debug, Default)]
pub struct Node {
    data: Data,
    next: PAtomic<Node>,
}

impl Collectable for Node {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.next, tid, gc, pool);
    }
}

/// State of Enqueue PBComb
#[derive(Debug)]
struct EStateRec {
    tail: [PAtomic<Node>; 2],
    requests: PAtomic<[PAtomic<EnqRequestRec>; MAX_THREADS + 1]>,
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.tail[0], tid, gc, pool);
        PAtomic::filter(&mut s.tail[1], tid, gc, pool);

        let mut req_arr = s.requests.load(Ordering::SeqCst, unsafe { unprotected() });
        if !req_arr.is_null() {
            let req_arr_ref = unsafe { req_arr.deref_mut(pool) };
            for mut req in req_arr_ref.iter_mut() {
                PAtomic::filter(req, tid, gc, global_pool().unwrap());
            }
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: [PAtomic<Node>; 2],
    requests: PAtomic<[PAtomic<DeqRequestRec>; MAX_THREADS + 1]>,
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.head[0], tid, gc, pool);
        PAtomic::filter(&mut s.head[1], tid, gc, pool);

        let mut req_arr = s.requests.load(Ordering::SeqCst, unsafe { unprotected() });
        if !req_arr.is_null() {
            let req_arr_ref = unsafe { req_arr.deref_mut(pool) };
            for mut req in req_arr_ref.iter_mut() {
                PAtomic::filter(req, tid, gc, global_pool().unwrap());
            }
        }
    }
}

// Shared volatile variables
lazy_static::lazy_static! {
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: VSpinLock = VSpinLock::default();
    static ref E_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0));

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: VSpinLock = VSpinLock::default();
    static ref D_DEACTIVATE_LOCK: [AtomicUsize; MAX_THREADS + 1] = array_init(|_| AtomicUsize::new(0));
}

/// Combining Queue
#[derive(Debug)]
pub struct Queue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,
    e_state: CachePadded<EStateRec>,
    d_state: CachePadded<DStateRec>,
}

impl Collectable for Queue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PPtr::filter(&mut s.dummy, tid, gc, pool);
        EStateRec::filter(&mut *s.e_state, tid, gc, pool);
        DStateRec::filter(&mut *s.d_state, tid, gc, pool);

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

        let e_state = EStateRec {
            tail: [PAtomic::from(dummy), PAtomic::from(dummy)],
            requests: PAtomic::new(array_init(|_| PAtomic::null()), pool),
        };
        let d_state = DStateRec {
            head: [PAtomic::from(dummy), PAtomic::from(dummy)],
            requests: PAtomic::new(array_init(|_| PAtomic::null()), pool),
        };
        Self {
            dummy,
            e_state: CachePadded::new(e_state),
            d_state: CachePadded::new(d_state),
        }
    }
}

/// Enq
impl Queue {
    /// enq
    pub fn PBQueueEnq<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> EnqRetVal {
        // perform
        self.PerformEnqReq::<REC>(arg, enq, tid, guard, pool)
    }

    fn PerformEnqReq<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> EnqRetVal {
        let node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        );
        persist_obj(unsafe { node.deref(pool) }, true);
        let req = POwned::new(
            EnqRequestRec {
                arg: PAtomic::from(node),
                retval: None,
                deactivate: AtomicBool::new(false),
            },
            pool,
        );
        persist_obj(unsafe { req.deref(pool) }, true);
        let my_req = ok_or!(
            enq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                let guard = unprotected();
                let dup_req = e.new.load(Ordering::Relaxed, guard);
                drop(
                    dup_req
                        .deref(pool)
                        .arg
                        .load(Ordering::Relaxed, guard)
                        .into_owned(),
                );
                drop(dup_req.into_owned());
                panic!("??");
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);
        let my_req_ref = unsafe { my_req.deref(pool) };

        // decide enq combiner
        let (lval, lockguard) = loop {
            // try to be combiner.
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };
            let req_arr = self.e_state.requests.load(Ordering::SeqCst, guard);
            let req_arr_ref = unsafe { req_arr.deref(pool) };
            req_arr_ref[tid].store(my_req, Ordering::SeqCst);

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == E_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            // if my_req_ref.deactivate.load(Ordering::SeqCst) {
            if my_req_ref.deactivate.load(Ordering::SeqCst) {
                // wait until the combiner that processed my op is finished
                let deactivate_lval = E_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while deactivate_lval >= E_LOCK.peek().0 {
                    backoff.snooze();
                }
                return my_req_ref.retval.unwrap();
            }
        };

        // enq combiner executes the enq requests
        let req_arr = self.e_state.requests.load(Ordering::SeqCst, guard);
        let req_arr_ref = unsafe { req_arr.deref(pool) };
        req_arr_ref[tid].store(my_req, Ordering::SeqCst);
        let old_tail = self.e_state.tail[req_arr.tag()].load(Ordering::SeqCst, guard);
        let ind = 1 - req_arr.tag();
        self.e_state.tail[ind].store(old_tail, Ordering::SeqCst);
        OLD_TAIL.store(old_tail.into_usize(), Ordering::SeqCst);

        // collect the enqueued nodes here and persist them all at once
        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `q` thread has a request that is not yet applied
                let req_q = req_arr_ref[q].load(Ordering::SeqCst, guard);
                if !req_q.is_null()
                    && !unsafe { req_q.deref(pool) }
                        .deactivate
                        .load(Ordering::SeqCst)
                {
                    // reserve persist(current tail)
                    let tail_addr = self.e_state.tail[ind]
                        .load(Ordering::SeqCst, guard)
                        .into_usize();
                    match to_persist.binary_search(&tail_addr) {
                        Ok(_) => {} // no duplicate
                        Err(idx) => to_persist.insert(idx, tail_addr),
                    }

                    // enq
                    let q_req_ref =
                        unsafe { req_arr_ref[q].load(Ordering::SeqCst, guard).deref_mut(pool) };
                    Self::raw_enqueue(&self.e_state.tail[ind], &q_req_ref.arg, guard, pool);
                    E_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                    q_req_ref.retval = Some(());
                    q_req_ref.deactivate.store(true, Ordering::SeqCst);
                    // 여기서 crash 나면 버그
                    // e.g. t0의 req는 deactivate한게 남아있어서 재시도 안하지만, tail은 옛날로 돌아감. 즉 t0껏도 재실행해야하는데 재시도 못함
                    persist_obj(q_req_ref, false);

                    // count
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }

        let tail_addr = self.e_state.tail[ind]
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
        let new_req_arr = POwned::new(array_init(|_| PAtomic::null()), pool);
        persist_obj(unsafe { new_req_arr.deref(pool) }, true);
        self.e_state
            .requests
            .store(new_req_arr.with_tag(ind), Ordering::SeqCst);
        persist_obj(&*self.e_state, true);
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        drop(lockguard); // release E_LOCK
        my_req_ref.retval.unwrap()
    }

    fn raw_enqueue(
        tail: &PAtomic<Node>,
        new_node: &PAtomic<Node>,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let new_node = new_node.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, guard).deref_mut(pool) };
        tail_ref.next.store(new_node, Ordering::SeqCst); // tail.next = new node
        tail.store(new_node, Ordering::SeqCst); // tail = new node
    }
}

/// Deq
impl Queue {
    /// deq
    pub fn PBQueueDeq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> DeqRetVal {
        self.PerformDeqReq::<REC>(deq, tid, guard, pool)
    }

    fn PerformDeqReq<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> DeqRetVal {
        let req = POwned::new(DeqRequestRec::default(), pool);
        persist_obj(unsafe { req.deref(pool) }, true);

        let my_req = ok_or!(
            deq.req.checkpoint::<REC>(PAtomic::from(req), tid, pool),
            e,
            unsafe {
                drop(e.new.load(Ordering::Relaxed, unprotected()).into_owned());
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);
        let my_req_ref = unsafe { my_req.deref(pool) };

        // decide enq combiner
        let (lval, lockguard) = loop {
            // try to be combiner.
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };

            let req_arr = self.d_state.requests.load(Ordering::SeqCst, guard);
            let req_arr_ref = unsafe { req_arr.deref(pool) };
            req_arr_ref[tid].store(my_req, Ordering::SeqCst);

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == D_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            if my_req_ref.deactivate.load(Ordering::SeqCst) {
                // wait until the combiner that processed my op is finished
                let deactivate_lval = D_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while deactivate_lval >= D_LOCK.peek().0 {
                    backoff.snooze();
                }
                return my_req_ref.retval.unwrap();
            }
        };

        // deq combiner executes the deq requests
        let req_arr = self.d_state.requests.load(Ordering::SeqCst, guard);
        let req_arr_ref = unsafe { req_arr.deref(pool) };
        req_arr_ref[tid].store(my_req, Ordering::SeqCst);
        let ind = 1 - req_arr.tag();
        self.d_state.head[ind].store(
            self.d_state.head[req_arr.tag()].load(Ordering::SeqCst, guard),
            Ordering::SeqCst,
        );

        for _ in 0..COMBINING_ROUNDS {
            let mut serve_reqs = 0;

            for q in 1..unsafe { NR_THREADS } + 1 {
                // if `t` thread has a request that is not yet applied
                let req_q = req_arr_ref[q].load(Ordering::SeqCst, guard);
                if !req_arr_ref[q].load(Ordering::SeqCst, guard).is_null()
                    && !unsafe { req_q.deref(pool) }
                        .deactivate
                        .load(Ordering::SeqCst)
                {
                    let ret_val;
                    // only nodes that are persisted can be dequeued.
                    // from `OLD_TAIL`, persist is not guaranteed as it is currently enqueud.
                    if OLD_TAIL.load(Ordering::SeqCst)
                        != self.d_state.head[ind]
                            .load(Ordering::SeqCst, guard)
                            .into_usize()
                    {
                        ret_val = Self::raw_dequeue(&self.d_state.head[ind], guard, pool);
                    } else {
                        ret_val = None;
                    }
                    let q_req_ref =
                        unsafe { req_arr_ref[q].load(Ordering::SeqCst, guard).deref_mut(pool) };
                    D_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                    q_req_ref.retval = Some(ret_val);
                    q_req_ref.deactivate.store(true, Ordering::SeqCst);
                    persist_obj(q_req_ref, false);

                    // cnt
                    serve_reqs += 1;
                }
            }

            if serve_reqs == 0 {
                break;
            }
        }

        let new_req_arr = POwned::new(array_init(|_| PAtomic::null()), pool);
        persist_obj(unsafe { new_req_arr.deref(pool) }, true);
        self.d_state
            .requests
            .store(new_req_arr.with_tag(ind), Ordering::SeqCst);
        persist_obj(&*self.d_state, true);
        drop(lockguard); // release D_LOCK
        my_req_ref.retval.unwrap()
    }

    fn raw_dequeue(head: &PAtomic<Node>, guard: &Guard, pool: &PoolHandle) -> DeqRetVal {
        let head_shared = head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head_shared.deref(pool) };

        let next = head_ref.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            head.store(next, Ordering::SeqCst);
            // unsafe { drop(head_shared.into_owned()) };
            unsafe { guard.defer_pdestroy(head_shared) }; // NOTE: The original implementation does not free because it returns a node rather than data.
            return Some(unsafe { next.deref(pool) }.data);
        }
        None
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
                    assert!(res.is_none());

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
                        queue.PBQueueEnq::<true>(val, &mut enq_deq.enqs[i], tid, guard, pool);

                        let res = queue.PBQueueDeq::<true>(&mut enq_deq.deqs[i], tid, guard, pool);
                        assert!(!res.is_none());

                        // send output of deq
                        let v = res.unwrap();
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
