//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
#![allow(non_snake_case)]
#![allow(warnings)]
use crate::ds::spin_lock_volatile::VSpinLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, PDestroyable, POwned};
use crate::ploc::Checkpoint;
use crate::pmem::{persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::ok_or;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinyvec::tiny_vec;

const MAX_THREADS: usize = 64;
type Data = usize; // TODO: generic

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

type EnqRetVal = ();
type DeqRetVal = Option<Data>;

/// client for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    req: Checkpoint<EnqRequestRec>,
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.req, tid, gc, pool);
    }
}

/// client for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    req: Checkpoint<DeqRequestRec>,
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.req, tid, gc, pool);
    }
}

#[derive(Debug, Default)]
struct EnqRequestRec {
    arg: usize,
    activate: AtomicBool,
    return_val: Option<EnqRetVal>,
}

impl Collectable for EnqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Clone for EnqRequestRec {
    fn clone(&self) -> Self {
        Self {
            arg: self.arg,
            activate: AtomicBool::new(self.activate.load(Ordering::SeqCst)),
            return_val: self.return_val.clone()
        }
    }
}


#[derive(Debug, Default)]
struct DeqRequestRec {
    activate: AtomicBool,
    return_val: Option<DeqRetVal>,
}

impl Collectable for DeqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl Clone for DeqRequestRec {
    fn clone(&self) -> Self {
        Self {
            activate: AtomicBool::new(self.activate.load(Ordering::SeqCst)),
            return_val: self.return_val.clone()
        }
    }
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
    tail: PAtomic<Node>,
    requests: [EnqRequestRec; MAX_THREADS + 1],
    // return_val: [EnqRetVal; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.tail, tid, gc, pool);
    }
}

impl EStateRec {
    fn new(tail: PAtomic<Node>) -> Self {
        Self {
            tail: tail.clone(),
            requests:  array_init(|i| Default::default()),
            deactivate: array_init(|i| Default::default()),
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: PAtomic<Node>, // Atomic type to restrict reordering. We use this likes plain pointer.
    requests: [DeqRequestRec; MAX_THREADS + 1],
    // return_val: [DeqRetVal; MAX_THREADS + 1],
    deactivate: [AtomicBool; MAX_THREADS + 1],
}

impl DStateRec {
    fn new(head: PAtomic<Node>) -> Self {
        Self {
            head: head.clone(),
            requests:  array_init(|i| Default::default()),
            deactivate: array_init(|i| Default::default()),
        }
    }
}


impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.head, tid, gc, pool);
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

/// TODO: doc
#[derive(Debug)]
pub struct Queue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,

    /// Shared non-volatile variables used by the PBQueueENQ instance of PBCOMB
    // e_request: [CachePadded<PAtomic<EnqRequestRec>>; MAX_THREADS + 1],
    // e_state: [CachePadded<EStateRec>; 2],
    // e_index: AtomicUsize,
    e_state: CachePadded<PAtomic<EStateRec>>,

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_state: CachePadded<PAtomic<DStateRec>>,
    // d_request: [CachePadded<PAtomic<DeqRequestRec>>; MAX_THREADS + 1],
    // d_state: [CachePadded<DStateRec>; 2],
    // d_index: AtomicUsize,
}

impl Collectable for Queue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // PPtr::filter(&mut s.dummy, tid, gc, pool);
        // for tid in 0..MAX_THREADS + 1 {
        //     PAtomic::filter(&mut *s.e_request[tid], tid, gc, pool);
        //     PAtomic::filter(&mut *s.d_request[tid], tid, gc, pool);
        // }
        // EStateRec::filter(&mut *s.e_state[0], tid, gc, pool);
        // EStateRec::filter(&mut *s.e_state[1], tid, gc, pool);
        // DStateRec::filter(&mut *s.d_state[0], tid, gc, pool);
        // DStateRec::filter(&mut *s.d_state[1], tid, gc, pool);

        // initialize global volatile variable manually
        // OLD_TAIL.store(s.dummy.into_offset(), Ordering::SeqCst);
        todo!()
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
            e_state: CachePadded::new(PAtomic::null()),
            d_state: CachePadded::new(PAtomic::null()),
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
        let my_req = ok_or!(enq.req.checkpoint::<REC>(EnqRequestRec {activate: AtomicBool::new(true), arg, return_val: None}, tid, pool), e, e.current);

        // decide enq combiner
        let (lval, lockguard) = loop {
            let mut state = self.e_state.load(Ordering::SeqCst, guard); // TODO: 아직 Null이라면?
            unsafe { state.deref_mut(pool).requests[tid] = my_req.clone() };

            // try to be combiner.
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while state == self.e_state.load(Ordering::SeqCst, guard) {
                backoff.snooze();
            }

            let state_ref = unsafe { state.deref(pool) };
            if state_ref.deactivate[tid].load(Ordering::SeqCst)
            {
                return state_ref.requests[tid].return_val.unwrap();
            }
        };

        // enq combiner executes the enq requests
        let old_state = self.e_state.load(Ordering::SeqCst, guard); // TODO: 아직 Null이라면?
        let old_tail = unsafe { old_state.deref(pool).tail.clone() };
        OLD_TAIL.store(old_tail
                .load(Ordering::SeqCst, guard)
                .into_usize(),
            Ordering::SeqCst,
        );
        let mut new_state = POwned::new(EStateRec::new(old_tail), pool).into_shared(guard);
        self.e_state.store(new_state, Ordering::SeqCst);

        // collect the enqueued nodes here and persist them all at once
        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);
        let mut new_state_ref = unsafe { new_state.deref_mut(pool) };
        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `q` thread has a request that is not yet applied
            if new_state_ref.requests[q].activate.load(Ordering::SeqCst)
            {
                // reserve persist(current tail)
                let tail_addr = new_state_ref.tail
                    .load(Ordering::SeqCst, guard)
                    .into_usize();
                match to_persist.binary_search(&tail_addr) {
                    Ok(_) => {} // no duplicate
                    Err(idx) => to_persist.insert(idx, tail_addr),
                }

                // enq
                Self::raw_enqueue(&new_state_ref.tail, new_state_ref.requests[q].arg, guard, pool);
                new_state_ref.requests[q].return_val = Some(());
                new_state_ref.deactivate[q]
                    .store(true, Ordering::SeqCst);
            }
        }
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
        persist_obj(new_state_ref, true);
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        drop(lockguard); // release E_LOCK

        new_state_ref.requests[tid].return_val.unwrap()
    }

    fn raw_enqueue(tail: &PAtomic<Node>, arg: Data, guard: &Guard, pool: &PoolHandle) {
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(unsafe { unprotected() });
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

        let my_req = ok_or!(deq.req.checkpoint::<REC>(DeqRequestRec {activate: AtomicBool::new(true), return_val: None}, tid, pool), e, e.current);

        // decide enq combiner
        let (lval, lockguard) = loop {
            let mut state = self.d_state.load(Ordering::SeqCst, guard); // TODO: 아직 Null이라면?
            unsafe { state.deref_mut(pool).requests[tid] = my_req.clone() };

            // try to be combiner.
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            while state == self.d_state.load(Ordering::SeqCst, guard) {
                backoff.snooze();
            }

            let state_ref = unsafe { state.deref(pool) };
            if state_ref.deactivate[tid].load(Ordering::SeqCst)
            {
                return state_ref.requests[tid].return_val.unwrap();
            }
        };

        // deq combiner executes the deq requests
        let old_state = self.d_state.load(Ordering::SeqCst, guard); // TODO: 아직 Null이라면?
        let old_head = unsafe { old_state.deref(pool).head.clone() };
        let mut new_state = POwned::new(DStateRec::new(old_head), pool).into_shared(guard);
        self.d_state.store(new_state, Ordering::SeqCst);

        let mut new_state_ref = unsafe { new_state.deref_mut(pool) };
        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if new_state_ref.requests[q].activate.load(Ordering::SeqCst)
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
                    ret_val = Self::raw_dequeue(&new_state_ref.head, guard, pool);
                } else {
                    ret_val = None;
                }
                new_state_ref.requests[q].return_val = Some(ret_val);
                new_state_ref.deactivate[q]
                    .store(true, Ordering::SeqCst);
            }
        }
        persist_obj(new_state_ref, true);
        drop(lockguard); // release D_LOCK
        new_state_ref.requests[tid].return_val.unwrap()
    }

    fn raw_dequeue(head: &PAtomic<Node>, guard: &Guard, pool: &PoolHandle) -> DeqRetVal {
        let head_shared = head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head_shared.deref(pool) };

        let next = head_ref.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            head.store(next, Ordering::SeqCst);
            unsafe { guard.defer_pdestroy(head_shared) }; // NOTE: The original implementation does not free because it returns a node rather than data.
            return Some(unsafe { next.deref(pool) }.data);
        }
        None
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use crate::ds::queue_combining::Queue;
    use crate::pmem::{Collectable, GarbageCollection, PoolHandle, RootObj};
    use crate::test_utils::tests::{run_test, TestRootObj, JOB_FINISHED, RESULTS};
    use crossbeam_epoch::Guard;

    use super::{Dequeue, Enqueue};

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
                    assert!((2..NR_THREAD + 2)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
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
        const FILE_NAME: &str = "pbcomb_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}
