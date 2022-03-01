//! Implementation of PBComb queue (Persistent Software Combining, Arxiv '21)
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
type Data = usize; // TODO: generic

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
    retval: PAtomic<usize>,
}

impl Collectable for EnqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Clone for EnqRequestRec {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl EnqRequestRec {
    const ACK: usize = 1;

    fn set_retval(&self, retval: ()) {
        let pool = global_pool().unwrap();
        let node = PAtomic::null()
            .load(Ordering::SeqCst, unsafe { unprotected() })
            .with_tag(Self::ACK);

        self.retval.store(node, Ordering::SeqCst);
        assert!(self.get_retval().is_some())
    }

    fn get_retval(&self) -> Option<()> {
        if self
            .retval
            .load(Ordering::SeqCst, unsafe { unprotected() })
            .tag()
            == Self::ACK
        {
            return Some(());
        }
        None
    }
}

#[derive(Debug, Default, Clone)]
struct DeqRequestRec {
    retval: PAtomic<Node>,
}

impl Collectable for DeqRequestRec {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl DeqRequestRec {
    const EMPTY: usize = 1;

    fn set_retval(&self, node: PShared<'_, Node>) {
        self.retval.store(node, Ordering::SeqCst);
    }

    fn get_retval(&self, guard: &Guard, pool: &PoolHandle) -> Option<DeqRetVal> {
        let node = self.retval.load(Ordering::SeqCst, guard);
        // println!("{:?}", node);
        if node.is_null() {
            if node.tag() != Self::EMPTY {
                return None; // not yet finished
            }
            return Some(None); // finished with EMPTY
        }

        // finished with some Node
        Some(Some(unsafe {
            guard.defer_pdestroy(node);
            node.deref(pool)
                .next
                .load(Ordering::SeqCst, guard)
                .deref(pool)
                .data
        }))
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
    tail: [PAtomic<Node>; 2],
    requests: [PAtomic<EnqRequestRec>; MAX_THREADS + 1],
}

impl Collectable for EStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // PAtomic::filter(&mut s.tail, tid, gc, pool);
        todo!()
    }
}

impl EStateRec {
    fn new(tail: PAtomic<Node>) -> Self {
        Self {
            tail: [tail.clone(), tail.clone()],
            requests: array_init(|i| Default::default()),
        }
    }
}

/// State of Dequeue PBComb
#[derive(Debug)]
struct DStateRec {
    head: [PAtomic<Node>; 2],
    requests: [PAtomic<DeqRequestRec>; MAX_THREADS + 1],
}

impl DStateRec {
    fn new(head: PAtomic<Node>) -> Self {
        Self {
            head: [head.clone(), head.clone()],
            requests: array_init(|i| Default::default()),
        }
    }
}

impl Collectable for DStateRec {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // PAtomic::filter(&mut s.head, tid, gc, pool);
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
    e_state: CachePadded<PAtomic<EStateRec>>,

    /// Shared non-volatile variables used by the PBQueueDEQ instance of PBCOMB
    d_state: CachePadded<PAtomic<DStateRec>>,
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

        let e_state = EStateRec::new(PAtomic::from(dummy));
        let d_state = DStateRec::new(PAtomic::from(dummy));
        println!("size of e_state: {}", std::mem::size_of::<EStateRec>());
        println!("size of d_state: {}", std::mem::size_of::<DStateRec>());
        Self {
            dummy,
            e_state: CachePadded::new(PAtomic::new(e_state, pool)),
            d_state: CachePadded::new(PAtomic::new(d_state, pool)),
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
        let req = POwned::new(
            EnqRequestRec {
                arg: PAtomic::new(
                    Node {
                        data: arg,
                        next: PAtomic::null(),
                    },
                    pool,
                ),
                retval: PAtomic::null(),
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
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // decide enq combiner
        let (lval, lockguard) = loop {
            let mut state = self.e_state.load(Ordering::SeqCst, guard);
            let state_ref = unsafe { state.deref_mut(pool) };
            state_ref.requests[tid].store(my_req, Ordering::SeqCst);

            // try to be combiner.
            let lval = match E_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == E_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            if let Some(ret) = unsafe { my_req.deref(pool) }.get_retval() {
                // wait until the combiner that processed my op is finished
                let deactivate_lval = E_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while deactivate_lval >= E_LOCK.peek().0 {
                    backoff.snooze();
                }
                return ret;
            }
        };

        // enq combiner executes the enq requests
        let mut state = self.e_state.load(Ordering::SeqCst, guard);
        let mut state_ref = unsafe { state.deref_mut(pool) };
        state_ref.requests[tid].store(my_req, Ordering::SeqCst); // need to remove
        OLD_TAIL.store(
            unsafe { state.deref(pool).tail[state.tag()].clone() }
                .load(Ordering::SeqCst, guard)
                .into_usize(),
            Ordering::SeqCst,
        );
        let ind = 1 - state.tag();

        // collect the enqueued nodes here and persist them all at once
        let mut to_persist = tiny_vec!([usize; MAX_THREADS]);
        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `q` thread has a request that is not yet applied
            if !state_ref.requests[q]
                .load(Ordering::SeqCst, guard)
                .is_null()
            {
                // reserve persist(current tail)
                let tail_addr = state_ref.tail[ind]
                    .load(Ordering::SeqCst, guard)
                    .into_usize();
                match to_persist.binary_search(&tail_addr) {
                    Ok(_) => {} // no duplicate
                    Err(idx) => to_persist.insert(idx, tail_addr),
                }

                // enq
                let q_req_ref = unsafe {
                    state_ref.requests[q]
                        .load(Ordering::SeqCst, guard)
                        .deref(pool)
                };
                Self::raw_enqueue(&state_ref.tail[ind], &q_req_ref.arg, guard, pool);
                E_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                q_req_ref.set_retval(());
            }
        }
        let tail_addr = state_ref.tail[ind]
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
        persist_obj(state_ref, true);

        self.e_state.store(
            POwned::new(EStateRec::new(state_ref.tail[ind].clone()), pool)
                .into_shared(guard)
                .with_tag(ind),
            Ordering::SeqCst,
        );
        OLD_TAIL.store(PPtr::<Node>::null().into_offset(), Ordering::SeqCst); // clear old_tail
        drop(lockguard); // release E_LOCK

        unsafe { my_req.deref(pool) }.get_retval().unwrap()
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

        // decide enq combiner
        let (lval, lockguard) = loop {
            let mut state = self.d_state.load(Ordering::SeqCst, guard);
            let state_ref = unsafe { state.deref_mut(pool) };
            state_ref.requests[tid].store(my_req, Ordering::SeqCst);

            // try to be combiner.
            let lval = match D_LOCK.try_lock::<REC>(tid) {
                Ok(ret) => break ret, // i am combiner
                Err((lval, _)) => lval,
            };

            // on-comibner waits until the combiner unlocks the lock, and only receives the result given by the combiner
            let backoff = Backoff::new();
            if lval % 2 == 1 {
                while lval == D_LOCK.peek().0 {
                    backoff.snooze();
                }
            }

            if let Some(ret) = unsafe { my_req.deref(pool) }.get_retval(guard, pool) {
                // wait until the combiner that processed my op is finished
                let deactivate_lval = D_DEACTIVATE_LOCK[tid].load(Ordering::SeqCst);
                backoff.reset();
                while deactivate_lval >= D_LOCK.peek().0 {
                    backoff.snooze();
                }
                return ret;
            }
        };

        // deq combiner executes the deq requests
        // println!("[deq] {tid} start combine");

        let mut state = self.d_state.load(Ordering::SeqCst, guard);
        let mut state_ref = unsafe { state.deref_mut(pool) };
        state_ref.requests[tid].store(my_req, Ordering::SeqCst);
        let ind = 1 - state.tag();

        for q in 1..unsafe { NR_THREADS } + 1 {
            // if `t` thread has a request that is not yet applied
            if !state_ref.requests[q]
                .load(Ordering::SeqCst, guard)
                .is_null()
            {
                let ret_val;
                // only nodes that are persisted can be dequeued.
                // from `OLD_TAIL`, persist is not guaranteed as it is currently enqueud.
                if OLD_TAIL.load(Ordering::SeqCst)
                    != state_ref.head[ind]
                        .load(Ordering::SeqCst, guard)
                        .into_usize()
                {
                    ret_val = Self::raw_dequeue(&state_ref.head[ind], guard, pool);
                } else {
                    panic!("[queue_combining] old tail");
                    ret_val = PShared::null().with_tag(DeqRequestRec::EMPTY);
                }
                // println!("[deq] {tid} perform {q}'s request, ret_val: {:?}", ret_val);
                D_DEACTIVATE_LOCK[q].store(lval, Ordering::SeqCst);
                unsafe {
                    state_ref.requests[q]
                        .load(Ordering::SeqCst, guard)
                        .deref(pool)
                }
                .set_retval(ret_val);
            } else {
                assert!(q != tid);
            }
        }
        persist_obj(state_ref, true);
        self.d_state.store(
            POwned::new(DStateRec::new(state_ref.head[ind].clone()), pool)
                .into_shared(guard)
                .with_tag(ind),
            Ordering::SeqCst,
        );
        drop(lockguard); // release D_LOCK
        unsafe { my_req.deref(pool) }
            .get_retval(guard, pool)
            .unwrap()
    }

    fn raw_dequeue<'g>(
        head: &PAtomic<Node>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, Node> {
        let head_shared = head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head_shared.deref(pool) };

        let next = head_ref.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            head.store(next, Ordering::SeqCst);
            return head_shared;
        }
        panic!("!!");
        PShared::null().with_tag(DeqRequestRec::EMPTY)
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

    const NR_THREAD: usize = 2;
    const COUNT: usize = 1000;

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
                    // let mut tmp_deq = Dequeue::default();
                    // let res = queue.PBQueueDeq::<true>(&mut tmp_deq, tid, guard, pool);
                    // assert!(res.is_none());

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
        const FILE_NAME: &str = "combining_enq_deq.pool";
        const FILE_SIZE: usize = 32 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}
