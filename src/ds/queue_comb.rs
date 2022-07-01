//! Detectable Combining queue
#![allow(non_snake_case)]
#![allow(warnings)]
use crate::ds::tlock::ThreadRecoverableSpinLock;
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
use lazy_static::__Deref;
use libc::c_void;
use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use tinyvec::{tiny_vec, TinyVec};

use super::combining::{CombStateRec, CombStruct, CombThreadState, Combinable, Combining, Node};

const MAX_THREADS: usize = 64;
type Data = usize;

const COMBINING_ROUNDS: usize = 20;

/// restriction of combining iteration
pub static mut NR_THREADS: usize = MAX_THREADS;

/// client for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    activate: Checkpoint<usize>,
}

impl Combinable for Enqueue {
    fn checkpoint_activate<const REC: bool>(
        &mut self,
        activate: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize {
        ok_or!(
            self.activate.checkpoint::<REC>(activate, tid, pool),
            e,
            e.current
        )
    }

    fn checkpoint_return_value<const REC: bool>(
        &mut self,
        return_value: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize {
        return_value
    }
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.activate, tid, gc, pool);
    }
}

/// client for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    activate: Checkpoint<usize>,
    return_val: Checkpoint<usize>,
}

impl Combinable for Dequeue {
    fn checkpoint_activate<const REC: bool>(
        &mut self,
        activate: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize {
        ok_or!(
            self.activate.checkpoint::<REC>(activate, tid, pool),
            e,
            e.current
        )
    }

    fn checkpoint_return_value<const REC: bool>(
        &mut self,
        return_value: usize,
        tid: usize,
        pool: &PoolHandle,
    ) -> usize {
        ok_or!(
            self.return_val.checkpoint::<REC>(return_value, tid, pool),
            e,
            e.current
        )
    }
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.activate, tid, gc, pool);
        Checkpoint::filter(&mut deq.return_val, tid, gc, pool);
    }
}

// Shared volatile variables
lazy_static::lazy_static! {
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    /// Used by the PBQueueENQ instance of PBCOMB
    static ref E_LOCK: CachePadded<ThreadRecoverableSpinLock> = CachePadded::new(ThreadRecoverableSpinLock::default());
    static ref E_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));

    /// Used by the PBQueueDEQ instance of PBCOMB
    static ref D_LOCK: CachePadded<ThreadRecoverableSpinLock> = CachePadded::new(ThreadRecoverableSpinLock::default());
    static ref D_LOCK_VALUE: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
}

static mut NEW_NODES: Option<TinyVec<[usize; 1024]>> = None;

struct EnqueueStruct {
    inner: CachePadded<CombStruct>,
    tail: CachePadded<PAtomic<Node>>,
}

struct DequeueStruct {
    inner: CachePadded<CombStruct>,
    head: CachePadded<PAtomic<Node>>,
}

/// Detectable Combining Queue
// #[derive(Debug)]
#[allow(missing_debug_implementations)]
pub struct CombiningQueue {
    /// Shared non-volatile variables
    dummy: PPtr<Node>,

    // Shared non-volatile variables used by the Enqueue
    enqueue_struct: CachePadded<EnqueueStruct>,
    enqueue_thread_state: CachePadded<CombThreadState>, // TODO: cachepadded 하는 게 맞나?

    // Shared non-volatile variables used by the Dequeue
    dequeue_struct: CachePadded<DequeueStruct>,
    dequeue_thread_state: CachePadded<CombThreadState>, // TODO: cachepadded 하는 게 맞나?
}

unsafe impl Sync for CombiningQueue {}
unsafe impl Send for CombiningQueue {}

impl Collectable for CombiningQueue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        // assert!(s.dummy.is_null());
        // Collectable::mark(unsafe { s.dummy.deref_mut(pool) }, tid, gc);

        // for t in 1..MAX_THREADS + 1 {
        //     Collectable::filter(&mut *s.e_request[t], tid, gc, pool);
        //     Collectable::filter(&mut *s.d_request[t], tid, gc, pool);
        // }

        // // initialize global volatile variable manually
        // OLD_TAIL.store(s.dummy.into_offset(), Ordering::SeqCst);
        todo!()
    }
}

impl PDefault for CombiningQueue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let dummy = pool.alloc::<Node>();
        let dummy_ref = unsafe { dummy.deref_mut(pool) };
        dummy_ref.data = 0;
        dummy_ref.next = PAtomic::null();

        // initialize global volatile variables
        OLD_TAIL.store(dummy.into_offset(), Ordering::SeqCst);
        unsafe {
            NEW_NODES = Some(tiny_vec!());
        }

        let enqueue_struct = EnqueueStruct {
            inner: CachePadded::new(CombStruct::new(
                Some(&Self::persist_new_nodes), // persist new nodes
                Some(&Self::update_old_tail),   // update old tail
                &*E_LOCK,
                &*E_LOCK_VALUE,
                array_init(|_| CachePadded::new(Default::default())),
                CachePadded::new(PAtomic::new(CombStateRec::new(PAtomic::from(dummy)), pool)),
            )),
            tail: CachePadded::new(PAtomic::from(dummy)),
        };

        let dequeue_struct = DequeueStruct {
            inner: CachePadded::new(CombStruct::new(
                None,
                None,
                &*D_LOCK,
                &*D_LOCK_VALUE,
                array_init(|_| CachePadded::new(Default::default())),
                CachePadded::new(PAtomic::new(CombStateRec::new(PAtomic::from(dummy)), pool)),
            )),
            head: CachePadded::new(PAtomic::from(dummy)),
        };

        Self {
            dummy,
            enqueue_struct: CachePadded::new(enqueue_struct),
            enqueue_thread_state: CachePadded::new(CombThreadState::new(
                PAtomic::from(dummy), // TODO: 이게 맞나..
                pool,
            )),
            dequeue_struct: CachePadded::new(dequeue_struct),
            dequeue_thread_state: CachePadded::new(CombThreadState::new(
                PAtomic::from(dummy), // TODO: 이게 맞나..
                pool,
            )),
        }
    }
}

impl CombiningQueue {
    const EMPTY: usize = usize::MAX;

    /// enq
    pub fn comb_enqueue<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        Combining::apply_op::<REC, _>(
            enq,
            &self.enqueue_struct.inner,
            &self.enqueue_thread_state,
            &Self::enqueue_raw,
            arg,
            tid,
            guard,
            pool,
        )
    }

    fn persist_new_nodes(_: &CombStruct, _: &Guard, pool: &PoolHandle) {
        let new_nodes = unsafe { NEW_NODES.as_mut().unwrap() };
        while !new_nodes.is_empty() {
            let node = PPtr::<Node>::from(new_nodes.pop().unwrap());
            persist_obj(unsafe { node.deref(pool) }, false);
        }
        sfence();
    }

    fn update_old_tail(str: &CombStruct, guard: &Guard, pool: &PoolHandle) {
        // TODO: non-general 버전보다 deref 한 번 더함

        let a = str.pstate.load(Ordering::SeqCst, guard);
        let a_ref = unsafe { a.deref(pool) }; // TODO: non-general 버전보다 deref 한 번 더함
        let tail = a_ref.data.load(Ordering::SeqCst, guard);
        OLD_TAIL.store(tail.into_usize(), Ordering::SeqCst);
    }

    fn enqueue_raw(
        tail: &PAtomic<c_void>,
        arg: usize,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        let tail = unsafe { (tail as *const _ as *const PAtomic<Node>).as_ref().unwrap() };

        // Enqueue new node
        let new_node = POwned::new(
            Node {
                data: arg,
                next: PAtomic::null(),
            },
            pool,
        )
        .into_shared(guard);
        let tail_ref = unsafe { tail.load(Ordering::SeqCst, guard).deref_mut(pool) };
        tail_ref.next.store(new_node, Ordering::SeqCst);
        tail.store(new_node, Ordering::SeqCst);

        // Reserve persist of new node
        let new_node_addr = new_node.as_ptr().into_offset();
        let new_nodes = unsafe { NEW_NODES.as_mut().unwrap() };
        match new_nodes.binary_search(&new_node_addr) {
            Ok(_) => {} // no duplicate
            Err(idx) => new_nodes.insert(idx, new_node_addr),
        }

        0
    }

    /// deq
    pub fn comb_dequeue<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        Combining::apply_op::<REC, _>(
            deq,
            &self.dequeue_struct.inner,
            &self.dequeue_thread_state,
            &Self::dequeue_raw,
            0, // TODO: option?
            tid,
            guard,
            pool,
        )
    }

    fn dequeue_raw(
        head: &PAtomic<c_void>,
        arg: usize,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        let head = unsafe { (head as *const _ as *mut PAtomic<Node>).as_ref().unwrap() };
        let head_shared = head.load(Ordering::SeqCst, guard);

        // only nodes that persisted can be dequeued.
        // persist of nodes from `OLD_TAIL` is not guaranteed because it is currently enqueud.
        if OLD_TAIL.load(Ordering::SeqCst) == head_shared.into_usize() {
            return Self::EMPTY;
        }

        // try dequeue
        let head_ref = unsafe { head_shared.deref(pool) };
        let ret = head_ref.next.load(Ordering::SeqCst, guard);
        if !ret.is_null() {
            head.store(ret, Ordering::SeqCst);
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

    use super::{CombiningQueue, Dequeue, Enqueue};

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

    impl RootObj<EnqDeq> for TestRootObj<CombiningQueue> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            // Get &mut queue
            let queue = unsafe { (&self.obj as *const _ as *mut CombiningQueue).as_mut() }.unwrap();

            match tid {
                // T1: Check results of other threads
                1 => {
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {
                        // println!("JOB_FINISHED: {}", JOB_FINISHED.load(Ordering::SeqCst));
                        std::thread::sleep(std::time::Duration::from_secs_f64(0.1));
                    }

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::default();
                    let v = queue.comb_dequeue::<true>(&mut tmp_deq, tid, guard, pool);
                    assert!(v == CombiningQueue::EMPTY);

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
                        let _ =
                            queue.comb_enqueue::<true>(tid, &mut enq_deq.enqs[i], tid, guard, pool);

                        let v = queue.comb_dequeue::<true>(&mut enq_deq.deqs[i], tid, guard, pool);
                        assert!(v != CombiningQueue::EMPTY);

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

        run_test::<TestRootObj<CombiningQueue>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}

// unsafe impl Sync for (dyn for<'r> Fn(&'r CombStruct) + 'static) {}
