//! Detectable Combining queue
#![allow(missing_docs)]
use crate::ds::comb::combining_lock::CombiningLock;
use crate::pepoch::atomic::Pointer;
use crate::pepoch::{unprotected, PAtomic, PDestroyable, POwned};
use crate::ploc::{Checkpoint, Handle};
use crate::pmem::{persist_obj, sfence, Collectable, GarbageCollection, PPtr, PoolHandle};
use crate::PDefault;
use array_init::array_init;
use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;
use libc::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinyvec::{tiny_vec, TinyVec};

use super::comb::{
    CombStateRec, CombStruct, CombThreadState, Combinable, Combining, Node, MAX_THREADS,
};

/// memento for enqueue
#[derive(Debug, Default)]
pub struct Enqueue {
    activate: Checkpoint<usize>,
}

impl Combinable for Enqueue {
    fn chk_activate(&mut self, activate: usize, handle: &Handle) -> usize {
        self.activate.checkpoint(|| activate, handle)
    }

    fn peek_retval(&mut self) -> usize {
        0 // unit-like
    }

    fn backup_retval(&mut self, _: usize) {
        // no-op
    }
}

impl Collectable for Enqueue {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.activate, tid, gc, pool);
    }
}

/// memento for dequeue
#[derive(Debug, Default)]
pub struct Dequeue {
    activate: Checkpoint<usize>,
    return_val: CachePadded<usize>,
}

impl Combinable for Dequeue {
    fn chk_activate(&mut self, activate: usize, handle: &Handle) -> usize {
        self.activate.checkpoint(|| activate, handle)
    }

    fn peek_retval(&mut self) -> usize {
        *self.return_val
    }

    fn backup_retval(&mut self, return_value: usize) {
        *self.return_val = return_value;
        persist_obj(&*self.return_val, true);
    }
}

impl Collectable for Dequeue {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut deq.activate, tid, gc, pool);
    }
}

// Shared volatile variables
static mut NEW_NODES: Option<TinyVec<[usize; 1024]>> = None;

lazy_static::lazy_static! {
    static ref OLD_TAIL: AtomicUsize = AtomicUsize::new(0);

    static ref E_LOCK: CachePadded<CombiningLock> = CachePadded::new(Default::default());
    static ref D_LOCK: CachePadded<CombiningLock> = CachePadded::new(Default::default());
}

struct EnqueueCombStruct {
    tail: CachePadded<PAtomic<Node>>,
    inner: CachePadded<CombStruct>,
}

impl Collectable for EnqueueCombStruct {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut *s.tail, tid, gc, pool);
        Collectable::filter(&mut *s.inner, tid, gc, pool);
    }
}

struct DequeueCombStruct {
    head: CachePadded<PAtomic<Node>>,
    inner: CachePadded<CombStruct>,
}

impl Collectable for DequeueCombStruct {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut *s.head, tid, gc, pool);
        Collectable::filter(&mut *s.inner, tid, gc, pool);
    }
}

/// Detectable Combining Queue
// #[derive(Debug)]
#[allow(missing_debug_implementations)]
pub struct CombiningQueue {
    // Shared non-volatile variables used by Enqueue
    enqueue_struct: CachePadded<EnqueueCombStruct>,
    enqueue_thread_state: [CachePadded<CombThreadState>; MAX_THREADS + 1],

    // Shared non-volatile variables used by Dequeue
    dequeue_struct: CachePadded<DequeueCombStruct>,
    dequeue_thread_state: [CachePadded<CombThreadState>; MAX_THREADS + 1],
}

unsafe impl Sync for CombiningQueue {}
unsafe impl Send for CombiningQueue {}

impl Collectable for CombiningQueue {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut *s.enqueue_struct, tid, gc, pool);
        for tstate in &mut s.enqueue_thread_state {
            Collectable::filter(&mut **tstate, tid, gc, pool);
        }
        Collectable::filter(&mut *s.dequeue_struct, tid, gc, pool);
        for tstate in &mut s.dequeue_thread_state {
            Collectable::filter(&mut **tstate, tid, gc, pool);
        }

        // initialize global volatile variables
        let tail = s
            .enqueue_struct
            .tail
            .load(Ordering::Relaxed, unsafe { unprotected() });
        OLD_TAIL.store(tail.into_usize(), Ordering::SeqCst);
        unsafe {
            NEW_NODES = Some(tiny_vec!());
        }
        lazy_static::initialize(&E_LOCK);
        lazy_static::initialize(&D_LOCK);
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
        lazy_static::initialize(&E_LOCK);
        lazy_static::initialize(&D_LOCK);

        // initialize persistent variables
        Self {
            enqueue_struct: CachePadded::new(EnqueueCombStruct {
                inner: CachePadded::new(CombStruct::new(
                    Some(&Self::persist_new_nodes), // persist new nodes
                    Some(&Self::update_old_tail),   // update old tail
                    &*E_LOCK,
                    array_init(|_| CachePadded::new(Default::default())),
                    CachePadded::new(PAtomic::new(CombStateRec::new(PAtomic::from(dummy)), pool)),
                )),
                tail: CachePadded::new(PAtomic::from(dummy)),
            }),
            enqueue_thread_state: array_init(|_| {
                CachePadded::new(CombThreadState::new(PAtomic::from(dummy), pool))
            }),
            dequeue_struct: CachePadded::new(DequeueCombStruct {
                inner: CachePadded::new(CombStruct::new(
                    None,
                    None,
                    &*D_LOCK,
                    array_init(|_| CachePadded::new(Default::default())),
                    CachePadded::new(PAtomic::new(CombStateRec::new(PAtomic::from(dummy)), pool)),
                )),
                head: CachePadded::new(PAtomic::from(dummy)),
            }),
            dequeue_thread_state: array_init(|_| {
                CachePadded::new(CombThreadState::new(PAtomic::from(dummy), pool))
            }),
        }
    }
}

/// enq
impl CombiningQueue {
    pub fn comb_enqueue(&self, arg: usize, enq: &mut Enqueue, handle: &Handle) -> usize {
        Combining::apply_op(
            arg,
            (
                &self.enqueue_struct.inner,
                &self.enqueue_thread_state[handle.tid],
                &Self::enqueue_raw,
            ),
            enq,
            handle,
        )
    }

    fn enqueue_raw(tail: &PAtomic<c_void>, arg: usize, handle: &Handle) -> usize {
        let (guard, pool) = (&handle.guard, handle.pool);
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

        0 // unit-like
    }

    fn persist_new_nodes(_: &CombStruct, _: &Guard, pool: &PoolHandle) {
        let new_nodes = unsafe { NEW_NODES.as_mut().unwrap() };
        while !new_nodes.is_empty() {
            let node = PPtr::<Node>::from(new_nodes.pop().unwrap());
            persist_obj(unsafe { node.deref(pool) }, false);
        }
        sfence();
    }

    fn update_old_tail(s: &CombStruct, guard: &Guard, pool: &PoolHandle) {
        let latest_state = unsafe { s.pstate.load(Ordering::SeqCst, guard).deref(pool) };
        let tail = latest_state.data.load(Ordering::SeqCst, guard);
        OLD_TAIL.store(tail.into_usize(), Ordering::SeqCst);
    }
}

/// deq
impl CombiningQueue {
    const EMPTY: usize = usize::MAX;

    pub fn comb_dequeue(&self, deq: &mut Dequeue, handle: &Handle) -> usize {
        Combining::apply_op(
            0, // unit-like
            (
                &self.dequeue_struct.inner,
                &self.dequeue_thread_state[handle.tid],
                &Self::dequeue_raw,
            ),
            deq,
            handle,
        )
    }

    fn dequeue_raw(head: &PAtomic<c_void>, _: usize, handle: &Handle) -> usize {
        let (guard, pool) = (&handle.guard, handle.pool);
        let head = unsafe { (head as *const _ as *mut PAtomic<Node>).as_ref().unwrap() };
        let head_shared = head.load(Ordering::SeqCst, guard);

        // only nodes that persisted can be dequeued.
        // nodes from 'OLD_TAIL' are not guaranteed to persist as they are currently queued.
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
    use crate::test_utils::tests::*;
    use crate::{ploc::Handle, pmem::*};

    use super::{CombiningQueue, Dequeue, Enqueue};

    const NR_THREAD: usize = 4;
    const NR_COUNT: usize = 100_000;

    struct EnqDeq {
        enqs: [Enqueue; NR_COUNT],
        deqs: [Dequeue; NR_COUNT],
    }

    impl Default for EnqDeq {
        fn default() -> Self {
            Self {
                enqs: array_init::array_init(|_| Default::default()),
                deqs: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for EnqDeq {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..NR_COUNT {
                Enqueue::filter(&mut m.enqs[i], tid, gc, pool);
                Dequeue::filter(&mut m.deqs[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<CombiningQueue> {
        fn run(&self, enq_deq: &mut EnqDeq, handle: &Handle) {
            let testee = unsafe { TESTER.as_ref().unwrap().testee(handle.tid, true) };

            for seq in 0..NR_COUNT {
                let _ = self.obj.comb_enqueue(
                    TestValue::new(handle.tid, seq).into_usize(),
                    &mut enq_deq.enqs[seq],
                    handle,
                );
                let res = self.obj.comb_dequeue(&mut enq_deq.deqs[seq], handle);

                testee.report(seq, TestValue::from_usize(res));
            }
        }
    }

    // - We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - You can check gc operation from the second time you open the pool:
    //   - The output statement says COUNT * NR_THREAD + 2 blocks are reachable
    //   - where +2 is a pointer to Root, Queue
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "queue_comb";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<CombiningQueue>, EnqDeq>(FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT);
    }
}
