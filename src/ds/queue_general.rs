//! Persistent queue

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic, Handle};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::alloc::{Collectable, GarbageCollection};
use crate::pmem::{global_pool, ll::*, pool::*};
use crate::*;
use mmt_derive::Collectable;

/// Failure of queue operations
#[derive(Debug)]
pub struct TryFail;

/// Queue node
#[derive(Debug, Collectable)]
#[repr(align(128))]
pub struct Node<T: Collectable> {
    data: MaybeUninit<T>,
    next: DetectableCASAtomic<Self>,
}

impl<T: Collectable> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: DetectableCASAtomic::default(),
        }
    }
}

impl<T: Collectable> Default for Node<T> {
    fn default() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: DetectableCASAtomic::default(),
        }
    }
}

/// Try enqueue memento
#[derive(Debug, Memento, Collectable)]
pub struct TryEnqueue<T: Clone + Collectable> {
    tail: Checkpoint<PAtomic<Node<T>>>,
    insert: Cas<Node<T>>,
    forward_tail: Cas<Node<T>>,
}

impl<T: Clone + Collectable> Default for TryEnqueue<T> {
    fn default() -> Self {
        Self {
            tail: Default::default(),
            insert: Default::default(),
            forward_tail: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryEnqueue<T> {}

/// Enqueue memento
#[derive(Debug, Memento, Collectable)]
pub struct Enqueue<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_enq: TryEnqueue<T>,
}

impl<T: Clone + Collectable> Default for Enqueue<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_enq: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Enqueue<T> {}

/// Try dequeue memento
#[derive(Debug, Memento, Collectable)]
pub struct TryDequeue<T: Clone + Collectable> {
    delete: Cas<Node<T>>,
    head_next: Checkpoint<(PAtomic<Node<T>>, PAtomic<Node<T>>)>,
}

impl<T: Clone + Collectable> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
            head_next: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryDequeue<T> {}

/// Dequeue memento
#[derive(Debug, Memento, Collectable)]
pub struct Dequeue<T: Clone + Collectable> {
    try_deq: TryDequeue<T>,
}

impl<T: Clone + Collectable> Default for Dequeue<T> {
    fn default() -> Self {
        Self {
            try_deq: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Dequeue<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct QueueGeneral<T: Clone + Collectable> {
    head: CachePadded<DetectableCASAtomic<Node<T>>>,
    tail: CachePadded<DetectableCASAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> PDefault for QueueGeneral<T> {
    fn pdefault(handle: &Handle) -> Self {
        let sentinel = POwned::new(Node::default(), handle.pool).into_shared(&handle.guard);
        persist_obj(unsafe { sentinel.deref(handle.pool) }, true);

        Self {
            head: CachePadded::new(DetectableCASAtomic::from(sentinel)),
            tail: CachePadded::new(DetectableCASAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone + Collectable> Collectable for QueueGeneral<T> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut queue.head, tid, gc, pool);

        // Align head and tail
        let tmp_handle = Handle::new(tid, epoch::pin(), global_pool().unwrap());
        let head = queue.head.load(Ordering::SeqCst, &tmp_handle);
        let tail = queue.tail.load(Ordering::SeqCst, &tmp_handle);
        let _ = queue.tail.cas_non_detectable(tail, head, &tmp_handle);
    }
}

impl<T: Clone + Collectable> QueueGeneral<T> {
    /// Try enqueue
    pub fn try_enqueue(
        &self,
        node: PShared<'_, Node<T>>,
        try_enq: &mut TryEnqueue<T>,
        handle: &Handle,
    ) -> Result<(), TryFail> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let tail = try_enq
            .tail
            .checkpoint(
                || {
                    let tail = loop {
                        let tail = self.tail.load(Ordering::SeqCst, handle);
                        let tail_ref = unsafe { tail.deref(pool) };
                        let next = tail_ref.next.load(Ordering::SeqCst, handle);

                        if next.is_null() {
                            break tail;
                        }

                        // tail is stale
                        let _ = self.tail.cas_non_detectable(tail, next, handle);
                    };
                    PAtomic::from(tail)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);
        let tail_ref = unsafe { tail.deref(pool) };

        if tail_ref
            .next
            .cas(PShared::null(), node, &mut try_enq.insert, handle)
            .is_err()
        {
            return Err(TryFail);
        }

        let _ = self.tail.cas(tail, node, &mut try_enq.forward_tail, handle);

        Ok(())
    }

    /// Enqueue
    pub fn enqueue(&self, value: T, enq: &mut Enqueue<T>, handle: &Handle) {
        let node = enq
            .node
            .checkpoint(
                || {
                    let node = POwned::new(Node::from(value), handle.pool);
                    persist_obj(unsafe { node.deref(handle.pool) }, true);
                    PAtomic::from(node)
                },
                handle,
            )
            .load(Ordering::Relaxed, &handle.guard);

        loop {
            if self.try_enqueue(node, &mut enq.try_enq, handle).is_ok() {
                return;
            }
        }
    }

    /// Try dequeue
    pub fn try_dequeue(
        &self,
        try_deq: &mut TryDequeue<T>,
        handle: &Handle,
    ) -> Result<Option<T>, TryFail> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let chk = try_deq.head_next.checkpoint(
            || {
                let (head, next) = loop {
                    let head = self.head.load(Ordering::SeqCst, handle);
                    let head_ref = unsafe { head.deref(pool) };
                    let next = head_ref.next.load(Ordering::SeqCst, handle);
                    let tail = self.tail.load(Ordering::SeqCst, handle);

                    if head.as_ptr() != tail.as_ptr() || next.is_null() {
                        break (head, next);
                    }

                    // tail is stale
                    let _ = self.tail.cas_non_detectable(tail, next, handle);
                };
                (PAtomic::from(head), PAtomic::from(next))
            },
            handle,
        );
        let head = chk.0.load(Ordering::Relaxed, guard);
        let next = chk.1.load(Ordering::Relaxed, guard);

        if next.is_null() {
            return Ok(None);
        }

        if self
            .head
            .cas(head, next, &mut try_deq.delete, handle)
            .is_err()
        {
            return Err(TryFail);
        }

        Ok(unsafe {
            guard.defer_pdestroy(head);
            Some((*next.deref(pool).data.as_ptr()).clone())
        })
    }

    /// Dequeue
    pub fn dequeue(&self, deq: &mut Dequeue<T>, handle: &Handle) -> Option<T> {
        loop {
            if let Ok(ret) = self.try_dequeue(&mut deq.try_deq, handle) {
                return ret;
            }
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for QueueGeneral<T> {}

#[allow(dead_code)]
pub(crate) mod test {
    use super::*;
    use crate::{ploc::Handle, pmem::alloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 2;
    #[cfg(not(feature = "pmcheck"))]
    const NR_COUNT: usize = 10_000;
    #[cfg(feature = "pmcheck")]
    const NR_COUNT: usize = 5;

    struct EnqDeq {
        enqs: [Enqueue<TestValue>; NR_COUNT],
        deqs: [Dequeue<TestValue>; NR_COUNT],
    }

    impl Memento for EnqDeq {
        fn clear(&mut self) {
            for i in 0..NR_COUNT {
                self.enqs[i].clear();
                self.deqs[i].clear();
            }
        }
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

    impl RootObj<EnqDeq> for TestRootObj<QueueGeneral<TestValue>> {
        fn run(&self, enq_deq: &mut EnqDeq, handle: &Handle) {
            let testee = unsafe { TESTER.as_ref().unwrap().testee(true, handle) };

            for seq in 0..NR_COUNT {
                let _ = self.obj.enqueue(
                    TestValue::new(handle.tid, seq),
                    &mut enq_deq.enqs[seq],
                    handle,
                );
                let res = self.obj.dequeue(&mut enq_deq.deqs[seq], handle);

                assert!(res.is_some(), "tid:{}, seq:{seq}", handle.tid);

                testee.report(seq, res.unwrap());
            }
        }
    }

    // - We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - You can check gc operation from the second time you open the pool:
    //   - The output statement says COUNT * NR_THREAD + 2 blocks are reachable
    //   - where +2 is a pointer to Root, Queue
    // #[cfg(not(feature = "pmcheck"))]
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "queue_general";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        run_test::<TestRootObj<QueueGeneral<TestValue>>, EnqDeq>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }

    /// Test function for pmcheck
    #[cfg(feature = "pmcheck")]
    pub(crate) fn enqdeq(pool_postfix: &str) {
        const FILE_NAME: &str = "queue_O0";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let filename = format!("{}_{}", FILE_NAME, pool_postfix);
        run_test::<TestRootObj<QueueGeneral<TestValue>>, EnqDeq>(
            &filename, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}
