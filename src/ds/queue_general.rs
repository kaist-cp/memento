//! Persistent queue

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic, Handle};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// Failure of queue operations
#[derive(Debug)]
pub struct TryFail;

/// Queue node
#[derive(Debug)]
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

impl<T: Collectable> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        MaybeUninit::filter(&mut node.data, tid, gc, pool);
        DetectableCASAtomic::filter(&mut node.next, tid, gc, pool);
    }
}

/// Try enqueue memento
#[derive(Debug)]
pub struct TryEnqueue<T: Clone + Collectable> {
    tail: Checkpoint<PAtomic<Node<T>>>,
    insert: Cas,
}

impl<T: Clone + Collectable> Default for TryEnqueue<T> {
    fn default() -> Self {
        Self {
            tail: Default::default(),
            insert: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryEnqueue<T> {}

impl<T: Clone + Collectable> Collectable for TryEnqueue<T> {
    fn filter(try_push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut try_push.tail, tid, gc, pool);
        Cas::filter(&mut try_push.insert, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> TryEnqueue<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.tail.clear();
        self.insert.clear();
    }
}

/// Enqueue memento
#[derive(Debug)]
pub struct Enqueue<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_enq: TryEnqueue<T>,
}

impl<T: Clone + Collectable> Memento for Enqueue<T> {
    fn clear(&mut self) {
        self.node.clear();
        self.try_enq.clear();
    }
}

impl<T: Clone + Collectable> Default for Enqueue<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_enq: Default::default(),
        }
    }
}

impl<T: Clone + Collectable> Collectable for Enqueue<T> {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.node, tid, gc, pool);
        TryEnqueue::filter(&mut enq.try_enq, tid, gc, pool);
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Enqueue<T> {}

/// Try dequeue memento
#[derive(Debug)]
pub struct TryDequeue<T: Clone + Collectable> {
    delete: Cas,
    head_next: Checkpoint<(PAtomic<Node<T>>, PAtomic<Node<T>>)>,
}

impl<T: Clone + Collectable> Memento for TryDequeue<T> {
    fn clear(&mut self) {
        self.delete.clear();
        self.head_next.clear();
    }
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

impl<T: Clone + Collectable> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_deq.delete, tid, gc, pool);
        Checkpoint::filter(&mut try_deq.head_next, tid, gc, pool);
    }
}

/// Dequeue memento
#[derive(Debug)]
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

impl<T: Clone + Collectable> Collectable for Dequeue<T> {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        TryDequeue::filter(&mut deq.try_deq, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> Dequeue<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.try_deq.clear();
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Dequeue<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct QueueGeneral<T: Clone + Collectable> {
    head: CachePadded<DetectableCASAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> PDefault for QueueGeneral<T> {
    fn pdefault(pool: &PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(DetectableCASAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone + Collectable> Collectable for QueueGeneral<T> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut queue.head, tid, gc, pool);

        // Align head and tail
        let head = queue
            .head
            .load(Ordering::SeqCst, unsafe { epoch::unprotected() }, pool);
        queue.tail.store(head, Ordering::SeqCst);
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
                        let tail = self.tail.load(Ordering::SeqCst, guard);
                        let tail_ref = unsafe { tail.deref(pool) };
                        let next = tail_ref.next.load(Ordering::SeqCst, guard, pool);

                        if next.is_null() {
                            break tail;
                        }

                        // tail is stale
                        let _ = self.tail.compare_exchange(
                            tail,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
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

        let _ = self
            .tail
            .compare_exchange(tail, node, Ordering::SeqCst, Ordering::SeqCst, guard);
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
                    let head = self.head.load(Ordering::SeqCst, guard, pool);
                    let head_ref = unsafe { head.deref(pool) };
                    let next = head_ref.next.load(Ordering::SeqCst, guard, pool);
                    let tail = self.tail.load(Ordering::SeqCst, guard);

                    if head.as_ptr() != tail.as_ptr() || next.is_null() {
                        break (head, next);
                    }

                    // tail is stale
                    let _ = self.tail.compare_exchange(
                        tail,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ploc::Handle, pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 2;
    const NR_COUNT: usize = 10_000;

    struct EnqDeq {
        enqs: [Enqueue<TestValue>; NR_COUNT],
        deqs: [Dequeue<TestValue>; NR_COUNT],
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
            let testee = unsafe { TESTER.as_ref().unwrap().testee(handle.tid, true) };

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
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "queue_general";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<QueueGeneral<TestValue>>, EnqDeq>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}
