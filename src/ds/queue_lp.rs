//! Persistent opt queue

use crate::ploc::insert_delete::{self, SMOAtomic};
use crate::ploc::{not_deleted, Checkpoint, Handle, Traversable};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use insert_delete::{Delete, Insert};
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::pmem::alloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
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
    next: SMOAtomic<Self>,
    repl: PAtomic<Self>,
}

impl<T: Collectable> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: SMOAtomic::default(),
            repl: PAtomic::from(not_deleted()),
        }
    }
}

impl<T: Collectable> Default for Node<T> {
    fn default() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: SMOAtomic::default(),
            repl: PAtomic::from(not_deleted()),
        }
    }
}

impl<T: Collectable> insert_delete::Node for Node<T> {
    #[inline]
    fn replacement(&self) -> &PAtomic<Self> {
        &self.repl
    }
}

/// Try enqueue memento
#[derive(Debug, Default, Memento, Collectable)]
pub struct TryEnqueue {
    ins: Insert,
}

unsafe impl Send for TryEnqueue {}

/// Enqueue memento
#[derive(Debug, Memento, Collectable)]
pub struct Enqueue<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_enq: TryEnqueue,
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
    head_next: Checkpoint<(PAtomic<Node<T>>, PAtomic<Node<T>>)>,
    del: Delete,
}

impl<T: Clone + Collectable> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            head_next: Default::default(),
            del: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryDequeue<T> {}

/// Dequeue client
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

/// Must dequeue a value from Queue
#[derive(Debug, Memento, Collectable)]
pub struct DequeueSome<T: Clone + Collectable> {
    deq: Dequeue<T>,
}

impl<T: Clone + Collectable> Default for DequeueSome<T> {
    fn default() -> Self {
        Self {
            deq: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for DequeueSome<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct Queue<T: Clone + Collectable> {
    head: CachePadded<SMOAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> PDefault for Queue<T> {
    fn pdefault(handle: &Handle) -> Self {
        let sentinel = POwned::new(Node::default(), handle.pool).into_shared(&handle.guard);
        persist_obj(unsafe { sentinel.deref(handle.pool) }, true);

        Self {
            head: CachePadded::new(SMOAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone + Collectable> Collectable for Queue<T> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        SMOAtomic::filter(&mut queue.head, tid, gc, pool);

        // Align head and tail
        let head = queue
            .head
            .load_lp(Ordering::SeqCst, unsafe { epoch::unprotected() });
        queue.tail.store(head, Ordering::SeqCst);
    }
}

impl<T: Clone + Collectable> Traversable<Node<T>> for Queue<T> {
    fn contains(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.head.load_lp(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == target {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load_lp(Ordering::SeqCst, guard);
        }

        false
    }
}

impl<T: Clone + Collectable> Queue<T> {
    /// Try enqueue
    pub fn try_enqueue(
        &self,
        node: PShared<'_, Node<T>>,
        try_enq: &mut TryEnqueue,
        handle: &Handle,
    ) -> Result<(), TryFail> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let (tail, tail_ref) = loop {
            let tail = self.tail.load(Ordering::SeqCst, guard);
            let tail_ref = unsafe { tail.deref(pool) };
            let next = tail_ref.next.load_lp(Ordering::SeqCst, guard);

            if next.is_null() {
                break (tail, tail_ref);
            }

            // tail is stale
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        if tail_ref
            .next
            .insert_lp(node, self, &mut try_enq.ins, handle)
            .is_err()
        {
            return Err(TryFail);
        }

        if !handle.rec.load(Ordering::Relaxed) {
            let _ =
                self.tail
                    .compare_exchange(tail, node, Ordering::SeqCst, Ordering::SeqCst, guard);
        }

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

        while self.try_enqueue(node, &mut enq.try_enq, handle).is_err() {}
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
                    let head = self.head.load_lp(Ordering::SeqCst, guard);
                    let head_ref = unsafe { head.deref(pool) };
                    let next = head_ref.next.load_lp(Ordering::SeqCst, guard);
                    let tail = self.tail.load(Ordering::SeqCst, guard);

                    if head != tail || next.is_null() {
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
            .delete(head, next, &mut try_deq.del, handle)
            .is_err()
        {
            return Err(TryFail);
        }

        Ok(unsafe {
            let next_ref = next.deref(pool);
            Some((*next_ref.data.as_ptr()).clone())
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

    /// Dequeue Some
    pub fn dequeue_some(&self, deq_some: &mut DequeueSome<T>, handle: &Handle) -> T {
        loop {
            if let Some(v) = self.dequeue(&mut deq_some.deq, handle) {
                return v;
            }
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Queue<T> {}

// #[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test {
    use super::*;
    use crate::{pmem::alloc::Collectable, test_utils::tests::*};

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

    impl RootObj<EnqDeq> for TestRootObj<Queue<TestValue>> {
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
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "queue_lp";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue<TestValue>>, EnqDeq>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }

    /// Test function for psan
    #[cfg(feature = "pmcheck")]
    pub(crate) fn enqdeq(pool_postfix: &str) {
        const FILE_NAME: &str = "queue_O1";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let filename = format!("{}_{}", FILE_NAME, pool_postfix);
        run_test::<TestRootObj<Queue<TestValue>>, EnqDeq>(
            &filename, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}
