//! Persistent queue

use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, DetectableCASAtomic};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::ok_or;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
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

impl<T: Clone + Collectable> Enqueue<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.node.clear();
        self.try_enq.clear();
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Enqueue<T> {}

/// Try dequeue memento
#[derive(Debug)]
pub struct TryDequeue<T: Clone + Collectable> {
    delete: Cas,
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

impl<T: Clone + Collectable> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_deq.delete, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> TryDequeue<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.delete.clear();
        self.head_next.clear();
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
    pub fn try_enqueue<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        try_enq: &mut TryEnqueue<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let tail = loop {
            let tail = self.tail.load(Ordering::SeqCst, guard);
            let tail_ref = unsafe { tail.deref(pool) };
            let next = tail_ref.next.load(Ordering::SeqCst, guard, pool);

            if next.is_null() {
                break tail;
            }

            // tail is stale
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        let tail = ok_or!(
            try_enq
                .tail
                .checkpoint::<REC>(PAtomic::from(tail), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);
        let tail_ref = unsafe { tail.deref(pool) };

        if tail_ref
            .next
            .cas::<REC>(PShared::null(), node, &mut try_enq.insert, tid, guard, pool)
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
    pub fn enqueue<const REC: bool>(
        &self,
        value: T,
        enq: &mut Enqueue<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = ok_or!(
            enq.node.checkpoint::<REC>(PAtomic::from(node), tid, pool),
            e,
            unsafe {
                drop(
                    e.new
                        .load(Ordering::Relaxed, epoch::unprotected())
                        .into_owned(),
                );
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        if self
            .try_enqueue::<REC>(node, &mut enq.try_enq, tid, guard, pool)
            .is_ok()
        {
            return;
        }

        loop {
            if self
                .try_enqueue::<false>(node, &mut enq.try_enq, tid, guard, pool)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Try dequeue
    pub fn try_dequeue<const REC: bool>(
        &self,
        try_deq: &mut TryDequeue<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        let (head, next) = loop {
            let head = self.head.load(Ordering::SeqCst, guard, pool);
            let head_ref = unsafe { head.deref(pool) };
            let next = head_ref.next.load(Ordering::SeqCst, guard, pool);
            let tail = self.tail.load(Ordering::SeqCst, guard);

            if head.as_ptr() != tail.as_ptr() || next.is_null() {
                break (head, next);
            }

            // tail is stale
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        let chk = ok_or!(
            try_deq.head_next.checkpoint::<REC>(
                (PAtomic::from(head), PAtomic::from(next)),
                tid,
                pool
            ),
            e,
            e.current
        );
        let head = chk.0.load(Ordering::Relaxed, guard);
        let next = chk.1.load(Ordering::Relaxed, guard);

        if next.is_null() {
            return Ok(None);
        }

        if self
            .head
            .cas::<REC>(head, next, &mut try_deq.delete, tid, guard, pool)
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
    pub fn dequeue<const REC: bool>(
        &self,
        deq: &mut Dequeue<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Option<T> {
        if let Ok(ret) = self.try_dequeue::<REC>(&mut deq.try_deq, tid, guard, pool) {
            return ret;
        }

        loop {
            if let Ok(ret) = self.try_dequeue::<false>(&mut deq.try_deq, tid, guard, pool) {
                return ret;
            }
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for QueueGeneral<T> {}

#[cfg(test)]
mod test {
    use libc::gettid;
    use std::{io::Write, thread};

    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 20_000;

    struct EnqDeq {
        enqs: [Enqueue<(usize, usize, usize)>; COUNT], // (tid, op seq, value)
        deqs: [Dequeue<(usize, usize, usize)>; COUNT], // (tid, op seq, value)
    }

    impl Default for EnqDeq {
        fn default() -> Self {
            Self {
                enqs: array_init::array_init(|_| Enqueue::<(usize, usize, usize)>::default()),
                deqs: array_init::array_init(|_| Dequeue::<(usize, usize, usize)>::default()),
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

    impl RootObj<EnqDeq> for TestRootObj<QueueGeneral<(usize, usize, usize)>> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let unix_tid = unsafe { gettid() };
            // println!("[run] t{tid} start (unix_tid: {unix_tid})");
            // thread::sleep(std::time::Duration::from_secs_f64(0.5));

            match tid {
                // T1: Check the execution results of other threads
                1 => {
                    // Wait for all other threads to finish
                    let mut cnt = 0;
                    while JOB_FINISHED.load(Ordering::SeqCst) < NR_THREAD {
                        if cnt > 300 {
                            println!("Stop testing. Maybe there is a bug...");
                            std::process::exit(1);
                        }

                        println!(
                            "[run] t{tid} JOB_FINISHED: {} (unix_tid: {unix_tid}, cnt: {cnt})",
                            JOB_FINISHED.load(Ordering::SeqCst)
                        );
                        thread::sleep(std::time::Duration::from_secs_f64(0.1));
                        cnt += 1;
                    }

                    println!("[run] t{tid} pass the busy lock (unix_tid: {unix_tid})");

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::<(usize, usize, usize)>::default();
                    let must_none = self.obj.dequeue::<true>(&mut tmp_deq, tid, guard, pool);
                    assert!(must_none.is_none());

                    // Check results
                    let mut results = RESULTS_TCRASH.lock_poisonable().clone();
                    for tid in 2..NR_THREAD + 2 {
                        for seq in 0..COUNT {
                            assert_eq!(results.remove(&(tid, seq)).unwrap(), get_value(tid, seq));
                        }
                    }
                    assert!(results.is_empty());
                }
                // Threads other than T1 perform { enq; deq; }
                _ => {
                    // enq; deq;
                    for seq in 0..COUNT {
                        let _ = self.obj.enqueue::<true>(
                            (tid, seq, get_value(tid, seq)), // value = hash((tid, seq))
                            &mut enq_deq.enqs[seq],
                            tid,
                            guard,
                            pool,
                        );
                        let res =
                            self.obj
                                .dequeue::<true>(&mut enq_deq.deqs[seq], tid, guard, pool);
                        assert!(res.is_some());

                        // Transfer the deq result to the result array
                        let (tid, seq, value) = res.unwrap();
                        if let Some(prev) =
                            RESULTS_TCRASH.lock_poisonable().insert((tid, seq), value)
                        {
                            assert_eq!(prev, value);
                        }
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    // - We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - You can check gc operation from the second time you open the pool:
    //   - The output statement says COUNT * NR_THREAD + 2 blocks are reachable
    //   - where +2 is a pointer to Root, Queue
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "general_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<QueueGeneral<(usize, usize, usize)>>, EnqDeq, _>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 1,
        );
    }
}
