//! Persistent queue

use crate::pepoch::atomic::invalid_ptr;
use crate::ploc::detectable_cas::Cas;
use crate::ploc::{Checkpoint, Checkpointable, DetectableCASAtomic, Traversable};
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
pub struct Node<T> {
    data: MaybeUninit<T>,
    next: DetectableCASAtomic<Self>,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: DetectableCASAtomic::default(),
        }
    }
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: DetectableCASAtomic::default(),
        }
    }
}

// TODO(must): T should be collectable
impl<T> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        DetectableCASAtomic::filter(&mut node.next, tid, gc, pool);
    }
}

/// ComposedQueue의 try push operation
#[derive(Debug)]
pub struct TryEnqueue<T: Clone> {
    tail: Checkpoint<PAtomic<Node<T>>>,

    /// push를 위해 할당된 node
    insert: Cas<Node<T>>,
}

impl<T: Clone> Default for TryEnqueue<T> {
    fn default() -> Self {
        Self {
            tail: Default::default(),
            insert: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryEnqueue<T> {}

impl<T: Clone> Collectable for TryEnqueue<T> {
    fn filter(try_push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Cas::filter(&mut try_push.insert, tid, gc, pool);
    }
}

impl<T: Clone> TryEnqueue<T> {
    /// Reset TryEnqueue memento
    #[inline]
    pub fn reset(&mut self) {
        self.tail.reset();
        self.insert.reset();
    }
}

/// Queue의 enqueue
#[derive(Debug)]
pub struct Enqueue<T: Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_enq: TryEnqueue<T>,
}

impl<T: Clone> Default for Enqueue<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_enq: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Enqueue<T> {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut enq.node, tid, gc, pool);
        TryEnqueue::filter(&mut enq.try_enq, tid, gc, pool);
    }
}

impl<T: Clone> Enqueue<T> {
    /// Reset Enqueue memento
    #[inline]
    pub fn reset(&mut self) {
        self.node.reset();
        self.try_enq.reset();
    }
}

unsafe impl<T: Clone> Send for Enqueue<T> {}

impl<T> Checkpointable for (PAtomic<Node<T>>, PAtomic<Node<T>>) {
    fn invalidate(&mut self) {
        self.1.store(invalid_ptr(), Ordering::Relaxed);
    }

    fn is_invalid(&self) -> bool {
        let guard = unsafe { epoch::unprotected() };
        let cur = self.1.load(Ordering::Relaxed, guard);
        cur == invalid_ptr()
    }
}

/// Queue의 try dequeue operation
#[derive(Debug)]
pub struct TryDequeue<T: Clone> {
    delete: Cas<Node<T>>,
    head_next: Checkpoint<(PAtomic<Node<T>>, PAtomic<Node<T>>)>,
}

impl<T: Clone> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
            head_next: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryDequeue<T> {}

impl<T: Clone> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Cas::filter(&mut try_deq.delete, tid, gc, pool);
    }
}

impl<T: Clone> TryDequeue<T> {
    /// Reset TryDequeue memento
    #[inline]
    pub fn reset(&mut self) {
        self.delete.reset();
        self.head_next.reset();
    }
}

/// Queue의 Dequeue
#[derive(Debug)]
pub struct Dequeue<T: Clone> {
    try_deq: TryDequeue<T>,
}

impl<T: Clone> Default for Dequeue<T> {
    fn default() -> Self {
        Self {
            try_deq: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Dequeue<T> {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        TryDequeue::filter(&mut deq.try_deq, tid, gc, pool);
    }
}

impl<T: Clone> Dequeue<T> {
    /// Reset Dequeue memento
    #[inline]
    pub fn reset(&mut self) {
        self.try_deq.reset();
    }
}

unsafe impl<T: Clone> Send for Dequeue<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct QueueGeneral<T: Clone> {
    head: CachePadded<DetectableCASAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone> PDefault for QueueGeneral<T> {
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

impl<T: Clone> Collectable for QueueGeneral<T> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        DetectableCASAtomic::filter(&mut queue.head, tid, gc, pool);
    }
}

impl<T: Clone> Traversable<Node<T>> for QueueGeneral<T> {
    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard, pool);

        // TODO(opt): null 나올 때까지 하지 않고 tail을 통해서 범위를 제한할 수 있을지?
        while !curr.is_null() {
            if curr == target {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard, pool);
        }

        false
    }
}

impl<T: Clone> QueueGeneral<T> {
    /// Try enqueue
    pub fn try_enqueue<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        try_enq: &mut TryEnqueue<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let (tail, tail_ref) = loop {
            let tail = self.tail.load(Ordering::SeqCst, guard);
            let tail_ref = unsafe { tail.deref(pool) }; // TODO(must): filter 에서 tail align 해야 함
            let next = tail_ref.next.load(Ordering::SeqCst, guard, pool);

            if next.is_null() {
                break (tail, tail_ref);
            }

            // tail is stale
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        let tail = ok_or!(
            try_enq.tail.checkpoint::<REC>(PAtomic::from(tail)),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

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

        let node = ok_or!(enq.node.checkpoint::<REC>(PAtomic::from(node)), e, unsafe {
            drop(
                e.new
                    .load(Ordering::Relaxed, epoch::unprotected())
                    .into_owned(),
            );
            e.current
        })
        .load(Ordering::Relaxed, guard);

        if self
            .try_enqueue::<REC>(node, &mut enq.try_enq, tid, guard, pool)
            .is_ok()
        {
            return;
        }

        loop {
            enq.try_enq.reset();
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
            persist_obj(&unsafe { tail.deref(pool) }.next, false);
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        let chk = ok_or!(
            try_deq
                .head_next
                .checkpoint::<REC>((PAtomic::from(head), PAtomic::from(next))),
            e,
            e.current
        );
        let head = chk.0.load(Ordering::Relaxed, guard);
        let next = chk.1.load(Ordering::Relaxed, guard);

        if next.is_null() {
            return Ok(None);
        }

        self.head
            .cas::<REC>(head, next, &mut try_deq.delete, tid, guard, pool)
            .map(|()| unsafe {
                guard.defer_pdestroy(head);
                Some((*next.deref(pool).data.as_ptr()).clone())
            })
            .map_err(|_| TryFail)
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
            deq.try_deq.reset();
            if let Ok(ret) = self.try_dequeue::<false>(&mut deq.try_deq, tid, guard, pool) {
                return ret;
            }
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for QueueGeneral<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    struct EnqDeq {
        enqs: [Enqueue<usize>; COUNT],
        deqs: [Dequeue<usize>; COUNT],
    }

    impl Default for EnqDeq {
        fn default() -> Self {
            Self {
                enqs: array_init::array_init(|_| Enqueue::<usize>::default()),
                deqs: array_init::array_init(|_| Dequeue::<usize>::default()),
            }
        }
    }

    impl Collectable for EnqDeq {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            for i in 0..COUNT {
                Enqueue::filter(&mut m.enqs[i], tid, gc, pool);
                Dequeue::filter(&mut m.deqs[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<QueueGeneral<usize>> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            match tid {
                // T0: 다른 스레드들의 실행결과를 확인
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::<usize>::default();
                    let must_none = self.obj.dequeue::<true>(&mut tmp_deq, tid, guard, pool);
                    assert!(must_none.is_none());

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    assert!((1..NR_THREAD + 1)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T0이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let _ =
                            self.obj
                                .enqueue::<true>(tid, &mut enq_deq.enqs[i], tid, guard, pool);
                        let res = self
                            .obj
                            .dequeue::<true>(&mut enq_deq.deqs[i], tid, guard, pool);
                        assert!(res.is_some());

                        // deq 결과를 실험결과에 전달
                        let _ = RESULTS[res.unwrap()].fetch_add(1, Ordering::SeqCst);
                    }

                    // "나 끝났다"
                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    // TODO(opt): queue의 enq_deq과 합치기
    // - 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - pool을 2번째 열 때부터 gc 동작 확인가능:
    //      - 출력문으로 COUNT * NR_THREAD + 2개의 block이 reachable하다고 나옴
    //      - 여기서 +2는 Root, Queue를 가리키는 포인터
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "general_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<QueueGeneral<usize>>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
