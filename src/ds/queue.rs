//! Persistent opt queue

use crate::pepoch::atomic::invalid_ptr;
use crate::ploc::smo::{self, Delete, SMOAtomic};
use crate::ploc::{no_owner, Checkpoint, Checkpointable, InsertError, Traversable};
use core::sync::atomic::Ordering;
use crossbeam_utils::{Backoff, CachePadded};
use etrace::{ok_or, some_or};
use smo::DeleteMode;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// TODO(doc)
#[derive(Debug)]
pub struct TryFail;

/// TODO(doc)
#[derive(Debug)]
pub struct Node<T: Clone> {
    data: MaybeUninit<T>,
    next: SMOAtomic<Queue<T>, Self>,
    owner: PAtomic<Self>,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: SMOAtomic::default(),
            owner: PAtomic::from(no_owner()),
        }
    }
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: SMOAtomic::default(),
            owner: PAtomic::from(no_owner()),
        }
    }
}

// TODO(must): T should be collectable
impl<T: Clone> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        SMOAtomic::filter(&mut node.next, tid, gc, pool);
    }
}

impl<T: Clone> smo::Node for Node<T> {
    #[inline]
    fn owner(&self) -> &PAtomic<Self> {
        &self.owner
    }
}

/// try push operation for Queue
#[derive(Debug)]
pub struct TryEnqueue;

impl Default for TryEnqueue {
    fn default() -> Self {
        Self
    }
}

unsafe impl Send for TryEnqueue {}

impl Collectable for TryEnqueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl TryEnqueue {
    /// Reset TryEnqueue memento
    #[inline]
    pub fn reset(&mut self) {}
}

/// Queue의 enqueue
#[derive(Debug)]
pub struct Enqueue<T: Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_enq: TryEnqueue,
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

impl<T: Clone> Checkpointable for (PAtomic<Node<T>>, PAtomic<Node<T>>) {
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
    delete: Delete<Node<T>>,
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
        Delete::filter(&mut try_deq.delete, tid, gc, pool);
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
    pub fn reset(&mut self) {
        self.try_deq.reset();
    }
}

unsafe impl<T: Clone> Send for Dequeue<T> {}

/// Must dequeue a value from Queue
#[derive(Debug)]
pub struct DequeueSome<T: 'static + Clone> {
    deq: Dequeue<T>,
}

impl<T: Clone> Default for DequeueSome<T> {
    fn default() -> Self {
        Self {
            deq: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for DequeueSome<T> {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Dequeue::filter(&mut deq.deq, tid, gc, pool);
    }
}

impl<T: Clone> DequeueSome<T> {
    /// Reset DequeueSome memento
    #[inline]
    pub fn reset(&mut self) {
        self.deq.reset();
    }
}

unsafe impl<T: Clone> Send for DequeueSome<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: CachePadded<SMOAtomic<Self, Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone> PDefault for Queue<T> {
    fn pdefault(pool: &PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(SMOAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone> Collectable for Queue<T> {
    fn filter(queue: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        SMOAtomic::filter(&mut queue.head, tid, gc, pool);
    }
}

impl<T: Clone> Traversable<Node<T>> for Queue<T> {
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.head.load_helping(guard, pool).unwrap();

        // TODO(opt): null 나올 때까지 하지 않고 tail을 통해서 범위를 제한할 수 있을지?
        while !curr.is_null() {
            if curr == target {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }
}

impl<T: Clone> Queue<T> {
    /// Try enqueue
    pub fn try_enqueue<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        try_enq: &mut TryEnqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let tail = self.tail.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.deref(pool) }; // TODO(must): filter 에서 tail align 해야 함

        tail_ref
            .next
            .insert::<REC>(node, self, guard, pool)
            .map(|_| {
                if REC {
                    return;
                }

                let _ = self.tail.compare_exchange(
                    tail,
                    node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
            .map_err(|e| {
                if let InsertError::NonNull = e {
                    // tail is stale
                    persist_obj(&tail_ref.next, true);
                    let next = tail_ref.next.load(Ordering::SeqCst, guard); // TODO(opt): 또 로드 해서 성능 저하 생길지도?
                    let _ = self.tail.compare_exchange(
                        tail,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }

                TryFail
            })
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
        .load(Ordering::Relaxed, guard); // TODO(opt): usize를 checkpoint 해보기 (using `PShared::from_usize()`)

        if self
            .try_enqueue::<REC>(node, &mut enq.try_enq, tid, guard, pool)
            .is_ok()
        {
            return;
        }

        let backoff = Backoff::default();
        loop {
            backoff.snooze();
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
        let head = self.head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head.deref(pool) };
        let next = head_ref.next.load(Ordering::SeqCst, guard);
        let tail = self.tail.load(Ordering::SeqCst, guard);

        let chk = ok_or!(
            try_deq
                .head_next
                .checkpoint::<REC>((PAtomic::from(head), PAtomic::from(next))),
            e,
            e.current
        );
        let head = chk.0.load(Ordering::Relaxed, guard); // TODO(opt): usize를 checkpoint 해보기 (using `PShared::from_usize()`)
        let next = chk.1.load(Ordering::Relaxed, guard);
        let next_ref = some_or!(unsafe { next.as_ref(pool) }, return Ok(None));

        if head == tail {
            let tail_ref = unsafe { tail.deref(pool) };
            persist_obj(&tail_ref.next, true);

            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);

            return Err(TryFail);
        }

        self.head
            .delete::<REC>(
                head,
                next,
                DeleteMode::Drop,
                &mut try_deq.delete,
                tid,
                guard,
                pool,
            )
            .map(|_| unsafe { Some((*next_ref.data.as_ptr()).clone()) })
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

        let backoff = Backoff::default();
        loop {
            backoff.snooze();
            if let Ok(ret) = self.try_dequeue::<false>(&mut deq.try_deq, tid, guard, pool) {
                return ret;
            }
        }
    }

    /// Dequeue Some
    pub fn dequeue_some<const REC: bool>(
        &self,
        deq_some: &mut DequeueSome<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> T {
        if let Some(v) = self.dequeue::<REC>(&mut deq_some.deq, tid, guard, pool) {
            return v;
        }

        loop {
            if let Some(v) = self.dequeue::<false>(&mut deq_some.deq, tid, guard, pool) {
                return v;
            }
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Queue<T> {}

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

    impl RootObj<EnqDeq> for TestRootObj<Queue<usize>> {
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
        const FILE_NAME: &str = "queue_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue<usize>>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
