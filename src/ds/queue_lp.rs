//! Persistent opt queue

use crate::ploc::insert_delete::{self, SMOAtomic};
use crate::ploc::{not_deleted, Checkpoint, Traversable};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use etrace::ok_or;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// Failure of queue operations
#[derive(Debug)]
pub struct TryFail;

/// Queue node
#[derive(Debug)]
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

impl<T: Collectable> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        MaybeUninit::filter(&mut node.data, tid, gc, pool);
        SMOAtomic::filter(&mut node.next, tid, gc, pool);
        PAtomic::filter(&mut node.repl, tid, gc, pool)
    }
}

impl<T: Collectable> insert_delete::Node for Node<T> {
    #[inline]
    fn replacement(&self) -> &PAtomic<Self> {
        &self.repl
    }
}

/// Queue의 enqueue
#[derive(Debug)]
pub struct Enqueue<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> Default for Enqueue<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
        }
    }
}

impl<T: Clone + Collectable> Collectable for Enqueue<T> {
    fn filter(enq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut enq.node, tid, gc, pool);
    }
}

unsafe impl<T: Clone + Collectable> Send for Enqueue<T> {}

/// Queue의 try dequeue operation
#[derive(Debug)]
pub struct TryDequeue<T: Clone + Collectable> {
    head_next: Checkpoint<(PAtomic<Node<T>>, PAtomic<Node<T>>)>,
}

impl<T: Clone + Collectable> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            head_next: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryDequeue<T> {}

impl<T: Clone + Collectable> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut try_deq.head_next, tid, gc, pool);
    }
}

/// Queue의 Dequeue
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

unsafe impl<T: Clone + Collectable> Send for Dequeue<T> {}

/// Must dequeue a value from Queue
#[derive(Debug)]
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

impl<T: Clone + Collectable> Collectable for DequeueSome<T> {
    fn filter(deq: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Dequeue::filter(&mut deq.deq, tid, gc, pool);
    }
}

unsafe impl<T: Clone + Collectable> Send for DequeueSome<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct Queue<T: Clone + Collectable> {
    head: CachePadded<SMOAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> PDefault for Queue<T> {
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
    pub fn try_enqueue<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let (tail, tail_ref) = loop {
            let tail = self.tail.load(Ordering::SeqCst, guard);
            let tail_ref = unsafe { tail.deref(pool) };
            let next = tail_ref.next.load_lp(Ordering::SeqCst, guard);

            if next.is_null() {
                break (tail, tail_ref);
            }

            // tail is stale
            persist_obj(&tail_ref.next, false);
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        };

        if tail_ref
            .next
            .insert_lp::<_, REC>(node, self, guard, pool)
            .is_err()
        {
            return Err(TryFail);
        }

        if !REC {
            let _ =
                self.tail
                    .compare_exchange(tail, node, Ordering::SeqCst, Ordering::SeqCst, guard);
        }

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
        let node = POwned::new(Node::from(value), pool); // TODO(opt): persist_obj를 new 안으로 넣기
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

        if self.try_enqueue::<REC>(node, guard, pool).is_ok() {
            return;
        }

        loop {
            if self.try_enqueue::<false>(node, guard, pool).is_ok() {
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
            let head = self.head.load_lp(Ordering::SeqCst, guard);
            let head_ref = unsafe { head.deref(pool) };
            let next = head_ref.next.load_lp(Ordering::SeqCst, guard);
            let tail = self.tail.load(Ordering::SeqCst, guard);

            if head != tail || next.is_null() {
                break (head, next);
            }

            // tail is stale
            persist_obj(&unsafe { tail.deref(pool) }.next, false);
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
            .delete::<REC>(head, next, tid, guard, pool)
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

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Queue<T> {}

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
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..COUNT {
                Enqueue::filter(&mut m.enqs[i], tid, gc, pool);
                Dequeue::filter(&mut m.deqs[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<EnqDeq> for TestRootObj<Queue<usize>> {
        fn run(&self, enq_deq: &mut EnqDeq, tid: usize, guard: &Guard, pool: &PoolHandle) {
            match tid {
                // T1: 다른 스레드들의 실행결과를 확인
                1 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::<usize>::default();
                    let must_none = self.obj.dequeue::<true>(&mut tmp_deq, tid, guard, pool);
                    assert!(must_none.is_none());

                    // Check results
                    assert!(RESULTS[1].load(Ordering::SeqCst) == 0);
                    assert!((2..NR_THREAD + 2)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T1이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
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

    // - 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - pool을 2번째 열 때부터 gc 동작 확인가능:
    //      - 출력문으로 COUNT * NR_THREAD + 2개의 block이 reachable하다고 나옴
    //      - 여기서 +2는 Root, Queue를 가리키는 포인터
    #[test]
    fn enq_deq() {
        const FILE_NAME: &str = "queue_lp_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Queue<usize>>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
