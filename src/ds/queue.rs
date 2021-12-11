//! Persistent opt queue

use crate::node::Node;
use crate::ploc::smo::{Delete, UpdateDeleteInfo, Insert, SMOAtomic};
use crate::ploc::{Checkpoint, InsertErr, NeedRetry, RetryLoop, Traversable};
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// TODO(doc)
#[derive(Debug)]
pub struct TryFail;

/// ComposedQueue의 try push operation
#[derive(Debug)]
pub struct TryEnqueue<T: Clone> {
    /// push를 위해 할당된 node
    insert: Insert<Queue<T>, Node<MaybeUninit<T>>>,
}

impl<T: Clone> Default for TryEnqueue<T> {
    fn default() -> Self {
        Self {
            insert: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryEnqueue<T> {}

impl<T: Clone> Collectable for TryEnqueue<T> {
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Insert::filter(&mut try_push.insert, gc, pool);
    }
}

impl<T: Clone> TryEnqueue<T> {
    #[inline]
    fn prepare(_: &mut Node<MaybeUninit<T>>) -> bool {
        true
    }
}

impl<T: 'static + Clone> Memento for TryEnqueue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input<'o> = PShared<'o, Node<MaybeUninit<T>>>;
    type Output<'o> = ();
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let tail = queue.tail.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.deref(pool) }; // TODO(must): filter 에서 tail align 해야 함

        self.insert
            .run(
                &tail_ref.next,
                (node, queue, Self::prepare),
                rec,
                guard,
                pool,
            )
            .map(|_| {
                let _ = queue.tail.compare_exchange(
                    tail,
                    node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
            .map_err(|e| {
                if let InsertErr::PrepareFail = e {
                    // tail is stale
                    persist_obj(&tail_ref.next, true);
                    let next = tail_ref.next.load(Ordering::SeqCst, guard); // TODO(opt): 또 로드 해서 성능 저하 생길지도?
                    let _ = queue.tail.compare_exchange(
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

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.insert.reset(guard, pool);
    }
}

/// Queue의 enqueue
#[derive(Debug)]
pub struct Enqueue<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<MaybeUninit<T>>>>,
    try_enq: RetryLoop<TryEnqueue<T>>,
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
    fn filter(enq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut enq.node, gc, pool);
        RetryLoop::filter(&mut enq.try_enq, gc, pool);
    }
}

impl<T: Clone> Memento for Enqueue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input<'o> = T;
    type Output<'o>
    where
        T: 'o,
    = ();
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        value: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = POwned::new(Node::from(MaybeUninit::new(value)), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = self
            .node
            .run(
                (),
                (PAtomic::from(node), |aborted| {
                    let guard = unsafe { epoch::unprotected() };
                    let d = aborted.load(Ordering::Relaxed, guard);
                    unsafe { guard.defer_pdestroy(d) };
                }),
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        self.try_enq
            .run(queue, node, rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.node.reset(guard, pool);
        self.try_enq.reset(guard, pool);
    }
}

unsafe impl<T: 'static + Clone> Send for Enqueue<T> {}

/// Queue의 try dequeue operation
#[derive(Debug)]
pub struct TryDequeue<T: Clone> {
    delete: Delete<Queue<T>, Node<MaybeUninit<T>>, Self>,
}

impl<T: Clone> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryDequeue<T> {}

impl<T: Clone> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Delete::filter(&mut try_deq.delete, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryDequeue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.delete
            .run(&queue.head, (PShared::null(), queue), rec, guard, pool)
            .map(|ret| {
                ret.map(|popped| {
                    let next = unsafe { popped.deref(pool) }
                        .next
                        .load(Ordering::SeqCst, guard); // TODO(opt): next를 다시 load해서 성능 저하
                    let next_ref = unsafe { next.deref(pool) };
                    unsafe { guard.defer_pdestroy(popped) };
                    unsafe { (*next_ref.data.as_ptr()).clone() }
                })
            })
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.delete.reset(guard, pool);
    }
}

impl<T: Clone> UpdateDeleteInfo<Queue<T>, Node<MaybeUninit<T>>> for TryDequeue<T> {
    fn prepare_delete<'g>(
        old_head: PShared<'_, Node<MaybeUninit<T>>>,
        _: PShared<'_, Node<MaybeUninit<T>>>,
        queue: &Queue<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<MaybeUninit<T>>>>, NeedRetry> {
        let old_head_ref = unsafe { old_head.deref(pool) };
        let next = old_head_ref.next.load(Ordering::SeqCst, guard);
        let tail = queue.tail.load(Ordering::SeqCst, guard);

        if old_head == tail {
            if next.is_null() {
                return Ok(None);
            }

            let tail_ref = unsafe { tail.deref(pool) };
            persist_obj(&tail_ref.next, true);

            let _ =
                queue
                    .tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);

            return Err(NeedRetry);
        }

        Ok(Some(next))
    }

    // TODO(opt): How to refactor?
    fn prepare_update<'g>(
        _: PShared<'_, Node<MaybeUninit<T>>>,
        _: PShared<'_, Node<MaybeUninit<T>>>,
        _: &Queue<T>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> bool {
        unreachable!("not used.")
    }

    #[inline]
    fn node_when_deleted<'g>(
        old_head: PShared<'_, Node<MaybeUninit<T>>>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, Node<MaybeUninit<T>>> {
        let old_head_ref = unsafe { old_head.deref(pool) }; // SAFE: old_head는 null일 수 없음
        old_head_ref.next.load(Ordering::SeqCst, guard)
    }
}

/// Queue의 Dequeue
#[derive(Debug)]
pub struct Dequeue<T: 'static + Clone> {
    try_deq: RetryLoop<TryDequeue<T>>,
}

impl<T: Clone> Default for Dequeue<T> {
    fn default() -> Self {
        Self {
            try_deq: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Dequeue<T> {
    fn filter(deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        RetryLoop::filter(&mut deq.try_deq, gc, pool);
    }
}

impl<T: Clone> Memento for Dequeue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.try_deq
            .run(queue, (), rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_deq.reset(guard, pool);
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
    fn filter(deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Dequeue::filter(&mut deq.deq, gc, pool);
    }
}

impl<T: Clone> Memento for DequeueSome<T> {
    type Object<'o> = &'o Queue<T>;
    type Input<'o> = ();
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Some(v) = self.deq.run(queue, (), rec, guard, pool).unwrap() {
            return Ok(v);
        }

        loop {
            if let Some(v) = self.deq.run(queue, (), false, guard, pool).unwrap() {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.deq.reset(guard, pool);
    }
}

unsafe impl<T: Clone> Send for DequeueSome<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: CachePadded<SMOAtomic<Self, Node<MaybeUninit<T>>, TryDequeue<T>>>,
    tail: CachePadded<PAtomic<Node<MaybeUninit<T>>>>,
}

impl<T: Clone> PDefault for Queue<T> {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::from(MaybeUninit::uninit()), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(SMOAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone> Collectable for Queue<T> {
    fn filter(queue: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        SMOAtomic::filter(&mut queue.head, gc, pool);
    }
}

impl<T: Clone> Traversable<Node<MaybeUninit<T>>> for Queue<T> {
    fn search(
        &self,
        target: PShared<'_, Node<MaybeUninit<T>>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard);

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

unsafe impl<T: Clone + Send + Sync> Send for Queue<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};
    use rusty_fork::rusty_fork_test;

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
        fn filter(m: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            for i in 0..COUNT {
                Enqueue::filter(&mut m.enqs[i], gc, pool);
                Dequeue::filter(&mut m.deqs[i], gc, pool);
            }
        }
    }

    impl Memento for EnqDeq {
        type Object<'o> = &'o Queue<usize>;
        type Input<'o> = usize; // tid
        type Output<'o> = ();
        type Error<'o> = !;

        /// idempotent enq_deq
        fn run<'o>(
            &mut self,
            queue: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            match tid {
                // T0: 다른 스레드들의 실행결과를 확인
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check queue is empty
                    let mut tmp_deq = Dequeue::<usize>::default();
                    let must_none = tmp_deq.run(queue, (), rec, guard, pool).unwrap();
                    assert!(must_none.is_none());
                    tmp_deq.reset(guard, pool);

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    assert!((1..NR_THREAD + 1)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T0이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let _ = self.enqs[i].run(queue, tid, rec, guard, pool);
                        let ret = self.deqs[i].run(queue, (), rec, guard, pool).unwrap();
                        assert!(ret.is_some());
                    }

                    // deq 결과를 실험결과에 전달
                    for deq in self.deqs.as_mut() {
                        let ret = deq.run(queue, (), true, guard, pool).unwrap().unwrap();
                        let _ = RESULTS[ret].fetch_add(1, Ordering::SeqCst);
                    }

                    // "나 끝났다"
                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        }

        fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootObj for Queue<usize> {}
    impl TestRootMemento<Queue<usize>> for EnqDeq {}

    // TODO(opt): queue의 enq_deq과 합치기
    // - 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - pool을 2번째 열 때부터 gc 동작 확인가능:
    //      - 출력문으로 COUNT * NR_THREAD + 2개의 block이 reachable하다고 나옴
    //      - 여기서 +2는 Root, Queue를 가리키는 포인터
    rusty_fork_test! {
        #[test]
        fn enq_deq() {
            const FILE_NAME: &str = "composed_opt_enq_deq.pool";
            const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

            run_test::<Queue<usize>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
        }
    }
}
