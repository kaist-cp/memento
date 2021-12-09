//! Persistent opt queue using link-persist

use crate::ploc::smo::{self, InsertErr, Traversable, InsertLinkPersist, DeleteOptLinkPersist};
use crate::stack::DeallocNode;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::*;
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};

/// TODO: doc
// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
pub struct NodeOpt<T: Clone> {
    /// TODO: doc
    pub data: MaybeUninit<T>,

    /// TODO: doc
    pub next: PAtomic<NodeOpt<T>>,

    /// 누가 dequeue 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    dequeuer: AtomicUsize,
}

impl<T: Clone> Default for NodeOpt<T> {
    fn default() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: PAtomic::null(),
            dequeuer: AtomicUsize::new(DeleteOptLinkPersist::<ComposedQueueOpt<T>, _>::no_owner()),
        }
    }
}

impl<T: Clone> From<T> for NodeOpt<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: PAtomic::null(),
            dequeuer: AtomicUsize::new(DeleteOptLinkPersist::<ComposedQueueOpt<T>, _>::no_owner()),
        }
    }
}

impl<T: Clone> Collectable for NodeOpt<T> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next = unsafe { next.deref_mut(pool) };
            NodeOpt::<T>::mark(next, gc);
        }
    }
}

impl<T: Clone> smo::Node for NodeOpt<T> {
    #[inline]
    fn ack(&self) {}

    #[inline]
    fn acked(&self) -> bool {
        self.owner().load(Ordering::SeqCst) != DeleteOptLinkPersist::<ComposedQueueOpt<T>, Self>::no_owner()
    }

    #[inline]
    fn owner(&self) -> &AtomicUsize {
        &self.dequeuer
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct TryFail;

/// ComposedQueue의 try push operation
#[derive(Debug)]
pub struct TryEnqueue<T: Clone> {
    /// push를 위해 할당된 node
    insert: InsertLinkPersist<ComposedQueueOpt<T>, NodeOpt<T>>,
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
        InsertLinkPersist::filter(&mut try_push.insert, gc, pool);
    }
}

impl<T: Clone> TryEnqueue<T> {
    #[inline]
    fn before_cas(_: &mut NodeOpt<T>, old_tail_next: PShared<'_, NodeOpt<T>>) -> bool {
        old_tail_next.is_null()
    }
}

impl<T: 'static + Clone> Memento for TryEnqueue<T> {
    type Object<'o> = &'o ComposedQueueOpt<T>;
    type Input<'o> = PShared<'o, NodeOpt<T>>;
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
        let tail_ref = unsafe { tail.deref(pool) }; // TODO: filter 에서 tail align 해야 함

        self.insert
            .run(
                queue,
                (node, &tail_ref.next, Self::before_cas),
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
                if let InsertErr::AbortedBeforeCAS = e {
                    // tail is stale
                    persist_obj(&tail_ref.next, true);
                    let next = tail_ref.next.load(Ordering::SeqCst, guard); // TODO: 또 로드 해서 성능 저하. 어쩌면 insert의 로직을 더 줄여야 할 지도 모름.
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
    node: PAtomic<NodeOpt<T>>,
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
    fn filter(enq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut node = enq.node.load(Ordering::Relaxed, guard);
        if !node.is_null() {
            let node_ref = unsafe { node.deref_mut(pool) };
            NodeOpt::<T>::mark(node_ref, gc);
        }

        TryEnqueue::<T>::filter(&mut enq.try_enq, gc, pool);
    }
}

impl<T: Clone> Drop for Enqueue<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let node = self.node.load(Ordering::Relaxed, guard);
        assert!(node.is_null(), "reset 되어있지 않음.")
        // TODO: tryenq의 리셋여부 파악?
    }
}

impl<T: Clone> Memento for Enqueue<T> {
    type Object<'o> = &'o ComposedQueueOpt<T>;
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
        let node = if rec {
            let node = self.node.load(Ordering::Relaxed, guard);
            if node.is_null() {
                self.new_node(value, guard, pool)
            } else {
                node
            }
        } else {
            self.new_node(value, guard, pool)
        };

        if self.try_enq.run(queue, node, rec, guard, pool).is_ok() {
            return Ok(());
        }

        while self.try_enq.run(queue, node, false, guard, pool).is_err() {}
        Ok(())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_enq.reset(guard, pool);
    }
}

impl<T: Clone> Enqueue<T> {
    #[inline]
    fn new_node<'g>(
        &self,
        value: T,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> PShared<'g, NodeOpt<T>> {
        let node = POwned::new(NodeOpt::from(value), pool).into_shared(guard);
        self.node.store(node, Ordering::Relaxed);
        persist_obj(&self.node, true);
        node
    }
}

unsafe impl<T: 'static + Clone> Send for Enqueue<T> {}

/// Queue의 try dequeue operation
#[derive(Debug)]
pub struct TryDequeue<T: Clone> {
    /// pop를 위해 할당된 node
    delete_opt: DeleteOptLinkPersist<ComposedQueueOpt<T>, NodeOpt<T>>,
}

impl<T: Clone> Default for TryDequeue<T> {
    fn default() -> Self {
        Self {
            delete_opt: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryDequeue<T> {}

impl<T: Clone> Collectable for TryDequeue<T> {
    fn filter(try_deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        DeleteOptLinkPersist::filter(&mut try_deq.delete_opt, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryDequeue<T> {
    type Object<'o> = &'o ComposedQueueOpt<T>;
    type Input<'o> = &'o PAtomic<NodeOpt<T>>;
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        mine_loc: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.delete_opt
            .run(
                queue,
                (mine_loc, &queue.head, Self::get_next),
                rec,
                guard,
                pool,
            )
            .map(|ret| {
                ret.map(|popped| {
                    let next = unsafe { popped.deref(pool) }
                        .next
                        .load(Ordering::SeqCst, guard); // TODO: next를 다시 load해서 성능 저하
                    let next_ref = unsafe { next.deref(pool) };
                    unsafe { (*next_ref.data.as_ptr()).clone() }
                })
            })
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.delete_opt.reset(guard, pool);
    }
}

impl<T: Clone> DeallocNode<T, NodeOpt<T>> for TryDequeue<T> {
    #[inline]
    fn dealloc(&self, target: PShared<'_, NodeOpt<T>>, guard: &Guard, pool: &PoolHandle) {
        self.delete_opt.dealloc(target, guard, pool);
    }
}

impl<T: Clone> TryDequeue<T> {
    fn get_next<'g>(
        old_head: PShared<'_, NodeOpt<T>>,
        queue: &ComposedQueueOpt<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, NodeOpt<T>>>, ()> {
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

            return Err(());
        }

        Ok(Some(next))
    }
}

/// Queue의 Dequeue
#[derive(Debug)]
pub struct Dequeue<T: 'static + Clone> {
    mine: PAtomic<NodeOpt<T>>,
    try_deq: TryDequeue<T>,
}

impl<T: Clone> Default for Dequeue<T> {
    fn default() -> Self {
        Self {
            mine: Default::default(),
            try_deq: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Dequeue<T> {
    fn filter(deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut mine = deq.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            let mine_ref = unsafe { mine.deref_mut(pool) };
            NodeOpt::<T>::mark(mine_ref, gc);
        }

        TryDequeue::<T>::filter(&mut deq.try_deq, gc, pool);
    }
}

impl<T: Clone> Memento for Dequeue<T> {
    type Object<'o> = &'o ComposedQueueOpt<T>;
    type Input<'o> = ();
    type Output<'o>
    where
        T: 'o,
    = Option<T>;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Ok(v) = self.try_deq.run(queue, &self.mine, rec, guard, pool) {
            return Ok(v);
        }

        loop {
            if let Ok(v) = self.try_deq.run(queue, &self.mine, false, guard, pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        let mine = self.mine.load(Ordering::Relaxed, guard);

        // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
        // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
        self.mine.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.mine, true);
        self.try_deq.dealloc(mine, guard, pool);

        self.try_deq.reset(guard, pool);
    }
}

impl<T: Clone> Drop for Dequeue<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let mine = self.mine.load(Ordering::Relaxed, guard);
        assert!(mine.is_null(), "reset 되어있지 않음.")
        // TODO: trydeq의 리셋여부 파악?
    }
}

unsafe impl<T: Clone> Send for Dequeue<T> {}

/// Persistent Queue
#[derive(Debug)]
pub struct ComposedQueueOpt<T: Clone> {
    head: CachePadded<PAtomic<NodeOpt<T>>>,
    tail: CachePadded<PAtomic<NodeOpt<T>>>,
}

impl<T: Clone> PDefault for ComposedQueueOpt<T> {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(NodeOpt::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }
    }
}

impl<T: Clone> Collectable for ComposedQueueOpt<T> {
    fn filter(queue: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut head = queue.head.load(Ordering::SeqCst, guard);
        if !head.is_null() {
            let head_ref = unsafe { head.deref_mut(pool) };
            NodeOpt::mark(head_ref, gc);
        }
    }
}

impl<T: Clone> Traversable<NodeOpt<T>> for ComposedQueueOpt<T> {
    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, target: PShared<'_, NodeOpt<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard);

        // TODO: null 나올 때까지 하지 않고 tail을 통해서 범위를 제한할 수 있을지?
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

unsafe impl<T: Clone + Send + Sync> Send for ComposedQueueOpt<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pmem::ralloc::Collectable, test_utils::tests::*};
    use serial_test::serial;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 1_000_000;

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
        type Object<'o> = &'o ComposedQueueOpt<usize>;
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
                    tmp_deq.reset(false, guard, pool);

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    assert!((1..NR_THREAD + 1)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T0이 아닌 다른 스레드들은 queue에 { enq; deq; } 수행
                _ => {
                    // enq; deq;
                    for i in 0..COUNT {
                        let _ = self.enqs[i].run(queue, tid, rec, guard, pool); // TODO: 두번째 실행에서 run이 아니라 result로 확인하도록 바꿔야 함
                        assert!(self.deqs[i]
                            .run(queue, (), rec, guard, pool)
                            .unwrap()
                            .is_some());
                    }

                    // deq 결과를 실험결과에 전달
                    for deq in self.deqs.as_mut() {
                        let ret = deq.run(queue, (), rec, guard, pool).unwrap().unwrap();
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

    impl TestRootObj for ComposedQueueOpt<usize> {}
    impl TestRootMemento<ComposedQueueOpt<usize>> for EnqDeq {}

    // TODO: stack의 enq_deq과 합치기
    // - 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - pool을 2번째 열 때부터 gc 동작 확인가능:
    //      - 출력문으로 COUNT * NR_THREAD + 2개의 block이 reachable하다고 나옴
    //      - 여기서 +2는 Root, Queue를 가리키는 포인터
    //
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn enq_deq() {
        const FILE_NAME: &str = "composed_opt_link_enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<ComposedQueueOpt<usize>, EnqDeq, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
