use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use epoch::Guard;
use memento::pepoch::{PAtomic, PDestroyable, POwned};
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{ll::*, pool::*};
use memento::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicIsize, Ordering};

struct Node<T: Clone> {
    val: MaybeUninit<T>,
    next: PAtomic<Node<T>>,
    deq_tid: AtomicIsize,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        Self {
            val: MaybeUninit::uninit(),
            next: PAtomic::null(),
            deq_tid: AtomicIsize::new(-1),
        }
    }
}

impl<T: Clone> Node<T> {
    fn new(val: T) -> Self {
        Self {
            val: MaybeUninit::new(val),
            next: PAtomic::null(),
            deq_tid: AtomicIsize::new(-1),
        }
    }
}

#[derive(Debug)]
struct DurableQueue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    ret_val: [CachePadded<PAtomic<Option<T>>>; MAX_THREADS], // None: "EMPTY"
}

impl<T: Clone> Collectable for DurableQueue<T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl<T: Clone> PDefault for DurableQueue<T> {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
            ret_val: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
        }
    }
}

impl<T: Clone> DurableQueue<T> {
    fn enqueue(&self, val: T, guard: &Guard, pool: &'static PoolHandle) {
        let node = POwned::new(Node::new(val), pool).into_shared(guard);
        persist_obj(unsafe { node.deref(pool) }, true);

        loop {
            let last = self.tail.load(Ordering::SeqCst, guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, guard);

            if last == self.tail.load(Ordering::SeqCst, guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(next, node, Ordering::SeqCst, Ordering::SeqCst, guard)
                        .is_ok()
                    {
                        persist_obj(&last_ref.next, true);
                        let _ = self.tail.compare_exchange(
                            last,
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                        return;
                    }
                } else {
                    persist_obj(&last_ref.next, true);
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                };
            }
        }
    }

    fn dequeue(&self, tid: usize, guard: &Guard, pool: &'static PoolHandle) {
        // NOTE: Durable 큐의 하자 (1/1)
        // - 우리 큐: deq에서 새롭게 할당하는 것 없음
        // - Durable 큐: deq한 값을 가리킬 포인터 할당 및 persist
        //
        // ```
        let mut new_ret_val = POwned::new(None, pool).into_shared(guard);
        let new_ret_val_ref = unsafe { new_ret_val.deref_mut(pool) };
        persist_obj(new_ret_val_ref, true);

        let prev = self.ret_val[tid].load(Ordering::Relaxed, guard);
        self.ret_val[tid].store(new_ret_val, Ordering::Relaxed);
        persist_obj(&*self.ret_val[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist

        // ```
        // 이전 ret_val을 free
        if !prev.is_null() {
            unsafe { guard.defer_pdestroy(prev) };
        }

        loop {
            let first = self.head.load(Ordering::SeqCst, guard);
            let last = self.tail.load(Ordering::SeqCst, guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, guard);

            if first == self.head.load(Ordering::SeqCst, guard) {
                if first == last {
                    if next.is_null() {
                        // TODO: atomic data?
                        *new_ret_val_ref = None;
                        persist_obj(new_ret_val_ref, true);
                        return;
                    }
                    let last_ref = unsafe { last.deref(pool) };
                    persist_obj(&last_ref.next, true);
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                } else {
                    // NOTE: 여기서 Durable 큐가 우리 큐랑 persist하는 시점은 다르지만 persist하는 총 횟수는 똑같음
                    // - 우리 큐:
                    //      - if/else문 진입 전에 persist 1번: "나는(deq POp) 이 노드를 pop 시도할거다"
                    //      - if/else문 진입 후에 각각 persist 1번: "이 노드를 pop해간 deq POp은 얘다"
                    // - Durable 큐:
                    //      - if/else문 진입 전에 persist 0번
                    //      - if/else문 진입 후에 각각 persist 2번: "이 노드를 pop해간 스레드는 `deq tid`다, "`deq tid` 스레드가 pop한 값는 이거다"
                    // TODO: 이게 성능 차이에 영향 미칠지?
                    //      - e.g. KSC 실험은 T를 고작 usize로 했지만, pop value의 사이즈가 커지면 유의미한 차이를 보일 것으로 기대

                    let next_ref = unsafe { next.deref(pool) };
                    let val = Some(unsafe { (*next_ref.val.as_ptr()).clone() });

                    if next_ref
                        .deq_tid
                        .compare_exchange(-1, tid as isize, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        persist_obj(&next_ref.deq_tid, true);
                        *new_ret_val_ref = val;
                        persist_obj(new_ret_val_ref, true);
                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                        guard.defer_persist(&*self.head); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
                        unsafe { guard.defer_pdestroy(first) };
                        return;
                    } else {
                        let deq_tid = next_ref.deq_tid.load(Ordering::SeqCst);
                        let mut addr = self.ret_val[deq_tid as usize].load(Ordering::SeqCst, guard);

                        // Same context
                        if self.head.load(Ordering::SeqCst, guard) == first {
                            persist_obj(&next_ref.deq_tid, true);
                            let addr_ref = unsafe { addr.deref_mut(pool) };
                            *addr_ref = val;
                            persist_obj(addr_ref, true);
                            let _ = self.head.compare_exchange(
                                first,
                                next,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            );
                        }
                    }
                }
            }
        }
    }
}

impl<T: Clone> TestQueue for DurableQueue<T> {
    type EnqInput = T; // input
    type DeqInput = usize; // tid

    fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &'static PoolHandle) {
        self.enqueue(input, guard, pool);
    }

    fn dequeue(&self, tid: Self::DeqInput, guard: &Guard, pool: &'static PoolHandle) {
        self.dequeue(tid, guard, pool);
    }
}

#[derive(Debug)]
pub struct TestDurableQueue {
    queue: DurableQueue<usize>,
}

impl Collectable for TestDurableQueue {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestDurableQueue {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = DurableQueue::pdefault(pool);
        let mut guard = epoch::pin();

        // 초기 노드 삽입
        for i in 0..QUEUE_INIT_SIZE {
            queue.enqueue(i, &guard, pool);
        }
        Self { queue }
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct DurableQueueEnqDeqPair;

impl Collectable for DurableQueueEnqDeqPair {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for DurableQueueEnqDeqPair {}

impl Memento for DurableQueueEnqDeqPair {
    type Object<'o> = &'o TestDurableQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        _: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'_>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_input = tid;
                let deq_input = tid;
                enq_deq_pair(q, enq_input, deq_input, guard, pool);
            },
            tid,
            duration,
            guard,
        );
        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct DurableQueueEnqDeqProb;

impl Collectable for DurableQueueEnqDeqProb {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for DurableQueueEnqDeqProb {}

impl Memento for DurableQueueEnqDeqProb {
    type Object<'o> = &'o TestDurableQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        _: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'_>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_input = tid;
                let deq_input = tid;
                enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);

        Ok(())
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}
