use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::{Backoff, CachePadded};
use epoch::Guard;
use memento::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use memento::persistent::*;
use memento::plocation::ralloc::{Collectable, GarbageCollection};
use memento::plocation::{ll::*, pool::*};
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

struct Node<T: Clone> {
    _val: MaybeUninit<T>,
    next: PAtomic<Node<T>>,
    log_insert: PAtomic<LogEntry<T>>,
    log_remove: PAtomic<LogEntry<T>>,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        Self {
            _val: MaybeUninit::uninit(),
            next: PAtomic::null(),
            log_insert: PAtomic::null(),
            log_remove: PAtomic::null(),
        }
    }
}

impl<T: Clone> Node<T> {
    fn new(val: T) -> Self {
        Self {
            _val: MaybeUninit::new(val),
            next: PAtomic::null(),
            log_insert: PAtomic::null(),
            log_remove: PAtomic::null(),
        }
    }
}

struct LogEntry<T: Clone> {
    _op_num: usize,
    _op: Operation,
    status: bool,
    node: PAtomic<Node<T>>,
}

impl<T: Clone> LogEntry<T> {
    fn new(status: bool, node_with_log: PAtomic<Node<T>>, op: Operation, op_num: usize) -> Self {
        Self {
            _op_num: op_num,
            _op: op,
            status,
            node: node_with_log,
        }
    }
}

enum Operation {
    Enqueue,
    Dequeue,
}

#[derive(Debug)]
struct LogQueue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    logs: [CachePadded<PAtomic<LogEntry<T>>>; MAX_THREADS],
}

impl<T: Clone> Collectable for LogQueue<T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl<T: Clone> PDefault for LogQueue<T> {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
            logs: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
        }
    }
}

impl<T: Clone> LogQueue<T> {
    fn enqueue(
        &self,
        val: T,
        tid: usize,
        op_num: &mut usize,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        // NOTE: Log 큐의 하자 (1/2)
        // - 우리 큐: enq할 노드만 새롭게 할당 & persist함
        // - Log 큐: enq할 노드 뿐 아니라 enq log 또한 할당하고 persist함
        //
        // ```
        let log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Enqueue, *op_num),
            pool,
        )
        .into_shared(unsafe { epoch::unprotected() }); // 이 log는 `tid`만 건드리니 unprotect해도 안전
        let log_ref = unsafe { log.deref(pool) };
        let node = POwned::new(Node::new(val), pool);
        let node_ref = unsafe { node.deref(pool) };

        log_ref.node.store(node, Ordering::SeqCst);
        node_ref.log_insert.store(log, Ordering::SeqCst);
        persist_obj(node_ref, true);
        persist_obj(log_ref, true);

        let prev = self.logs[tid].load(Ordering::SeqCst, guard);
        self.logs[tid].store(log, Ordering::SeqCst);
        persist_obj(&*self.logs[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist

        // ```

        // 이전 로그를 free
        if !prev.is_null() {
            unsafe { guard.defer_pdestroy(prev) };
            // NOTE: 로그가 가리키고 있는 deq한 노드는 free하면 안됨. queue의 센티넬 노드로 쓰이고 있을 수 있음
        }

        let node = log_ref.node.load(Ordering::SeqCst, guard);
        let backoff = Backoff::new();
        loop {
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, &guard);

            if last == self.tail.load(Ordering::SeqCst, &guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(next, node, Ordering::SeqCst, Ordering::SeqCst, &guard)
                        .is_ok()
                    {
                        persist_obj(&last_ref.next, true);
                        let _ = self.tail.compare_exchange(
                            last,
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
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
                        &guard,
                    );
                }
            }

            backoff.snooze();
        }
    }

    fn dequeue(
        &self,
        tid: usize,
        op_num: &mut usize,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        // NOTE: Log 큐의 하자 (2/2)
        // - 우리 큐: deq에서 새롭게 할당하는 것 없음
        // - Log 큐: deq 로그 할당 및 persist
        //
        // ```
        let mut log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Dequeue, *op_num),
            pool,
        )
        .into_shared(unsafe { epoch::unprotected() }); // 이 log는 `tid`만 건드리니 unprotect해도 안전
        let log_ref = unsafe { log.deref_mut(pool) };
        persist_obj(log_ref, true);

        let prev = self.logs[tid].load(Ordering::SeqCst, guard);
        self.logs[tid].store(log, Ordering::SeqCst);
        persist_obj(&*self.logs[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist

        // ```

        // 이전 로그를 free
        if !prev.is_null() {
            unsafe { guard.defer_pdestroy(prev) };
            // NOTE: 로그가 가리키고 있는 deq한 노드는 free하면 안됨. queue의 센티넬 노드로 쓰이고 있을 수 있음
        }

        let backoff = Backoff::new();
        loop {
            let first = self.head.load(Ordering::SeqCst, &guard);
            let first_ref = unsafe { first.deref(pool) };
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let next = first_ref.next.load(Ordering::SeqCst, &guard);

            if first == self.head.load(Ordering::SeqCst, &guard) {
                if first == last {
                    if next.is_null() {
                        // TODO: atomic data?
                        log_ref.status = true;
                        persist_obj(&log_ref.status, true);
                        return;
                    }
                    let last_ref = unsafe { last.deref(pool) };
                    persist_obj(&last_ref.next, true);
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    );
                } else {
                    // NOTE: 여기서 Log 큐가 우리 큐랑 persist하는 시점은 다르지만 persist하는 총 횟수는 똑같음
                    // - 우리 큐:
                    //      - if/else문 진입 전에 persist 1번: "나는(deq POp) 이 노드를 pop 시도할거다"
                    //      - if/else문 진입 후에 각각 persist 1번: "이 노드를 pop해간 deq POp은 얘다"
                    // - Log 큐:
                    //      - if/else문 진입 전에 persist 0번
                    //      - if/else문 진입 후에 각각 persist 2번: "이 노드를 pop해간 deq log는 얘다", "deq log가 pop한 노드는 이거다"
                    // TODO: 이게 성능 차이에 영향 미칠지?

                    let next_ref = unsafe { next.deref(pool) };
                    if next_ref
                        .log_remove
                        .compare_exchange(
                            PShared::null(),
                            log,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        )
                        .is_ok()
                    {
                        persist_obj(&next_ref.log_remove, true);
                        let log_remove = next_ref.log_remove.load(Ordering::SeqCst, &guard);
                        let log_remove_ref = unsafe { log_remove.deref(pool) };
                        log_remove_ref.node.store(
                            first_ref.next.load(Ordering::SeqCst, &guard),
                            Ordering::SeqCst,
                        );
                        persist_obj(&log_remove_ref.node, true);

                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                        guard.defer_persist(&*self.head); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
                        unsafe { guard.defer_pdestroy(first) };
                        return;
                    } else if self.head.load(Ordering::SeqCst, &guard) == first {
                        persist_obj(&next_ref.log_remove, true);
                        let log_remove = next_ref.log_remove.load(Ordering::SeqCst, &guard);
                        let log_remove_ref = unsafe { log_remove.deref(pool) };
                        log_remove_ref.node.store(
                            first_ref.next.load(Ordering::SeqCst, &guard),
                            Ordering::SeqCst,
                        );
                        persist_obj(&log_remove_ref.node, true);

                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                    }
                }
            }

            backoff.snooze();
        }
    }
}

impl<T: Clone> TestQueue for LogQueue<T> {
    type EnqInput = (T, usize, &'static mut usize); // input, tid, op_num
    type DeqInput = (usize, &'static mut usize); // tid, op_num

    fn enqueue(
        &self,
        (input, tid, op_num): Self::EnqInput,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        self.enqueue(input, tid, op_num, guard, pool);
        *op_num += 1;
        persist_obj(op_num, true);
    }

    fn dequeue(&self, (tid, op_num): Self::DeqInput, guard: &mut Guard, pool: &'static PoolHandle) {
        self.dequeue(tid, op_num, guard, pool);
        *op_num += 1;
        persist_obj(op_num, true);
    }
}

#[derive(Debug)]
pub struct TestLogQueue {
    queue: LogQueue<usize>,
}

impl Collectable for TestLogQueue {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestLogQueue {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = LogQueue::pdefault(pool);
        let mut guard = epoch::pin();

        // 초기 노드 삽입
        for i in 0..QUEUE_INIT_SIZE {
            queue.enqueue(i, 0, &mut 0, &mut guard, pool);
        }
        Self { queue }
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct LogQueueEnqDeqPair {
    /// unique operation number
    op_num: CachePadded<usize>,
}

impl Collectable for LogQueueEnqDeqPair {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for LogQueueEnqDeqPair {}

impl Memento for LogQueueEnqDeqPair {
    type Object<'o> = &'o TestLogQueue;
    type Input = usize; // tid
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };

        let ops = self.test_nops(
            &|tid, guard| {
                // TODO: 더 깔끔하게 op_num의 ref 전달
                let op_num = unsafe { (&*self.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let op_num_same =
                    unsafe { (&*self.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let enq_input = (tid, tid, op_num);
                let deq_input = (tid, op_num_same);
                enq_deq_pair(q, enq_input, deq_input, guard, pool);
            },
            tid,
            duration,
            guard,
        );
        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &mut Guard, _: &'static PoolHandle) {
        // no-op
    }

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct LogQueueEnqDeqProb {
    /// unique operation number
    op_num: CachePadded<usize>,
}

impl Collectable for LogQueueEnqDeqProb {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for LogQueueEnqDeqProb {}

impl Memento for LogQueueEnqDeqProb {
    type Object<'o> = &'o TestLogQueue;
    type Input = usize; // tid
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                // TODO: 더 깔끔하게 op_num의 ref 전달
                let op_num = unsafe { (&*self.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let op_num_same =
                    unsafe { (&*self.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let enq_input = (tid, tid, op_num);
                let deq_input = (tid, op_num_same);
                enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
            },
            tid,
            duration,
            guard,
        );
        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &mut Guard, _: &'static PoolHandle) {
        // no-op
    }

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}
