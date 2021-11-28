use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestKind, TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
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
    op: Operation,
    status: bool,
    node: PAtomic<Node<T>>,
}

impl<T: Clone> LogEntry<T> {
    fn new(status: bool, node_with_log: PAtomic<Node<T>>, op: Operation, op_num: usize) -> Self {
        Self {
            _op_num: op_num,
            op,
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
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
        op_num: usize,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        // NOTE: Log 큐의 하자 (1/2)
        // - 우리 큐: enq할 노드만 새롭게 할당 & persist함
        // - Log 큐: enq할 노드 뿐 아니라 enq log 또한 할당하고 persist함
        //
        // ```
        let log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Enqueue, op_num),
            pool,
        )
        .into_shared(&guard);
        let log_ref = unsafe { log.deref(pool) };
        let node = POwned::new(Node::new(val), pool).into_shared(&guard);
        let node_ref = unsafe { node.deref(pool) };

        log_ref.node.store(node, Ordering::SeqCst);
        node_ref.log_insert.store(log, Ordering::SeqCst);
        persist_obj(node_ref, true);
        persist_obj(log_ref, true);

        let prev = self.logs[tid].swap(log, Ordering::SeqCst, guard);
        persist_obj(&self.logs[tid], true);
        // ```

        // ``` prev 로그 및 prev 로그가 가리키는 노드를 free
        {
            // Log queue의 recovery phase는 수행됐다고 생각
            //   1. Enq Log: enq 성공못한 로그는 recovery phase에서 전부 enq
            //   2. Deq Log: 노드를 가리켰지만 Queue에서 노드를 Deq하지 못한 로그는 recovery phase에서 마저 전부 deq
            //   3. 이전 Log array는 버리고 새로 만듦
            //
            // 따라서 이 로직에 도달하는 것은 crash-free execution
            if prev.is_null() {
                let prev_ref = unsafe { prev.deref(pool) };

                // 이 시점(crash-free execution)의 prev 로그는 항상 enq or deq 성공한 로그
                // - enq 로그라면 가리키는 노드를 free하면 안됨
                // - deq 로그라면 가리키는 노드를 free. 단 "EMPTY"로 성공했었다면 free하면 안됨
                if let Operation::Dequeue = prev_ref.op {
                    // status=true면 dequeue를 EMPTY로 성공한 것임
                    if !prev_ref.status {
                        let node = prev_ref.node.load(Ordering::SeqCst, guard);
                        unsafe { guard.defer_pdestroy(node) };
                    }
                }
                unsafe { guard.defer_pdestroy(prev) };
            }
        }
        // ```

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
        }
    }

    fn dequeue(&self, tid: usize, op_num: usize, guard: &mut Guard, pool: &'static PoolHandle) {
        // NOTE: Log 큐의 하자 (2/2)
        // - 우리 큐: deq에서 새롭게 할당하는 것 없음
        // - Log 큐: deq 로그 할당 및 persist
        //
        // ```
        let mut log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Dequeue, op_num),
            pool,
        )
        .into_shared(&guard);
        let log_ref = unsafe { log.deref_mut(pool) };
        persist_obj(log_ref, true);

        let prev = self.logs[tid].swap(log, Ordering::SeqCst, guard);
        persist_obj(&self.logs[tid], true);
        // ```

        // ``` prev 로그 및 prev 로그가 가리키는 노드를 free
        {
            // Log queue의 recovery phase는 수행됐다고 생각
            //   1. Enq Log: enq 성공못한 로그는 recovery phase에서 전부 enq
            //   2. Deq Log: 노드를 가리켰지만 Queue에서 노드를 Deq하지 못한 로그는 recovery phase에서 마저 전부 deq
            //   3. 이전 Log array는 버리고 새로 만듦
            //
            // 따라서 이 로직에 도달하는 것은 crash-free execution
            if prev.is_null() {
                let prev_ref = unsafe { prev.deref(pool) };

                // 이 시점(crash-free execution)의 prev 로그는 항상 enq or deq 성공한 로그
                // - enq 로그라면 가리키는 노드를 free하면 안됨
                // - deq 로그라면 가리키는 노드를 free. 단 "EMPTY"로 성공했었다면 free하면 안됨
                if let Operation::Dequeue = prev_ref.op {
                    // status=true면 dequeue를 EMPTY로 성공한 것임
                    if !prev_ref.status {
                        let node = prev_ref.node.load(Ordering::SeqCst, guard);
                        unsafe { guard.defer_pdestroy(node) };
                    }
                }
                unsafe { guard.defer_pdestroy(prev) };
            }
        }
        // ```

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
                        // NOTE: 로그가 가리키고 있을 수 있으니 여기서 deq한 노드를 free하면 안됨
                        persist_obj(&self.head, true);
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
        }
    }
}

impl<T: Clone> TestQueue for LogQueue<T> {
    type EnqInput = (T, usize, usize); // input, tid, op_num
    type DeqInput = (usize, usize); // tid, op_num

    fn enqueue(
        &self,
        (input, tid, op_num): Self::EnqInput,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        self.enqueue(input, tid, op_num, guard, pool);
    }

    fn dequeue(&self, (tid, op_num): Self::DeqInput, guard: &mut Guard, pool: &'static PoolHandle) {
        self.dequeue(tid, op_num, guard, pool);
    }
}

#[derive(Debug)]
pub struct TestLogQueue {
    queue: LogQueue<usize>,
}

impl Collectable for TestLogQueue {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestLogQueue {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = LogQueue::pdefault(pool);
        let mut guard = epoch::pin();

        // 초기 노드 삽입
        for i in 0..QUEUE_INIT_SIZE {
            queue.enqueue(i, 0, 0, &mut guard, pool);
        }
        Self { queue }
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct LogQueueEnqDeqPair;

impl Collectable for LogQueueEnqDeqPair {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
                let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
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

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        // no-op
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct LogQueueEnqDeqProb;

impl Collectable for LogQueueEnqDeqProb {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
                let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
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

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        // no-op
    }
}
