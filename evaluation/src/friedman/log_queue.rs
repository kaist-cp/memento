use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned, PShared};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::{ll::*, pool::*};
use crossbeam_utils::CachePadded;
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

impl<T: Clone> LogQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> POwned<Self> {
        let guard = unsafe { pepoch::unprotected(pool) };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(
            Self {
                head: CachePadded::new(PAtomic::from(sentinel)),
                tail: CachePadded::new(PAtomic::from(sentinel)),
                logs: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
            },
            pool,
        );
        persist_obj(unsafe { ret.deref(pool) }, true);
        ret
    }

    fn enqueue<O: POp>(&self, val: T, tid: usize, op_num: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);

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

        self.logs[tid].store(log, Ordering::SeqCst);
        persist_obj(&self.logs[tid], true);
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

    fn dequeue<O: POp>(&self, tid: usize, op_num: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);

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
        self.logs[tid].store(log, Ordering::SeqCst);
        persist_obj(&self.logs[tid], true);
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
                    // - 우리 큐: if/else문 진입 전에 persist 1번, 진입 후 각각 persist 1번
                    // - Log 큐: if/else문 진입 전에 persist 0번, 진입 후 각각 persist 2번
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

    fn enqueue<O: POp>(&self, (input, tid, op_num): Self::EnqInput, pool: &PoolHandle<O>) {
        self.enqueue(input, tid, op_num, pool);
    }
    fn dequeue<O: POp>(&self, (tid, op_num): Self::DeqInput, pool: &PoolHandle<O>) {
        self.dequeue(tid, op_num, pool);
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct GetLogQueueNOps;

impl TestNOps for GetLogQueueNOps {}

impl POp for GetLogQueueNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수
    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        let q = LogQueue::<usize>::new(pool);
        let q_ref = unsafe { q.deref(pool) };

        for i in 0..QUEUE_INIT_SIZE {
            q_ref.enqueue(i, 0, 0, pool);
        }

        match kind {
            TestKind::QueuePair => {
                self.test_nops(
                    &|tid| {
                        let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                        let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                        enq_deq_pair(q_ref, enq_input, deq_input, pool);
                    },
                    nr_thread,
                    duration,
                )
            }
            TestKind::QueueProb(prob) => {
                self.test_nops(
                    &|tid| {
                        let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                        let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                        enq_deq_prob(q_ref, enq_input, deq_input, prob, pool);
                    },
                    nr_thread,
                    duration,
                )
            }
            _ => unreachable!("Queue를 위한 테스트만 해야함"),
        }
    }

    fn reset(&mut self, _: bool) {
        // no-ops
    }
}
