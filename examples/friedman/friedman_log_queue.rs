use crate::abstract_queue::*;
use crate::{TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned, PShared};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::pool::*;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

struct Node<T: Clone> {
    val: MaybeUninit<T>,
    next: PAtomic<Node<T>>,
    log_insert: PAtomic<LogEntry<T>>,
    log_remove: PAtomic<LogEntry<T>>,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        Self {
            val: MaybeUninit::uninit(),
            next: PAtomic::null(),
            log_insert: PAtomic::null(),
            log_remove: PAtomic::null(),
        }
    }
}

impl<T: Clone> Node<T> {
    fn new(val: T) -> Self {
        Self {
            val: MaybeUninit::new(val),
            next: PAtomic::null(),
            log_insert: PAtomic::null(),
            log_remove: PAtomic::null(),
        }
    }
}

struct LogEntry<T: Clone> {
    op_num: usize,
    op: Operation,
    status: bool,
    node: PAtomic<Node<T>>,
}

impl<T: Clone> LogEntry<T> {
    fn new(status: bool, node_with_log: PAtomic<Node<T>>, op: Operation, op_num: usize) -> Self {
        Self {
            op_num,
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

#[derive(Debug, Default)]
struct LogQueue<T: Clone> {
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
    logs: [PAtomic<LogEntry<T>>; MAX_THREADS],
}

impl<T: Clone> LogQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> Self {
        let sentinel = Node::default();
        unsafe {
            let guard = pepoch::unprotected(pool);
            let sentinel = POwned::new(sentinel, pool).into_shared(guard);
            Self {
                head: PAtomic::from(sentinel), // TODO: flush
                tail: PAtomic::from(sentinel), // TODO: flush
                logs: Default::default(),      // TODO: flush
            }
        }
    }

    pub fn enqueue<O: POp>(&self, val: T, tid: usize, op_num: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);

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
        // TODO: flush node
        // TODO: flush log;
        self.logs[tid].store(log, Ordering::SeqCst);
        // TODO: flush &logs[tid];

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
                        // TODO: flush(&last->next);
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
                    // TODO: flush(&last->next);
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

    pub fn dequeue<O: POp>(&self, tid: usize, op_num: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);

        let log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Dequeue, op_num),
            pool,
        )
        .into_shared(&guard);
        // TODO: flush log;
        self.logs[tid].store(log, Ordering::SeqCst);
        // TODO: flush &logs[tid];

        loop {
            let first = self.head.load(Ordering::SeqCst, &guard);
            let first_ref = unsafe { first.deref(pool) };
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let next = first_ref.next.load(Ordering::SeqCst, &guard);

            if first == self.head.load(Ordering::SeqCst, &guard) {
                if first == last {
                    if next.is_null() {
                        let mut log = self.logs[tid].load(Ordering::SeqCst, &guard);
                        let log_ref = unsafe { log.deref_mut(pool) };
                        log_ref.status = true;
                        // TODO: flush &log_ref.status
                        return;
                    }
                    // TODO: flush(&last->next);
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    );
                } else {
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
                        // TODO: flush(&first->next->logRemove);
                        let log_remove = next_ref.log_remove.load(Ordering::SeqCst, &guard);
                        let log_remove_ref = unsafe { log_remove.deref(pool) };
                        log_remove_ref.node.store(
                            first_ref.next.load(Ordering::SeqCst, &guard),
                            Ordering::SeqCst,
                        );
                        // TODO: flush(&first->next->logRemove->node);

                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                        return;
                    } else {
                        if self.head.load(Ordering::SeqCst, &guard) == first {
                            // TODO: flush(&first->next->logRemove);
                            let log_remove = next_ref.log_remove.load(Ordering::SeqCst, &guard);
                            let log_remove_ref = unsafe { log_remove.deref(pool) };
                            log_remove_ref.node.store(
                                first_ref.next.load(Ordering::SeqCst, &guard),
                                Ordering::SeqCst,
                            );
                            // TODO: flush(&first->next->logRemove->node);

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

#[derive(Default)]
pub struct GetLogQueueNOps {
    queue: LogQueue<usize>,
}

impl GetLogQueueNOps {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        self.queue = LogQueue::new(pool);
        for i in 0..QUEUE_INIT_SIZE {
            self.queue.enqueue(i, 0, 0, pool);
        }
    }
}

impl TestNOps for GetLogQueueNOps {}

impl POp for GetLogQueueNOps {
    type Object<'o> = ();
    type Input = (usize, f64, u32); // (n개 스레드로 m초 동안 테스트, p%/100-p% 확률로 enq/deq)
    type Output<'o> = usize; // 실행한 operation 수
    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (nr_thread, duration, probability): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        self.init(pool);

        // TODO: 현재는 input `p`로 실행할 테스트를 구분. 더 우아한 방법으로 바꾸기
        if probability != 65535 {
            // Test1: p% 확률로 enq 혹은 100-p% 확률로 deq
            self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                    let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                    enq_deq_prob(&self.queue, enq_input, deq_input, probability, pool);
                },
                nr_thread,
                duration,
            )
        } else {
            // Test2: enq; deq;
            self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                    let deq_input = (tid, 0); // TODO: op_num=0 으로 고정했음. 이래도 괜찮나?
                    enq_deq_pair(&self.queue, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            )
        }
    }

    fn reset(&mut self, _: bool) {
        // no-ops
    }
}

#[allow(warnings)]
fn main() {
    // no-op
}
