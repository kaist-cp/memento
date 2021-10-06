use super::*;
use crate::abstract_queue::{enq_deq_both, enq_deq_either, DeqInput, DurableQueue, EnqInput};
use crate::{TestNOps, INIT_COUNT, MAX_THREADS};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned, PShared};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::pool::*;
use std::sync::atomic::Ordering;

// TODO: `T`에 Default 강제해도 ㄱㅊ?
#[derive(Default)]
struct Node<T: Default + Clone> {
    val: T, // TODO: default 강제하지 말고 MaybeUninit 사용?
    next: PAtomic<Node<T>>,
    log_insert: PAtomic<LogEntry<T>>,
    log_remove: PAtomic<LogEntry<T>>,
}

impl<T: Default + Clone> Node<T> {
    fn new(val: T) -> Self {
        Self {
            val,
            next: PAtomic::null(),
            log_insert: PAtomic::null(),
            log_remove: PAtomic::null(),
        }
    }
}

struct LogEntry<T: Default + Clone> {
    op_num: usize,
    op: Operation,
    status: bool,
    node: PAtomic<Node<T>>,
}

impl<T: Default + Clone> LogEntry<T> {
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
struct LogQueue<T: Default + Clone> {
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
    logs: [PAtomic<LogEntry<T>>; MAX_THREADS],
}

impl<T: Default + Clone> LogQueue<T> {
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

impl<T: Default + Clone> DurableQueue<T> for LogQueue<T> {
    fn enqueue<O: POp>(&self, input: EnqInput<T>, pool: &PoolHandle<O>) {
        if let EnqInput::FriedmanLogQ(input, tid, op_num) = input {
            self.enqueue(input.clone(), tid, op_num, pool);
        }
    }

    fn dequeue<O: POp>(&self, input: DeqInput<T>, pool: &PoolHandle<O>) {
        if let DeqInput::FriedmanLogQ(tid, op_num) = input {
            self.dequeue(tid, op_num, pool);
        }
    }
}

#[derive(Default)]
pub struct GetLogQueueThroughput {
    queue: LogQueue<usize>,
}

impl GetLogQueueThroughput {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        self.queue = LogQueue::new(pool);

        // TODO: 큐 초기상태는 원소 몇개로 설정할 건가?
        for i in 0..INIT_COUNT {
            self.queue.enqueue(i, 0, 0, pool);
        }
    }
}

impl TestNOps for GetLogQueueThroughput {}

impl POp for GetLogQueueThroughput {
    type Object<'o> = ();
    type Input = (usize, f64, u32); // (n개 스레드로 m초 동안 테스트, p%/100-p% 확률로 enq/deq)
    type Output<'o> = Result<usize, ()>; // 실행한 operation 수
    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (nr_thread, duration, probability): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        self.init(pool);

        // TODO: refactoring
        if probability != 65535 {
            // Test: p% 확률로 enq, 100-p% 확률로 deq
            Ok(self.test_nops(
                &|tid| {
                    let enq_input = EnqInput::FriedmanLogQ(tid, tid, 0); // op_num=0?
                    let deq_input = DeqInput::FriedmanLogQ(tid, 0); // op_num=0?
                    enq_deq_either(&self.queue, enq_input, deq_input, probability, pool);
                },
                nr_thread,
                duration,
            ))
        } else {
            Ok(self.test_nops(
                &|tid| {
                    let enq_input = EnqInput::FriedmanLogQ(tid, tid, 0); // op_num=0?
                    let deq_input = DeqInput::FriedmanLogQ(tid, 0); // op_num=0?
                    enq_deq_both(&self.queue, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ))
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
