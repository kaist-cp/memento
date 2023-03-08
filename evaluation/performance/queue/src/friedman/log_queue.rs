use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use epoch::Guard;
use memento::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use memento::ploc::Handle;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{ll::*, pool::*};
use memento::*;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

#[repr(align(128))]
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
    logs: [CachePadded<PAtomic<LogEntry<T>>>; MAX_THREADS + 1],
}

impl<T: Clone> Collectable for LogQueue<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl<T: Clone> PDefault for LogQueue<T> {
    fn pdefault(handle: &Handle) -> Self {
        let sentinel = POwned::new(Node::default(), handle.pool).into_shared(&handle.guard);
        persist_obj(unsafe { sentinel.deref(handle.pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
            logs: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
        }
    }
}

impl<T: Clone> LogQueue<T> {
    fn enqueue(&self, val: T, tid: usize, op_num: &mut usize, guard: &Guard, pool: &PoolHandle) {
        // ```
        let log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Enqueue, *op_num),
            pool,
        )
        .into_shared(guard);
        let log_ref = unsafe { log.deref(pool) };
        let node = POwned::new(Node::new(val), pool).into_shared(guard);
        let node_ref = unsafe { node.deref(pool) };

        log_ref.node.store(node, Ordering::SeqCst);
        node_ref.log_insert.store(log, Ordering::SeqCst);
        persist_obj(node_ref, true);
        persist_obj(log_ref, true);

        let prev = self.logs[tid].load(Ordering::Relaxed, guard);
        self.logs[tid].store(log, Ordering::Relaxed);
        persist_obj(&*self.logs[tid], true); // persist inner of CachePadded

        // ```

        // deallocate previous log
        if !prev.is_null() {
            unsafe { guard.defer_pdestroy(prev) };
        }

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
                }
            }
        }
    }

    fn dequeue(&self, tid: usize, op_num: &mut usize, guard: &Guard, pool: &PoolHandle) {
        let mut log = POwned::new(
            LogEntry::<T>::new(false, PAtomic::null(), Operation::Dequeue, *op_num),
            pool,
        )
        .into_shared(guard);
        let log_ref = unsafe { log.deref_mut(pool) };
        persist_obj(log_ref, true);

        let prev = self.logs[tid].load(Ordering::Relaxed, guard);
        self.logs[tid].store(log, Ordering::Relaxed);
        persist_obj(&*self.logs[tid], true); // persist inner of CachePadded

        // deallocate previous log
        if !prev.is_null() {
            unsafe { guard.defer_pdestroy(prev) };
        }

        loop {
            let first = self.head.load(Ordering::SeqCst, guard);
            let first_ref = unsafe { first.deref(pool) };
            let last = self.tail.load(Ordering::SeqCst, guard);
            let next = first_ref.next.load(Ordering::SeqCst, guard);

            if first == self.head.load(Ordering::SeqCst, guard) {
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
                        guard,
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
                            guard,
                        )
                        .is_ok()
                    {
                        persist_obj(&next_ref.log_remove, true);
                        let log_remove = next_ref.log_remove.load(Ordering::SeqCst, guard);
                        let log_remove_ref = unsafe { log_remove.deref(pool) };
                        let val = first_ref.next.load(Ordering::SeqCst, guard);
                        log_remove_ref.node.store(val, Ordering::SeqCst);
                        persist_obj(&log_remove_ref.node, true);

                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                        guard.defer_persist(&*self.head); // persist inner of CachePadded
                        unsafe { guard.defer_pdestroy(first) };
                        return;
                    } else if self.head.load(Ordering::SeqCst, guard) == first {
                        persist_obj(&next_ref.log_remove, true);
                        let log_remove = next_ref.log_remove.load(Ordering::SeqCst, guard);
                        let log_remove_ref = unsafe { log_remove.deref(pool) };
                        let val = first_ref.next.load(Ordering::SeqCst, guard);
                        log_remove_ref.node.store(val, Ordering::SeqCst);
                        persist_obj(&log_remove_ref.node, true);

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

impl<T: Clone> TestQueue for LogQueue<T> {
    type EnqInput = (T, usize, &'static mut usize); // value, tid, op_num
    type DeqInput = (usize, &'static mut usize); // tid, op_num

    fn enqueue(&self, (input, tid, op_num): Self::EnqInput, handle: &Handle) {
        self.enqueue(input, tid, op_num, &handle.guard, handle.pool);
        *op_num += 1;
        persist_obj(op_num, true);
    }

    fn dequeue(&self, (tid, op_num): Self::DeqInput, handle: &Handle) {
        self.dequeue(tid, op_num, &handle.guard, handle.pool);
        *op_num += 1;
        persist_obj(op_num, true);
    }
}

#[derive(Debug, Collectable)]
pub struct TestLogQueue {
    queue: LogQueue<usize>,
}

impl PDefault for TestLogQueue {
    fn pdefault(handle: &Handle) -> Self {
        let queue = LogQueue::pdefault(handle);

        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue(i, handle.tid, &mut 0, &handle.guard, handle.pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestLogQueue {}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestLogQueueEnqDeq<const PAIR: bool> {
    /// unique operation number
    op_num: CachePadded<usize>,
}

impl<const PAIR: bool> RootObj<TestLogQueueEnqDeq<PAIR>> for TestLogQueue {
    fn run(&self, mmt: &mut TestLogQueueEnqDeq<PAIR>, handle: &Handle) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, _| {
                let op_num = unsafe { (&*mmt.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let op_num_same =
                    unsafe { (&*mmt.op_num as *const _ as *mut usize).as_mut() }.unwrap();
                let enq_input = (tid, tid, op_num);
                let deq_input = (tid, op_num_same);

                if PAIR {
                    enq_deq_pair(q, enq_input, deq_input, handle);
                } else {
                    enq_deq_prob(q, enq_input, deq_input, prob, handle);
                }
            },
            handle.tid,
            duration,
            &handle.guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
