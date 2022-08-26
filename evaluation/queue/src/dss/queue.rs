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
use std::sync::atomic::{AtomicIsize, Ordering};

#[repr(align(128))]
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

/// TAGS
//
//  0000 0000
//          ^(LSB 0) 1: enq_prepared
//         ^(LSB 1) 1: enq_completed
//        ^(LSB 2) 1: deq_prepared
//       ^(LSB 3) 1: deq_completed_empty

// - non-prepared: null with tag                            0b000
// - enq-prepared: node with tag                            0b001  (ENQ_PREP_TAG)
// - enq-prepared-and-completed: node with tag              0b101  (ENQ_PREP_TAG | END_COMPL_TAG)
// - deq-prepared: null with tag                            0b010  (DEQ_PREP_TAG)
// - deq-prepared-and-completed(empty): null wth tag        0b110  (DEQ_PREP_TAG | EMPTY_TAG)
// - deq-prepared-and-completed?(not-empty): node with tag  0b010  (DEQ_PREP_TAG) // check `deq_tid` of node to discern if i am dequeuer or not
//
//  0000 0000
//          ^(LSB 0) 1: enq_prepared
//         ^(LSB 1) 1: dnq_prepared
//        ^(LSB 2) 1: completed (END_COMPL_TAG if enq_prepared, EMPTY_TAG if dnq_prepared)
const ENQ_PREP_TAG: usize = 1;
const ENQ_COMPL_TAG: usize = 4;
const DEQ_PREP_TAG: usize = 2;
const EMPTY_TAG: usize = 4;

enum _OpResolved {
    Enqueue,
    Dequeue,
}

#[derive(Debug)]
pub struct DSSQueue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    x: [CachePadded<PAtomic<Node<T>>>; MAX_THREADS + 1],
}

impl<T: Clone> Collectable for DSSQueue<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<T: Clone> PDefault for DSSQueue<T> {
    fn pdefault(handle: &Handle) -> Self {
        let sentinel = POwned::new(Node::default(), handle.pool).into_shared(&handle.guard);
        persist_obj(unsafe { sentinel.deref(handle.pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
            x: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
        }
    }
}

impl<T: Clone> DSSQueue<T> {
    fn prep_enqueue(&self, val: T, tid: usize, pool: &PoolHandle) {
        let node = POwned::new(Node::new(val), pool);
        persist_obj(unsafe { node.deref(pool) }, true);
        self.x[tid].store(node.with_tag(ENQ_PREP_TAG), Ordering::Relaxed);
        persist_obj(&*self.x[tid], true); // persist inner of CachePadded
    }

    fn exec_enqueue(&self, tid: usize, guard: &Guard, pool: &PoolHandle) {
        let node = self.x[tid].load(Ordering::Relaxed, guard);

        loop {
            let last = self.tail.load(Ordering::SeqCst, guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, guard);

            if last == self.tail.load(Ordering::SeqCst, guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(
                            PShared::null(),
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        )
                        .is_ok()
                    {
                        persist_obj(&last_ref.next, true);

                        self.x[tid]
                            .store(node.with_tag(node.tag() | ENQ_COMPL_TAG), Ordering::Relaxed);
                        persist_obj(&*self.x[tid], true);

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

    fn _resolve_enqueue(&self, tid: usize, guard: &Guard, pool: &'static PoolHandle) -> (T, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        let node_ref = unsafe { x_tid.deref(pool) };
        let value = unsafe { (*node_ref.val.as_ptr()).clone() };
        if (x_tid.tag() & ENQ_COMPL_TAG) != 0 {
            // enqueue was prepared and took effect
            (value, true)
        } else {
            // enqueue was prepared and did not take effect
            (value, false)
        }
    }

    fn prep_dequeue(&self, tid: usize) {
        self.x[tid].store(PShared::null().with_tag(DEQ_PREP_TAG), Ordering::Relaxed);
        persist_obj(&*self.x[tid], true);
    }

    fn exec_dequeue(&self, tid: usize, guard: &Guard, pool: &PoolHandle) -> Option<T> {
        loop {
            let first = self.head.load(Ordering::SeqCst, guard);
            let last = self.tail.load(Ordering::SeqCst, guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, guard);

            if first == self.head.load(Ordering::SeqCst, guard) {
                if first == last {
                    // empty queue
                    if next.is_null() {
                        // nothing new appended at tail
                        let node = self.x[tid].load(Ordering::Relaxed, guard);
                        self.x[tid].store(node.with_tag(node.tag() | EMPTY_TAG), Ordering::Relaxed);
                        persist_obj(&*self.x[tid], true);
                        return None; // EMPTY
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
                    // non-empty queue
                    self.x[tid].store(first.with_tag(DEQ_PREP_TAG), Ordering::Relaxed); // save predecessor of node to be dequeued
                    persist_obj(&*self.x[tid], true);

                    let next_ref = unsafe { next.deref(pool) };
                    if next_ref
                        .deq_tid
                        .compare_exchange(-1, tid as isize, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        persist_obj(&next_ref.deq_tid, true);
                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                        guard.defer_persist(&*self.head);
                        return Some(unsafe { (*next_ref.val.as_ptr()).clone() });
                    } else if self.head.load(Ordering::SeqCst, guard) == first {
                        // help another dequeueing thread
                        persist_obj(&next_ref.deq_tid, true);
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

    fn _resolve_dequeue(
        &self,
        tid: usize,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> (Option<T>, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        if x_tid == PShared::null().with_tag(DEQ_PREP_TAG) {
            // dequeue was prepared but did not take effect
            (None, false)
        } else if x_tid == PShared::null().with_tag(DEQ_PREP_TAG | EMPTY_TAG) {
            // empty queue
            (None, true)
        } else {
            let x_tid_ref = unsafe { x_tid.deref(pool) };
            let next = x_tid_ref.next.load(Ordering::SeqCst, guard);
            let next_ref = unsafe { next.deref(pool) };
            if next_ref.deq_tid.load(Ordering::SeqCst) == tid as isize {
                // non-empty queue
                let value = unsafe { (*next_ref.val.as_ptr()).clone() };
                (Some(value), true)
            } else {
                // X holds a node pointer, crashed before completing dequeue
                (None, false)
            }
        }
    }

    // return: ((op, op value), op was finished or not)
    fn _resolve(
        &self,
        tid: usize,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> (Option<(_OpResolved, Option<T>)>, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        if (x_tid.tag() & ENQ_PREP_TAG) != 0 {
            // Enq was prepared, so check the result
            let (value, completed) = self._resolve_enqueue(tid, guard, pool);
            (Some((_OpResolved::Enqueue, Some(value))), completed)
        } else if (x_tid.tag() & DEQ_PREP_TAG) != 0 {
            // Deq was prepared, so check the result
            let (value, completed) = self._resolve_dequeue(tid, guard, pool);
            (Some((_OpResolved::Dequeue, value)), completed)
        } else {
            // no operation was prepared
            (None, false)
        }
    }
}

impl<T: Clone> TestQueue for DSSQueue<T> {
    type EnqInput = (T, usize); // value, tid
    type DeqInput = usize; // tid

    fn enqueue(&self, (input, tid): Self::EnqInput, handle: &Handle) {
        self.prep_enqueue(input, tid, handle.pool);
        self.exec_enqueue(tid, &handle.guard, handle.pool);
    }

    fn dequeue(&self, tid: Self::DeqInput, handle: &Handle) {
        self.prep_dequeue(tid);
        let val = self.exec_dequeue(tid, &handle.guard, handle.pool);

        if val.is_some() {
            // deallocate previouse node in `x[tid]`
            let node_tid = self.x[tid].load(Ordering::Relaxed, &handle.guard);
            unsafe { handle.guard.defer_pdestroy(node_tid) };
        }
    }
}

#[derive(Debug, Collectable)]
pub struct TestDSSQueue {
    queue: DSSQueue<usize>,
}

impl PDefault for TestDSSQueue {
    fn pdefault(handle: &Handle) -> Self {
        let queue = DSSQueue::pdefault(handle);

        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.prep_enqueue(i, 0, handle.pool);
            queue.exec_enqueue(0, &handle.guard, handle.pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestDSSQueue {}

#[derive(Default, Debug, Memento, Collectable)]
pub struct TestDSSQueueEnqDeq<const PAIR: bool> {}

impl<const PAIR: bool> RootObj<TestDSSQueueEnqDeq<PAIR>> for TestDSSQueue {
    fn run(&self, _: &mut TestDSSQueueEnqDeq<PAIR>, handle: &Handle) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, _| {
                let enq_input = (tid, tid);
                let deq_input = tid;

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
