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
    ret_val: [CachePadded<PAtomic<Option<T>>>; MAX_THREADS + 1], // None: "EMPTY"
}

impl<T: Clone> Collectable for DurableQueue<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<T: Clone> PDefault for DurableQueue<T> {
    fn pdefault(pool: &PoolHandle) -> Self {
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
    fn enqueue(&self, val: T, guard: &Guard, pool: &PoolHandle) {
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

    fn dequeue(&self, tid: usize, guard: &Guard, pool: &PoolHandle) {
        let mut new_ret_val = POwned::new(None, pool).into_shared(guard);
        let new_ret_val_ref = unsafe { new_ret_val.deref_mut(pool) };
        persist_obj(new_ret_val_ref, true);

        let prev = self.ret_val[tid].load(Ordering::Relaxed, guard);
        self.ret_val[tid].store(new_ret_val, Ordering::Relaxed);
        persist_obj(&*self.ret_val[tid], true); // persist inner of CachePadded

        // deallocate previous ret_val
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
                        guard.defer_persist(&*self.head); // persist inner of CachePadded
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
    type EnqInput = T; // value
    type DeqInput = usize; // tid

    fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &PoolHandle) {
        self.enqueue(input, guard, pool);
    }

    fn dequeue(&self, tid: Self::DeqInput, guard: &Guard, pool: &PoolHandle) {
        self.dequeue(tid, guard, pool);
    }
}

#[derive(Debug)]
pub struct TestDurableQueue {
    queue: DurableQueue<usize>,
}

impl Collectable for TestDurableQueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestDurableQueue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let queue = DurableQueue::pdefault(pool);
        let guard = epoch::pin();

        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue(i, &guard, pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestDurableQueue {}

#[derive(Default, Debug)]
pub struct TestDurableQueueEnqDeq<const PAIR: bool> {}

impl<const PAIR: bool> Collectable for TestDurableQueueEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestDurableQueueEnqDeq<PAIR>> for TestDurableQueue {
    fn run(
        &self,
        _: &mut TestDurableQueueEnqDeq<PAIR>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_input = tid;
                let deq_input = tid;

                if PAIR {
                    enq_deq_pair(q, enq_input, deq_input, guard, pool);
                } else {
                    enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
                }
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
    }
}
