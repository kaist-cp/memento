use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use memento::ds::queue::*;
use memento::ploc::Handle;
use memento::pmem::alloc::{Collectable, GarbageCollection};
use memento::pmem::pool::*;
use memento::{Collectable, Memento, PDefault};

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone + Collectable> TestQueue for Queue<T> {
    type EnqInput = (T, &'static mut Enqueue<T>); // value, memento
    type DeqInput = &'static mut Dequeue<T>; // memento

    fn enqueue(&self, input: Self::EnqInput, handle: &Handle) {
        let (value, enq_memento) = input;
        self.enqueue(value, enq_memento, handle);
    }

    fn dequeue(&self, input: Self::DeqInput, handle: &Handle) {
        let deq_memento = input;
        let _ = self.dequeue(deq_memento, handle);
    }
}

/// Root obj for evaluation of MementoQueue
#[derive(Debug)]
pub struct TestMementoQueue {
    queue: Queue<usize>,
}

impl Collectable for TestMementoQueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueue {
    fn pdefault(handle: &Handle) -> Self {
        let queue = Queue::pdefault(handle);

        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue(i, &mut push_init, handle);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueue {}

#[derive(Debug, Memento, Collectable)]
pub struct TestMementoQueueEnqDeq<const PAIR: bool> {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl<const PAIR: bool> Default for TestMementoQueueEnqDeq<PAIR> {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueEnqDeq<PAIR>> for TestMementoQueue {
    fn run(&self, mmt: &mut TestMementoQueueEnqDeq<PAIR>, handle: &Handle) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, _| {
                // unwrap CachePadded
                let enq =
                    unsafe { (&*mmt.enq as *const _ as *mut Enqueue<usize>).as_mut() }.unwrap();
                let deq =
                    unsafe { (&*mmt.deq as *const _ as *mut Dequeue<usize>).as_mut() }.unwrap();
                let enq_input = (tid, enq); // enq `tid`
                let deq_input = deq;

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
