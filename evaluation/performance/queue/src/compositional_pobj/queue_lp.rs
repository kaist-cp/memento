use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use memento::ds::queue_lp::*;
use memento::ploc::Handle;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::{Collectable, Memento, PDefault};

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone + Collectable> TestQueue for Queue<T> {
    type EnqInput = (T, &'static mut Enqueue<T>); // value, memento
    type DeqInput = &'static mut Dequeue<T>;

    fn enqueue(&self, input: Self::EnqInput, handle: &Handle) {
        let (value, enq_memento) = input;
        self.enqueue(value, enq_memento, handle);
    }

    fn dequeue(&self, deq_memento: Self::DeqInput, handle: &Handle) {
        let _ = self.dequeue(deq_memento, handle);
    }
}

#[derive(Debug)]
pub struct TestMementoQueueLp {
    queue: Queue<usize>,
}

impl Collectable for TestMementoQueueLp {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueLp {
    fn pdefault(handle: &Handle) -> Self {
        let queue = Queue::pdefault(handle);

        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue(i, &mut push_init, handle);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueueLp {}

#[derive(Debug, Memento, Collectable)]
pub struct TestMementoQueueLpEnqDeq<const PAIR: bool> {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl<const PAIR: bool> Default for TestMementoQueueLpEnqDeq<PAIR> {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueLpEnqDeq<PAIR>> for TestMementoQueueLp {
    fn run(&self, mmt: &mut TestMementoQueueLpEnqDeq<PAIR>, handle: &Handle) {
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
