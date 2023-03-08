#![allow(missing_docs)]
#![allow(missing_debug_implementations)]
use core::sync::atomic::Ordering;
use crossbeam_utils::CachePadded;
use memento::ds::queue_comb::*;
use memento::ploc::Handle;
use memento::pmem::alloc::{Collectable, GarbageCollection};
use memento::pmem::pool::*;
use memento::{Collectable, Memento, PDefault};

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl TestQueue for CombiningQueue {
    type EnqInput = (usize, &'static mut Enqueue); // value, memento
    type DeqInput = &'static mut Dequeue;

    fn enqueue(&self, input: Self::EnqInput, handle: &Handle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut CombiningQueue).as_mut() }.unwrap();

        // Enqueue
        let (value, enq_memento) = input;
        let _ = queue.comb_enqueue(value, enq_memento, handle);
    }

    fn dequeue(&self, deq_memento: Self::DeqInput, handle: &Handle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut CombiningQueue).as_mut() }.unwrap();

        // Dequeue
        let _ = queue.comb_dequeue(deq_memento, handle);
    }
}

/// Root obj for evaluation of MementoQueuePBComb
// #[derive(Debug)]
pub struct TestMementoQueueComb {
    queue: CombiningQueue,
}

impl Collectable for TestMementoQueueComb {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueComb {
    fn pdefault(handle: &Handle) -> Self {
        let queue = CombiningQueue::pdefault(handle);

        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            let _ = queue.comb_enqueue(i, &mut push_init, handle);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueueComb {}

#[derive(Memento, Collectable)]
pub struct TestMementoQueueCombEnqDeq<const PAIR: bool> {
    enq: CachePadded<Enqueue>,
    deq: CachePadded<Dequeue>,
}

impl<const PAIR: bool> Default for TestMementoQueueCombEnqDeq<PAIR> {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::default()),
            deq: CachePadded::new(Dequeue::default()),
        }
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueCombEnqDeq<PAIR>> for TestMementoQueueComb {
    fn run(&self, mmt: &mut TestMementoQueueCombEnqDeq<PAIR>, handle: &Handle) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, _| {
                // unwrap CachePadded
                let enq = unsafe { (&*mmt.enq as *const _ as *mut Enqueue).as_mut() }.unwrap();
                let deq = unsafe { (&*mmt.deq as *const _ as *mut Dequeue).as_mut() }.unwrap();
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
