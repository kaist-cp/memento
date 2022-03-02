use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::queue_comb::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::PDefault;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl TestQueue for Queue {
    type EnqInput = (usize, &'static mut Enqueue, usize); // value, memento, id
    type DeqInput = (&'static mut Dequeue, usize); // memento, tid

    fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut Queue).as_mut() }.unwrap();

        let (value, enq_memento, tid) = input;
        queue.PBQueueEnq::<false>(value, enq_memento, tid, guard, pool);
    }

    fn dequeue(&self, input: Self::DeqInput, guard: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut Queue).as_mut() }.unwrap();

        let (deq_memento, tid) = input;
        let _ = queue.PBQueueDeq::<false>(deq_memento, tid, guard, pool);
    }
}

/// Root obj for evaluation of MementoQueuePBComb
#[derive(Debug)]
pub struct TestMementoQueueComb {
    queue: Queue,
}

impl Collectable for TestMementoQueueComb {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueComb {
    fn pdefault(pool: &PoolHandle) -> Self {
        let mut queue = Queue::pdefault(pool);

        let guard = epoch::pin();
        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.PBQueueEnq::<false>(i, &mut push_init, 1, &guard, pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueueComb {}

#[derive(Debug)]
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

impl<const PAIR: bool> Collectable for TestMementoQueueCombEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueCombEnqDeq<PAIR>> for TestMementoQueueComb {
    fn run(
        &self,
        mmt: &mut TestMementoQueueCombEnqDeq<PAIR>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                // unwrap CachePadded
                let enq = unsafe { (&*mmt.enq as *const _ as *mut Enqueue).as_mut() }.unwrap();
                let deq = unsafe { (&*mmt.deq as *const _ as *mut Dequeue).as_mut() }.unwrap();
                let enq_input = (tid, enq, tid); // enq `tid`
                let deq_input = (deq, tid);

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
