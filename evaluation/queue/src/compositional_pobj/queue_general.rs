use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::queue_general::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::PDefault;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone + Collectable> TestQueue for QueueGeneral<T> {
    type EnqInput = (T, &'static mut Enqueue<T>, usize); // value, memento, tid
    type DeqInput = (&'static mut Dequeue<T>, usize); // memento, tid

    fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &PoolHandle) {
        let (value, enq_memento, tid) = input;
        self.enqueue::<false>(value, enq_memento, tid, guard, pool);
    }

    fn dequeue(&self, input: Self::DeqInput, guard: &Guard, pool: &PoolHandle) {
        let (deq_memento, tid) = input;
        let _ = self.dequeue::<false>(deq_memento, tid, guard, pool);
    }
}

/// Root obj for evaluation of MementoQueueGeneral
#[derive(Debug)]
pub struct TestMementoQueueGeneral {
    queue: QueueGeneral<usize>,
}

impl Collectable for TestMementoQueueGeneral {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueGeneral {
    fn pdefault(pool: &PoolHandle) -> Self {
        let queue = QueueGeneral::pdefault(pool);
        let guard = epoch::pin();

        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue::<false>(i, &mut push_init, 1, &guard, pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueueGeneral {}

#[derive(Debug)]
pub struct TestMementoQueueGeneralEnqDeq<const PAIR: bool> {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl<const PAIR: bool> Default for TestMementoQueueGeneralEnqDeq<PAIR> {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl<const PAIR: bool> Collectable for TestMementoQueueGeneralEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueGeneralEnqDeq<PAIR>> for TestMementoQueueGeneral {
    fn run(
        &self,
        mmt: &mut TestMementoQueueGeneralEnqDeq<PAIR>,
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
                let enq =
                    unsafe { (&*mmt.enq as *const _ as *mut Enqueue<usize>).as_mut() }.unwrap();
                let deq =
                    unsafe { (&*mmt.deq as *const _ as *mut Dequeue<usize>).as_mut() }.unwrap();
                let enq_input = (tid, enq, tid); // enqueue `tid`
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
