use core::sync::atomic::Ordering;
use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;
use memento::ds::queue_pbcomb::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{persist_obj, pool::*};
use memento::PDefault;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl TestQueue for QueuePBComb {
    type EnqInput = (usize, usize, &'static mut usize); // value, tid, sequence number
    type DeqInput = (usize, &'static mut usize); // tid, sequence number

    fn enqueue(&self, (value, tid, seq): Self::EnqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const QueuePBComb as *mut QueuePBComb).as_mut() }.unwrap();

        // enq
        let _ = queue.PBQueue(Func::ENQUEUE, value, *seq, tid, pool);
        *seq += 1;
        persist_obj(seq, true);
    }

    fn dequeue(&self, (tid, seq): Self::DeqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const QueuePBComb as *mut QueuePBComb).as_mut() }.unwrap();

        // deq
        let _ = queue.PBQueue(Func::DEQUEUE, 0, *seq, tid, pool);
        *seq += 1;
        persist_obj(seq, true);
    }
}

#[derive(Debug)]
pub struct TestPBCombQueue {
    queue: QueuePBComb,
}

impl Collectable for TestPBCombQueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestPBCombQueue {
    fn pdefault(pool: &PoolHandle) -> Self {
        let mut queue = QueuePBComb::pdefault(pool);

        // 초기 노드 삽입
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            let _ = queue.PBQueue(Func::ENQUEUE, i, 0, 1, pool); // tid 1
        }
        Self { queue }
    }
}

impl TestNOps for TestPBCombQueue {}

#[derive(Debug, Default)]
pub struct TestPBCombQueueEnqDeq<const PAIR: bool> {
    enq_seq: CachePadded<usize>,
    deq_seq: CachePadded<usize>,
}

impl<const PAIR: bool> Collectable for TestPBCombQueueEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestPBCombQueueEnqDeq<PAIR>> for TestPBCombQueue {
    fn run(
        &self,
        mmt: &mut TestPBCombQueueEnqDeq<PAIR>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_seq =
                    unsafe { (&*mmt.enq_seq as *const _ as *mut usize).as_mut() }.unwrap();
                let deq_seq =
                    unsafe { (&*mmt.deq_seq as *const _ as *mut usize).as_mut() }.unwrap();
                let enq_input = (tid, tid, enq_seq); // `tid` 값을 enq. 특별한 이유는 없음
                let deq_input = (tid, deq_seq);

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
