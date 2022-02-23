use core::sync::atomic::Ordering;
use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;
use memento::ds::queue_pbcomb::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::PDefault;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl TestQueue for Queue {
    type EnqInput = (usize, &'static mut Enqueue, usize); // value, memento, id
    type DeqInput = (&'static mut Dequeue, usize); // memento, tid

    fn enqueue(&self, input: Self::EnqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut Queue).as_mut() }.unwrap();

        let (value, enq_memento, tid) = input;
        queue.PBQueueEnq::<false>(value, enq_memento, tid, pool);
    }

    fn dequeue(&self, input: Self::DeqInput, _: &Guard, pool: &PoolHandle) {
        // Get &mut queue
        let queue = unsafe { (self as *const _ as *mut Queue).as_mut() }.unwrap();

        let (deq_memento, tid) = input;
        let _ = queue.PBQueueDeq::<false>(deq_memento, tid, pool);
    }
}

/// 초기화시 세팅한 노드 수만큼 넣어줌
#[derive(Debug)]
pub struct TestMementoQueuePBComb {
    queue: Queue,
}

impl Collectable for TestMementoQueuePBComb {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueuePBComb {
    fn pdefault(pool: &PoolHandle) -> Self {
        let mut queue = Queue::pdefault(pool);

        // 초기 노드 삽입
        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.PBQueueEnq::<false>(i, &mut push_init, 1, pool);
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueuePBComb {}

#[derive(Debug)]
pub struct TestMementoQueuePBCombEnqDeq<const PAIR: bool> {
    enq: CachePadded<Enqueue>,
    deq: CachePadded<Dequeue>,
}

impl<const PAIR: bool> Default for TestMementoQueuePBCombEnqDeq<PAIR> {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::default()),
            deq: CachePadded::new(Dequeue::default()),
        }
    }
}

impl<const PAIR: bool> Collectable for TestMementoQueuePBCombEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueuePBCombEnqDeq<PAIR>> for TestMementoQueuePBComb {
    fn run(
        &self,
        mmt: &mut TestMementoQueuePBCombEnqDeq<PAIR>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let q = &self.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                // NOTE!!!: &CahePadded<T>를 &T로 읽으면 안됨. 지금처럼 &*로 &T를 가져와서 &T로 읽어야함
                let enq = unsafe { (&*mmt.enq as *const _ as *mut Enqueue).as_mut() }.unwrap();
                let deq = unsafe { (&*mmt.deq as *const _ as *mut Dequeue).as_mut() }.unwrap();
                let enq_input = (tid, enq, tid); // `tid` 값을 enq. 특별한 이유는 없음
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
