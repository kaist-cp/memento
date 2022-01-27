use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::queue_lp::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::PDefault;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone + Collectable> TestQueue for Queue<T> {
    type EnqInput = (T, &'static mut Enqueue<T>); // value, memento
    type DeqInput = (&'static mut Dequeue<T>, usize); // memento, tid

    fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &PoolHandle) {
        let (value, enq_memento) = input;
        self.enqueue::<false>(value, enq_memento, guard, pool);
        enq_memento.reset();
    }

    fn dequeue(&self, input: Self::DeqInput, guard: &Guard, pool: &PoolHandle) {
        let (deq_memento, tid) = input;
        let _ = self.dequeue::<false>(deq_memento, tid, guard, pool);
        deq_memento.reset();
    }
}

#[derive(Debug)]
pub struct TestMementoQueueLp {
    queue: Queue<usize>,
}

impl Collectable for TestMementoQueueLp {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueLp {
    fn pdefault(pool: &PoolHandle) -> Self {
        let queue = Queue::pdefault(pool);
        let guard = epoch::pin();

        // 초기 노드 삽입
        let mut push_init = Enqueue::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            queue.enqueue::<false>(i, &mut push_init, &guard, pool);
            push_init.reset();
        }
        Self { queue }
    }
}

impl TestNOps for TestMementoQueueLp {}

#[derive(Debug)]
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

impl<const PAIR: bool> Collectable for TestMementoQueueLpEnqDeq<PAIR> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl<const PAIR: bool> RootObj<TestMementoQueueLpEnqDeq<PAIR>> for TestMementoQueueLp {
    fn run(
        &self,
        mmt: &mut TestMementoQueueLpEnqDeq<PAIR>,
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
                let enq =
                    unsafe { (&*mmt.enq as *const _ as *mut Enqueue<usize>).as_mut() }.unwrap();
                let deq =
                    unsafe { (&*mmt.deq as *const _ as *mut Dequeue<usize>).as_mut() }.unwrap();
                let enq_input = (tid, enq); // `tid` 값을 enq. 특별한 이유는 없음
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
