use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::queue_general::*;
use memento::pmem::pool::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::{Memento, PDefault};

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone> TestQueue for QueueGeneral<T> {
    type EnqInput = (&'static mut Enqueue<T>, T, usize); // Memento, input, tid
    type DeqInput = (&'static mut Dequeue<T>, usize); // Memento, tid

    fn enqueue(&self, (enq, input, tid): Self::EnqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = enq.run(self, input, tid, false, guard, pool);

        // TODO: custom logic 추상화
        enq.reset(guard, pool);
    }

    fn dequeue(&self, (deq, tid): Self::DeqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = deq.run(self, (), tid, false, guard, pool);
        deq.reset(guard, pool);
    }
}

/// 초기화시 세팅한 노드 수만큼 넣어줌
#[derive(Debug)]
pub struct TestMementoQueueGeneral {
    queue: QueueGeneral<usize>,
}

impl Collectable for TestMementoQueueGeneral {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueGeneral {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = QueueGeneral::pdefault(pool);
        let guard = epoch::pin();

        // 초기 노드 삽입
        let mut push_init = Enqueue::default();
        for i in 0..QUEUE_INIT_SIZE {
            let _ = push_init.run(&queue, i, 0, false, &guard, pool);
            push_init.reset(&guard, pool);
        }
        Self { queue }
    }
}

#[derive(Debug)]
pub struct MementoQueueGeneralEnqDeqPair {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl Default for MementoQueueGeneralEnqDeqPair {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl Collectable for MementoQueueGeneralEnqDeqPair {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for MementoQueueGeneralEnqDeqPair {}

impl Memento for MementoQueueGeneralEnqDeqPair {
    type Object<'o> = &'o TestMementoQueueGeneral;
    type Input<'o> = (); // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        _: Self::Input<'o>,
        tid: usize,
        _: bool, // TODO: template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };

        let ops = self.test_nops(
            &|tid, guard| {
                // NOTE!!!: &CahePadded<T>를 &T로 읽으면 안됨. 지금처럼 &*로 &T를 가져와서 &T로 읽어야함
                let enq =
                    unsafe { (&*self.enq as *const _ as *mut Enqueue<usize>).as_mut() }.unwrap();
                let deq =
                    unsafe { (&*self.deq as *const _ as *mut Dequeue<usize>).as_mut() }.unwrap();
                let enq_input = (enq, tid, tid); // `tid` 값을 enq. 특별한 이유는 없음
                let deq_input = (deq, tid);
                enq_deq_pair(q, enq_input, deq_input, guard, pool);
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);

        Ok(())
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}

#[derive(Debug)]
pub struct MementoQueueGeneralEnqDeqProb {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl Default for MementoQueueGeneralEnqDeqProb {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl Collectable for MementoQueueGeneralEnqDeqProb {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for MementoQueueGeneralEnqDeqProb {}

impl Memento for MementoQueueGeneralEnqDeqProb {
    type Object<'o> = &'o TestMementoQueueGeneral;
    type Input<'o> = ();
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        _: Self::Input<'o>,
        tid: usize,
        _: bool, // TODO: template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                // NOTE!!!: &CahePadded<T>를 &T로 읽으면 안됨. 지금처럼 &*로 &T를 가져와서 &T로 읽어야함
                let enq =
                    unsafe { (&*self.enq as *const _ as *mut Enqueue<usize>).as_mut() }.unwrap();
                let deq =
                    unsafe { (&*self.deq as *const _ as *mut Dequeue<usize>).as_mut() }.unwrap();
                let enq_input = (enq, tid, tid);
                let deq_input = (deq, tid);
                enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}
