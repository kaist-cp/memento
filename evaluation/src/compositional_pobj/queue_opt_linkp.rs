use core::sync::atomic::Ordering;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::persistent::*;
use memento::plocation::pool::*;
use memento::plocation::ralloc::{Collectable, GarbageCollection};
use memento::queue_opt_link_persist::*;

use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};

impl<T: 'static + Clone> TestQueue for ComposedQueueOpt<T> {
    type EnqInput = (&'static mut Enqueue<T>, T); // Memento, input
    type DeqInput = &'static mut Dequeue<T>; // Memento

    fn enqueue(&self, (enq, input): Self::EnqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = enq.run(self, input, false, guard, pool);

        // TODO: custom logic 추상화
        enq.reset(false, guard, pool);
    }

    fn dequeue(&self, deq: Self::DeqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = deq.run(self, (), false, guard, pool);
        deq.reset(false, guard, pool);
    }
}

/// 초기화시 세팅한 노드 수만큼 넣어줌
#[derive(Debug)]
pub struct TestMementoQueueOptLinkp {
    queue: ComposedQueueOpt<usize>,
}

impl Collectable for TestMementoQueueOptLinkp {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestMementoQueueOptLinkp {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = ComposedQueueOpt::pdefault(pool);
        let guard = epoch::pin();

        // 초기 노드 삽입
        let mut push_init = Enqueue::default();
        for i in 0..QUEUE_INIT_SIZE {
            let _ = push_init.run(&queue, i, false, &guard, pool);
            push_init.reset(false, &guard, pool);
        }
        Self { queue }
    }
}

#[derive(Debug)]
pub struct MementoQueueOptLinkpEnqDeqPair {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl Default for MementoQueueOptLinkpEnqDeqPair {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl Collectable for MementoQueueOptLinkpEnqDeqPair {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for MementoQueueOptLinkpEnqDeqPair {}

impl Memento for MementoQueueOptLinkpEnqDeqPair {
    type Object<'o> = &'o TestMementoQueueOptLinkp;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
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
                let enq_input = (enq, tid);
                let deq_input = deq;
                enq_deq_pair(q, enq_input, deq_input, guard, pool);
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);

        Ok(())
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}

#[derive(Debug)]
pub struct MementoQueueOptLinkpEnqDeqProb {
    enq: CachePadded<Enqueue<usize>>,
    deq: CachePadded<Dequeue<usize>>,
}

impl Default for MementoQueueOptLinkpEnqDeqProb {
    fn default() -> Self {
        Self {
            enq: CachePadded::new(Enqueue::<usize>::default()),
            deq: CachePadded::new(Dequeue::<usize>::default()),
        }
    }
}

impl Collectable for MementoQueueOptLinkpEnqDeqProb {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for MementoQueueOptLinkpEnqDeqProb {}

impl Memento for MementoQueueOptLinkpEnqDeqProb {
    type Object<'o> = &'o TestMementoQueueOptLinkp;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
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
                let enq_input = (enq, tid);
                let deq_input = deq;
                enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
            },
            tid,
            duration,
            guard,
        );

        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}
