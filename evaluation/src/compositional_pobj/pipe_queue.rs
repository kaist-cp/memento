use std::sync::atomic::Ordering;

use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::{
    pipe::Pipe,
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    queue::{Dequeue, DequeueSome, Enqueue, Queue},
    {Memento, PDefault},
};

use crate::common::{
    queue::{enq_deq_pair, enq_deq_prob, TestQueue},
    TestNOps, DURATION, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS,
};

#[derive(Debug, Default)]
struct EnqueuePipeQ {
    enq: Enqueue<usize>,
    pipe: Pipe<DequeueSome<usize>, Enqueue<usize>>,

    /// reset 중인지 나타내는 flag
    resetting: bool,
}

impl Collectable for EnqueuePipeQ {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl Memento for EnqueuePipeQ {
    type Object<'o> = &'o PipeQueue;
    type Input<'o> = usize;
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        pipeq: Self::Object<'o>,
        value: Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        if self.resetting {
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false, guard, pool);
        }

        self.enq
            .run(&pipeq.q1, value, guard, pool)
            .map_err(|_| ())?;
        self.pipe
            .run((&pipeq.q1, &pipeq.q2), (), guard, pool)
            .map_err(|_| ())
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        if !nested {
            self.resetting = true;
            persist_obj(&self.resetting, true);
        }

        self.enq.reset(true, guard, pool);
        self.pipe.reset(true, guard, pool);

        if !nested {
            self.resetting = false;
            persist_obj(&self.resetting, true);
        }
    }

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}

#[derive(Debug, Default)]
struct DequeuePipeQ {
    deq: Dequeue<usize>,
}

impl Collectable for DequeuePipeQ {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl Memento for DequeuePipeQ {
    type Object<'o> = &'o PipeQueue;
    type Input<'o> = ();
    type Output<'o> = Option<usize>;
    type Error = ();

    fn run<'o>(
        &'o mut self,
        pipeq: Self::Object<'o>,
        _: Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let ret = self.deq.run(&pipeq.q2, (), guard, pool);
        Ok(ret.unwrap())
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        self.deq.reset(nested, guard, pool);
    }

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}

// TODO: generic
#[derive(Debug)]
struct PipeQueue {
    q1: Queue<usize>,
    q2: Queue<usize>,
}

impl TestQueue for PipeQueue {
    type EnqInput = (&'static mut EnqueuePipeQ, usize); // Memento, input
    type DeqInput = &'static mut DequeuePipeQ; // Memento

    fn enqueue(&self, (enq, input): Self::EnqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = enq.run(self, input, guard, pool);
        enq.reset(false, guard, pool);
    }

    fn dequeue(&self, deq: Self::DeqInput, guard: &Guard, pool: &'static PoolHandle) {
        let _ = deq.run(self, (), guard, pool);
        deq.reset(false, guard, pool);
    }
}

/// 초기화시 세팅한 노드 수만큼 넣어주기 위한 wrapper
#[derive(Debug)]
pub struct TestPipeQueue {
    pipeq: PipeQueue,
}

impl Collectable for TestPipeQueue {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestPipeQueue {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let mut guard = epoch::pin();

        let pipeq = PipeQueue {
            q1: Queue::pdefault(pool),
            q2: Queue::pdefault(pool),
        };

        // 초기 노드 삽입
        let mut enq_init = EnqueuePipeQ::default();
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            let _ = enq_init.run(&pipeq, i, &guard, pool);
            enq_init.reset(false, &guard, pool);
        }

        Self { pipeq }
    }
}

#[derive(Debug)]
pub struct MementoPipeQueueEnqDeqPair {
    push: CachePadded<EnqueuePipeQ>,
    pop: CachePadded<DequeuePipeQ>,
}

impl Default for MementoPipeQueueEnqDeqPair {
    fn default() -> Self {
        Self {
            push: CachePadded::new(EnqueuePipeQ::default()),
            pop: CachePadded::new(DequeuePipeQ::default()),
        }
    }
}

impl Collectable for MementoPipeQueueEnqDeqPair {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for MementoPipeQueueEnqDeqPair {}

impl Memento for MementoPipeQueueEnqDeqPair {
    type Object<'o> = &'o TestPipeQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let q = &queue.pipeq;
        let duration = unsafe { DURATION };

        let ops = self.test_nops(
            &|tid, guard| {
                let push =
                    unsafe { (&*self.push as *const _ as *mut EnqueuePipeQ).as_mut() }.unwrap();
                let pop =
                    unsafe { (&*self.pop as *const _ as *mut DequeuePipeQ).as_mut() }.unwrap();
                let enq_input = (push, tid);
                let deq_input = pop;
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

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}

#[derive(Debug)]
pub struct MementoPipeQueueEnqDeqProb {
    push: CachePadded<EnqueuePipeQ>,
    pop: CachePadded<DequeuePipeQ>,
}

impl Default for MementoPipeQueueEnqDeqProb {
    fn default() -> Self {
        Self {
            push: CachePadded::new(EnqueuePipeQ::default()),
            pop: CachePadded::new(DequeuePipeQ::default()),
        }
    }
}

impl Collectable for MementoPipeQueueEnqDeqProb {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}
impl TestNOps for MementoPipeQueueEnqDeqProb {}

impl Memento for MementoPipeQueueEnqDeqProb {
    type Object<'o> = &'o TestPipeQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        let q = &queue.pipeq;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let push =
                    unsafe { (&*self.push as *const _ as *mut EnqueuePipeQ).as_mut() }.unwrap();
                let pop =
                    unsafe { (&*self.pop as *const _ as *mut DequeuePipeQ).as_mut() }.unwrap();
                let enq_input = (push, tid);
                let deq_input = pop;
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

    fn set_recovery(&mut self, _: &'static PoolHandle) {
        // no-op
    }
}
