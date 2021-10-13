use crate::bench_impl::abstract_queue::*;
use crate::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as epoch, PAtomic, POwned};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::pool::*;
use compositional_persistent_object::queue::*;
use core::sync::atomic::Ordering;

impl<T: 'static + Clone> TestQueue for Queue<T> {
    type EnqInput = (&'static mut Push<T>, T); // POp, input
    type DeqInput = &'static mut Pop<T>; // POp

    fn enqueue<O: POp>(&self, (push, input): Self::EnqInput, pool: &PoolHandle<O>) {
        push.run(self, input, pool);
        push.reset(false);
    }

    fn dequeue<O: POp>(&self, pop: Self::DeqInput, pool: &PoolHandle<O>) {
        pop.run(self, (), pool);
        pop.reset(false);
    }
}

pub struct GetOurQueueNOps {
    queue: PAtomic<Queue<usize>>,
    push: [Push<usize>; MAX_THREADS],
    pop: [Pop<usize>; MAX_THREADS],
}

impl Default for GetOurQueueNOps {
    fn default() -> Self {
        Self {
            queue: PAtomic::null(),
            push: array_init::array_init(|_| Push::<usize>::default()),
            pop: array_init::array_init(|_| Pop::<usize>::default()),
        }
    }
}

impl GetOurQueueNOps {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        let guard = unsafe { epoch::unprotected(pool) };
        let q = self.queue.load(Ordering::SeqCst, guard);

        // Initialize queue
        if q.is_null() {
            let q = POwned::new(Queue::<usize>::new(pool), pool);
            let q_ref = unsafe { q.deref(pool) };
            let mut push_init = Push::default();
            for i in 0..QUEUE_INIT_SIZE {
                push_init.run(q_ref, i, pool);
                push_init.reset(false);
            }
            self.queue.store(q, Ordering::SeqCst);
        }
    }
}

impl TestNOps for GetOurQueueNOps {}

impl POp for GetOurQueueNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        self.init(pool);

        // Alias
        let q = unsafe {
            self.queue
                .load(Ordering::SeqCst, epoch::unprotected(pool))
                .deref(pool)
        };

        match kind {
            TestKind::QueuePair => self.test_nops(
                &|tid| {
                    let push =
                        unsafe { (&self.push[tid] as *const _ as *mut Push<usize>).as_mut() }
                            .unwrap();
                    let pop = unsafe { (&self.pop[tid] as *const _ as *mut Pop<usize>).as_mut() }
                        .unwrap();
                    let enq_input = (push, tid);
                    let deq_input = pop;
                    enq_deq_pair(q, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ),
            TestKind::QueueProb(prob) => self.test_nops(
                &|tid| {
                    let push =
                        unsafe { (&self.push[tid] as *const _ as *mut Push<usize>).as_mut() }
                            .unwrap();
                    let pop = unsafe { (&self.pop[tid] as *const _ as *mut Pop<usize>).as_mut() }
                        .unwrap();
                    let enq_input = (push, tid);
                    let deq_input = pop;
                    enq_deq_prob(q, enq_input, deq_input, prob, pool);
                },
                nr_thread,
                duration,
            ),
            _ => unreachable!("Queue를 위한 테스트만 해야함"),
        }
    }

    fn reset(&mut self, _: bool) {
        // no-op
    }
}
