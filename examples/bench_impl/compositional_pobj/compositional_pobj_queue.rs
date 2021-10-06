use crate::bench_impl::abstract_queue::*;
use crate::{TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
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
    init_pushes: [Push<usize>; QUEUE_INIT_SIZE],
    push: [Push<usize>; MAX_THREADS],
    pop: [Pop<usize>; MAX_THREADS],
}

impl Default for GetOurQueueNOps {
    fn default() -> Self {
        Self {
            queue: PAtomic::null(),
            init_pushes: array_init::array_init(|_| Push::<usize>::default()),
            push: Default::default(),
            pop: Default::default(),
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
            for i in 0..QUEUE_INIT_SIZE {
                self.init_pushes[i].run(q_ref, i, pool)
            }
            self.queue.store(q, Ordering::SeqCst);
        }
    }
}

impl TestNOps for GetOurQueueNOps {}

impl POp for GetOurQueueNOps {
    type Object<'o> = ();
    type Input = (usize, f64, u32); // (n개 스레드로 m초 동안 테스트, p%/100-p% 확률로 enq/deq)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (nr_thread, duration, prob): Self::Input, // TODO: generic (remove prob)
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

        // TODO: 현재는 input `p`로 실행할 테스트를 구분. 더 우아한 방법으로 바꾸기
        if prob != 65535 {
            // Test1: p% 확률로 enq 혹은 100-p% 확률로 deq
            self.test_nops(
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
            )
        } else {
            // Test2: enq; deq;
            self.test_nops(
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
            )
        }
    }

    fn reset(&mut self, _: bool) {
        // no-op
    }
}
