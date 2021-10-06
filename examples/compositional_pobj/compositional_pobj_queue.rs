use crate::abstract_queue::*;
use crate::TestNOps;
use crate::INIT_COUNT;
use compositional_persistent_object::pepoch::{self as epoch, PAtomic, POwned};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::pool::*;
use compositional_persistent_object::queue::*;
use core::sync::atomic::Ordering;

impl<T: 'static + Clone> DurableQueue<T> for Queue<T> {
    fn enqueue<O: POp>(&self, input: EnqInput<T>, pool: &PoolHandle<O>) {
        if let EnqInput::POpBased(push, q, input) = input {
            push.run(q, input.clone(), pool);
        }
    }

    fn dequeue<O: POp>(&self, input: DeqInput<T>, pool: &PoolHandle<O>) {
        if let DeqInput::POpBased(pop, q) = input {
            pop.run(q, (), pool);
        }
    }
}

pub struct GetOurQueueThroughput {
    queue: PAtomic<Queue<usize>>,
    init_pushes: [Push<usize>; INIT_COUNT],
}

impl Default for GetOurQueueThroughput {
    fn default() -> Self {
        Self {
            queue: PAtomic::null(),
            init_pushes: array_init::array_init(|_| Push::<usize>::default()),
        }
    }
}

impl GetOurQueueThroughput {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        let guard = unsafe { epoch::unprotected(&pool) };
        let q = self.queue.load(Ordering::SeqCst, guard);

        // Initialize queue
        if q.is_null() {
            // 큐 생성
            let q = POwned::new(Queue::<usize>::new(pool), pool);
            let q_ref = unsafe { q.deref(pool) };
            // TODO: 여기서 crash나면 leak남

            // 큐 초기상태 설정: 10^6개 원소 가짐
            for i in 0..INIT_COUNT {
                self.init_pushes[i].run(q_ref, i, pool)
            }

            self.queue.store(q, Ordering::SeqCst);
        }
    }
}

impl TestNOps for GetOurQueueThroughput {}

impl POp for GetOurQueueThroughput {
    type Object<'o> = ();
    type Input = (usize, f64, u32); // (n개 스레드로 m초 동안 테스트, p%/100-p% 확률로 enq/deq)
    type Output<'o> = Result<usize, ()>; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (nr_thread, duration, probability): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        self.init(pool);

        // Alias
        let q = unsafe {
            self.queue
                .load(Ordering::SeqCst, epoch::unprotected(&pool))
                .deref(pool)
        };

        // TODO: refactoring
        if probability != 65535 {
            // Test: p% 확률로 enq, 100-p% 확률로 deq
            Ok(self.test_nops(
                &|tid| {
                    let mut push = Push::default();
                    let mut pop = Pop::default();
                    let enq_input = EnqInput::POpBased(&mut push, &q, tid);
                    let deq_input = DeqInput::POpBased(&mut pop, &q);
                    enq_deq_either(q, enq_input, deq_input, probability, pool);
                },
                nr_thread,
                duration,
            ))
        } else {
            Ok(self.test_nops(
                &|tid| {
                    let mut push = Push::default();
                    let mut pop = Pop::default();
                    let enq_input = EnqInput::POpBased(&mut push, &q, tid);
                    let deq_input = DeqInput::POpBased(&mut pop, &q);
                    enq_deq_both(q, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ))
        }
    }

    fn reset(&mut self, _: bool) {
        // no-op
    }
}

#[allow(warnings)]
fn main() {}
