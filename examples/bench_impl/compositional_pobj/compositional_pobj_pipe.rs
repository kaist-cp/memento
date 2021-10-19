use compositional_persistent_object::{
    pepoch::{self, PAtomic, POwned},
    persistent::POp,
    pipe::Pipe,
    plocation::PoolHandle,
    queue::{Pop, Push, Queue},
};
use crossbeam_utils::CachePadded;
use std::sync::atomic::Ordering;

use crate::{TestKind, TestNOps, MAX_THREADS, PIPE_INIT_SIZE};

struct MustPop<T: Clone> {
    pop: Pop<T>,
}

impl<T: Clone> Default for MustPop<T> {
    fn default() -> Self {
        Self {
            pop: Default::default(),
        }
    }
}

impl<T: 'static + Clone> POp for MustPop<T> {
    type Object<'q> = &'q Queue<T>;
    type Input = ();
    type Output<'q> = T;

    fn run<'o, O: POp>(
        &mut self,
        queue: Self::Object<'o>,
        _: Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        loop {
            if let Some(v) = self.pop.run(queue, (), pool) {
                return v;
            }
            self.pop.reset(false);
        }
    }

    fn reset(&mut self, _: bool) {
        self.pop.reset(true);
    }
}

pub struct GetOurPipeNOps {
    q1: PAtomic<Queue<usize>>,
    q2: PAtomic<Queue<usize>>,
    pipes: [CachePadded<Pipe<MustPop<usize>, Push<usize>>>; MAX_THREADS],
}

impl Default for GetOurPipeNOps {
    fn default() -> Self {
        Self {
            q1: PAtomic::null(),
            q2: PAtomic::null(),
            pipes: array_init::array_init(|_| CachePadded::new(Pipe::default())),
        }
    }
}

impl GetOurPipeNOps {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        let guard = unsafe { pepoch::unprotected(pool) };
        let q1 = self.q1.load(Ordering::SeqCst, guard);
        let q2 = self.q2.load(Ordering::SeqCst, guard);

        // Initialize q1
        if q1.is_null() {
            let q = Queue::<usize>::new(pool);
            let q_ref = unsafe { q.deref(pool) };
            let mut push_init = Push::default();
            for i in 0..PIPE_INIT_SIZE {
                push_init.run(q_ref, i, pool);
                push_init.reset(false);
            }
            self.q1.store(q, Ordering::SeqCst);
        }

        // Initialize q2
        if q2.is_null() {
            let q = Queue::<usize>::new(pool);
            self.q2.store(q, Ordering::SeqCst);
        }
    }
}

impl TestNOps for GetOurPipeNOps {}

impl POp for GetOurPipeNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize
        println!("initialize..");
        self.init(pool);

        // Alias
        let guard = unsafe { pepoch::unprotected(pool) };
        let q1 = unsafe { self.q1.load(Ordering::SeqCst, guard).deref(pool) };
        let q2 = unsafe { self.q2.load(Ordering::SeqCst, guard).deref(pool) };

        // Run
        println!("Run!");
        match kind {
            TestKind::Pipe => self.test_nops(
                &|tid| {
                    let pipe = unsafe {
                        (&self.pipes[tid] as *const _ as *mut Pipe<MustPop<usize>, Push<usize>>)
                            .as_mut()
                    }
                    .unwrap();

                    // TODO: add abstract_pipe?
                    pipe.run((q1, q2), (), pool);
                    pipe.reset(false);
                    // pipe.run((q2, q1), (), pool);
                    // pipe.reset(false);
                },
                nr_thread,
                duration,
            ),
            _ => unreachable!("Pipe를 위한 테스트만 해야함"),
        }
    }

    fn reset(&mut self, _: bool) {
        // no-op
    }
}
