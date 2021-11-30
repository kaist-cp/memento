use crate::common::{TestKind, TestNOps, MAX_THREADS, PIPE_INIT_SIZE};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use memento::{
    pepoch::PAtomic,
    persistent::{Memento, PDefault},
    pipe::Pipe,
    plocation::{ralloc::Collectable, PoolHandle},
    queue::{DequeueSome, Enqueue, Queue},
};
use std::sync::atomic::Ordering;
#[derive(Debug)]
pub struct GetOurPipeNOps {
    q1: PAtomic<Queue<usize>>,
    q2: PAtomic<Queue<usize>>,
    pipes: [CachePadded<Pipe<DequeueSome<usize>, Enqueue<usize>>>; MAX_THREADS],
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
    fn init<O: Memento>(&mut self, pool: &'static PoolHandle) {
        let guard = unsafe { epoch::unprotected() };
        let q1 = self.q1.load(Ordering::SeqCst, guard);
        let q2 = self.q2.load(Ordering::SeqCst, guard);

        // Initialize q1
        if q1.is_null() {
            let q = Queue::<usize>::pdefault(pool);
            let q_ref = unsafe { q.deref(pool) };
            let mut push_init = Enqueue::default();
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

impl Collectable for GetOurPipeNOps {
    fn filter(
        s: &mut Self,
        gc: &mut memento::plocation::ralloc::GarbageCollection,
        pool: &'static PoolHandle,
    ) {
        todo!()
    }
}

impl TestNOps for GetOurPipeNOps {}

impl Memento for GetOurPipeNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수
    type Error = ();

    fn run<'o>(
        &'o mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        guard: &mut epoch::Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        // Initialize
        println!("initialize..");
        self.init(pool);

        // Alias
        let guard = unsafe { epoch::unprotected() };
        let q1 = unsafe { self.q1.load(Ordering::SeqCst, guard).deref(pool) };
        let q2 = unsafe { self.q2.load(Ordering::SeqCst, guard).deref(pool) };

        // Run
        println!("Run!");
        let nops = match kind {
            TestKind::Pipe => self.test_nops(
                &|tid| {
                    let pipe = unsafe {
                        (&self.pipes[tid] as *const _
                            as *mut Pipe<DequeueSome<usize>, Enqueue<usize>>)
                            .as_mut()
                    }
                    .unwrap();

                    pipe.run((q1, q2), (), pool);
                    pipe.reset(false);
                },
                nr_thread,
                duration,
            ),
            _ => unreachable!("Pipe를 위한 테스트만 해야함"),
        };
        Ok(nops)
    }

    fn reset(&mut self, _: bool, _: &mut epoch::Guard, _: &'static PoolHandle) {
        // no-op
    }
}
