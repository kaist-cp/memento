//! Abstraction for evaluation

use crossbeam_epoch::Guard;
use memento::pmem::{Collectable, Pool, RootObj};
use rand::Rng;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use structopt::StructOpt;

/// file size
pub const FILE_SIZE: usize = 128 * 1024 * 1024 * 1024;

/// number of init nodes
pub static mut QUEUE_INIT_SIZE: usize = 0;

/// max threads
pub const MAX_THREADS: usize = 64;

/// test duration
pub static mut DURATION: f64 = 0.0;

/// probability for specific test (e.g. queue prob50)
pub static mut PROB: u32 = 0;

/// period of repin
pub static mut RELAXED: usize = 0;

pub static TOTAL_NOPS: AtomicUsize = AtomicUsize::new(0);

pub trait TestNOps {
    // Count number of executions of `op` in `duration` seconds
    fn test_nops<'f, F: Fn(usize, &Guard)>(
        &self,
        op: &'f F,
        tid: usize,
        duration: f64,
        guard: &Guard,
    ) -> usize
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
        let guard = &mut unsafe { ptr::read(guard) };
        while start.elapsed() < dur {
            op(tid, guard);
            ops += 1;

            if ops % unsafe { RELAXED } == 0 {
                guard.repin_after(|| {});
            }
        }
        ops
    }
}

pub fn get_total_nops() -> usize {
    TOTAL_NOPS.load(Ordering::SeqCst)
}

#[derive(Debug)]
pub enum TestTarget {
    MementoQueue(TestKind),
    MementoQueueLp(TestKind), // link and persist
    MementoQueueGeneral(TestKind),
    MementoQueueComb(TestKind), // combining
    FriedmanDurableQueue(TestKind),
    FriedmanLogQueue(TestKind),
    DSSQueue(TestKind),
    PBCombQueue(TestKind),
    PBCombQueueFullDetectable(TestKind),
    CrndmQueue(TestKind), // TODO: CrndmQueue -> CorundumQueue
}

#[derive(Clone, Copy, Debug)]
pub enum TestKind {
    QueueProb(u32), // { p% enq or 100-p% deq }
    QueuePair,      // { enq; deq; }
}

#[inline]
pub fn pick(prob: u32) -> bool {
    rand::thread_rng().gen_ratio(prob, 100)
}

pub fn get_nops<O, M>(filepath: &str, nr_thread: usize) -> usize
where
    O: RootObj<M> + Send + Sync + 'static,
    M: Collectable + Default + Send + Sync,
{
    let _ = Pool::remove(filepath);

    let pool_handle = Pool::create::<O, M>(filepath, FILE_SIZE, nr_thread).unwrap();

    // Each thread executes op for `duration` seconds and accumulates execution count in `TOTAL_NOPS`
    pool_handle.execute::<O, M>();

    // Load `TOTAL_NOPS`
    get_total_nops()
}

#[derive(StructOpt, Debug)]
#[structopt(name = "bench")]
pub struct Opt {
    /// filepath
    #[structopt(short, long)]
    pub filepath: String,

    /// target
    #[structopt(short = "a", long)]
    pub target: String,

    /// test kind
    #[structopt(short, long)]
    pub kind: String,

    /// number of threads
    #[structopt(short, long)]
    pub threads: usize,

    /// test duration
    #[structopt(short, long, default_value = "5")]
    pub duration: f64,

    /// output path
    #[structopt(short, long)]
    pub output: Option<String>,

    /// period of repin (default: repin_after once every 10000 ops)
    #[structopt(short, long, default_value = "10000")]
    pub relax: usize,

    /// number of initial nodes
    #[structopt(short, long, default_value = "0")]
    pub init: usize,
}

/// Abstraction of queue
pub mod queue {
    use corundum::default::*;
    use corundum::open_flags::{O_128GB, O_CF};
    use crossbeam_epoch::Guard;
    use memento::pmem::PoolHandle;

    use crate::pbcomb::{PBComb_NR_THREAD, TestPBCombQueue, TestPBCombQueueEnqDeq};
    use crate::{
        common::{get_nops, PROB, QUEUE_INIT_SIZE},
        compositional_pobj::*,
        crndm::*,
        dss::*,
        friedman::*,
    };

    use super::{pick, Opt, TestKind, TestTarget};

    pub trait TestQueue {
        type EnqInput;
        type DeqInput;

        fn enqueue(&self, input: Self::EnqInput, guard: &Guard, pool: &PoolHandle);
        fn dequeue(&self, input: Self::DeqInput, guard: &Guard, pool: &PoolHandle);
    }

    pub fn enq_deq_prob<Q: TestQueue>(
        q: &Q,
        enq: Q::EnqInput,
        deq: Q::DeqInput,
        prob: u32,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        if pick(prob) {
            q.enqueue(enq, guard, pool);
        } else {
            q.dequeue(deq, guard, pool);
        }
    }

    pub fn enq_deq_pair<Q: TestQueue>(
        q: &Q,
        enq: Q::EnqInput,
        deq: Q::DeqInput,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        q.enqueue(enq, guard, pool);
        q.dequeue(deq, guard, pool);
    }

    pub fn bench_queue(opt: &Opt, target: TestTarget) -> usize {
        unsafe { QUEUE_INIT_SIZE = opt.init };
        match target {
            TestTarget::MementoQueue(kind) => match kind {
                TestKind::QueuePair => get_nops::<TestMementoQueue, TestMementoQueueEnqDeq<true>>(
                    &opt.filepath,
                    opt.threads,
                ),
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestMementoQueue, TestMementoQueueEnqDeq<false>>(
                        &opt.filepath,
                        opt.threads,
                    )
                }
            },
            TestTarget::MementoQueueLp(kind) => {
                match kind {
                    TestKind::QueuePair => get_nops::<
                        TestMementoQueueLp,
                        TestMementoQueueLpEnqDeq<true>,
                    >(&opt.filepath, opt.threads),
                    TestKind::QueueProb(prob) => {
                        unsafe { PROB = prob };
                        get_nops::<TestMementoQueueLp, TestMementoQueueLpEnqDeq<false>>(
                            &opt.filepath,
                            opt.threads,
                        )
                    }
                }
            }
            TestTarget::MementoQueueGeneral(kind) => match kind {
                TestKind::QueuePair => get_nops::<
                    TestMementoQueueGeneral,
                    TestMementoQueueGeneralEnqDeq<true>,
                >(&opt.filepath, opt.threads),
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestMementoQueueGeneral, TestMementoQueueGeneralEnqDeq<false>>(
                        &opt.filepath,
                        opt.threads,
                    )
                }
            },
            TestTarget::MementoQueueComb(kind) => {
                unsafe { MementoPBComb_NR_THREAD = opt.threads }; // restriction of combining iteration
                match kind {
                    TestKind::QueuePair => get_nops::<
                        TestMementoQueueComb,
                        TestMementoQueueCombEnqDeq<true>,
                    >(&opt.filepath, opt.threads),
                    TestKind::QueueProb(prob) => {
                        unsafe { PROB = prob };
                        get_nops::<TestMementoQueueComb, TestMementoQueueCombEnqDeq<false>>(
                            &opt.filepath,
                            opt.threads,
                        )
                    }
                }
            }
            TestTarget::FriedmanDurableQueue(kind) => match kind {
                TestKind::QueuePair => get_nops::<TestDurableQueue, TestDurableQueueEnqDeq<true>>(
                    &opt.filepath,
                    opt.threads,
                ),
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestDurableQueue, TestDurableQueueEnqDeq<false>>(
                        &opt.filepath,
                        opt.threads,
                    )
                }
            },
            TestTarget::FriedmanLogQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestLogQueue, TestLogQueueEnqDeq<true>>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestLogQueue, TestLogQueueEnqDeq<false>>(&opt.filepath, opt.threads)
                }
            },
            TestTarget::DSSQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestDSSQueue, TestDSSQueueEnqDeq<true>>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestDSSQueue, TestDSSQueueEnqDeq<false>>(&opt.filepath, opt.threads)
                }
            },
            TestTarget::PBCombQueue(kind) => {
                unsafe { PBComb_NR_THREAD = opt.threads }; // restriction of combining iteration
                match kind {
                    TestKind::QueuePair => get_nops::<
                        TestPBCombQueue,
                        TestPBCombQueueEnqDeq<true, false>,
                    >(&opt.filepath, opt.threads),
                    TestKind::QueueProb(prob) => {
                        unsafe { PROB = prob };
                        get_nops::<TestPBCombQueue, TestPBCombQueueEnqDeq<false, false>>(
                            &opt.filepath,
                            opt.threads,
                        )
                    }
                }
            }
            TestTarget::PBCombQueueFullDetectable(kind) => {
                unsafe { PBComb_NR_THREAD = opt.threads }; // restriction of combining iteration
                match kind {
                    TestKind::QueuePair => get_nops::<
                        TestPBCombQueue,
                        TestPBCombQueueEnqDeq<true, true>,
                    >(&opt.filepath, opt.threads),
                    TestKind::QueueProb(prob) => {
                        unsafe { PROB = prob };
                        get_nops::<TestPBCombQueue, TestPBCombQueueEnqDeq<false, true>>(
                            &opt.filepath,
                            opt.threads,
                        )
                    }
                }
            }
            TestTarget::CrndmQueue(kind) => {
                let root = P::open::<TestCrndmQueue>(&opt.filepath, O_128GB | O_CF).unwrap();

                match kind {
                    TestKind::QueuePair => root.get_nops(opt.threads, opt.duration, None),
                    TestKind::QueueProb(prob) => {
                        root.get_nops(opt.threads, opt.duration, Some(prob))
                    }
                }
            }
        }
    }
}
