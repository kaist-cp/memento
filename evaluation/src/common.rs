//! Abstraction for evaluation

use crossbeam_epoch::Guard;
use memento::persistent::{Memento, PDefault};
use memento::plocation::Pool;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use structopt::StructOpt;

/// 테스트시 만들 풀 파일의 크기
pub const FILE_SIZE: usize = 80 * 1024 * 1024 * 1024;

/// Queue 테스트시 초기 노드 수
pub const QUEUE_INIT_SIZE: usize = 1_000_000;

/// Pipe 테스트시 Queue 1의 초기 노드 수
// TODO: cpp의 PIPE_INIT_SIZE는 별도로 있음(commons.hpp). 이를 하나의 컨픽 파일로 통일하기
pub const PIPE_INIT_SIZE: usize = 10_000_000;

/// 테스트할 수 있는 최대 스레드 수
// - 우리 큐, 로그 큐 등에서 사물함을 MAX_THREAD만큼 정적할당해야하니 필요
// - TODO: 이 상수 없앨 수 있는지 고민 (e.g. MAX_THREAD=32 ./run.sh처럼 가능한가?)
pub const MAX_THREADS: usize = 256;

// ``` thread-local하게 사용하는 변수
// TODO: 더 좋은 방법? 현재는 인자로 tid 밖에 전달해줄 수 없으니 이렇게 해둠

/// op 반복을 지속할 시간
pub static mut DURATION: f64 = 0.0;

/// 확률값
pub static mut PROB: u32 = 0;

/// repin 호출 주기 (op을 `RELAXED`번 수행시마다 repin 호출)
pub static mut RELAXED: usize = 0;
// ```

pub static TOTAL_NOPS: AtomicUsize = AtomicUsize::new(0);

pub trait TestNOps {
    /// `duration`초 동안의 `op` 실행횟수 계산
    fn test_nops<'f, F: Fn(usize, &mut Guard)>(
        &self,
        op: &'f F,
        tid: usize,
        duration: f64,
        guard: &mut Guard,
    ) -> usize
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
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
    MementoPipeQueue(TestKind),
    FriedmanDurableQueue(TestKind),
    FriedmanLogQueue(TestKind),
    DSSQueue(TestKind),
    MementoPipe(TestKind),
    CrndmPipe(TestKind),
}

#[derive(Clone, Copy, Debug)]
pub enum TestKind {
    QueueProb(u32), // { p% 확률로 enq 혹은 deq }를 반복
    QueuePair,      // { enq; deq; }를 반복
    Pipe,
}

#[inline]
fn pick(prob: u32) -> bool {
    rand::thread_rng().gen_ratio(prob, 100)
}

// 우리의 pool API로 만든 테스트 로직 실행
fn get_nops<O, M>(filepath: &str, nr_thread: usize) -> usize
where
    O: PDefault + Send + Sync,
    M: for<'o> Memento<Object<'o> = &'o O, Input = usize> + Send + Sync,
{
    let _ = Pool::remove(filepath);

    let pool_handle = Pool::create::<O, M>(filepath, FILE_SIZE, nr_thread).unwrap();

    // 루트 op 실행: 각 스레드가 `duration` 초 동안 op 실행하고 `TOTAL_NOPS`에 실행 수 누적
    pool_handle.execute::<O, M>();

    // Load `TOTAL_NOPS`
    get_total_nops()
}

#[derive(StructOpt, Debug)]
#[structopt(name = "bench")]
pub struct Opt {
    /// PMEM pool로서 사용할 파일 경로
    #[structopt(short, long)]
    pub filepath: String,

    // /// 처리율 측정할 자료구조
    // #[structopt(short = "j", long)]
    // obj: String,
    //
    // /// 무엇으로 구현한 자료구조의 처리율을 측정할 것인가
    // #[structopt(short = "a", long)]
    // target: String,
    /// 처리율 측정대상
    #[structopt(short = "a", long)]
    pub target: String,

    /// 실험종류
    #[structopt(short, long)]
    pub kind: String,

    /// 동작시킬 스레드 수
    #[structopt(short, long)]
    pub threads: usize,

    /// 처리율 1번 측정시 실험 수행시간
    #[structopt(short, long, default_value = "5")]
    pub duration: f64,

    /// 출력 파일. 주어지지 않으면 ./out/{target}.csv에 저장
    #[structopt(short, long)]
    pub output: Option<String>,

    /// TODO
    #[structopt(short, long, default_value = "10000")]
    pub relax: usize,
}

/// Abstraction of queue
pub mod queue {
    use crossbeam_epoch::Guard;
    use memento::plocation::PoolHandle;

    use crate::{
        common::{get_nops, PROB},
        compositional_pobj::{
            MementoPipeQueueEnqDeqPair, MementoPipeQueueEnqDeqProb, MementoQueueEnqDeqPair,
            MementoQueueEnqDeqProb, TestMementoQueue, TestPipeQueue,
        },
        dss::{DSSQueueEnqDeqPair, DSSQueueEnqDeqProb, TestDSSQueue},
        friedman::{
            DurableQueueEnqDeqPair, DurableQueueEnqDeqProb, LogQueueEnqDeqPair, LogQueueEnqDeqProb,
            TestDurableQueue, TestLogQueue,
        },
    };

    use super::{pick, Opt, TestKind, TestTarget};

    pub trait TestQueue {
        type EnqInput;
        type DeqInput;

        fn enqueue(&self, input: Self::EnqInput, guard: &mut Guard, pool: &'static PoolHandle);
        fn dequeue(&self, input: Self::DeqInput, guard: &mut Guard, pool: &'static PoolHandle);
    }

    pub fn enq_deq_prob<Q: TestQueue>(
        q: &Q,
        enq: Q::EnqInput,
        deq: Q::DeqInput,
        prob: u32,
        guard: &mut Guard,
        pool: &'static PoolHandle,
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
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) {
        q.enqueue(enq, guard, pool);
        q.dequeue(deq, guard, pool);
    }

    pub fn bench_queue(opt: &Opt, target: TestTarget) -> usize {
        match target {
            TestTarget::MementoQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestMementoQueue, MementoQueueEnqDeqPair>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestMementoQueue, MementoQueueEnqDeqProb>(&opt.filepath, opt.threads)
                }
                _ => unreachable!("Queue를 위한 테스트만 해야함"),
            },
            TestTarget::MementoPipeQueue(kind) => match kind {
                TestKind::QueuePair => get_nops::<TestPipeQueue, MementoPipeQueueEnqDeqPair>(
                    &opt.filepath,
                    opt.threads,
                ),
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestPipeQueue, MementoPipeQueueEnqDeqProb>(
                        &opt.filepath,
                        opt.threads,
                    )
                }
                _ => unreachable!("Queue를 위한 테스트만 해야함"),
            },
            TestTarget::FriedmanDurableQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestDurableQueue, DurableQueueEnqDeqPair>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestDurableQueue, DurableQueueEnqDeqProb>(&opt.filepath, opt.threads)
                }
                _ => unreachable!("Queue를 위한 테스트만 해야함"),
            },
            TestTarget::FriedmanLogQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestLogQueue, LogQueueEnqDeqPair>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestLogQueue, LogQueueEnqDeqProb>(&opt.filepath, opt.threads)
                }
                _ => unreachable!("Queue를 위한 테스트만 해야함"),
            },
            TestTarget::DSSQueue(kind) => match kind {
                TestKind::QueuePair => {
                    get_nops::<TestDSSQueue, DSSQueueEnqDeqPair>(&opt.filepath, opt.threads)
                }
                TestKind::QueueProb(prob) => {
                    unsafe { PROB = prob };
                    get_nops::<TestDSSQueue, DSSQueueEnqDeqProb>(&opt.filepath, opt.threads)
                }
                _ => unreachable!("Queue를 위한 테스트만 해야함"),
            },
            _ => unreachable!("queue만"),
        }
    }
}

// TODO: add abstraction of pipe?
