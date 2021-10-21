//! Abstraction for evaluation

use crossbeam_utils::thread;
use rand::Rng;
use std::time::{Duration, Instant};

/// 테스트시 만들 풀 파일의 크기
pub const FILE_SIZE: usize = 80 * 1024 * 1024 * 1024;

/// Queue 테스트시 초기 노드 수
pub const QUEUE_INIT_SIZE: usize = 10;

/// Pipe 테스트시 Queue 1의 초기 노드 수
// TODO: cpp의 PIPE_INIT_SIZE는 별도로 있음(commons.hpp). 이를 하나의 컨픽 파일로 통일하기
pub const PIPE_INIT_SIZE: usize = 10_000_000;

/// 테스트할 수 있는 최대 스레드 수
// - 우리 큐, 로그 큐 등에서 사물함을 MAX_THREAD만큼 정적할당해야하니 필요
// - TODO: 이 상수 없앨 수 있는지 고민 (e.g. MAX_THREAD=32 ./run.sh처럼 가능한가?)
pub const MAX_THREADS: usize = 256;

pub trait TestNOps {
    /// `nr_thread`개 스레드로 `duration`초 동안 `op`이 몇번 실행되는지 계산
    fn test_nops<'f, F: Fn(usize)>(&self, op: &'f F, nr_thread: usize, duration: f64) -> usize
    where
        &'f F: Send,
    {
        let mut sum_ops = 0;
        #[allow(box_pointers)]
        thread::scope(|scope| {
            let mut handles = Vec::new();
            for tid in 0..nr_thread {
                let handle = scope.spawn(move |_| {
                    let mut ops = 0;
                    let start = Instant::now();
                    let dur = Duration::from_secs_f64(duration);
                    while start.elapsed() < dur {
                        op(tid);
                        ops += 1;
                    }
                    ops
                });
                handles.push(handle);
            }

            for h in handles {
                sum_ops += h.join().unwrap();
            }
        })
        .unwrap();
        sum_ops
    }
}

#[derive(Debug)]
pub enum TestTarget {
    OurQueue(TestKind),
    FriedmanDurableQueue(TestKind),
    FriedmanLogQueue(TestKind),
    DSSQueue(TestKind),
    OurPipe(TestKind),
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

/// Abstraction of queue
pub mod queue {
    use compositional_persistent_object::{persistent::POp, plocation::PoolHandle};

    use super::pick;

    pub trait TestQueue {
        type EnqInput;
        type DeqInput;

        fn enqueue<O: POp>(&self, input: Self::EnqInput, pool: &PoolHandle<O>);
        fn dequeue<O: POp>(&self, input: Self::DeqInput, pool: &PoolHandle<O>);
    }

    pub fn enq_deq_prob<O: POp, Q: TestQueue>(
        q: &Q,
        enq: Q::EnqInput,
        deq: Q::DeqInput,
        prob: u32,
        pool: &PoolHandle<O>,
    ) {
        if pick(prob) {
            q.enqueue(enq, pool);
        } else {
            q.dequeue(deq, pool);
        }
    }

    pub fn enq_deq_pair<O: POp, Q: TestQueue>(
        q: &Q,
        enq: Q::EnqInput,
        deq: Q::DeqInput,
        pool: &PoolHandle<O>,
    ) {
        q.enqueue(enq, pool);
        q.dequeue(deq, pool);
    }
}

// TODO: add abstraction of pipe?
