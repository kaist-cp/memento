#![feature(generic_associated_types)]

mod bench_impl;
use bench_impl::{GetDurableQueueNOps, GetLogQueueNOps, GetOurQueueNOps};

use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::*;
use core::time;
use crossbeam_utils::thread;
use regex::Regex;
use std::env;
use std::fs::remove_file;
use std::sync::atomic::*;
use std::thread::sleep;

const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024; // 테스트에 사용할 풀 파일의 크기
const QUEUE_INIT_SIZE: usize = 100; // 테스트시 Queue의 초기 노드 수
const MAX_THREADS: usize = 32;

trait TestNOps {
    // `nr_thread`개 스레드로 `duration`초 동안 `op`이 몇번 실행되는지 계산
    fn test_nops<'f, F: Fn(usize)>(&self, op: &'f F, nr_thread: usize, duration: f64) -> usize
    where
        &'f F: Send,
    {
        let (ops, end) = (AtomicUsize::new(0), AtomicBool::new(false));
        let (ops, end) = (&ops, &end);

        thread::scope(|scope| {
            for tid in 0..nr_thread {
                scope.spawn(move |_| {
                    loop {
                        op(tid);
                        ops.fetch_add(1, Ordering::SeqCst);

                        // `duration` 시간 지났으면 break
                        // TODO: off 없애기. 메인 스레드가 직접 kill 하는 게 나을듯
                        if end.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                });
            }
            // 메인스레드는 `duration` 시간동안 sleep한 후 "시간 끝났다" 표시
            // TODO: use `chrono` crate?
            sleep(time::Duration::from_secs_f64(duration));
            end.store(true, Ordering::SeqCst)
        })
        .unwrap();

        ops.load(Ordering::SeqCst)
    }
}

// - 우리의 pool API로 만든 테스트 로직 실행
// - root op으로 operation 실행 수를 카운트하는 로직을 가짐
//      - input: n개 스레드로 m초 동안 테스트, p%/100-p% 확률로 enq/deq (TODO: 3번째 input은 테스트 종류마다 다름. 어떻게 다룰지 고민 필요)
//      - output: m초 동안 실행된 operation 수
fn get_nops<'o, O: POp<Object<'o> = (), Input = (usize, f64, TestKind), Output<'o> = usize>>(
    filepath: &str,
    nr_thread: usize,
    duration: f64,
    kind: TestKind,
) -> usize {
    let _ = remove_file(filepath);
    let pool_handle = Pool::create::<O>(filepath, FILE_SIZE).unwrap();
    pool_handle
        .get_root()
        .run((), (nr_thread, duration, kind), &pool_handle)
}

enum TestTarget {
    OurQueue(TestKind),
    FriedmanDurableQueue(TestKind),
    FriedmanLogQueue(TestKind),
    DSSQueue(TestKind),
    CrndmPipe(TestKind),
}

#[derive(Clone, Copy)]
pub enum TestKind {
    QueueProb(u32), // { p% 확률로 enq 혹은 deq }를 반복
    QueuePair,      // { enq; deq; }를 반복
    Pipe,
}

fn parse_test_kind(text: &str) -> TestKind {
    // 앞 4글자는 테스트 종류 구분 역할, 뒤에 더 붙는 글자는 부가 입력 역할
    // e.g. "prob50"이면 prob 테스트, 확률은 50%으로 설정
    // e.g. "prob30"이면 prob 테스트, 확률은 30%으로 설정
    let re = Regex::new(r"(\w{4})(\d*)").unwrap();
    let cap = re.captures(text).unwrap();
    let (kind, arg) = (&cap[1], &cap[2]);
    match kind {
        "prob" => TestKind::QueueProb(arg.parse::<u32>().unwrap()),
        "pair" => TestKind::QueuePair,
        "pipe" => TestKind::Pipe,
        _ => unreachable!(),
    }
}

// executable 사용예시
//
// `/mnt/pmem0`에 생성한 풀 파일로 `5`초씩 `10`번 테스트 진행
// ```
// bench /mnt/pmem 5 10 our_queue prob50        # 테스트: 우리 큐로 50/50% enq or deq 실행
// bench /mnt/pmem 5 10 our_queue prob30        # 테스트: 우리 큐로 30/70% enq or deq 실행
// bench /mnt/pmem 5 10 friedman_log_queue pair # 테스트: 로그 큐로 enq-deq pair 실행
// ```
// TODO: clap 사용하여 argument parsing  
fn main() {
    let args: Vec<std::string::String> = env::args().collect();
    let filepath = &args[1];
    let test_duration = args[2].parse::<f64>().unwrap();
    let test_cnt = args[3].parse::<usize>().unwrap();
    let test_target = match args[4].as_str() {
        "our_queue" => TestTarget::OurQueue(parse_test_kind(&args[5])),
        "friedman_durable_queue" => TestTarget::FriedmanDurableQueue(parse_test_kind(&args[5])),
        "friedman_log_queue" => TestTarget::FriedmanLogQueue(parse_test_kind(&args[5])),
        "dss_queue" => TestTarget::DSSQueue(parse_test_kind(&args[5])),
        "crndm_pipe" => TestTarget::CrndmPipe(parse_test_kind(&args[5])),
        _ => unreachable!("invalid target"),
    };

    let mut res = vec![0.0; MAX_THREADS + 1];
    // 스레드 `nr_thread`개 일때의 처리율 계산하기
    for nr_thread in 1..MAX_THREADS + 1 {
        println!("Test throguhput using {} threads", nr_thread);
        let mut sum = 0;
        // `cnt`번 테스트하여 평균냄
        for cnt in 0..test_cnt {
            let nops = match test_target {
                TestTarget::OurQueue(kind) => {
                    get_nops::<GetOurQueueNOps>(filepath, nr_thread, test_duration, kind)
                }
                TestTarget::FriedmanDurableQueue(kind) => {
                    get_nops::<GetDurableQueueNOps>(filepath, nr_thread, test_duration, kind)
                }
                TestTarget::FriedmanLogQueue(kind) => {
                    get_nops::<GetLogQueueNOps>(filepath, nr_thread, test_duration, kind)
                }
                TestTarget::DSSQueue(_) => todo!(),
                TestTarget::CrndmPipe(_) => todo!(),
            };
            sum += nops;
            println!("try #{} : {} operation was executed.", cnt, nops);
        }
        // 평균 op/s 계산하여 저장
        res[nr_thread] = (sum as f64 / test_cnt as f64) / test_duration;
    }

    // 처리율(평균 Mop/s) 출력
    for nr_thread in 1..MAX_THREADS + 1 {
        println!(
            "avg mops when nr_thread={}: {}",
            nr_thread,
            res[nr_thread] / 1_000_000 as f64
        );
    }
}
