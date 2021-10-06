#![feature(generic_associated_types)]

mod abstract_queue;
mod compositional_pobj;
mod corundum;
mod dss;
mod friedman;

use compositional_pobj::*;
use friedman::*;

use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::*;
use core::time;
use crossbeam_utils::thread;
use std::env;
use std::fs::remove_file;
use std::sync::atomic::*;
use std::thread::sleep;

const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024; // 테스트에 사용할 풀 파일의 크기
const QUEUE_INIT_SIZE: usize = 100; // 테스트시 Queue의 초기 노드 수
const MAX_THREADS: usize = 3;

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
fn get_nops<'o, O: POp<Object = (), Input = (usize, f64, u32), Output = usize>>(
    filepath: &str,
    nr_thread: usize,
    duration: f64,
    enq_probability: u32,
) -> usize {
    let _ = remove_file(filepath);
    let pool_handle = Pool::create::<O>(&filepath, FILE_SIZE).unwrap();
    pool_handle
        .get_root()
        .run((), (nr_thread, duration, enq_probability), &pool_handle)
}

enum Target {
    OurQueue,
    FriedmanDurableQueue,
    FriedmanLogQueue,
    DSSQueue,
    CrndmPipe,
}

fn main() {
    let args: Vec<std::string::String> = env::args().collect();

    let filepath = &args[1];
    let test_target = match args[2].as_str() {
        "our_queue" => Target::OurQueue,
        "friedman_durable_queue" => Target::FriedmanDurableQueue,
        "friedman_log_queue" => Target::FriedmanLogQueue,
        "dss_queue" => Target::DSSQueue,
        "crndm_pipe" => Target::CrndmPipe,
        _ => unreachable!("invalid target"),
    };
    let test_duration = args[3].parse::<f64>().unwrap();
    let test_cnt = args[4].parse::<usize>().unwrap();
    let test_enq_probability = args[5].parse::<u32>().unwrap();

    let mut res = vec![0.0; MAX_THREADS + 1];

    // 스레드 `nr_thread`개 일때의 처리율 계산하기
    for nr_thread in 1..MAX_THREADS + 1 {
        println!("Test throguhput using {} threads", nr_thread);
        let mut sum = 0;

        // `cnt`번 테스트하여 평균냄
        for cnt in 0..test_cnt {
            let nops = match test_target {
                Target::OurQueue => get_nops::<GetOurQueueNOps>(
                    filepath,
                    nr_thread,
                    test_duration,
                    test_enq_probability,
                ),
                Target::FriedmanDurableQueue => get_nops::<GetDurableQueueNOps>(
                    filepath,
                    nr_thread,
                    test_duration,
                    test_enq_probability,
                ),
                Target::FriedmanLogQueue => get_nops::<GetLogQueueNOps>(
                    filepath,
                    nr_thread,
                    test_duration,
                    test_enq_probability,
                ),
                Target::DSSQueue => todo!(),
                Target::CrndmPipe => todo!(),
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
