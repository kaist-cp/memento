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

const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
const INIT_COUNT: usize = 100; // TODO: 제대로 사용 중인지 확인
const MAX_THREADS: usize = 3; // TODO: 스크립트의 인자로 넣을 순 없나?

trait TestNOps {
    // `nr_thread`개 스레드로 `duration`초 동안 `f`가 몇번 실행되는지 계산
    fn test_nops<'f, F: Fn(usize)>(&self, op: &'f F, nr_thread: usize, duration: f64) -> usize
    where
        &'f F: Send,
    {
        let (ops, off) = (AtomicUsize::new(0), AtomicBool::new(false));
        let (ops, off) = (&ops, &off);

        // Test: p% 확률로 enq, 100-p% 확률로 deq
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                scope.spawn(move |_| {
                    loop {
                        op(tid);
                        ops.fetch_add(1, Ordering::SeqCst);

                        // `duration` 시간 지났으면 break
                        // TODO: off 없애기. 메인 스레드가 직접 kill 하는 게 나을듯
                        if off.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                });
            }
            // 메인스레드는 `duration` 시간동안 sleep한 후 "시간 끝났다" 표시
            // TODO: use `chrono` crate?
            sleep(time::Duration::from_secs_f64(duration));
            off.store(true, Ordering::SeqCst)
        })
        .unwrap();

        ops.load(Ordering::SeqCst)
    }
}

enum Target {
    OurQueue,
    FriedmanDurableQueue,
    FriedmanLogQueue,
    DSSQueue,
    CrndmPipe,
}

fn get_throughput<
    'o,
    O: POp<Object<'o> = (), Input = (usize, f64, u32), Output<'o> = Result<usize, ()>>,
>(
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
        .unwrap()
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
    // let max_thread = num_cpus::get();

    let mut res = vec![0.0; MAX_THREADS + 1];
    for nr_thread in 1..MAX_THREADS + 1 {
        println!("Test throguhput using {} threads", nr_thread);
        let mut sum = 0;

        for cnt in 0..test_cnt {
            // 스레드 `nr_thread`개 일때 operation 실행한 횟수 계산
            let nops = match test_target {
                Target::OurQueue => get_throughput::<GetOurQueueThroughput>(
                    filepath,
                    nr_thread,
                    test_duration,
                    test_enq_probability,
                ),
                Target::FriedmanDurableQueue => get_throughput::<GetDurableQueueThroughput>(
                    filepath,
                    nr_thread,
                    test_duration,
                    test_enq_probability,
                ),
                Target::FriedmanLogQueue => get_throughput::<GetLogQueueThroughput>(
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
        // 스레드 `nr_thread`개 일때의 평균 op 실행 수 저장
        res[nr_thread] = sum as f64 / test_cnt as f64;
    }

    // Calucatle and print average
    for nr_thread in 1..MAX_THREADS + 1 {
        let avg_mops = res[nr_thread] / (test_duration * 1_000_000 as f64);
        println!("avg mops when nr_thread={} : {}", nr_thread, avg_mops);
    }
}
