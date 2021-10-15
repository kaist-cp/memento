#![feature(generic_associated_types)]

mod bench_impl;
use bench_impl::*;

use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::*;
use corundum::alloc::*;
use corundum::default::BuddyAlloc;
use core::time;
use std::path::Path;
use crossbeam_utils::thread;
use csv::Writer;
use regex::Regex;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::fs::File;
use std::fs::OpenOptions;
use std::sync::atomic::*;
use std::thread::sleep;

/// 테스트시 만들 풀 파일의 크기
const FILE_SIZE: usize = 80 * 1024 * 1024 * 1024;

/// Queue 테스트시 초기 노드 수
const QUEUE_INIT_SIZE: usize = 10;

/// Pipe 테스트시 Queue 1의 초기 노드 수
// TODO: cpp의 PIPE_INIT_SIZE는 별도로 있음(examples_cpp/bench.hpp). 이를 하나의 컨픽 파일로 통일하기
const PIPE_INIT_SIZE: usize = 100 * 1000 * 1000;


/// 테스트할 수 있는 최대 스레드 수
// - 우리 큐, 로그 큐 등에서 사물함을 MAX_THREAD만큼 정적할당해야하니 필요
// - TODO: 이 상수 없앨 수 있는지 고민 (e.g. MAX_THREAD=32 ./run.sh처럼 가능한가?)
const MAX_THREADS: usize = 256;

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
                        // TODO: 스레드별 ops 계산 후 마지막에 합치기? (pebr 벤치마크 코드 참고)
                        ops.fetch_add(1, Ordering::SeqCst);

                        // `duration` 시간 지났으면 break
                        // TODO: end 없애기. 메인 스레드가 직접 kill 하는 게 나을듯
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
//      - input: 테스트 종류, n개 스레드로 m초 동안 테스트
//      - output: m초 동안 실행된 operation 수
fn get_nops<'o, O: POp<Object<'o> = (), Input = (TestKind, usize, f64), Output<'o> = usize>>(
    filepath: &str,
    kind: TestKind,
    nr_thread: usize,
    duration: f64,
) -> usize {
    let _ = remove_file(filepath);
    let pool_handle = Pool::create::<O>(filepath, FILE_SIZE).unwrap();
    pool_handle
        .get_root()
        .run((), (kind, nr_thread, duration), &pool_handle)
}

enum TestTarget {
    OurQueue(TestKind),
    FriedmanDurableQueue(TestKind),
    FriedmanLogQueue(TestKind),
    DSSQueue(TestKind),
    OurPipe(TestKind),
    CrndmPipe(TestKind),
}

#[derive(Clone, Copy)]
pub enum TestKind {
    QueueProb(u32), // { p% 확률로 enq 혹은 deq }를 반복
    QueuePair,      // { enq; deq; }를 반복
    Pipe,
}

fn parse_target(target: &str, kind: &str) -> TestTarget {
    // 앞 4글자는 테스트 종류 구분 역할, 뒤에 더 붙는 글자는 부가 입력 역할
    // e.g. "prob50"이면 prob 테스트, 확률은 50%으로 설정
    // e.g. "prob30"이면 prob 테스트, 확률은 30%으로 설정
    let re = Regex::new(r"(\w{4})(\d*)").unwrap();
    let cap = re.captures(kind).unwrap();
    let (kind, arg) = (&cap[1], &cap[2]);
    let kind = match kind {
        "prob" => TestKind::QueueProb(arg.parse::<u32>().unwrap()),
        "pair" => TestKind::QueuePair,
        "pipe" => TestKind::Pipe,
        _ => unreachable!(),
    };
    match target {
        "our_queue" => TestTarget::OurQueue(kind),
        "durable_queue" => TestTarget::FriedmanDurableQueue(kind),
        "log_queue" => TestTarget::FriedmanLogQueue(kind),
        "dss_queue" => TestTarget::DSSQueue(kind),
        "our_pipe" => TestTarget::OurPipe(kind),
        "crndm_pipe" => TestTarget::CrndmPipe(kind),
        _ => unreachable!("invalid target"),
    }
}

use structopt::StructOpt;
#[derive(StructOpt, Debug)]
#[structopt(name = "bench")]
struct Opt {
    /// PMEM pool로서 사용할 파일 경로
    #[structopt(short, long)]
    filepath: String,

    /// 처리율 측정대상
    #[structopt(short = "a", long)]
    target: String,

    /// 실험종류
    #[structopt(short, long)]
    kind: String,

    /// 동작시킬 스레드 수
    #[structopt(short, long)]
    threads: usize,

    /// 처리율 1번 측정시 실험 수행시간
    #[structopt(short, long, default_value = "5")]
    duration: f64,

    /// 출력 파일. 주어지지 않으면 ./out/{target}.csv에 저장
    #[structopt(short, long)]
    output: Option<String>,
}

fn setup() -> (Opt, Writer<File>) {
    let opt = Opt::from_args();

    let output_name = match &opt.output {
        Some(o) => o.clone(),
        None => format!("./out/{}.csv", opt.target.split('_').last().unwrap()),
    };
    create_dir_all(Path::new(&output_name).parent().unwrap()).unwrap();
    let output = match OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .open(&output_name)
    {
        Ok(f) => csv::Writer::from_writer(f),
        Err(_) => {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&output_name)
                .unwrap();
            let mut output = csv::Writer::from_writer(f);
            output
                .write_record(&["target", "bench kind", "threads", "duration", "throughput"])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    (opt, output)
}

// 스레드 `nr_thread`개를 사용할 때의 처리율 계산
fn bench(opt: &Opt) -> f64 {
    println!(
        "bench {}:{} using {} threads",
        opt.target, opt.kind, opt.threads
    );
    let target = parse_target(&opt.target, &opt.kind);
    let nops = match target {
        TestTarget::OurQueue(kind) => {
            get_nops::<GetOurQueueNOps>(&opt.filepath, kind, opt.threads, opt.duration)
        }
        TestTarget::FriedmanDurableQueue(kind) => {
            get_nops::<GetDurableQueueNOps>(&opt.filepath, kind, opt.threads, opt.duration)
        }
        TestTarget::FriedmanLogQueue(kind) => {
            get_nops::<GetLogQueueNOps>(&opt.filepath, kind, opt.threads, opt.duration)
        }
        TestTarget::DSSQueue(kind) => {
            get_nops::<GetDSSQueueNOps>(&opt.filepath, kind, opt.threads, opt.duration)
        }
        TestTarget::OurPipe(kind) => {
            get_nops::<GetOurPipeNOps>(&opt.filepath, kind, opt.threads, opt.duration)
        }
        TestTarget::CrndmPipe(kind) => {
            let root = BuddyAlloc::open::<CrndmPipe>(&opt.filepath, O_16GB | O_CF).unwrap();
            root.get_nops(kind, opt.threads, opt.duration)
        }
    };
    let avg_ops = (nops as f64) / opt.duration; // 평균 op/s
    println!("avg ops: {}", avg_ops);
    avg_ops
}

fn main() {
    let (opt, mut output) = setup();
    let avg_mops = bench(&opt);

    // Write result
    output
        .write_record(&[
            opt.target,
            opt.kind,
            opt.threads.to_string(),
            opt.duration.to_string(),
            avg_mops.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
}
