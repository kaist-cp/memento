#![feature(generic_associated_types)]

use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::*;
use corundum::alloc::*;
use corundum::default::BuddyAlloc;
use csv::Writer;
use evaluation::common::{TestKind, TestTarget, FILE_SIZE};
use evaluation::compositional_pobj::{GetOurPipeNOps, GetOurQueueNOps};
use evaluation::crndm::CrndmPipe;
use evaluation::dss::GetDSSQueueNOps;
use evaluation::friedman::{GetDurableQueueNOps, GetLogQueueNOps};
use regex::Regex;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

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