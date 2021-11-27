#![feature(generic_associated_types)]

// use corundum::alloc::*;
// use corundum::default::BuddyAlloc;
use csv::Writer;
use evaluation::common::queue::bench_queue;
use evaluation::common::{
    get_total_nops, Opt, TestKind, TestNOps, TestTarget, DURATION, FILE_SIZE,
};
// use evaluation::compositional_pobj::{GetOurPipeNOps, GetOurQueueNOps};
use evaluation::compositional_pobj::{MementoQueueEnqDeqPair, TestMementoQueue};
// use evaluation::crndm::CrndmPipe;
// use evaluation::dss::GetDSSQueueNOps;
// use evaluation::friedman::{GetDurableQueueNOps, GetLogQueueNOps};
use memento::persistent::*;
use memento::plocation::*;
use regex::Regex;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use structopt::StructOpt;

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
        "our_pipe_queue" => TestTarget::OurPipeQueue(kind),
        "durable_queue" => TestTarget::FriedmanDurableQueue(kind),
        "log_queue" => TestTarget::FriedmanLogQueue(kind),
        "dss_queue" => TestTarget::DSSQueue(kind),
        "our_pipe" => TestTarget::OurPipe(kind),
        "crndm_pipe" => TestTarget::CrndmPipe(kind),
        _ => unreachable!("invalid target"),
    }
}

fn setup() -> (Opt, Writer<File>) {
    let opt = Opt::from_args();
    unsafe { DURATION = opt.duration }; // 각 스레드가 수행할 시간 설정

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
        TestTarget::OurQueue(kind) => bench_queue(opt, target, kind),
        TestTarget::OurPipeQueue(kind) => bench_queue(opt, target, kind),
        TestTarget::FriedmanDurableQueue(kind) => bench_queue(opt, target, kind),
        TestTarget::FriedmanLogQueue(kind) => bench_queue(opt, target, kind),
        TestTarget::DSSQueue(kind) => bench_queue(opt, target, kind),
        TestTarget::OurPipe(kind) => {
            // get_nops::<GetOurPipeNOps>(&opt.filepath, kind, opt.threads, opt.duration)
            todo!()
        }
        TestTarget::CrndmPipe(kind) => {
            // let root = BuddyAlloc::open::<CrndmPipe>(&opt.filepath, O_16GB | O_CF).unwrap();
            //   root.get_nops(kind, opt.threads, opt.duration)
            todo!()
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
