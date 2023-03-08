#![feature(generic_associated_types)]

use csv::Writer;
use evaluation::common::queue::bench_queue;
use evaluation::common::{Opt, TestKind, TestTarget, DURATION, RELAXED};
use regex::Regex;
use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use structopt::StructOpt;

fn parse_target(target: &str, kind: &str) -> TestTarget {
    // The first 4 letters serve as test type classification, and the following letters serve as additional input
    // e.g. "prob50": 50% probability test (enqueue 50%, dequeue 50%)
    // e.g. "prob30": 30% probability test (enqueue 30%, dequeue 70%)
    let re = Regex::new(r"(\w{4})(\d*)").unwrap();
    let cap = re.captures(kind).unwrap();
    let (kind, arg) = (&cap[1], &cap[2]);
    let kind = match kind {
        "prob" => TestKind::QueueProb(arg.parse::<u32>().unwrap()),
        "pair" => TestKind::QueuePair,
        _ => unreachable!(),
    };
    match target {
        // Queue
        "memento_queue" => TestTarget::MementoQueue(kind),
        "memento_queue_lp" => TestTarget::MementoQueueLp(kind),
        "memento_queue_general" => TestTarget::MementoQueueGeneral(kind),
        "memento_queue_comb" => TestTarget::MementoQueueComb(kind),
        "durable_queue" => TestTarget::FriedmanDurableQueue(kind),
        "log_queue" => TestTarget::FriedmanLogQueue(kind),
        "dss_queue" => TestTarget::DSSQueue(kind),
        "pbcomb_queue" => TestTarget::PBCombQueue(kind),
        "pbcomb_queue_full_detectable" => TestTarget::PBCombQueueFullDetectable(kind),
        "crndm_queue" => TestTarget::CorundumQueue(kind),
        _ => unreachable!("invalid target"),
    }
}

fn setup() -> (Opt, Writer<File>) {
    let opt = Opt::from_args();
    unsafe { DURATION = opt.duration };
    unsafe { RELAXED = opt.relax };

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
                .write_record(&[
                    "target",
                    "bench kind",
                    "threads",
                    "duration",
                    "relaxed",
                    "init nodes",
                    "throughput",
                ])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    (opt, output)
}

//  the throughput (op execution/s) when using `nr_thread` threads
fn bench(opt: &Opt) -> f64 {
    println!(
        "bench {}:{} using {} threads",
        opt.target, opt.kind, opt.threads
    );
    let target = parse_target(&opt.target, &opt.kind);
    let nops = match target {
        TestTarget::MementoQueue(_)
        | TestTarget::MementoQueueLp(_)
        | TestTarget::MementoQueueGeneral(_)
        | TestTarget::MementoQueueComb(_)
        | TestTarget::FriedmanDurableQueue(_)
        | TestTarget::FriedmanLogQueue(_)
        | TestTarget::DSSQueue(_)
        | TestTarget::PBCombQueue(_)
        | TestTarget::PBCombQueueFullDetectable(_)
        | TestTarget::CorundumQueue(_) => bench_queue(opt, target),
    };
    let avg_ops = (nops as f64) / opt.duration;
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
            opt.relax.to_string(),
            opt.init.to_string(),
            avg_mops.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
}
