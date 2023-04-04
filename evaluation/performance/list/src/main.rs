#![feature(generic_associated_types)]

use csv::Writer;
use list::common::list::bench_list;
use list::common::Opt;
use list::common::TestTarget;
use list::common::DURATION;
use list::common::RELAXED;
use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use structopt::StructOpt;

fn parse_target(target: &str) -> TestTarget {
    match target {
        "list-mmt" => TestTarget::MementoList,
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
                    "threads",
                    "duration",
                    "relaxed",
                    "key range",
                    "insert",
                    "delete",
                    "read",
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
        "bench {} using {} threads (key range: 1~{}, insert: {}%, delete: {}%, read: {}%)",
        opt.target,
        opt.threads,
        opt.key_range,
        opt.insert_ratio * 100.0,
        opt.delete_ratio * 100.0,
        opt.read_ratio * 100.0
    );
    let target = parse_target(&opt.target);
    let nops = match target {
        TestTarget::MementoList => bench_list(opt, target),
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
            opt.threads.to_string(),
            opt.duration.to_string(),
            opt.relax.to_string(),
            opt.key_range.to_string(),
            opt.insert_ratio.to_string(),
            opt.delete_ratio.to_string(),
            opt.read_ratio.to_string(),
            avg_mops.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
}
