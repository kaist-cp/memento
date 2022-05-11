use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::Path,
};

use cas_eval::{
    cas::{TestCas, TestCasMmt},
    mcas::{TestMCas, TestMCasMmt},
    pcas::{TestPCas, TestPCasMmt},
};
use csv::Writer;
use evaluation::common::{get_nops, DURATION};
use structopt::StructOpt;

pub enum TestTarget {
    Cas,
    MCas,
    PCas,
    PMwCas,
    RCas,
}

fn parse_target(target: &str) -> TestTarget {
    match target {
        "cas" => TestTarget::Cas,
        "mcas" => TestTarget::MCas,
        "pcas" => TestTarget::PCas,
        // "pmwcas" => TestTarget::PMwCas,
        // "rcas" => TestTarget::RCas,
        _ => unreachable!("invalid target"),
    }
}

fn setup() -> (Opt, Writer<File>) {
    let opt = Opt::from_args();
    unsafe { DURATION = opt.duration };

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
                .write_record(&["target", "threads", "duration", "throughput"])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    (opt, output)
}

//  the throughput (op execution/s) when using `nr_thread` threads
fn bench(opt: &Opt) -> f64 {
    println!("bench {}: {} threads", opt.target, opt.threads);
    let target = parse_target(&opt.target);
    let nops = bench_cas(opt, target);
    let avg_ops = (nops as f64) / opt.duration;
    println!("avg ops: {}", avg_ops);
    avg_ops
}

pub fn bench_cas(opt: &Opt, target: TestTarget) -> usize {
    match target {
        TestTarget::Cas => get_nops::<TestCas, TestCasMmt>(&opt.filepath, opt.threads),
        TestTarget::MCas => get_nops::<TestMCas, TestMCasMmt>(&opt.filepath, opt.threads),
        TestTarget::PCas => get_nops::<TestPCas, TestPCasMmt>(&opt.filepath, opt.threads),
        _ => unimplemented!(),
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "cas_bench")]
pub struct Opt {
    /// filepath
    #[structopt(short, long)]
    pub filepath: String,

    /// target
    #[structopt(short = "a", long)]
    pub target: String,

    /// number of threads
    #[structopt(short, long)]
    pub threads: usize,

    /// test duration
    #[structopt(short, long, default_value = "5")]
    pub duration: f64,

    /// output path
    #[structopt(short, long)]
    pub output: Option<String>,
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
            avg_mops.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
}
