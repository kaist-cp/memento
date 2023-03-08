//! Abstraction for evaluation

use crossbeam_epoch::Guard;
use memento::pmem::{Pool, RootObj};
use memento::Memento;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use structopt::StructOpt;

/// file size
pub const FILE_SIZE: usize = 128 * 1024 * 1024 * 1024;

/// number of init nodes
pub static mut INIT_SIZE: usize = 0;

/// max threads
pub const MAX_THREADS: usize = 64;

/// test duration
pub static mut DURATION: f64 = 0.0;

/// period of repin
pub static mut RELAXED: usize = 0;

/// range of key
pub static mut KEY_RANGE: usize = 0;

/// insert ratio
pub static mut INSERT_RATIO: usize = 0;

/// delete ratio
pub static mut DELETE_RATIO: usize = 0;

/// read ratio
pub static mut READ_RATIO: usize = 0;

pub static TOTAL_NOPS: AtomicUsize = AtomicUsize::new(0);

pub trait TestNOps {
    // Count number of executions of `op` in `duration` seconds
    fn test_nops<'f, F: Fn(usize, &Guard)>(
        &self,
        op: &'f F,
        tid: usize,
        duration: f64,
        guard: &Guard,
    ) -> usize
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
        let guard = &mut unsafe { ptr::read(guard) };
        while start.elapsed() < dur {
            op(tid, guard);
            ops += 1;

            if ops % unsafe { RELAXED } == 0 {
                guard.repin_after(|| {});
            }
        }
        ops
    }
}

pub fn get_total_nops() -> usize {
    TOTAL_NOPS.load(Ordering::SeqCst)
}

#[derive(Debug)]
pub enum TestTarget {
    MementoList,
}

pub fn get_nops<O, M>(filepath: &str, nr_thread: usize) -> usize
where
    O: RootObj<M> + Send + Sync + 'static,
    M: Memento + Send + Sync,
{
    let _ = Pool::remove(filepath);

    let pool_handle = Pool::create::<O, M>(filepath, FILE_SIZE, nr_thread).unwrap();

    // Each thread executes op for `duration` seconds and accumulates execution count in `TOTAL_NOPS`
    pool_handle.execute::<O, M>();

    // Load `TOTAL_NOPS`
    get_total_nops()
}

#[derive(StructOpt, Debug)]
#[structopt(name = "bench")]
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

    /// period of repin (default: repin_after once every 10000 ops)
    #[structopt(short, long, default_value = "10000")]
    pub relax: usize,

    /// range of key
    #[structopt(short, long, default_value = "500")]
    pub key_range: usize,

    /// % insert
    #[structopt(short, long, default_value = "0")]
    pub insert_ratio: f64,

    /// % delete
    #[structopt(short, long, default_value = "0")]
    pub delete_ratio: f64,

    /// % read
    #[structopt(short, long, default_value = "0")]
    pub read_ratio: f64,
}

/// list
pub mod list {
    use crate::{
        common::{DELETE_RATIO, INSERT_RATIO, KEY_RANGE, READ_RATIO},
        mmt::{TestMementoInsDelRd, TestMementoList},
    };

    use super::{get_nops, Opt, TestTarget, INIT_SIZE};

    pub fn bench_list(opt: &Opt, target: TestTarget) -> usize {
        unsafe { KEY_RANGE = opt.key_range };
        unsafe { INIT_SIZE = opt.key_range / 2 };
        unsafe { INSERT_RATIO = (opt.insert_ratio * 100.0) as usize };
        unsafe { DELETE_RATIO = (opt.delete_ratio * 100.0) as usize };
        unsafe { READ_RATIO = (opt.read_ratio * 100.0) as usize };

        unsafe {
            assert!(INSERT_RATIO + DELETE_RATIO + READ_RATIO == 100);
        }

        match target {
            TestTarget::MementoList => {
                get_nops::<TestMementoList, TestMementoInsDelRd>(&opt.filepath, opt.threads)
            }
        }
    }
}

use std::cell::Cell;

thread_local! {
    pub static FAST_RANDOM_NEXT: Cell<usize> = Cell::new(1);
    pub static FAST_RANDOM_NEXT_Z: Cell<u32> = Cell::new(2);
    pub static FAST_RANDOM_NEXT_W: Cell<u32> = Cell::new(2);
}

pub fn fast_random_set_seed(seed: u32) {
    let _ = FAST_RANDOM_NEXT.try_with(|x| x.set(seed as usize));
    let _ = FAST_RANDOM_NEXT_Z.try_with(|x| x.set(seed));
    let _ = FAST_RANDOM_NEXT_W.try_with(|x| x.set(seed / 2));

    let z = FAST_RANDOM_NEXT_Z.try_with(|x| x.get()).unwrap();
    let w = FAST_RANDOM_NEXT_W.try_with(|x| x.get()).unwrap();
    if z == 0 || z == 0x9068ffff {
        let _ = FAST_RANDOM_NEXT_Z.try_with(|x| x.set(z + 1));
    }
    if w == 0 || w == 0x464fffff {
        let _ = FAST_RANDOM_NEXT_Z.try_with(|x| x.set(w + 1));
    }
}

#[inline]
fn fast_random() -> usize {
    FAST_RANDOM_NEXT
        .try_with(|x| {
            let new = x.get() * 1103515245 + 12345;
            x.set(new);
            (new / 65536) % 32768
        })
        .unwrap()
}

#[inline]
pub fn fast_random_range(low: usize, high: usize) -> usize {
    return low + ((high as f64) * (fast_random() as f64 / (SIM_RAND_MAX as f64 + 1.0))) as usize;
}

const SIM_RAND_MAX: usize = 32768;

// #[inline]
// pub fn pick_range(min: usize, max: usize) -> usize {
//     rand::thread_rng().gen_range(min..=max)
// }
