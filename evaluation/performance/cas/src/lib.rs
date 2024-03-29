use std::{
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use crossbeam_epoch::Guard;
use memento::{
    pepoch::{PAtomic, POwned},
    ploc::Handle,
    pmem::{Collectable, GarbageCollection, Pool, PoolHandle, RootObj},
    Memento, PDefault,
};
use rand::Rng;

pub mod cas;
pub mod mcas;
pub mod nrlcas;
pub mod pcas;

/// file size
pub const FILE_SIZE: usize = 128 * 1024 * 1024 * 1024;

/// max threads
pub const MAX_THREADS: usize = 64;

/// test duration
pub static mut DURATION: f64 = 0.0;

/// period of repin
pub static mut RELAXED: usize = 0;

pub static mut CONTENTION_WIDTH: usize = 1;

pub static mut NR_THREADS: usize = 1;

pub static TOTAL_NOPS: AtomicUsize = AtomicUsize::new(0);
pub static TOTAL_NOPS_FAILED: AtomicUsize = AtomicUsize::new(0);

pub fn get_total_nops() -> usize {
    TOTAL_NOPS.load(Ordering::SeqCst)
}
#[derive(Debug, Default)]
pub struct Node(usize); // `usize` for low tag

impl Collectable for Node {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

#[inline]
pub fn pick_range(min: usize, max: usize) -> usize {
    rand::thread_rng().gen_range(min..max)
}

/// A fixed-size vec with each item in the persistent heap
struct PFixedVec<T> {
    items: PAtomic<[MaybeUninit<T>]>,
}

impl<T> Collectable for PFixedVec<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl<T: PDefault> PFixedVec<T> {
    fn new(size: usize, handle: &Handle) -> Self {
        let mut locs = POwned::<[MaybeUninit<T>]>::init(size, handle.pool);
        let locs_ref = unsafe { locs.deref_mut(handle.pool) };
        for i in 0..size {
            locs_ref[i].write(T::pdefault(handle));
        }
        assert_eq!(size, locs_ref.len());

        Self {
            items: PAtomic::from(locs),
        }
    }
}

impl<T> PFixedVec<T> {
    fn as_ref<'g>(&self, guard: &'g Guard, pool: &'g PoolHandle) -> &'g [MaybeUninit<T>] {
        unsafe { self.items.load(Ordering::SeqCst, guard).deref(pool) }
    }

    fn as_mut<'g>(&self, guard: &'g Guard, pool: &'g PoolHandle) -> &'g mut [MaybeUninit<T>] {
        unsafe { self.items.load(Ordering::SeqCst, guard).deref_mut(pool) }
    }
}

pub trait TestNOps {
    // Count number of executions of `op` in `duration` seconds
    fn test_nops<'f, F: Fn(usize) -> bool>(
        &self,
        op: &'f F,
        tid: usize,
        duration: f64,
    ) -> (usize, usize)
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let mut failed = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
        while start.elapsed() < dur {
            if op(tid) {
                ops += 1;
            } else {
                failed += 1;
            }
        }

        (ops, failed)
    }
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

pub trait TestableCas {
    type Input;
    type Location;

    fn cas(&self, input: Self::Input, loc: &Self::Location, handle: &Handle) -> bool;
}

pub fn cas_random_loc<C: TestableCas>(
    cas: &C,
    input: C::Input,
    locs: &[MaybeUninit<C::Location>],
    handle: &Handle,
) -> bool {
    let ix = pick_range(0, unsafe { CONTENTION_WIDTH });
    cas.cas(input, unsafe { locs[ix].assume_init_ref() }, handle)
}
