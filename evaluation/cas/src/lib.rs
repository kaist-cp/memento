use std::{
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use crossbeam_epoch::Guard;
use memento::{
    pepoch::{PAtomic, POwned},
    pmem::{Collectable, GarbageCollection, PoolHandle},
    PDefault,
};
use rand::Rng;

pub mod cas;
pub mod mcas;
pub mod nrlcas;
pub mod pcas;

pub static TOTAL_NOPS_FAILED: AtomicUsize = AtomicUsize::new(0);
pub static mut CONTENTION_WIDTH: usize = 1;

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
    fn new(size: usize, pool: &PoolHandle) -> Self {
        let mut locs = POwned::<[MaybeUninit<T>]>::init(size, pool);
        let locs_ref = unsafe { locs.deref_mut(pool) };
        for i in 0..size {
            locs_ref[i].write(T::pdefault(pool));
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

pub trait TestableCas {
    type Input;
    type Location;

    fn cas(
        &self,
        input: Self::Input,
        loc: &Self::Location,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool;
}

pub fn cas_random_loc<C: TestableCas>(
    cas: &C,
    input: C::Input,
    locs: &[MaybeUninit<C::Location>],
    guard: &Guard,
    pool: &PoolHandle,
) -> bool {
    let ix = pick_range(0, unsafe { CONTENTION_WIDTH });
    cas.cas(input, unsafe { locs[ix].assume_init_ref() }, guard, pool)
}
