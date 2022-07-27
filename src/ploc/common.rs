//! Atomic Update Common

use std::{
    ops::{Add, Sub},
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam_utils::CachePadded;

use crate::{
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        rdtscp, PoolHandle, CACHE_LINE_SHIFT,
    },
    test_utils::ordo::get_ordo_boundary,
};

use super::{CASHelpArr, CasInfo};

pub(crate) const NR_MAX_THREADS: usize = 511;

/// Get specific bit range in a word
#[macro_export]
macro_rules! impl_left_bits {
    ($func:ident, $pos:expr, $nr:expr, $type:ty) => {
        #[inline]
        pub(crate) fn $func() -> $type {
            ((<$type>::MAX >> $pos) ^ (<$type>::MAX >> ($pos + $nr)))
        }
    };
}

// Auxiliary Bit
// aux bit: 0b100000000000000000000000000000000000000000000000000000000000000000 in 64-bit
// Used for:
// - PAtomic: Aux bit
// - Detectable CAS: Indicating CAS parity (Odd/Even)
// - Insert: Indicating if the pointer is persisted
pub(crate) const POS_AUX_BITS: u32 = 0;
pub(crate) const NR_AUX_BITS: u32 = 1;
impl_left_bits!(aux_bits, POS_AUX_BITS, NR_AUX_BITS, usize);

/// Compose aux bit (1-bit, MSB)
#[inline]
pub fn compose_aux_bit(cas_bit: usize, data: usize) -> usize {
    (aux_bits() & (cas_bit.rotate_right(POS_AUX_BITS + NR_AUX_BITS))) | (!aux_bits() & data)
}

/// Decompose aux bit (1-bit, MSB)
#[inline]
pub fn decompose_aux_bit(data: usize) -> (usize, usize) {
    (
        (data & aux_bits()).rotate_left(POS_AUX_BITS + NR_AUX_BITS),
        !aux_bits() & data,
    )
}

/// Timestamp struct
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct Timestamp(u64);

impl From<u64> for Timestamp {
    #[inline]
    fn from(t: u64) -> Self {
        Self(t)
    }
}

impl From<Timestamp> for u64 {
    #[inline]
    fn from(t: Timestamp) -> u64 {
        t.0
    }
}

impl Add for Timestamp {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Timestamp::from(self.0 + rhs.0)
    }
}

impl Sub for Timestamp {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Timestamp::from(self.0 - rhs.0)
    }
}

#[derive(Debug)]
pub(crate) struct LocalMaxTime {
    inner: [AtomicU64; NR_MAX_THREADS + 1],
}

impl Default for LocalMaxTime {
    fn default() -> Self {
        Self {
            inner: array_init::array_init(|_| AtomicU64::new(0)),
        }
    }
}

impl LocalMaxTime {
    #[inline]
    pub(crate) fn load(&self, tid: usize) -> Timestamp {
        Timestamp::from(self.inner[tid].load(Ordering::Relaxed))
    }

    #[inline]
    pub(crate) fn store(&self, tid: usize, t: Timestamp) {
        self.inner[tid].store(t.into(), Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct ExecInfo {
    /// Maximum checkpoint time checked per thread
    pub(crate) local_max_time: LocalMaxTime,

    /// Maximum checkpoint time in last execution (not changed after main execution)
    pub(crate) global_max_time: Timestamp,

    /// CAS information
    pub(crate) cas_info: CasInfo,

    /// Checkpoint information (not changed after main execution)
    pub(crate) chk_info: Timestamp,

    /// Program initial time (not changed after main execution)
    pub(crate) init_time: Timestamp,

    /// Global tsc offset
    pub(crate) tsc_offset: Timestamp,
}

impl From<&'static CASHelpArr> for ExecInfo {
    fn from(help: &'static CASHelpArr) -> Self {
        Self {
            local_max_time: LocalMaxTime::default(),
            global_max_time: Timestamp::from(0),
            chk_info: Timestamp::from(0),
            cas_info: CasInfo::new(help),
            init_time: Timestamp::from(rdtscp()),
            tsc_offset: get_ordo_boundary(),
        }
    }
}

impl ExecInfo {
    pub(crate) fn set_info(&mut self) {
        let max = self.cas_info.own.max_ts();
        let max = std::cmp::max(max, self.cas_info.help.max_ts());
        let max = std::cmp::max(max, self.chk_info);

        self.global_max_time = max;
    }

    #[inline]
    pub(crate) fn exec_time(&self) -> Timestamp {
        Timestamp::from(rdtscp()) - self.init_time + self.global_max_time
    }
}

/// Checkpoint memento
#[derive(Debug)]
pub struct Checkpoint<T: Default + Clone + Collectable> {
    saved: [CachePadded<(T, Timestamp)>; 2],
}

unsafe impl<T: Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        Self {
            saved: [
                CachePadded::new((T::default(), Timestamp::from(0))),
                CachePadded::new((T::default(), Timestamp::from(0))),
            ],
        }
    }
}

impl<T: Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(chk: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let (_, latest) = chk.stale_latest_idx();

        // Record the one with max timestamp among checkpoints
        if chk.saved[latest].1 > pool.exec_info.chk_info {
            pool.exec_info.chk_info = chk.saved[latest].1;
        }

        if chk.saved[latest].1 > Timestamp::from(0) {
            T::filter(&mut chk.saved[latest].0, tid, gc, pool);
        }
    }
}

/// Error of checkpoint containing existing/new value
#[derive(Debug)]
pub struct CheckpointError<T> {
    /// Existing value
    pub current: T,

    /// New value
    pub new: T,
}

impl<T> Checkpoint<T>
where
    T: Default + Clone + Collectable,
{
    /// Checkpoint
    pub fn checkpoint<const REC: bool, F: FnOnce() -> T>(
        &mut self,
        val_func: F,
        tid: usize,
        pool: &PoolHandle,
    ) -> T {
        if REC {
            if let Some(v) = self.peek(tid, pool) {
                return v;
            }
        }

        let new = val_func();
        let (stale, _) = self.stale_latest_idx();

        // Normal run
        let t = pool.exec_info.exec_time();
        if std::mem::size_of::<(T, Timestamp)>() <= 1 << CACHE_LINE_SHIFT {
            self.saved[stale] = CachePadded::new((new.clone(), t));
            persist_obj(&*self.saved[stale], true);
        } else {
            self.saved[stale].0 = new.clone();
            persist_obj(&self.saved[stale].0, true);
            self.saved[stale].1 = t;
            persist_obj(&self.saved[stale].1, true);
        }

        pool.exec_info.local_max_time.store(tid, t);
        new
    }

    #[inline]
    fn is_valid(&self, idx: usize, tid: usize, pool: &PoolHandle) -> bool {
        self.saved[idx].1 > pool.exec_info.local_max_time.load(tid)
    }

    #[inline]
    fn stale_latest_idx(&self) -> (usize, usize) {
        if self.saved[0].1 < self.saved[1].1 {
            (0, 1)
        } else {
            (1, 0)
        }
    }

    /// Peek
    pub fn peek(&self, tid: usize, pool: &PoolHandle) -> Option<T> {
        let (_, latest) = self.stale_latest_idx();

        if self.is_valid(latest, tid, pool) {
            pool.exec_info
                .local_max_time
                .store(tid, self.saved[latest].1);
            Some((self.saved[latest].0).clone())
        } else {
            None
        }
    }

    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.saved = [
            CachePadded::new((T::default(), Timestamp::from(0))),
            CachePadded::new((T::default(), Timestamp::from(0))),
        ];
        persist_obj(&*self.saved[0], false);
        persist_obj(&*self.saved[1], false);
    }
}
