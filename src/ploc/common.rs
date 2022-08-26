//! Atomic Update Common

use std::{
    ops::{Add, Sub},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use crossbeam_epoch::Guard;

use super::{CasHelpArr, CasInfo};
use crate::{
    pmem::{lfence, rdtscp, PoolHandle},
    test_utils::ordo::get_ordo_boundary,
};

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
#[derive(Debug, Default, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
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

/// Maximum checkpoint time checked per thread
#[derive(Debug)]
pub(crate) struct LocalMaxTime {
    inner: AtomicU64,
}

impl Default for LocalMaxTime {
    fn default() -> Self {
        Self {
            inner: AtomicU64::new(0),
        }
    }
}

impl LocalMaxTime {
    #[inline]
    pub(crate) fn load(&self) -> Timestamp {
        Timestamp::from(self.inner.load(Ordering::Relaxed))
    }

    #[inline]
    pub(crate) fn store(&self, t: Timestamp) {
        self.inner.store(t.into(), Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct ExecInfo {
    /// Maximum checkpoint time in last execution (not changed after main execution)
    pub(crate) global_max_time: Timestamp,

    /// Checkpoint information (not changed after main execution)
    pub(crate) chk_max_time: Timestamp,

    /// CAS information
    pub(crate) cas_info: CasInfo,

    /// Program initial time (not changed after main execution)
    pub(crate) init_time: Timestamp,

    /// Global tsc offset
    pub(crate) tsc_offset: Timestamp,
}

impl From<&'static CasHelpArr> for ExecInfo {
    fn from(help: &'static CasHelpArr) -> Self {
        Self {
            global_max_time: Timestamp::from(0),
            chk_max_time: Timestamp::from(0),
            cas_info: CasInfo::new(help),
            init_time: Timestamp::from(rdtscp()),
            tsc_offset: get_ordo_boundary(),
        }
    }
}

impl ExecInfo {
    #[inline]
    pub(crate) fn set_info(&mut self) {
        self.global_max_time = std::cmp::max(self.cas_info.max_ts(), self.chk_max_time);
    }

    #[inline]
    pub(crate) fn exec_time(&self) -> Timestamp {
        let ret = Timestamp::from(rdtscp()) - self.init_time + self.global_max_time;
        lfence();
        ret
    }
}

/// Handle for each thread
#[derive(Debug)]
pub struct Handle {
    /// Logical tid
    pub tid: usize,

    /// Maximum checkpoint time checked per thread
    pub(crate) local_max_time: LocalMaxTime,

    /// Recovery flag
    pub(crate) rec: AtomicBool,

    /// Guard
    pub guard: Guard,

    /// Pool
    pub pool: &'static PoolHandle,
}

impl Handle {
    pub(crate) fn new(tid: usize, guard: Guard, pool: &'static PoolHandle) -> Self {
        Self {
            tid,
            local_max_time: LocalMaxTime::default(),
            rec: AtomicBool::new(true),
            guard,
            pool,
        }
    }
}

unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}
