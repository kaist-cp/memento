//! Atomic Update Common

use core::fmt;
use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

use crate::pmem::{
    ll::persist_obj,
    ralloc::{Collectable, GarbageCollection},
    rdtscp, PoolHandle, CACHE_LINE_SHIFT,
};

use super::{CASCheckpointArr, CasInfo};

pub(crate) const NR_MAX_THREADS: usize = 511;

/// TODO(doc)
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

/// TODO doc
#[inline]
pub fn compose_aux_bit(cas_bit: usize, data: usize) -> usize {
    (aux_bits() & (cas_bit.rotate_right(POS_AUX_BITS + NR_AUX_BITS))) | (!aux_bits() & data)
}

/// TODO doc
#[inline]
pub fn decompose_aux_bit(data: usize) -> (usize, usize) {
    (
        (data & aux_bits()).rotate_left(POS_AUX_BITS + NR_AUX_BITS),
        !aux_bits() & data,
    )
}

/// TODO(doc)
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Timestamp(u64);

impl From<u64> for Timestamp {
    #[inline]
    fn from(t: u64) -> Self {
        Self(t)
    }
}

impl Into<u64> for Timestamp {
    #[inline]
    fn into(self) -> u64 {
        self.0
    }
}

impl PartialOrd for Timestamp {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (_, t1) = self.decompose();
        let (_, t2) = other.decompose();
        t1.cmp(&t2)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (aux, time) = self.decompose();

        f.debug_struct("Timestamp")
            .field("aux", &aux)
            .field("timestamp", &time)
            .finish()
    }
}

impl Timestamp {
    /// TODO(doc)
    /// 62-bit timestamp with 2-bit high tag
    #[inline]
    pub fn new(high_tag: u64, t: u64) -> Self {
        Self(Self::compose_high_tag(high_tag, t))
    }

    const POS_HIGH_BITS: u32 = 0;
    const NR_HIGH_BITS: u32 = 2;
    impl_left_bits!(high_bits, 0, 2, u64);

    #[inline]
    fn compose_high_tag(high_tag: u64, data: u64) -> u64 {
        (Self::high_bits() & (high_tag.rotate_right(Self::POS_HIGH_BITS + Self::NR_HIGH_BITS)))
            | (!Self::high_bits() & data)
    }

    #[inline]
    fn decompose_high_tag(data: u64) -> (u64, u64) {
        (
            (data & Self::high_bits()).rotate_left(Self::POS_HIGH_BITS + Self::NR_HIGH_BITS),
            !Self::high_bits() & data,
        )
    }

    /// TODO(doc)
    #[inline]
    pub fn decompose(&self) -> (u64, u64) {
        let (htag, t) = Self::decompose_high_tag(self.0);
        (htag, t)
    }

    /// TODO(doc)
    #[inline]
    pub fn high_tag(&self) -> u64 {
        self.decompose().0
    }

    /// TODO(doc)
    #[inline]
    pub fn time(&self) -> u64 {
        self.decompose().1
    }
}

#[derive(Debug)]
pub(crate) struct ExecInfo {
    /// 스레드별로 확인된 최대 체크포인트 시간
    // TODO(opt): AtomicTimestamp로 wrapping 하는 게 나을지도? u64로 바꾸다가 사고날 수도..
    pub(crate) local_max_time: [AtomicU64; NR_MAX_THREADS + 1],

    /// 지난 실행에서 최대 체크포인트 시간 (not changed after main execution)
    pub(crate) global_max_time: Timestamp,

    /// CAS 정보
    pub(crate) cas_info: CasInfo,

    /// Checkpoint 정보 (not changed after main execution)
    pub(crate) chk_info: Timestamp,

    /// 프로그램 초기 시간 (not changed after main execution)
    pub(crate) init_time: Timestamp,
}

impl From<&'static [CASCheckpointArr; 2]> for ExecInfo {
    fn from(chk_ref: &'static [CASCheckpointArr; 2]) -> Self {
        Self {
            local_max_time: array_init::array_init(|_| AtomicU64::new(0)),
            global_max_time: Timestamp::from(0),
            chk_info: Timestamp::from(0),
            cas_info: CasInfo::from(chk_ref),
            init_time: Timestamp::from(rdtscp()),
        }
    }
}

impl ExecInfo {
    pub(crate) fn set_info(&mut self) {
        let max = self
            .cas_info
            .own
            .iter()
            .fold(Timestamp::from(0), |m, chk| {
                let t = Timestamp::from(chk.load(Ordering::Relaxed));
                std::cmp::max(m, t)
            });
        let max = self.cas_info.help.iter().fold(max, |m, chk_arr| {
            chk_arr.iter().fold(m, |mm, chk| {
                let t = Timestamp::from(chk.load(Ordering::Relaxed));
                std::cmp::max(mm, t)
            })
        });
        let max = std::cmp::max(max, self.chk_info);

        self.global_max_time = max;
    }

    #[inline]
    pub(crate) fn exec_time(&self) -> u64 {
        rdtscp() - self.init_time.time() + self.global_max_time.time()
    }
}

/// TODO(doc)
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
        let idx = chk.max_idx();

        // Checkpoint 중 max timestamp를 가진 걸로 기록해줌
        if chk.saved[idx].1 > pool.exec_info.chk_info {
            pool.exec_info.chk_info = chk.saved[idx].1;
        }

        if chk.saved[idx].1.time() > 0 {
            T::filter(&mut chk.saved[idx].0, tid, gc, pool);
        }
    }
}

/// TODO(doc)
#[derive(Debug)]
pub struct CheckpointError<T> {
    /// TODO(doc)
    pub current: T,

    /// TODO(doc)
    pub new: T,
}

impl<T> Checkpoint<T>
where
    T: Default + Clone + Collectable,
{
    /// TODO(doc)
    pub fn checkpoint<const REC: bool>(
        &mut self,
        new: T,
        tid: usize,
        pool: &PoolHandle,
    ) -> Result<T, CheckpointError<T>> {
        if REC {
            if let Some(v) = self.peek(tid, pool) {
                return Err(CheckpointError { current: v, new });
            }
        }

        let idx = self.min_idx();

        // Normal run
        self.saved[idx].1 = Timestamp::from(0); // First, invalidate existing data.
        if std::mem::size_of::<(T, Timestamp)>() > 1 << CACHE_LINE_SHIFT {
            persist_obj(&self.saved[idx].1, true);
        }
        compiler_fence(Ordering::Release);

        let t = pool.exec_info.exec_time();
        self.saved[idx] = CachePadded::new((new.clone(), Timestamp::from(t)));
        pool.exec_info.local_max_time[tid].store(self.saved[idx].1.into(), Ordering::Relaxed);
        persist_obj(&*self.saved[idx], true);
        Ok(new)
    }

    /// TODO(doc)
    #[inline]
    fn is_valid(&self, idx: usize, tid: usize, pool: &PoolHandle) -> bool {
        self.saved[idx].1.time() > 0
            && self.saved[idx].1
                > Timestamp::from(pool.exec_info.local_max_time[tid].load(Ordering::Relaxed))
    }

    #[inline]
    fn min_idx(&self) -> usize {
        if self.saved[0].1 <= self.saved[1].1 {
            0
        } else {
            1
        }
    }

    #[inline]
    fn max_idx(&self) -> usize {
        if self.saved[0].1 > self.saved[1].1 {
            0
        } else {
            1
        }
    }

    /// TODO(doc)
    pub fn peek(&self, tid: usize, pool: &PoolHandle) -> Option<T> {
        let idx = self.max_idx();

        if self.is_valid(idx, tid, pool) {
            pool.exec_info.local_max_time[tid].store(self.saved[idx].1.into(), Ordering::Relaxed);
            Some((self.saved[idx].0).clone())
        } else {
            None
        }
    }
}
