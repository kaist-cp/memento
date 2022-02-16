//! Atomic Update Common

use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

use crate::pmem::{
    ll::persist_obj,
    ralloc::{Collectable, GarbageCollection},
    rdtsc, rdtscp, PoolHandle, CACHE_LINE_SHIFT,
};

use super::{CASCheckpointArr, CasInfo};

pub(crate) const NR_MAX_THREADS: usize = 511;

/// TODO(doc)
#[macro_export]
macro_rules! impl_left_bits {
    ($func:ident, $pos:expr, $nr:expr) => {
        #[inline]
        pub(crate) fn $func() -> usize {
            ((usize::MAX >> $pos) ^ (usize::MAX >> ($pos + $nr)))
        }
    };
}

// Auxiliary Bit
// aux bit: 0b100000000000000000000000000000000000000000000000000000000000000000 in 64-bit
// Used for:
// - Detectable CAS: Indicating CAS parity (Odd/Even)
// - Insert: Indicating if the pointer is persisted
pub(crate) const POS_AUX_BITS: u32 = 0;
pub(crate) const NR_AUX_BITS: u32 = 1;
impl_left_bits!(aux_bits, POS_AUX_BITS, NR_AUX_BITS);

#[inline]
pub(crate) fn compose_aux_bit(cas_bit: usize, data: usize) -> usize {
    (aux_bits() & (cas_bit.rotate_right(POS_AUX_BITS + NR_AUX_BITS))) | (!aux_bits() & data)
}

#[inline]
pub(crate) fn decompose_aux_bit(data: usize) -> (usize, usize) {
    (
        (data & aux_bits()).rotate_left(POS_AUX_BITS + NR_AUX_BITS),
        !aux_bits() & data,
    )
}

/// TODO(doc)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl Timestamp {
    /// TODO(doc)
    #[inline]
    pub fn new(aux: bool, t: u64) -> Self {
        Self(compose_aux_bit(Self::aux_to_bit(aux), t as usize) as u64)
    }

    /// TODO(doc)
    #[inline]
    pub fn decompose(&self) -> (bool, u64) {
        let (aux, t) = decompose_aux_bit(self.0 as usize);
        (aux == 1, t as u64)
    }

    /// TODO(doc)
    #[inline]
    pub fn aux(&self) -> bool {
        self.decompose().0
    }

    /// TODO(doc)
    #[inline]
    pub fn time(&self) -> u64 {
        self.decompose().1
    }

    /// TODO(doc)
    #[inline]
    pub fn aux_to_bit(aux: bool) -> usize {
        if aux {
            1
        } else {
            0
        }
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
            local_max_time: array_init::array_init(|_| AtomicU64::default()),
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
            .cas_own
            .iter()
            .fold(Timestamp::from(0), |m, chk| {
                let t = Timestamp::from(chk.load(Ordering::Relaxed));
                std::cmp::max(m, t)
            });
        let max = self.cas_info.cas_help.iter().fold(max, |m, chk_arr| {
            chk_arr.iter().fold(m, |mm, chk| {
                let t = Timestamp::from(chk.load(Ordering::Relaxed));
                std::cmp::max(mm, t)
            })
        });
        let max = std::cmp::max(max, self.chk_info);

        self.global_max_time = max;
    }

    #[inline]
    pub(crate) fn calc_checkpoint(&self, t: u64) -> u64 {
        t - self.init_time.time() + self.global_max_time.time()
    }
}

/// TODO(doc)
/// TODO(must): 두 개 운용하고 0,1을 통해서 valid한 쪽을 나타내게 해야할 듯 (이유: normal run에서 덮어쓰다가 error날 경우)
///             혹은 checkpoint는 여러 개 동시에 하지말고 한 큐에 되는 것만 하자 <- 안 된다... 사이즈 큰 거 checkpoint할 경우엔...
#[derive(Debug)]
pub struct Checkpoint<T: Default + Clone + Collectable> {
    saved: CachePadded<(T, Timestamp)>,
}

unsafe impl<T: Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        Self {
            saved: (CachePadded::new((T::default(), Timestamp::new(false, 0)))),
        }
    }
}

impl<T: Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(chk: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut chk.saved.0, tid, gc, pool);

        // Checkpoint 중 max timestamp를 가진 걸로 기록해줌
        if chk.saved.1 > pool.exec_info.chk_info {
            pool.exec_info.chk_info = chk.saved.1;
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
            // TODO(must): checkpoint variable이 atomic하게 바뀌도록 해야 함
            if self.is_valid()
                && self.saved.1
                    > Timestamp::from(pool.exec_info.local_max_time[tid].load(Ordering::Relaxed))
            {
                return Err(CheckpointError {
                    current: (self.saved.0).clone(),
                    new,
                });
            }
        }

        // Normal run
        self.saved.1 = Timestamp::new(false, 0); // First, invalidate existing data.
        if std::mem::size_of::<(T, Timestamp)>() > 1 << CACHE_LINE_SHIFT {
            persist_obj(&self.saved.1, true);
        }
        compiler_fence(Ordering::Release);

        self.saved = CachePadded::new((new.clone(), Timestamp::new(true, rdtsc())));
        pool.exec_info.local_max_time[tid].store(self.saved.1.into(), Ordering::Relaxed);
        persist_obj(&*self.saved, true);
        Ok(new)
    }

    /// TODO(doc)
    #[inline]
    fn is_valid(&self) -> bool {
        self.saved.1.aux()
    }

    /// TODO(doc)
    #[inline]
    pub fn peek(&self) -> Option<T> {
        if self.is_valid() {
            Some((self.saved.0).clone())
        } else {
            None
        }
    }
}
