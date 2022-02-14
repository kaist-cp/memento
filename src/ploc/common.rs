//! Atomic Update Common

use crossbeam_utils::CachePadded;

use crate::pmem::{
    ll::persist_obj,
    ralloc::{Collectable, GarbageCollection},
    PoolHandle,
};

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
#[derive(Debug, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    /// TODO(doc)
    #[inline]
    pub fn new(aux: bool, t: u64) -> Self {
        let a = if aux { 1 } else { 0 };
        Self(compose_aux_bit(a, t as usize) as u64)
    }

    /// TODO(doc)
    #[inline]
    pub fn decompose(&self) -> (bool, u64) {
        let (aux, t) = decompose_aux_bit(self.0 as usize);
        (aux == 1, t as u64)
    }
}

/// TODO(doc)
pub trait Checkpointable {
    /// TODO(doc)
    fn invalidate(&mut self);

    /// TODO(doc)
    fn is_invalid(&self) -> bool;
}

/// TODO(doc)
/// TODO(must): 두 개 운용하고 0,1을 통해서 valid한 쪽을 나타내게 해야할 듯 (이유: normal run에서 덮어쓰다가 error날 경우)
///             혹은 checkpoint는 여러 개 동시에 하지말고 한 큐에 되는 것만 하자 <- 안 된다... 사이즈 큰 거 checkpoint할 경우엔...
#[derive(Debug)]
pub struct Checkpoint<T: Checkpointable + Default + Clone + Collectable> {
    saved: CachePadded<T>,
}

unsafe impl<T: Checkpointable + Default + Clone + Collectable + Send + Sync> Send
    for Checkpoint<T>
{
}
unsafe impl<T: Checkpointable + Default + Clone + Collectable + Send + Sync> Sync
    for Checkpoint<T>
{
}

impl<T: Checkpointable + Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        let mut t = T::default();
        t.invalidate();

        Self {
            saved: CachePadded::new(t),
        }
    }
}

impl<T: Checkpointable + Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        T::filter(&mut s.saved, tid, gc, pool);
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
    T: Checkpointable + Default + Clone + Collectable,
{
    /// TODO(doc)
    pub fn checkpoint<const REC: bool>(&mut self, new: T) -> Result<T, CheckpointError<T>> {
        if REC {
            if let Some(saved) = self.peek() {
                return Err(CheckpointError {
                    current: saved.clone(),
                    new,
                });
            }
        }

        // Normal run
        self.saved = CachePadded::new(new.clone());
        persist_obj(&*self.saved, true);
        Ok(new)
    }

    /// TODO(doc)
    #[inline]
    pub fn reset(&mut self) {
        self.saved.invalidate();
        persist_obj(&*self.saved, false);
    }
}

impl<T: Checkpointable + Default + Clone + Collectable> Checkpoint<T> {
    /// TODO(doc)
    #[inline]
    pub fn peek(&self) -> Option<T> {
        if self.saved.is_invalid() {
            None
        } else {
            Some((*self.saved).clone())
        }
    }
}

/// TODO(doc)
#[derive(Debug, Clone, Copy)]
pub struct CheckpointableUsize(pub usize);

impl CheckpointableUsize {
    const INVALID: usize = usize::MAX - u32::MAX as usize;
}

impl Default for CheckpointableUsize {
    fn default() -> Self {
        Self(Self::INVALID)
    }
}

impl Collectable for CheckpointableUsize {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl Checkpointable for CheckpointableUsize {
    fn invalidate(&mut self) {
        self.0 = CheckpointableUsize::INVALID;
    }

    fn is_invalid(&self) -> bool {
        self.0 == CheckpointableUsize::INVALID
    }
}
