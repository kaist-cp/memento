//! Thread-recoverable spin lock
use core::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;

use crossbeam_utils::Backoff;

use crate::impl_left_bits;

// Auxiliary Bits
// aux bits: MSB 55-bit in 64-bit
// Used for:
// - PBComb: Indicating sequence of combine
pub(crate) const POS_AUX_BITS: u32 = 0;
pub(crate) const NR_AUX_BITS: u32 = 55;
impl_left_bits!(aux_bits, POS_AUX_BITS, NR_AUX_BITS, usize);

#[inline]
fn compose_aux_bit(aux: usize, data: usize) -> usize {
    (aux_bits() & (aux.rotate_right(POS_AUX_BITS + NR_AUX_BITS))) | (!aux_bits() & data)
}

#[inline]
fn decompose_aux_bit(data: usize) -> (usize, usize) {
    (
        (data & aux_bits()).rotate_left(POS_AUX_BITS + NR_AUX_BITS),
        !aux_bits() & data,
    )
}

/// thread-recoverable spin lock
#[derive(Debug, Default)]
pub struct ThreadRecoverableSpinLock {
    inner: AtomicUsize, // 55:lock sequence (even:no owner), 9:tid
}

impl ThreadRecoverableSpinLock {
    const RELEASED: usize = 0;

    /// Try lock
    ///
    /// return Ok: (seq, guard)
    /// return Err: (seq, tid)
    pub fn try_lock<const REC: bool>(
        &self,
        tid: usize,
    ) -> Result<(usize, SpinLockGuard<'_>), (usize, usize)> {
        let current = self.inner.load(Ordering::Relaxed);
        let (_seq, _tid) = decompose_aux_bit(current);

        if REC && tid == _tid {
            return Ok((_seq, SpinLockGuard { lock: self }));
        }

        if _tid != Self::RELEASED {
            return Err((_seq, _tid));
        }

        self.inner
            .compare_exchange(
                current,
                compose_aux_bit(_seq + 1, tid),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .map(|_| (_seq + 1, SpinLockGuard { lock: self }))
            .map_err(|_| (_seq, _tid))
    }

    /// lock
    pub fn lock<const REC: bool>(&self, tid: usize) -> (usize, SpinLockGuard<'_>) {
        let backoff = Backoff::new();
        loop {
            if let Ok((seq, g)) = self.try_lock::<REC>(tid) {
                return (seq, g);
            }
            backoff.snooze();
        }
    }

    /// peek
    ///
    /// return (seq, tid)
    pub fn peek(&self) -> (usize, usize) {
        decompose_aux_bit(self.inner.load(Ordering::Acquire))
    }

    unsafe fn unlock(&self) {
        let (seq, _) = decompose_aux_bit(self.inner.load(Ordering::Relaxed));
        self.inner
            .store(compose_aux_bit(seq + 1, Self::RELEASED), Ordering::Release);
    }
}

/// SpinLock Guard
#[derive(Debug)]
pub struct SpinLockGuard<'a> {
    lock: &'a ThreadRecoverableSpinLock,
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        unsafe { self.lock.unlock() };
    }
}
