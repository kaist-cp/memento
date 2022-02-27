//! volatile thread-recoverable spin lock
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

/// volatile thread-recoverable spin lock
#[derive(Debug, Default)]
pub struct VSpinLock {
    inner: AtomicUsize, // 55:lock sequence (짝수:lock 잡은애 없음, 홀수:누군가 lock 잡고 진행중), 9:tid
}

impl VSpinLock {
    const RELEASED: usize = 0;

    /// Try lock
    ///
    /// return Ok: (seq, guard)
    /// return Err: (seq, tid)
    pub fn try_lock<const REC: bool>(
        &self,
        tid: usize,
    ) -> Result<(usize, SpinLockGuard<'_>), (usize, usize)> {
        let (_seq, _tid) = decompose_aux_bit(self.inner.load(Ordering::Acquire));
        if REC && tid == _tid {
            return Ok((_seq, SpinLockGuard { lock: self }));
        }

        self.inner
            .compare_exchange(
                compose_aux_bit(_seq, Self::RELEASED),
                compose_aux_bit(_seq + 1, tid),
                Ordering::SeqCst,
                Ordering::Acquire,
            )
            .map(|_| (_seq + 1, SpinLockGuard { lock: self }))
            .map_err(decompose_aux_bit)
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
        decompose_aux_bit(self.inner.load(Ordering::SeqCst))
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
    lock: &'a VSpinLock,
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        unsafe { self.lock.unlock() };
    }
}
