//! volatile thread-recoverable spin lock
use core::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;

use crossbeam_utils::Backoff;

/// volatile thread-recoverable spin lock
#[derive(Debug, Default)]
pub struct VSpinLock {
    inner: AtomicUsize,
}

impl VSpinLock {
    const RELEASED: usize = 0;

    /// Try lock
    pub fn try_lock<const REC: bool>(&self, tid: usize) -> Result<SpinLockGuard<'_>, usize> {
        if REC && self.inner.load(Ordering::Acquire) == tid {
            return Ok(SpinLockGuard { lock: self });
        }

        self.inner
            .compare_exchange(Self::RELEASED, tid, Ordering::Acquire, Ordering::Relaxed)
            .map(|_| SpinLockGuard { lock: self })
            .map_err(|curr| curr)
    }

    /// lock
    pub fn lock<const REC: bool>(&self, tid: usize) -> SpinLockGuard<'_> {
        let backoff = Backoff::new();
        loop {
            if let Ok(g) = self.try_lock::<REC>(tid) {
                return g;
            }
            backoff.snooze();
        }
    }

    /// peek
    pub fn peek(&self) -> usize {
        self.inner.load(Ordering::SeqCst)
    }

    unsafe fn unlock(&self) {
        self.inner.store(Self::RELEASED, Ordering::Release);
    }
}

/// SpinLock Guard
#[derive(Debug)]
pub struct SpinLockGuard<'a> {
    lock: &'a VSpinLock,
}

impl<'a> Drop for SpinLockGuard<'a> {
    fn drop(&mut self) {
        unsafe { self.lock.unlock() };
    }
}
