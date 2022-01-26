//! Persistent Spin Lock

use std::sync::atomic::{AtomicUsize, Ordering};

use etrace::some_or;

use crate::{
    pepoch::{atomic::Pointer, PShared},
    ploc::{Checkpoint, CheckpointableUsize},
    pmem::{persist_obj, AsPPtr, Collectable, GarbageCollection, PoolHandle},
};

/// Try lock memento
#[derive(Debug, Default)]
pub struct TryLock {
    target: Checkpoint<CheckpointableUsize>,
}

unsafe impl Send for TryLock {}

impl Collectable for TryLock {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl TryLock {
    /// Reset TryLock memento
    pub fn reset(&mut self, pool: &PoolHandle) {
        let lock_ptr = some_or!(self.target.peek(), return);
        // I acquired lock before.

        let spin_lock = unsafe { PShared::<SpinLock>::from_usize(lock_ptr.0) }; // SAFE: Spin lock can't be dropped before released.
        let spin_lock_ref = unsafe { spin_lock.deref(pool) };
        let cur = spin_lock_ref.inner.load(Ordering::Relaxed);
        if cur == self.id(pool) {
            // id 체크 이유: 이미 release 해서 다른 애들이 쓰고 있는데 reset 다시 해서 또 release 해버릴 수도 있음
            spin_lock_ref
                .inner
                .store(SpinLock::RELEASED, Ordering::Release); // TODO(opt): Relaxed여도 됨. AtomicReset의 persist_obj에서 sfence를 함.
            persist_obj(&spin_lock_ref.inner, false);
        }
    }
}

impl TryLock {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        unsafe { self.as_pptr(pool) }.into_offset()
    }
}

/// Lock memento
#[derive(Debug, Default)]
pub struct Lock {
    try_lock: TryLock,
}

unsafe impl Send for Lock {}

impl Collectable for Lock {
    fn filter(lock: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        TryLock::filter(&mut lock.try_lock, tid, gc, pool);
    }
}

impl Lock {
    /// Reset Lock memento
    #[inline]
    pub fn reset(&mut self, pool: &PoolHandle) {
        self.try_lock.reset(pool);
    }
}

/// Spin lock
#[derive(Debug)]
pub struct SpinLock {
    inner: AtomicUsize,
}

impl Default for SpinLock {
    fn default() -> Self {
        Self {
            inner: AtomicUsize::new(SpinLock::RELEASED),
        }
    }
}

impl Collectable for SpinLock {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl SpinLock {
    const RELEASED: usize = 0;

    /// Try lock
    pub fn try_lock<const REC: bool>(
        &self,
        try_lock: &mut TryLock,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        if REC {
            let cur = self.inner.load(Ordering::Relaxed);

            if cur == try_lock.id(pool) {
                self.acq_succ::<REC>(try_lock, pool);
                return Ok(());
            }

            if cur != SpinLock::RELEASED {
                return Err(());
            }
        }

        self.inner
            .compare_exchange(
                SpinLock::RELEASED,
                try_lock.id(pool),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .map(|_| {
                persist_obj(&self.inner, true);
                self.acq_succ::<REC>(try_lock, pool);
            })
            .map_err(|_| ())
    }

    #[inline]
    fn acq_succ<const REC: bool>(&self, try_lock: &mut TryLock, pool: &PoolHandle) {
        let lock_ptr = unsafe { self.as_pptr(pool) }.into_offset();
        let _ = try_lock
            .target
            .checkpoint::<REC>(CheckpointableUsize(lock_ptr));
    }

    /// Lock
    pub fn lock<const REC: bool>(&self, lock: &mut Lock, pool: &PoolHandle) {
        if self.try_lock::<REC>(&mut lock.try_lock, pool).is_ok() {
            return;
        }

        loop {
            if self.try_lock::<REC>(&mut lock.try_lock, pool).is_ok() {
                return;
            }
        }
    }
}
