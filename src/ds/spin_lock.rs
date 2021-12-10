//! Persistent Spin Lock

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    pmem::{AsPPtr, Collectable, GarbageCollection, PoolHandle},
    Memento,
};

/// TODO(doc)
#[derive(Debug)]
pub struct TryLock;

impl Default for TryLock {
    fn default() -> Self {
        Self {}
    }
}

unsafe impl Send for TryLock {}

impl Collectable for TryLock {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl Memento for TryLock {
    type Object<'o> = &'o SpinLock;
    type Input<'o> = ();
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        spin_lock: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        _: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            let cur = spin_lock.inner.load(Ordering::Relaxed);

            if cur == self.id(pool) {
                return Ok(());
            }

            if cur != SpinLock::RELEASED {
                return Err(());
            }
        }

        spin_lock
            .inner
            .compare_exchange(
                SpinLock::RELEASED,
                self.id(pool),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {

        // TODO(must): unlock
    }
}

impl TryLock {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        unsafe { self.as_pptr(pool) }.into_offset()
    }
}

/// TODO(doc)
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
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl SpinLock {
    const RELEASED: usize = 0;
}
