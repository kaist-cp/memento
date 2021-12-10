//! Persistent Spin Lock

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    ploc::RetryLoop,
    pmem::{persist_obj, AsPPtr, Collectable, GarbageCollection, PoolHandle},
    Memento,
};

/// TODO(doc)
#[derive(Debug, Default)]
pub struct TryLock;

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
            .map(|_| {
                persist_obj(&spin_lock.inner, true);
                ()
            })
            .map_err(|_| ())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO(must): reset이 obj를 받아야 함.
        let cur = spin_lock.inner.load(Ordering::Relaxed);
        if cur != self.id(pool) {
            return;
        }

        spin_lock.inner.store(SpinLock::RELEASED, Ordering::Release); // TODO(opt): Relaxed여도 됨. AtomicReset의 persist_obj에서 sfence를 함.
        persist_obj(&spin_lock.inner, false);
    }
}

impl TryLock {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        unsafe { self.as_pptr(pool) }.into_offset()
    }
}

/// TODO(doc)
#[derive(Debug, Default)]
pub struct Lock {
    try_lock: RetryLoop<TryLock>,
}

unsafe impl Send for Lock {}

impl Collectable for Lock {
    fn filter(lock: &mut Self, gc: &mut GarbageCollection, _: &PoolHandle) {
        RetryLoop::mark(&mut lock.try_lock, gc);
    }
}

impl Memento for Lock {
    type Object<'o> = &'o SpinLock;
    type Input<'o> = ();
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        spin_lock: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.try_lock
            .run(spin_lock, (), rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_lock.reset(guard, pool);
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
