//! Atomic update memento collections

use std::sync::atomic::Ordering;

use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;
use etrace::*;

use crate::{
    pepoch::{PAtomic, PDestroyable, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        rdtsc, PoolHandle,
    },
};

use super::{ExecInfo, Handle, Timestamp};

/// Node for `Insert`/`Delete`
pub trait Node: Sized {
    /// Indication of acknowledgement for `Insert`
    fn acked(&self, guard: &Guard) -> bool {
        self.replacement().load(Ordering::SeqCst, guard) != not_deleted()
    }

    /// Deleter's tid and next pointer for helping
    fn replacement(&self) -> &PAtomic<Self>;
}

/// Indicate no owner
#[inline]
pub fn not_deleted<'g, T>() -> PShared<'g, T> {
    PShared::null()
}

/// Traversable object for recovery of `Insert`
pub trait Traversable<N> {
    /// Search specific target pointer through the object
    fn contains(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) -> bool;
}

#[derive(Debug, Default)]
struct Failed {
    t: CachePadded<Timestamp>,
}

impl Failed {
    fn record(&mut self, handle: &Handle) {
        *self.t = handle.pool.exec_info.exec_time();
        persist_obj(&*self.t, true);
    }

    fn check_failed(&self, handle: &Handle) -> bool {
        let t_mmt = *self.t;
        let t_local = handle.local_max_time.load();

        if t_mmt <= t_local {
            return false;
        }

        handle.local_max_time.store(t_mmt);
        true
    }
}

impl Collectable for Failed {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

/// Insert memento
#[derive(Debug, Default)]
pub struct Insert(Failed);

impl Collectable for Insert {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut s.0, tid, gc, pool);
    }
}

/// Delete memento
#[derive(Debug, Default)]
pub struct Delete(Failed);

impl Collectable for Delete {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut s.0, tid, gc, pool);
    }
}

/// Atomic pointer for use of `Insert' and `Delete`
#[derive(Debug)]
pub struct SMOAtomic<N: Node + Collectable> {
    inner: PAtomic<N>, // TODO: CachePadded
}

impl<N: Node + Collectable> Collectable for SMOAtomic<N> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut s.inner, tid, gc, pool);
    }
}

impl<N: Node + Collectable> Default for SMOAtomic<N> {
    fn default() -> Self {
        Self {
            inner: PAtomic::null(),
        }
    }
}

impl<N: Node + Collectable> From<PShared<'_, N>> for SMOAtomic<N> {
    fn from(node: PShared<'_, N>) -> Self {
        Self {
            inner: PAtomic::from(node),
        }
    }
}

impl<N: Node + Collectable> SMOAtomic<N> {
    /// Ok(ptr): The final ptr after all helps
    /// Err(ptr): The final ptr after all helps but the final ptr cannot be deleted for now.
    pub fn load_help<'g>(
        &self,
        old: PShared<'g, N>,
        handle: &'g Handle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let old_ref = some_or!(unsafe { old.as_ref(pool) }, return Ok(old));

        let repl = old_ref.replacement();
        let new = repl.load(Ordering::SeqCst, guard);
        if new == not_deleted() {
            return Ok(old);
        }

        if new.as_ptr() == old.as_ptr() {
            // Reflexive
            // Notice that it cannot be deleted at this time
            return Err(old);
        }

        // TODO: Change inner only at the end
        let start = rdtsc();
        loop {
            let cur = self.inner.load(Ordering::SeqCst, guard);
            if cur != old {
                return Ok(cur);
            }

            let now = rdtsc();
            if now < start + Self::PATIENCE {
                continue;
            }

            persist_obj(repl, false); // cas soon
            let ret = match self.inner.compare_exchange(
                cur,
                new.with_tid(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                Ok(n) => n,
                Err(e) => e.current,
            };
            return Ok(ret);
        }
    }

    const PATIENCE: u64 = 40000;

    /// Load
    pub fn load_lp<'g>(&self, ord: Ordering, guard: &'g Guard) -> PShared<'g, N> {
        let mut old = self.inner.load(ord, guard);
        'out: loop {
            if old.aux_bit() == 0 {
                return old;
            }

            let mut start = rdtsc();
            loop {
                let cur = self.inner.load(ord, guard);

                if cur.aux_bit() == 0 {
                    return cur;
                }

                if old != cur {
                    old = cur;
                    start = rdtsc();
                    continue;
                }

                let now = rdtsc();
                if now > start + Self::PATIENCE {
                    persist_obj(&self.inner, false);
                    match self.inner.compare_exchange(
                        old,
                        old.with_aux_bit(0),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    ) {
                        Ok(new) => return new,
                        Err(e) => {
                            old = e.current;
                            continue 'out;
                        }
                    };
                }
            }
        }
    }

    /// Insert link-persist
    pub fn insert_lp<'g, O: Traversable<N>>(
        &self,
        new: PShared<'_, N>,
        obj: &O,
        mmt: &mut Insert,
        handle: &'g Handle,
    ) -> Result<(), PShared<'g, N>> {
        let (guard, pool) = (&handle.guard, handle.pool);
        if handle.rec.load(Ordering::Relaxed) {
            if let Some(succ) = Self::insert_result(new, obj, mmt, handle) {
                return if succ {
                    Ok(())
                } else {
                    Err(self.load_lp(Ordering::SeqCst, &handle.guard)) // TODO: This makes this function not guarantee the returned value is not null.
                };
            }
            handle.rec.store(false, Ordering::Relaxed);
        }

        // Normal run
        while self
            .inner
            .compare_exchange(
                PShared::null(),
                new.with_aux_bit(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .is_err()
        {
            let cur = self.load_lp(Ordering::SeqCst, guard);
            if cur != PShared::null() {
                mmt.0.record(handle);
                return Err(cur);
            }
            // retry for the property of strong CAS
        }

        persist_obj(&self.inner, false);
        let _ = self.inner.compare_exchange(
            new.with_aux_bit(1),
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        );

        Ok(())
    }

    /// Load
    #[inline]
    pub fn load<'g>(&self, persist: bool, ord: Ordering, guard: &'g Guard) -> PShared<'g, N> {
        let cur = self.inner.load(ord, guard);
        if persist {
            persist_obj(&self.inner, true);
        }
        cur
    }

    /// Insert
    pub fn insert<'g, O: Traversable<N>>(
        &self,
        new: PShared<'_, N>,
        obj: &O,
        mmt: &mut Insert,
        handle: &'g Handle,
    ) -> Result<(), PShared<'g, N>> {
        if handle.rec.load(Ordering::Relaxed) {
            if let Some(succ) = Self::insert_result(new, obj, mmt, handle) {
                return if succ {
                    Ok(())
                } else {
                    Err(self.inner.load(Ordering::SeqCst, &handle.guard))
                };
            }
            handle.rec.store(false, Ordering::Relaxed);
        }

        // Normal run
        if let Err(e) = self.inner.compare_exchange(
            PShared::null(),
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
            &handle.guard,
        ) {
            mmt.0.record(handle);
            return Err(e.current);
        }

        persist_obj(&self.inner, true);
        Ok(())
    }

    #[inline]
    fn insert_result<O: Traversable<N>>(
        new: PShared<'_, N>,
        obj: &O,
        mmt: &mut Insert,
        handle: &Handle,
    ) -> Option<bool> {
        if mmt.0.check_failed(handle) {
            return Some(false);
        }

        let (guard, pool) = (&handle.guard, handle.pool);
        if unsafe { new.deref(pool) }.acked(guard)
            || obj.contains(new, guard, pool)
            || unsafe { new.deref(pool) }.acked(guard)
        {
            return Some(true);
        }

        None
    }

    /// Delete
    ///
    /// Requirement: `old` is not null
    pub fn delete<'g>(
        &'g self,
        old: PShared<'g, N>,
        new: PShared<'_, N>,
        mmt: &mut Delete,
        handle: &'g Handle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        if handle.rec.load(Ordering::Relaxed) {
            if let Some(succ) = self.delete_result(old, new, mmt, handle) {
                return if succ {
                    Ok(old)
                } else {
                    Err(ok_or!(self.load_help(old, handle), e, e))
                };
            }
            handle.rec.store(false, Ordering::Relaxed);
        }
        let (tid, guard, pool) = (handle.tid, &handle.guard, handle.pool);

        // Record the tid on the node to be deleted.
        let target_ref = unsafe { old.deref(pool) };
        let owner = target_ref.replacement();
        if owner
            .compare_exchange(
                PShared::null(),
                new.with_tid(tid),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .is_err()
        {
            mmt.0.record(handle);
            let cur = ok_or!(self.load_help(old, handle), e, e);
            return Err(cur);
        }

        // Now I own the location. flush the owner.
        persist_obj(owner, false); // we're doing CAS soon.

        // Now that the owner has been decided, the point is changed
        let _ = self
            .inner
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);

        // The changed location will persist before freeing the node I deleted.
        // defer_persist() does not break history in post-crash: if the next accessor is `Insert`, it will persist the location.
        // e.g. A --(defer per)--> B --(defer per)--> null --(per)--> C
        guard.defer_persist(&self.inner);
        unsafe { guard.defer_pdestroy(old) }

        Ok(old)
    }

    #[inline]
    fn delete_result(
        &self,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mmt: &mut Delete,
        handle: &Handle,
    ) -> Option<bool> {
        let (tid, guard, pool) = (handle.tid, &handle.guard, handle.pool);
        if mmt.0.check_failed(handle) {
            return Some(false);
        }

        let old_ref = unsafe { old.deref(pool) };

        // Failure if the owner is not me
        let owner = old_ref.replacement();
        let o = owner.load(Ordering::SeqCst, guard);
        if o.tid() != tid {
            return None;
        }

        persist_obj(owner, false);

        let _ = self
            .inner
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);
        guard.defer_persist(&self.inner);
        unsafe { guard.defer_pdestroy(old) };
        Some(true)
    }
}

unsafe impl<N: Node + Collectable + Send + Sync> Send for SMOAtomic<N> {}
unsafe impl<N: Node + Collectable> Sync for SMOAtomic<N> {}
