//! Atomic update memento collections

use std::sync::atomic::Ordering;

use crossbeam_epoch::Guard;
use etrace::*;

use crate::{
    pepoch::{PAtomic, PDestroyable, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        rdtsc, PoolHandle,
    },
};

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

/// Insert Error
#[derive(Debug)]
pub enum InsertError<'g, T> {
    /// CAS fail (Strong fail)
    CASFail(PShared<'g, T>),

    /// Fail judged when recovered (Weak fail)
    RecFail,
}

/// Atomic pointer for use of `Insert' and `Delete`
#[derive(Debug)]
pub struct SMOAtomic<N: Node + Collectable> {
    inner: PAtomic<N>,
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
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
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
                    persist_obj(&self.inner, true);
                    match self.inner.compare_exchange(
                        old,
                        old.with_aux_bit(0),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    ) {
                        Ok(_) => return old.with_aux_bit(0),
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
    pub fn insert_lp<'g, O: Traversable<N>, const REC: bool>(
        &self,
        new: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError<'g, N>> {
        if REC {
            return Self::insert_result(new, obj, guard, pool);
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
                return Err(InsertError::CASFail(cur));
            }
            // retry for the property of strong CAS
        }

        persist_obj(&self.inner, true);
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
    pub fn insert<'g, O: Traversable<N>, const REC: bool>(
        &self,
        new: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError<'g, N>> {
        if REC {
            return Self::insert_result(new, obj, guard, pool);
        }

        // Normal run
        if let Err(e) = self.inner.compare_exchange(
            PShared::null(),
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        ) {
            return Err(InsertError::CASFail(e.current));
        }

        persist_obj(&self.inner, true);
        Ok(())
    }

    #[inline]
    fn insert_result<'g, O: Traversable<N>>(
        new: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError<'g, N>> {
        if unsafe { new.deref(pool) }.acked(guard)
            || obj.contains(new, guard, pool)
            || unsafe { new.deref(pool) }.acked(guard)
        {
            return Ok(());
        }

        Err(InsertError::RecFail) // Fail type may change after crash. Insert is weak.
    }

    /// Delete
    pub fn delete<'g, const REC: bool>(
        &self,
        old: PShared<'g, N>,
        new: PShared<'_, N>,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        if REC {
            return self.delete_result(old, new, tid, guard, pool);
        }

        // Record the tid on the node to be deleted.
        let target_ref = unsafe { old.deref(pool) };
        let owner = target_ref.replacement();
        let _ = owner
            .compare_exchange(
                PShared::null(),
                new.with_tid(tid),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map_err(|_| self.load_help(old, guard, pool).unwrap())?;

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
    fn delete_result<'g>(
        &self,
        old: PShared<'g, N>,
        new: PShared<'_, N>,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        let old_ref = some_or!(
            unsafe { old.as_ref(pool) },
            return Err(ok_or!(self.load_help(old, guard, pool), e, e)) // if null, return failure.
        );

        // Failure if the owner is not me
        let owner = old_ref.replacement();
        let o = owner.load(Ordering::SeqCst, guard);
        if o.tid() != tid {
            return Err(ok_or!(self.load_help(old, guard, pool), e, e));
        }

        persist_obj(owner, false);

        let _ = self
            .inner
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);
        guard.defer_persist(&self.inner);
        unsafe { guard.defer_pdestroy(old) };
        Ok(old)
    }
}

unsafe impl<N: Node + Collectable + Send + Sync> Send for SMOAtomic<N> {}
unsafe impl<N: Node + Collectable> Sync for SMOAtomic<N> {}
