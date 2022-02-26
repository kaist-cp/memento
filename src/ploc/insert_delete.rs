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

/// No owner를 표시하기 위함
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

/// Atomic pointer for use of `Insert'/`Delete`
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
    /// Ok(ptr): helping 다 해준 뒤의 최종 ptr
    /// Err(ptr): helping 다 해준 뒤의 최종 ptr. 단, 최종 ptr은 지금 delete가 불가능함
    pub fn load_helping<'g>(
        &self,
        old: PShared<'g, N>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        let old_ref = some_or!(unsafe { old.as_ref(pool) }, return Ok(old));

        let owner = old_ref.replacement();
        let next = owner.load(Ordering::SeqCst, guard);
        if next == not_deleted() {
            return Ok(old);
        }

        if next.as_ptr() == old.as_ptr() {
            // 자기 자신이 owner인 상태
            // 현재로썬 delete는 사용할 수 없음을 알림
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

            persist_obj(owner, false); // cas soon
            let ret = match self.inner.compare_exchange(
                cur,
                next.with_tid(0),
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

    /// Insert
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

        return Ok(());
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

        Err(InsertError::RecFail) // Fail이 crash 이후 달라질 수 있음. Insert는 weak 함
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

        // 빼려는 node에 내 이름 새겨넣음
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
            .map_err(|_| self.load_helping(old, guard, pool).unwrap())?;

        // Now I own the location. flush the owner.
        persist_obj(owner, false); // we're doing CAS soon.

        // 주인을 정했으니 이제 point를 바꿔줌
        let _ = self
            .inner
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);

        // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
        // defer_persist이어도 post-crash에서 history가 끊기진 않음: 다음 접근자가 `Insert`라면, 그는 point를 persist 무조건 할 거임.
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
            return Err(ok_or!(self.load_helping(old, guard, pool), e, e)) // if null, return failure.
        );

        // owner가 내가 아니면 실패
        let owner = old_ref.replacement();
        let o = owner.load(Ordering::SeqCst, guard);
        if o.tid() != tid {
            return Err(ok_or!(self.load_helping(old, guard, pool), e, e));
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

unsafe impl<N: Node + Collectable> Send for SMOAtomic<N> {}
unsafe impl<N: Node + Collectable> Sync for SMOAtomic<N> {}
