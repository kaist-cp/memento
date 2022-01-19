//! Atomic update memento collections

use std::{marker::PhantomData, ops::Deref, sync::atomic::Ordering};

use crossbeam_epoch::Guard;
use etrace::*;

use crate::{
    pepoch::{PAtomic, PDestroyable, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// Node for `Insert`/`Delete`
pub trait Node: Sized {
    /// Indication of acknowledgement for `Insert`
    fn acked(&self, guard: &Guard) -> bool {
        self.tid_next().load(Ordering::SeqCst, guard) != not_deleted()
    }

    /// Deleter's tid and next pointer for helping
    fn tid_next(&self) -> &PAtomic<Self>;
}

/// No owner를 표시하기 위함
#[inline]
pub fn not_deleted<'g, T>() -> PShared<'g, T> {
    PShared::null()
}

/// Traversable object for recovery of `Insert`
pub trait Traversable<N> {
    /// Search specific target pointer through the object
    fn search(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) -> bool;
}

/// Insert
#[derive(Debug)]
pub struct Insert<O: Traversable<N>, N> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O: Traversable<N>, N> Send for Insert<O, N> {}
unsafe impl<O: Traversable<N>, N> Sync for Insert<O, N> {}

impl<O: Traversable<N>, N> Default for Insert<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O: Traversable<N>, N> Collectable for Insert<O, N> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O: Traversable<N>, N> Insert<O, N> {
    /// Reset Insert memento
    #[inline]
    pub fn reset(&mut self) {}
}

/// Insert Error
#[derive(Debug)]
pub enum InsertError<'g, T> {
    /// CAS fail (Strong fail)
    CASFail(PShared<'g, T>),

    /// Fail judged when recovered (Weak fail)
    RecFail,
}

/// Delete
///
/// Do not use LSB while using `Delete`.
/// It's reserved for it.
/// - 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
/// - Drop mode일 때는 new가 old와 같은 주소면 안 됨
#[derive(Debug)]
pub struct Delete<N: Node + Collectable> {
    target_loc: PAtomic<N>,
}

unsafe impl<N: Node + Collectable + Send + Sync> Send for Delete<N> {}
unsafe impl<N: Node + Collectable + Send + Sync> Sync for Delete<N> {}

impl<N: Node + Collectable> Default for Delete<N> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
        }
    }
}

impl<N: Node + Collectable> Collectable for Delete<N> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<N> Delete<N>
where
    N: Node + Collectable,
{
    /// Reset Delete memento
    #[inline]
    pub fn reset(&mut self) {
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, false);
    }
}

/// Atomic pointer for use of `Insert'/`Delete`
#[derive(Debug)]
pub struct SMOAtomic<N: Node + Collectable> {
    inner: PAtomic<N>,
}

impl<N: Node + Collectable> Collectable for SMOAtomic<N> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        let mut cur = self.inner.load(Ordering::SeqCst, guard);

        loop {
            let cur_ref = some_or!(unsafe { cur.as_ref(pool) }, return Ok(cur));

            let owner = cur_ref.tid_next();
            let next = owner.load(Ordering::SeqCst, guard);
            if next == not_deleted() {
                return Ok(cur);
            }

            if next.as_ptr() == cur.as_ptr() {
                // 자기 자신이 owner인 상태
                // 현재로썬 delete는 사용할 수 없음을 알림
                return Err(cur);
            }

            persist_obj(owner, false); // cas soon
            cur = match self.inner.compare_exchange(
                cur,
                next.with_tid(0),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            ) {
                Ok(n) => n,
                Err(e) => e.current,
            };
        }
    }

    /// Load
    pub fn load<'g>(&self, ord: Ordering, guard: &'g Guard) -> PShared<'g, N> {
        let ret = self.inner.load(ord, guard);
        persist_obj(&self.inner, true);
        ret
    }

    /// Insert
    pub fn insert<'g, O: Traversable<N>, const REC: bool>(
        &self,
        new: PShared<'_, N>,
        obj: &O,
        insert: &mut Insert<O, N>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError<'g, N>> {
        if REC {
            return Self::insert_result(new, obj, insert, guard, pool);
        }

        // Normal run
        let ret = self
            .inner
            .compare_exchange(
                PShared::null(),
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map(|_| ())
            .map_err(|e| InsertError::CASFail(e.current));

        persist_obj(&self.inner, true);
        ret
    }

    #[inline]
    fn insert_result<'g, O: Traversable<N>>(
        new: PShared<'_, N>,
        obj: &O,
        _: &mut Insert<O, N>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError<'g, N>> {
        if unsafe { new.deref(pool) }.acked(guard)
            || obj.search(new, guard, pool)
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
        delete: &mut Delete<N>,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        if REC {
            return self.delete_result(delete, tid, guard, pool);
        }

        // 우선 내가 target을 가리키고
        delete.target_loc.store(old, Ordering::Relaxed);
        persist_obj(&delete.target_loc, false); // we're doing CAS soon.

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { old.deref(pool) };
        let owner = target_ref.tid_next();
        let _ = owner
            .compare_exchange(
                PShared::null(),
                new.with_tid(tid),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map_err(|_| ())?;

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
        unsafe { guard.defer_pdestroy(old) } // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음

        Ok(old)
    }

    #[inline]
    fn delete_result<'g>(
        &self,
        delete: &mut Delete<N>,
        tid: usize,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        let target = delete.target_loc.load(Ordering::Relaxed, guard);
        let target_ref = some_or!(unsafe { target.as_ref(pool) }, return Err(())); // if null, return failure.

        // owner가 내가 아니면 실패
        let owner = target_ref.tid_next().load(Ordering::SeqCst, guard);
        if owner.tid() == tid {
            unsafe { guard.defer_pdestroy(target) };
            Ok(target)
        } else {
            Err(())
        }
    }
}

unsafe impl<N: Node + Collectable> Send for SMOAtomic<N> {}
unsafe impl<N: Node + Collectable> Sync for SMOAtomic<N> {}
