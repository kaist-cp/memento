//! Atomic update memento collections

use std::{marker::PhantomData, ops::Deref, sync::atomic::Ordering};

use crossbeam_epoch::Guard;
use etrace::*;

use super::{no_owner, InsertErr, Traversable};

use crate::{
    pepoch::{PAtomic, PDestroyable, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// TODO(doc)
pub trait Node: Sized {
    /// TODO(doc)
    fn acked(&self, guard: &Guard) -> bool {
        self.owner().load(Ordering::SeqCst, guard) != no_owner()
    }

    /// TODO(doc)
    fn owner(&self) -> &PAtomic<Self>;
}

/// TODO(doc)
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

impl<N: Node + Collectable> Deref for SMOAtomic<N> {
    type Target = PAtomic<N>;

    fn deref(&self) -> &Self::Target {
        &self.inner
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

            let owner = cur_ref.owner();
            let next = owner.load(Ordering::SeqCst, guard);
            if next == no_owner() {
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
}

unsafe impl<N: Node + Collectable> Send for SMOAtomic<N> {}
unsafe impl<N: Node + Collectable> Sync for SMOAtomic<N> {}

/// TODO(doc)
#[derive(Debug)]
pub struct Insert<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for Insert<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for Insert<O, N> {}

impl<O, N: Node + Collectable> Default for Insert<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for Insert<O, N> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for Insert<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o PAtomic<N>; // TODO(must): SMOAtomic?
    type Input<'o> = (
        PShared<'o, N>,
        &'o O,
        fn(&mut N) -> bool, // cas 전에 할 일 (bool 리턴값은 계속 진행할지 여부)
    );
    type Output<'o>
    where
        O: 'o,
        N: 'o,
    = ();
    type Error<'o> = InsertErr<'o, N>;

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (mut new, obj, prepare): Self::Input<'o>, // TODO(opt): prepare도 그냥 Prepare trait으로 할 수 있을 듯
        _: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(obj, new, guard, pool);
        }

        // Normal run
        let new_ref = unsafe { new.deref_mut(pool) };
        let old = point.load(Ordering::SeqCst, guard);

        if !old.is_null() || !prepare(new_ref) {
            // TODO: prepare(new_ref, old)? for tags
            return Err(InsertErr::PrepareFail);
        }

        let ret = point
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|e| InsertErr::CASFail(e.current));

        persist_obj(point, true);
        ret
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
}

impl<O: Traversable<N>, N: Node + Collectable> Insert<O, N> {
    #[inline]
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if unsafe { new.deref(pool) }.acked(guard)
            || obj.search(new, guard, pool)
            || unsafe { new.deref(pool) }.acked(guard)
        {
            return Ok(());
        }

        Err(InsertErr::RecFail) // Fail이 crash 이후 달라질 수 있음. Insert는 weak 함
    }
}

/// TODO(doc)
#[derive(Debug, Clone)]
pub enum DeleteMode {
    /// TODO(doc)
    Drop,

    /// TODO(doc)
    Recycle,
}

/// TODO(doc)
/// Do not use LSB while using `Delete`.
/// It's reserved for it.
/// - 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
/// - Drop mode일 때는 new가 old와 같은 주소면 안 됨
#[derive(Debug)]
pub struct Delete<N: Node + Collectable> {
    target_loc: PAtomic<N>,
    _marker: PhantomData<*const N>,
}

unsafe impl<N: Node + Collectable + Send + Sync> Send for Delete<N> {}
unsafe impl<N: Node + Collectable + Send + Sync> Sync for Delete<N> {}

impl<N: Node + Collectable> Default for Delete<N> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<N: Node + Collectable> Collectable for Delete<N> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<N> Memento for Delete<N>
where
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o SMOAtomic<N>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>, DeleteMode);
    type Output<'o>
    where
        N: 'o,
    = PShared<'o, N>;
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (old, new, mode): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(point, old, new, mode, tid, guard, pool);
        }

        // 우선 내가 target을 가리키고
        match mode {
            DeleteMode::Drop => {
                if old.as_ptr() == new.as_ptr() {
                    panic!("Delete: `new` must have a different pointer from `old`");
                }
                self.target_loc.store(old, Ordering::Relaxed)
            }
            DeleteMode::Recycle => self.target_loc.store(old.with_tid(0), Ordering::Relaxed), // TODO(opt): tid 0으로 세팅할 필요 있나? invariant 있음?
        };
        persist_obj(&self.target_loc, false); // we're doing CAS soon.

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { old.deref(pool) };
        let owner = target_ref.owner();
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
        let _ = point.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);

        match mode {
            DeleteMode::Drop => {
                // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
                // defer_persist이어도 post-crash에서 history가 끊기진 않음: 다음 접근자가 `Insert`라면, 그는 point를 persist 무조건 할 거임.
                // e.g. A --(defer per)--> B --(defer per)--> null --(per)--> C
                guard.defer_persist(point);
                unsafe { guard.defer_pdestroy(old) } // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
            }
            DeleteMode::Recycle => {
                persist_obj(point, false);

                self.target_loc
                    .store(old.with_tid(Self::RECYCLE_MID), Ordering::Relaxed);
                persist_obj(&self.target_loc, true);

                // clear owner
                owner.store(no_owner(), Ordering::SeqCst);
                persist_obj(owner, true);

                self.target_loc
                    .store(old.with_tid(Self::RECYCLE_END), Ordering::Relaxed);
                persist_obj(&self.target_loc, true);
            }
        };

        Ok(old)
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, false);
    }
}

impl<N> Delete<N>
where
    N: Node + Collectable,
{
    // Recycle 상태 저장을 target_loc에서 tid 필드에 사용할 것임
    // 0: 기본, 1: owner cas 성공, 2: owner 지운 후
    const RECYCLE_START: usize = 0;
    const RECYCLE_MID: usize = 1;
    const RECYCLE_END: usize = 2;

    #[inline]
    fn result<'g>(
        &self,
        point: &SMOAtomic<N>,
        old: PShared<'_, N>,
        new: PShared<'_, N>,
        mode: DeleteMode,
        tid: usize,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);
        let target_ref = some_or!(unsafe { target.as_ref(pool) }, return Err(())); // if null, return failure.

        match mode {
            DeleteMode::Drop => {
                // owner가 내가 아니면 실패
                let owner = target_ref.owner().load(Ordering::SeqCst, guard);
                if owner.tid() == tid {
                    unsafe { guard.defer_pdestroy(target) };
                    Ok(target)
                } else {
                    Err(())
                }
            }
            DeleteMode::Recycle => {
                let mut recycle_tag = target.tid();
                let mut cas = false;

                loop {
                    match recycle_tag {
                        Self::RECYCLE_START => {
                            let owner = target_ref.owner().load(Ordering::SeqCst, guard);
                            if owner.tid() != tid {
                                // owner가 내가 아니면 실패
                                return Err(());
                            }

                            let _ = point.compare_exchange(
                                old,
                                new,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            );
                            cas = true;

                            self.target_loc
                                .store(target.with_tid(Self::RECYCLE_MID), Ordering::Relaxed);
                            persist_obj(&self.target_loc, true);

                            recycle_tag = Self::RECYCLE_MID;
                        }
                        Self::RECYCLE_MID => {
                            // Recycle: owner 지우기 전 단계

                            if !cas {
                                let _ = point.compare_exchange(
                                    old,
                                    new,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    guard,
                                );
                            }
                            persist_obj(point, true);

                            target_ref.owner().store(no_owner(), Ordering::SeqCst);
                            persist_obj(target_ref.owner(), true);

                            self.target_loc
                                .store(target.with_tid(Self::RECYCLE_END), Ordering::Relaxed);
                            persist_obj(&self.target_loc, true);

                            recycle_tag = Self::RECYCLE_END;
                        }
                        Self::RECYCLE_END => return Ok(target),
                        _ => unreachable!("No more cases"),
                    }
                }
            }
        }
    }
}
