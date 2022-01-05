//! Atomic update memento collections

use std::{marker::PhantomData, ops::Deref, sync::atomic::Ordering};

use crossbeam_epoch::Guard;
use etrace::*;

use super::{no_owner, InsertErr, Traversable};

use crate::{
    pepoch::{PAtomic, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    Memento,
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
    /// TODO(doc)
    pub fn load_helping<'g>(&self, guard: &'g Guard, pool: &PoolHandle) -> PShared<'g, N> {
        let mut p = self.inner.load(Ordering::SeqCst, guard);
        loop {
            let p_ref = some_or!(unsafe { p.as_ref(pool) }, return p);

            let owner = p_ref.owner();
            let o = owner.load(Ordering::SeqCst, guard);
            if o == no_owner() {
                return p;
            }
            // TODO: reflexive면 무한루프 발생. prev node를 들고 있고 현재 owner가 prev랑 같다면 그냥 리턴하기. Err로 리턴해서 업데이트 불가능한 상황임을 알려야 할 듯

            persist_obj(owner, true); // TODO(opt): async reset
            p = ok_or!(self.help(p, o, guard), e, return e);
        }
    }

    /// Ok(ptr): required to be checked if the node is owned by someone
    /// Err(ptr): No need to help anymore
    #[inline]
    fn help<'g>(
        &self,
        old: PShared<'g, N>,
        next: PShared<'g, N>,
        guard: &'g Guard,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        // self를 승자가 원하는 node로 바꿔줌
        let ret =
            match self
                .inner
                .compare_exchange(old, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            {
                Ok(n) => n,
                Err(e) => e.current,
            };
        Ok(ret)
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
#[derive(Debug)]
pub struct NeedRetry;

/// TODO(doc)
/// Do not use LSB while using `Update`.
/// It's reserved for it.
/// 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
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
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>);
    type Output<'o>
    where
        N: 'o,
    = PShared<'o, N>;
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (old, new): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(tid, guard, pool);
        }

        // Normal run
        let new = new.with_tid(tid);

        // 우선 내가 target을 가리키고
        self.target_loc.store(old, Ordering::Relaxed);
        persist_obj(&self.target_loc, false); // we're doing CAS soon.

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { old.deref(pool) };
        let owner = target_ref.owner();
        let _ = owner
            .compare_exchange(
                PShared::null(),
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map_err(|_| ())?;

        // Now I own the location. flush the owner.
        persist_obj(owner, false); // we're doing CAS soon.

        // 주인을 정했으니 이제 point를 바꿔줌
        let _ = point.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard);

        // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
        // defer_persist이어도 post-crash에서 history가 끊기진 않음: 다음 접근자가 `Insert`라면, 그는 point를 persist 무조건 할 거임.
        // e.g. A --(defer per)--> B --(defer per)--> null --(per)--> C
        guard.defer_persist(point);

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
    #[inline]
    fn result<'g>(
        &self,
        tid: usize,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);
        let target_ref = some_or!(
            unsafe { target.as_ref(pool) },
            return Err(()) // 내가 찜한게 없으면 실패
        );

        // 내가 찜한 target의 owner가 나면 성공, 아니면 실패
        let owner = target_ref.owner().load(Ordering::SeqCst, guard);
        if owner.tid() == tid {
            Ok(target)
        } else {
            Err(())
        }
    }
}

/// TODO(doc)
///
/// # Safety
///
/// 내가 Insert/Update로 넣은 node를 내가 Delete 해서 뺐을 때 사용
/// - 남이 넣었던 건 하면 안 되는 이유: 넣었던 애가 `acked()`를 호출하기 때문에 owner를 건드리면 안 됨
/// - 내가 Update로 뺐을 때 하면 안 되는 이유: point CAS를 helping 하는 애들이 next node를 owner를 통해 알게 되므로 건드리면 안 됨
pub unsafe fn clear_owner<N: Node>(deleted_node: &N) {
    let owner = deleted_node.owner();
    owner.store(no_owner(), Ordering::SeqCst);
    persist_obj(owner, true);
}
