//! Atomic Update UnOpt

use std::{marker::PhantomData, sync::atomic::Ordering};

use crossbeam_epoch::Guard;
use etrace::*;

use super::{common::NodeUnOpt, InsertErr, Traversable, EMPTY};
use crate::{
    pepoch::{PAtomic, PShared},
    ploc::no_owner,
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
    Memento,
};

/// TODO(doc)
#[derive(Debug)]
pub struct InsertUnOpt<O, N: NodeUnOpt + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Send for InsertUnOpt<O, N> {}
unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Sync for InsertUnOpt<O, N> {}

impl<O, N: NodeUnOpt + Collectable> Default for InsertUnOpt<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: NodeUnOpt + Collectable> Collectable for InsertUnOpt<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for InsertUnOpt<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + NodeUnOpt + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (
        PShared<'o, N>,
        &'o O,
        fn(&mut N, PShared<'_, N>) -> bool, // cas 전에 할 일 (bool 리턴값은 계속 진행할지 여부)
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
        (mut new, obj, prepare): Self::Input<'o>,
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

        if !prepare(new_ref, old) {
            return Err(InsertErr::PrepareFail);
        }

        let ret = point
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|e| InsertErr::CASFail(e.current));

        persist_obj(point, true); // TODO(opt): stack에서는 성공한 놈만 해도 될지도?
        ret
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
}

impl<O: Traversable<N>, N: NodeUnOpt + Collectable> InsertUnOpt<O, N> {
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if unsafe { new.deref(pool) }.acked_unopt()
            || obj.search(new, guard, pool)
            || unsafe { new.deref(pool) }.acked_unopt()
        {
            return Ok(());
        }

        Err(InsertErr::RecFail)
    }
}

/// TODO(doc)
#[derive(Debug)]
pub struct DeleteUnOpt<O, N: NodeUnOpt + Collectable> {
    target_loc: PAtomic<N>,
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Send for DeleteUnOpt<O, N> {}
unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Sync for DeleteUnOpt<O, N> {}

impl<O, N: NodeUnOpt + Collectable> Default for DeleteUnOpt<O, N> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: NodeUnOpt + Collectable> Collectable for DeleteUnOpt<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for DeleteUnOpt<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + NodeUnOpt + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (
        &'o O,
        fn(PShared<'_, N>, &O, &'o Guard, &PoolHandle) -> Result<Option<PShared<'o, N>>, ()>, // OK(Some or None): next or empty, Err: need retry
    );
    type Output<'o>
    where
        O: 'o,
        N: 'o,
    = Option<PShared<'o, N>>;
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (obj, prepare): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(obj, guard, pool);
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);
        let next = ok_or!(prepare(target, obj, guard, pool), return Err(()));
        let next = some_or!(next, {
            self.target_loc
                .store(PShared::null().with_tag(EMPTY), Ordering::Relaxed);
            persist_obj(&self.target_loc, true);
            return Ok(None);
        });

        // 우선 내가 target을 가리키고
        self.target_loc.store(target, Ordering::Relaxed);
        persist_obj(&self.target_loc, false);

        // target을 ack해주고
        let target_ref = unsafe { target.deref(pool) };
        target_ref.ack_unopt();

        // point를 next로 바꿈
        let res = point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(point, true);

        if res.is_err() {
            self.target_loc.store(PShared::null(), Ordering::Relaxed);
            persist_obj(&self.target_loc, true);
            return Err(());
        }

        // 빼려는 node에 내 이름 새겨넣음
        // CAS인 이유: delete 복구 중인 스레드와 경합이 일어날 수 있음
        let result = target_ref
            .owner_unopt()
            .compare_exchange(
                no_owner(),
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| Some(target))
            .map_err(|_| ());

        persist_obj(target_ref.owner_unopt(), true);
        result
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, false);
    }
}

impl<O, N> DeleteUnOpt<O, N>
where
    O: Traversable<N>,
    N: NodeUnOpt + Collectable,
{
    fn result<'g>(
        &self,
        obj: &O,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);

        let target_ref = some_or!(unsafe { target.as_ref(pool) }, {
            return if target.tag() & EMPTY != 0 {
                Ok(None)
            } else {
                Err(())
            };
        });

        // target이 내가 pop한 게 맞는지 확인
        let owner = target_ref.owner_unopt().load(Ordering::SeqCst);
        if owner == self.id(pool) {
            return Ok(Some(target));
        }

        // target이 obj에 남아있거나 owner가 지정되지 않았으면 실패
        if obj.search(target, guard, pool) {
            return Err(());
        }

        let owner = target_ref.owner_unopt().load(Ordering::SeqCst);

        // target이 내가 pop한 게 맞는지 확인
        if owner == self.id(pool) {
            return Ok(Some(target));
        }

        // owner가 지정되었으면 실패
        if owner != no_owner() {
            return Err(());
        }

        // 누군가가 target을 obj에서 빼고 owner 기록 전에 crash가 남. 그러므로 owner를 마저 기록해줌
        // CAS인 이유: 서로 누가 진짜 owner인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
        let result = target_ref
            .owner_unopt()
            .compare_exchange(
                no_owner(),
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| Some(target))
            .map_err(|_| ());

        persist_obj(target_ref.owner_unopt(), true);
        result
    }

    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴 때마다 주소 바뀌니 상대주소로 식별해야 함
        unsafe { self.as_pptr(pool).into_offset() }
    }
}
