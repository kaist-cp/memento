//! Atomic Update UnOpt

use std::{marker::PhantomData, sync::atomic::Ordering};

use crossbeam_epoch::Guard;

use super::{
    common::{InsertErr, NodeUnOpt, EMPTY},
    Traversable,
};
use crate::{
    pepoch::{atomic::Pointer, PAtomic, PDestroyable, PShared},
    Memento,
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
};

/// TODO: doc
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
        &'o mut self,
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

        persist_obj(point, true); // TODO: stack에서는 성공한 놈만 해도 될지도?
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

/// TODO: doc
#[derive(Debug)]
pub struct DeleteUnOpt<O, N: NodeUnOpt + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Send for DeleteUnOpt<O, N> {}
unsafe impl<O, N: NodeUnOpt + Collectable + Send + Sync> Sync for DeleteUnOpt<O, N> {}

impl<O, N: NodeUnOpt + Collectable> Default for DeleteUnOpt<O, N> {
    fn default() -> Self {
        Self {
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
        &'o PAtomic<N>,
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
        &'o mut self,
        point: Self::Object<'o>,
        (target_loc, obj, get_next): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(obj, target_loc, guard, pool);
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);

        let next = match get_next(target, obj, guard, pool) {
            Ok(Some(n)) => n,
            Ok(None) => {
                target_loc.store(PShared::null().with_tag(EMPTY), Ordering::Relaxed);
                persist_obj(&target_loc, true);
                return Ok(None);
            }
            Err(()) => return Err(()),
        };

        // 우선 내가 target을 가리키고
        target_loc.store(target, Ordering::Relaxed);
        persist_obj(target_loc, false);

        // target을 ack해주고
        let target_ref = unsafe { target.deref(pool) };
        target_ref.ack_unopt();

        // point를 next로 바꿈
        let res = point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(point, true);

        if res.is_err() {
            return Err(());
        }

        // 빼려는 node에 내 이름 새겨넣음
        // CAS인 이유: delete 복구 중인 스레드와 경합이 일어날 수 있음
        target_ref
            .owner_unopt()
            .compare_exchange(
                Self::no_owner(), // TODO: no_owner 통합
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(target_ref.owner_unopt(), true);
                Some(target)
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, N> DeleteUnOpt<O, N>
where
    O: Traversable<N>,
    N: NodeUnOpt + Collectable,
{
    fn result<'g>(
        &self,
        obj: &O,
        target_loc: &PAtomic<N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = target_loc.load(Ordering::Relaxed, guard);

        if target.tag() & EMPTY == EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            let target_ref = unsafe { target.deref(pool) };
            let owner = target_ref.owner_unopt().load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if owner == self.id(pool) {
                return Ok(Some(target));
            };

            // target이 obj에서 빠지긴 했는지 확인
            if !obj.search(target, guard, pool) {
                // 누군가가 target을 obj에서 빼고 owner 기록 전에 crash가 남. 그러므로 owner를 마저 기록해줌
                // CAS인 이유: 서로 누가 진짜 owner인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
                if owner == Self::no_owner()
                    && target_ref
                        .owner_unopt()
                        .compare_exchange(
                            Self::no_owner(),
                            self.id(pool),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    persist_obj(target_ref.owner_unopt(), true);
                    return Ok(Some(target));
                }
            }
        }

        Err(())
    }

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) {
        if target.is_null() || target.tag() == EMPTY {
            return;
        }

        // owner가 내가 아닐 수 있음
        // 따라서 owner를 확인 후 내가 delete한게 맞는다면 free
        unsafe {
            if target.deref(pool).owner_unopt().load(Ordering::SeqCst) == self.id(pool) {
                guard.defer_pdestroy(target);
            }
        }
    }

    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴 때마다 주소 바뀌니 상대주소로 식별해야 함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    /// TODO: doc
    #[inline]
    pub fn no_owner() -> usize {
        let null = PShared::<Self>::null();
        null.into_usize()
    }
}
