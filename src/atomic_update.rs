//! Atomic update memento collections

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PDestroyable, PShared},
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
};

/// TODO: doc
pub trait Traversable<T> {
    /// TODO: doc
    fn search(&self, target: PShared<'_, T>, guard: &Guard, pool: &PoolHandle) -> bool;
}

/// TODO: doc
pub trait Node: Sized {
    /// TODO: doc
    fn ack(&self);

    /// TODO: doc
    fn acked(&self) -> bool;

    /// TODO: doc
    fn owner(&self) -> &AtomicUsize;
}

/// TODO: doc
#[derive(Debug)]
pub enum InsertErr<'g, T> {
    /// TODO: doc
    AbortedBeforeCAS,

    /// TODO: doc
    CASFailure(PShared<'g, T>),
}

/// TODO: doc
#[derive(Debug)]
pub struct Insert<O, T: Node + Collectable> {
    _marker: PhantomData<*const (O, T)>,
}

unsafe impl<O, T: Node + Collectable + Send + Sync> Send for Insert<O, T> {}
unsafe impl<O, T: Node + Collectable + Send + Sync> Sync for Insert<O, T> {}

impl<O, T: Node + Collectable> Default for Insert<O, T> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, T: Node + Collectable> Collectable for Insert<O, T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, T> Memento for Insert<O, T>
where
    O: 'static + Traversable<T>,
    T: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        PShared<'o, T>,
        &'o PAtomic<T>,
        fn(&mut T, PShared<'_, T>) -> bool, // cas 전에 할 일 (bool 리턴값은 계속 진행할지 여부)
    );
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = ();
    type Error<'o> = InsertErr<'o, T>;

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (mut new, point, before_cas): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            // TODO: result로 갈 것 같음
            if !new.is_null()
                && (obj.search(new, guard, pool) || unsafe { new.deref(pool) }.acked())
            {
                return Ok(());
            }
        }

        // Normal run
        let new_ref = unsafe { new.deref_mut(pool) };
        let old = point.load(Ordering::SeqCst, guard);

        if !before_cas(new_ref, old) {
            return Err(InsertErr::AbortedBeforeCAS);
        }

        let ret = point
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|e| InsertErr::CASFailure(e.current));

        persist_obj(point, true); // TODO: stack에서는 성공한 놈만 해도 될지도?
        ret
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, T> Insert<O, T>
where
    O: Traversable<T>,
    T: Node + Collectable,
{
}

/// TODO: doc
#[derive(Debug)]
pub struct Delete<O, T: Node + Collectable> {
    _marker: PhantomData<*const (O, T)>,
}

unsafe impl<O, T: Node + Collectable + Send + Sync> Send for Delete<O, T> {}
unsafe impl<O, T: Node + Collectable + Send + Sync> Sync for Delete<O, T> {}

impl<O, T: Node + Collectable> Default for Delete<O, T> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, T: Node + Collectable> Collectable for Delete<O, T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, T> Memento for Delete<O, T>
where
    O: 'static + Traversable<T>,
    T: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<T>,
        &'o PAtomic<T>,
        fn(PShared<'_, T>, &O, &'o Guard, &PoolHandle) -> Result<Option<PShared<'o, T>>, ()>, // OK(Some or None): next or empty, Err: need retry
    );
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = Option<PShared<'o, T>>;
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            let target = target_loc.load(Ordering::Relaxed, guard);

            if target.tag() & Self::EMPTY == Self::EMPTY {
                // post-crash execution (empty)
                return Ok(None);
            }

            if !target.is_null() {
                if target.tag() & Self::COMPLETE == Self::COMPLETE {
                    // TODO: COMPLETE 태그는 빼도 좋은 건지 생각하고 이유 적기
                    // post-crash execution (trying)
                    return Ok(Some(target));
                }

                let target_ref = unsafe { target.deref(pool) };
                let owner = target_ref.owner().load(Ordering::SeqCst);

                // target이 내가 pop한 게 맞는지 확인
                if owner == self.id(pool) {
                    target_loc.store(target.with_tag(Self::COMPLETE), Ordering::Relaxed);
                    return Ok(Some(target));
                };

                // target이 obj에서 빠지긴 했는지 확인
                if !obj.search(target, guard, pool) {
                    // 누군가가 target을 obj에서 빼고 owner 기록 전에 crash가 남. 그러므로 owner를 마저 기록해줌
                    // CAS인 이유: 서로 누가 진짜 owner인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
                    if owner == Self::no_owner()
                        && target_ref
                            .owner()
                            .compare_exchange(
                                Self::no_owner(),
                                self.id(pool),
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        persist_obj(target_ref.owner(), true);
                        target_loc.store(target.with_tag(Self::COMPLETE), Ordering::Relaxed);
                        return Ok(Some(target));
                    }
                }
            }
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);

        let next = match get_next(target, obj, guard, pool) {
            Ok(Some(n)) => n,
            Ok(None) => {
                target_loc.store(PShared::null().with_tag(Self::EMPTY), Ordering::Relaxed);
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
        target_ref.ack();

        // point를 next로 바꿈
        let res = point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        persist_obj(point, true);

        if res.is_err() {
            return Err(());
        }

        // 빼려는 node에 내 이름 새겨넣음
        // CAS인 이유: delete 복구 중인 스레드와 경합이 일어날 수 있음
        target_ref
            .owner()
            .compare_exchange(
                Self::no_owner(),
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(target_ref.owner(), true);
                target_loc.store(target.with_tag(Self::COMPLETE), Ordering::Relaxed);
                Some(target)
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, T> Delete<O, T>
where
    O: Traversable<T>,
    T: Node + Collectable,
{
    /// Direct tracking 검사를 하게 만들도록 하는 복구중 태그
    const COMPLETE: usize = 1;

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, T>, guard: &Guard, pool: &PoolHandle) {
        if target.is_null() || target.tag() == Self::EMPTY {
            return;
        }

        // owner가 내가 아닐 수 있음
        // 따라서 owner를 확인 후 내가 delete한게 맞는다면 free
        unsafe {
            if target.deref(pool).owner().load(Ordering::SeqCst) == self.id(pool) {
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

/// TODO: doc
// TODO: 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
#[derive(Debug)]
pub struct DeleteOpt<O, T: Node + Collectable> {
    _marker: PhantomData<*const (O, T)>,
}

unsafe impl<O, T: Node + Collectable + Send + Sync> Send for DeleteOpt<O, T> {}
unsafe impl<O, T: Node + Collectable + Send + Sync> Sync for DeleteOpt<O, T> {}

impl<O, T: Node + Collectable> Default for DeleteOpt<O, T> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, T: Node + Collectable> Collectable for DeleteOpt<O, T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, T> Memento for DeleteOpt<O, T>
where
    O: 'static + Traversable<T>,
    T: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<T>,
        &'o PAtomic<T>,
        fn(PShared<'_, T>, &O, &'o Guard, &PoolHandle) -> Result<Option<PShared<'o, T>>, ()>, // OK(Some or None): next or empty, Err: need retry
    );
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = Option<PShared<'o, T>>;
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            let target = target_loc.load(Ordering::Relaxed, guard);

            if target.tag() & Self::EMPTY == Self::EMPTY {
                // post-crash execution (empty)
                return Ok(None);
            }

            if !target.is_null() {
                if target.tag() & Self::COMPLETE == Self::COMPLETE {
                    // TODO: COMPLETE 태그는 빼도 좋은 건지 생각하고 이유 적기
                    // post-crash execution (trying)
                    return Ok(Some(target));
                }

                let target_ref = unsafe { target.deref(pool) };
                let owner = target_ref.owner().load(Ordering::SeqCst);

                // target이 내가 pop한 게 맞는지 확인
                if owner == self.id(pool) {
                    target_loc.store(target.with_tag(Self::COMPLETE), Ordering::Relaxed);
                    return Ok(Some(target));
                };
            }
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);

        let next = match get_next(target, obj, guard, pool) {
            Ok(Some(n)) => n,
            Ok(None) => {
                target_loc.store(PShared::null().with_tag(Self::EMPTY), Ordering::Relaxed);
                persist_obj(&target_loc, true);
                return Ok(None);
            }
            Err(()) => return Err(()),
        };

        // 우선 내가 target을 가리키고
        target_loc.store(target, Ordering::Relaxed);
        persist_obj(target_loc, false);

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { target.deref(pool) };
        let owner = target_ref.owner();
        owner
            .compare_exchange(
                Self::no_owner(),
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(owner, true);
                let _ =
                    point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
                guard.defer_persist(point);

                target_loc.store(target.with_tag(Self::COMPLETE), Ordering::Relaxed);
                Some(target)
            })
            .map_err(|_| {
                let cur = point.load(Ordering::SeqCst, guard);
                if cur == target { // same context
                    persist_obj(owner, true); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist
                    let _ = point.compare_exchange(
                        target,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }
            })
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, T> DeleteOpt<O, T>
where
    O: Traversable<T>,
    T: Node + Collectable,
{
    /// Direct tracking 검사를 하게 만들도록 하는 복구중 태그
    const COMPLETE: usize = 1;

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, T>, guard: &Guard, pool: &PoolHandle) {
        if target.is_null() || target.tag() == Self::EMPTY {
            return;
        }

        // owner가 내가 아닐 수 있음
        // 따라서 owner를 확인 후 내가 delete한게 맞는다면 free
        unsafe {
            if target.deref(pool).owner().load(Ordering::SeqCst) == self.id(pool) {
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
