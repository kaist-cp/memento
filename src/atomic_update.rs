//! Atomic update memento collections

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::Guard;

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

    /// TODO: doc
    RecFail,
}

/// Persist 아직 안 됐는지 표시하기 위한 태그
const NOT_PERSISTED: usize = 1;

/// Empty를 표시하기 위한 태그
const EMPTY: usize = 2;

/// TODO: doc
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
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for Insert<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        PShared<'o, N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (mut new, point, before_cas): Self::Input<'o>,
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

impl<O: Traversable<N>, N: Node + Collectable> Insert<O, N> {
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if obj.search(new, guard, pool) || unsafe { new.deref(pool) }.acked() {
            return Ok(());
        }

        Err(InsertErr::RecFail)
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct Delete<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for Delete<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for Delete<O, N> {}

impl<O, N: Node + Collectable> Default for Delete<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for Delete<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for Delete<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
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
                Some(target)
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, N> Delete<O, N>
where
    O: Traversable<N>,
    N: Node + Collectable,
{
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    fn result<'g>(
        &self,
        obj: &O,
        target_loc: &PAtomic<N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = target_loc.load(Ordering::Relaxed, guard);

        if target.tag() & Self::EMPTY == Self::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            let target_ref = unsafe { target.deref(pool) };
            let owner = target_ref.owner().load(Ordering::SeqCst);

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
                    return Ok(Some(target));
                }
            }
        }

        Err(())
    }

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) {
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
pub struct DeleteOpt<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for DeleteOpt<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for DeleteOpt<O, N> {}

impl<O, N: Node + Collectable> Default for DeleteOpt<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for DeleteOpt<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for DeleteOpt<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(target_loc, guard, pool);
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

                Some(target)
            })
            .map_err(|_| {
                let cur = point.load(Ordering::SeqCst, guard);
                if cur == target {
                    // same context
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

impl<O, N> DeleteOpt<O, N>
where
    O: Traversable<N>,
    N: Node + Collectable,
{
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    fn result<'g>(
        &self,
        target_loc: &PAtomic<N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = target_loc.load(Ordering::Relaxed, guard);

        if target.tag() & Self::EMPTY == Self::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            let target_ref = unsafe { target.deref(pool) };
            let owner = target_ref.owner().load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if owner == self.id(pool) {
                return Ok(Some(target));
            };
        }

        Err(())
    }

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) {
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
#[derive(Debug)]
pub struct InsertLinkPersist<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for InsertLinkPersist<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for InsertLinkPersist<O, N> {}

impl<O, N: Node + Collectable> Default for InsertLinkPersist<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for InsertLinkPersist<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for InsertLinkPersist<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        PShared<'o, N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (mut new, point, before_cas): Self::Input<'o>,
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

        if !before_cas(new_ref, old) {
            return Err(InsertErr::AbortedBeforeCAS);
        }

        let ret = point
            .compare_exchange(
                old,
                new.with_tag(NOT_PERSISTED),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map(|_| {
                persist_obj(point, true);
                let _ = point.compare_exchange( // link-persist
                    new.with_tag(NOT_PERSISTED),
                    new,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
            .map_err(|e| {
                let tag = e.current.tag();
                if tag & NOT_PERSISTED == NOT_PERSISTED { // link-persist
                    persist_obj(point, true);
                    let new = e.current.with_tag(tag & !NOT_PERSISTED);
                    let _ = point.compare_exchange(
                        e.current,
                        new,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }

                InsertErr::CASFailure(e.current)
            });

        ret
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O: Traversable<N>, N: Node + Collectable> InsertLinkPersist<O, N> {
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if obj.search(new, guard, pool) || unsafe { new.deref(pool) }.acked() {
            return Ok(());
        }

        Err(InsertErr::RecFail)
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct DeleteLinkPesist<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for DeleteLinkPesist<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for DeleteLinkPesist<O, N> {}

impl<O, N: Node + Collectable> Default for DeleteLinkPesist<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for DeleteLinkPesist<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for DeleteLinkPesist<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
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
        target_ref.ack();

        // point를 next로 바꿈
        let res = point
            .compare_exchange(
                target,
                next.with_tag(NOT_PERSISTED),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map(|_| {
                persist_obj(point, true);
                let _ = point.compare_exchange( // link-persist
                    next.with_tag(NOT_PERSISTED),
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
            .map_err(|e| {
                let tag = e.current.tag();
                if tag & NOT_PERSISTED == NOT_PERSISTED { // link-persist
                    persist_obj(point, true);
                    let new = e.current.with_tag(tag & !NOT_PERSISTED);
                    let _ = point.compare_exchange(
                        e.current,
                        new,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }
            });

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
                Some(target)
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, N> DeleteLinkPesist<O, N>
where
    O: Traversable<N>,
    N: Node + Collectable,
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
            let owner = target_ref.owner().load(Ordering::SeqCst);

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
pub struct DeleteOptLinkPersist<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for DeleteOptLinkPersist<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for DeleteOptLinkPersist<O, N> {}

impl<O, N: Node + Collectable> Default for DeleteOptLinkPersist<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for DeleteOptLinkPersist<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for DeleteOptLinkPersist<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (
        &'o PAtomic<N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (target_loc, point, get_next): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(target_loc, guard, pool);
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

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { target.deref(pool) };
        let owner = target_ref.owner();
        let id = self.id(pool);
        owner
            .compare_exchange(
                Self::no_owner(),
                id | NOT_PERSISTED, // TODO: 트레일링 제로에 맞게 안전하게 태깅하도록 래핑
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(owner, true);
                let _ = owner.compare_exchange(id | NOT_PERSISTED, id & !NOT_PERSISTED, Ordering::SeqCst, Ordering::SeqCst); // link-persist

                let _ =
                    point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
                guard.defer_persist(point);

                Some(target)
            })
            .map_err(|real_owner| {
                let p = point.load(Ordering::SeqCst, guard);
                if p == target && real_owner & NOT_PERSISTED == NOT_PERSISTED {
                    // same context
                    persist_obj(owner, true); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist
                    let _ = owner.compare_exchange(real_owner, real_owner & !NOT_PERSISTED, Ordering::SeqCst, Ordering::SeqCst); // link-persist

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

impl<O, N> DeleteOptLinkPersist<O, N>
where
    O: Traversable<N>,
    N: Node + Collectable,
{
    fn result<'g>(
        &self,
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
            let owner = target_ref.owner().load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if owner == self.id(pool) {
                return Ok(Some(target));
            };
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
