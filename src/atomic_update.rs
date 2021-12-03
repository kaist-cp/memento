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

    /// TODO: doc
    fn next<'g>(&self, guard: &'g Guard) -> PShared<'g, Self>;
}

/// TODO: doc
#[derive(Debug)]
pub struct Insert<O, T: Node + Collectable> {
    new: PAtomic<T>,
    _marker: PhantomData<*const O>,
}

unsafe impl<O, T: Node + Collectable + Send + Sync> Send for Insert<O, T> {}
unsafe impl<O, T: Node + Collectable + Send + Sync> Sync for Insert<O, T> {}

impl<O, T: Node + Collectable> Default for Insert<O, T> {
    fn default() -> Self {
        Self {
            new: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, T: Node + Collectable> Collectable for Insert<O, T> {
    fn filter(insert: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut new = insert.new.load(Ordering::SeqCst, guard);
        if !new.is_null() {
            let new_ref = unsafe { new.deref_mut(pool) };
            T::mark(new_ref, gc);
        }
    }
}

impl<O, T: Node + Collectable> Drop for Insert<O, T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let new = self.new.load(Ordering::SeqCst, guard);
        assert!(new.is_null(), "reset 되어있지 않음.")
    }
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
        fn(&mut T, PShared<'_, T>) -> bool, // bool 리턴값은 계속 진행할지 여부
    );
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        _: Self::Object<'o>,
        (new, point, before_cas): Self::Input<'o>,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.insert(new, point, before_cas, guard, pool)
    }

    fn reset(&mut self, _: bool, guard: &Guard, pool: &'static PoolHandle) {
        let mut new = self.new.load(Ordering::SeqCst, guard);
        if !new.is_null() {
            self.new.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.new, true);

            // crash-free execution이지만 try성 CAS라서 insert 실패했을 수 있음
            // 따라서 inserted 플래그로 (1) 성공여부 확인후, (2) insert 되지 않았으면 free
            //
            // NOTE:
            //  - 현재는 insert CAS 성공 후 inserted=true로 설정해주니까, 성공했다면 inserted=true가 보장됨
            //  - 만약 최적화하며 push CAS 성공 후 inserted=true를 안하게 바꾼다면, 여기서는 inserted 대신 Token에 담겨있는 Ok or Err 정보로 성공여부 판단해야함 (혹은 Direct tracking..)
            unsafe {
                if new.deref_mut(pool).acked() {
                    guard.defer_pdestroy(new);
                }
            }
        }
    }

    fn recover<'o>(&mut self, obj: Self::Object<'o>, pool: &'static PoolHandle) {
        let guard = unsafe { epoch::unprotected() };
        let new = self.new.load(Ordering::SeqCst, guard);

        if !new.is_null() && (obj.search(new, guard, pool) || unsafe { new.deref(pool) }.acked()) {
            // (2) obj 안에 n이 있으면 삽입된 것이다 (Direct tracking)
            // (3) acked 되었다면 삽입된 것이다
            self.new
                .store(new.with_tag(Self::COMPLETE), Ordering::SeqCst);
        }
    }
}

impl<O, T> Insert<O, T>
where
    O: Traversable<T>,
    T: Node + Collectable,
{
    /// Direct tracking 검사를 하게 만들도록 하는 복구중 태그
    const COMPLETE: usize = 1;

    fn insert<F>(
        &self,
        new: PShared<'_, T>,
        point: &PAtomic<T>,
        before_cas: F,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()>
    where
        F: Fn(&mut T, PShared<'_, T>) -> bool,
    {
        let mut n = self.new.load(Ordering::SeqCst, guard);

        if n.is_null() {
            self.new.store(new, Ordering::SeqCst);
            persist_obj(&self.new, false);
            n = new;
        } else if n.tag() & Self::COMPLETE == Self::COMPLETE {
            return Ok(());
        } else if n.as_ptr() != new.as_ptr() {
            unsafe { guard.defer_pdestroy(new) };
        }

        let mine_ref = unsafe { n.deref_mut(pool) };
        let old = point.load(Ordering::SeqCst, guard);

        if !before_cas(mine_ref, old) {
            return Err(());
        }

        let ret = point
            .compare_exchange(old, n, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|_| ());

        persist_obj(point, true); // TODO: stack에서는 성공한 놈만 해도 될지도?
        ret
    }
}

/// TODO: doc
#[derive(Debug)]
pub struct Delete<O, T: Node + Collectable> {
    target: PAtomic<T>,
    _marker: PhantomData<*const O>,
}

unsafe impl<O, T: Node + Collectable + Send + Sync> Send for Delete<O, T> {}
unsafe impl<O, T: Node + Collectable + Send + Sync> Sync for Delete<O, T> {}

impl<O, T: Node + Collectable> Default for Delete<O, T> {
    fn default() -> Self {
        Self {
            target: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, T: Node + Collectable> Collectable for Delete<O, T> {
    fn filter(delete: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut target = delete.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target_ref = unsafe { target.deref_mut(pool) };
            T::mark(target_ref, gc);
        }
    }
}

impl<O, T: Node + Collectable> Drop for Delete<O, T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let target = self.target.load(Ordering::SeqCst, guard);
        assert!(target.is_null(), "reset 되어있지 않음.")
    }
}

impl<O, T> Memento for Delete<O, T>
where
    O: 'static + Traversable<T>,
    T: 'static + Node + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = &'o PAtomic<T>;
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = Option<&'o T>;
    type Error = ();

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        point: Self::Input<'o>,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.delete(obj, point, guard, pool)
    }

    fn reset(&mut self, _: bool, guard: &Guard, pool: &'static PoolHandle) {
        let target = self.target.load(Ordering::SeqCst, guard);

        if target.tag() == Self::EMPTY {
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);
            return;
        }

        if !target.is_null() {
            // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
            // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);

            // crash-free execution이지만 try이니 owner가 내가 아닐 수 있음
            // 따라서 owner를 확인 후 내가 delete한게 맞는다면 free
            unsafe {
                if target.deref(pool).owner().load(Ordering::SeqCst) == self.id(pool) {
                    guard.defer_pdestroy(target);
                }
            }
        }
    }

    fn recover<'o>(&mut self, _: Self::Object<'o>, _: &'static PoolHandle) {
        let guard = unsafe { epoch::unprotected() };
        let target = self.target.load(Ordering::SeqCst, guard);

        let tag = target.tag();
        if tag & Self::EMPTY != Self::EMPTY && tag & Self::RECOVERY != Self::RECOVERY {
            self.target
                .store(target.with_tag(Self::RECOVERY), Ordering::SeqCst);
            // 복구해야 한다는 표시이므로 persist 필요 없음
        }
    }
}

impl<O, T> Delete<O, T>
where
    O: Traversable<T>,
    T: Node + Collectable,
{
    const DEFAULT: usize = 0;

    /// Direct tracking 검사를 하게 만들도록 하는 복구중 태그
    const RECOVERY: usize = 1;

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    fn delete<'g>(
        &self,
        obj: &O,
        point: &PAtomic<T>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<&'g T>, ()> {
        let target = self.target.load(Ordering::SeqCst, guard);

        if target.tag() & Self::EMPTY == Self::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        let my_id = self.id(pool);

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };

            // target이 내가 pop한 게 맞는지 확인
            if target_ref.owner().load(Ordering::SeqCst) == my_id {
                return Ok(Some(target_ref));
            };

            if target.tag() & Self::RECOVERY == Self::RECOVERY {
                // 복구 로직 실행
                self.target
                    .store(target.with_tag(Self::DEFAULT), Ordering::SeqCst); // 복구 플래그 해제 (복구와 관련된 것이므로 persist 필요 없음)

                // target이 obj에서 빠지긴 했는지 확인
                if !obj.search(target, guard, pool) {
                    // 누군가가 target을 obj에서 빼고 owner 기록 전에 crash가 남. 그러므로 owner를 마저 기록해줌
                    // CAS인 이유: 서로 누가 진짜 owner인 줄 모르고 모두가 복구하면서 같은 target을 노리고 있을 수 있음
                    if target_ref
                        .owner()
                        .compare_exchange(
                            Self::no_owner(),
                            my_id,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        persist_obj(target_ref.owner(), true);
                        return Ok(Some(target_ref));
                    }
                }
            }
        }

        let target = point.load(Ordering::SeqCst, guard);
        if target.is_null() {
            // TODO: Make generic `deletable()`
            // empty
            self.target
                .store(PShared::null().with_tag(Self::EMPTY), Ordering::SeqCst);
            persist_obj(&self.target, true);
            return Ok(None);
        };

        let target_ref = unsafe { target.deref(pool) };

        // 우선 내가 target을 가리키고
        self.target.store(target, Ordering::SeqCst);
        persist_obj(&self.target, false);

        // target을 ack해주고
        target_ref.ack();

        // point를 next로 바꿈
        let next = target_ref.next(guard);
        if point
            .compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            .is_err()
        {
            return Err(());
        }

        persist_obj(point, true);

        // top node에 내 이름 새겨넣음
        // CAS인 이유: pop 복구 중인 스레드와 경합이 일어날 수 있음
        target_ref
            .owner()
            .compare_exchange(Self::no_owner(), my_id, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| {
                persist_obj(target_ref.owner(), true);
                Some(target_ref)
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }

    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }

    /// TODO: doc
    #[inline]
    pub fn no_owner() -> usize {
        let null = PShared::<Self>::null();
        null.into_usize()
    }
}
