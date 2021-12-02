//! Atomic update memento collections

use std::{marker::PhantomData, sync::atomic::Ordering};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// TODO: doc
pub trait Traversable<T> {
    /// TODO: doc
    fn search(&self, target: PShared<'_, T>, guard: &Guard, pool: &PoolHandle) -> bool;
}

/// TODO: doc
pub trait Acked {
    /// TODO: doc
    fn acked(&self) -> bool;
}

/// TODO: doc
#[derive(Debug)]
pub struct Insert<O, T: Acked + Collectable> {
    new: PAtomic<T>,
    _marker: PhantomData<*const O>,
}

unsafe impl<O, T: Acked + Collectable + Send + Sync> Send for Insert<O, T> {}
unsafe impl<O, T: Acked + Collectable + Send + Sync> Sync for Insert<O, T> {}

impl<O, T: Acked + Collectable> Default for Insert<O, T> {
    fn default() -> Self {
        Self {
            new: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, T: Acked + Collectable> Collectable for Insert<O, T> {
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

impl<O, T: Acked + Collectable> Drop for Insert<O, T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let new = self.new.load(Ordering::SeqCst, guard);
        assert!(new.is_null(), "reset 되어있지 않음.")
    }
}

impl<O, T> Memento for Insert<O, T>
where
    O: 'static + Traversable<T>,
    T: 'static + Acked + Collectable,
{
    type Object<'o> = &'o O;
    type Input<'o> = (T, &'o PAtomic<T>, fn(&mut T, PShared<'_, T>));
    type Output<'o>
    where
        O: 'o,
        T: 'o,
    = ();
    type Error = ();

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (new, target, before_cas): Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.insert(new, target, obj, before_cas, guard, pool)
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, pool: &'static PoolHandle) {
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

    fn set_recovery(&mut self, _pool: &'static PoolHandle) {
        let guard = unsafe { epoch::unprotected() };
        let new = self.new.load(Ordering::SeqCst, guard);

        if new.tag() & Self::RECOVERY != Self::RECOVERY {
            self.new
                .store(new.with_tag(Self::RECOVERY), Ordering::SeqCst);
            // 복구해야 한다는 표시이므로 persist 필요 없음
        }
    }
}

impl<O, T> Insert<O, T>
where
    O: Traversable<T>,
    T: Acked + Collectable,
{
    const DEFAULT: usize = 0;
    const RECOVERY: usize = 1;

    fn insert<F>(
        &self,
        new: T,
        target: &PAtomic<T>,
        obj: &O,
        before_cas: F,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()>
    where
        F: Fn(&mut T, PShared<'_, T>),
    {
        let mut n = self.new.load(Ordering::SeqCst, guard);

        if n.is_null() {
            let own = POwned::new(new, pool).into_shared(guard);
            self.new.store(own, Ordering::SeqCst);
            persist_obj(&self.new, true);
            n = own;
        } else if n.tag() & Self::RECOVERY == Self::RECOVERY {
            // 복구 로직 실행
            self.new.store(n.with_tag(Self::DEFAULT), Ordering::SeqCst); // 복구 플래그 해제 (복구와 관련된 것이므로 persist 필요 없음)

            if obj.search(n, guard, pool) || unsafe { n.deref(pool) }.acked() {
                // (2) obj 안에 n이 있으면 삽입된 것이다 (Direct tracking)
                // (3) acked 되었다면 삽입된 것이다
                return Ok(());
            }
        }

        let mine_ref = unsafe { n.deref_mut(pool) };
        let old = target.load(Ordering::SeqCst, guard);

        before_cas(mine_ref, old);

        target
            .compare_exchange(old, n, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                persist_obj(target, true);
            })
            .map_err(|_| ()) // TODO: 실패했을 땐 정말 persist 안 해도 됨?
    }
}
