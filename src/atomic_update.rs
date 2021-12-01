//! Atomic update memento collections

use std::{marker::PhantomData, sync::atomic::Ordering};

use crossbeam_epoch::Guard;

use crate::{
    pepoch::{PAtomic, POwned, PShared},
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
pub trait Inserted {
    /// TODO: doc
    fn inserted(&mut self) -> &mut bool;
}

/// TODO: doc
#[derive(Debug)]
pub enum InsertCASError {
    /// TODO: doc
    NoNew,

    /// TODO: doc
    Fail,
}

/// TODO: doc
#[derive(Debug)]
pub struct InsertCAS<O, T: Inserted, F> {
    new: PAtomic<T>,
    _marker: PhantomData<*const (O, F)>,
}

impl<O, T: Inserted, F> Default for InsertCAS<O, T, F> {
    fn default() -> Self {
        todo!()
    }
}

impl<O, T: Inserted, F> Collectable for InsertCAS<O, T, F> {
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<O, T, F> Memento for InsertCAS<O, T, F>
where
    O: 'static + Traversable<T>,
    T: 'static + Inserted,
    F: Fn(&mut T, PShared<'_, T>),
{
    type Object<'o> = &'o O;
    type Input = (Option<T>, &'static PAtomic<T>, F);
    type Output<'o>
    where
        O: 'o,
        T: 'o,
        F: 'o,
    = ();
    type Error = InsertCASError;

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (new, target, before_cas): Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.insert(new, target, obj, before_cas, guard, pool)
    }

    fn reset(&mut self, _nested: bool, _guard: &mut Guard, _pool: &'static PoolHandle) {
        todo!();
    }

    fn set_recovery(&mut self, _pool: &'static PoolHandle) {
        todo!();
    }
}

impl<O, T, F> InsertCAS<O, T, F>
where
    O: Traversable<T>,
    T: Inserted,
    F: Fn(&mut T, PShared<'_, T>),
{
    const DEFAULT: usize = 0;
    const RECOVERY: usize = 1;

    fn insert(
        &self,
        new: Option<T>,
        target: &PAtomic<T>,
        obj: &O,
        before_cas: F,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertCASError> {
        let mut n = self.new.load(Ordering::SeqCst, guard);

        if n.is_null() {
            if new.is_none() {
                return Err(InsertCASError::NoNew);
            }

            let own = POwned::new(new.unwrap(), pool).into_shared(guard);
            self.new.store(own, Ordering::SeqCst);
            persist_obj(&self.new, true);
            n = own;
        } else if n.tag() & Self::RECOVERY == Self::RECOVERY {
            // 복구 로직 실행
            self.new.store(n.with_tag(Self::DEFAULT), Ordering::SeqCst); // 복구 플래그 해제 (복구와 관련된 것이므로 persist 필요 없음)

            if obj.search(n, guard, pool) || unsafe { *n.deref_mut(pool).inserted() } {
                // (2) obj 안에 n이 있으면 삽입된 것이다 (Direct tracking)
                // (3) inserted flag가 set 되었다면 삽입된 것이다
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
                let inserted = mine_ref.inserted();
                *inserted = true;
                persist_obj(inserted, true);
            })
            .map_err(|_| InsertCASError::Fail)
    }
}
