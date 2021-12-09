//! Atomic Update Common

// TODO: Alloc도 memento가 될 수도 있음

use std::{marker::PhantomData, sync::atomic::AtomicUsize};

use crossbeam_epoch::Guard;

use crate::{
    pepoch::PShared,
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    Memento,
};

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
pub trait NodeUnOpt: Sized {
    /// TODO: doc
    fn ack_unopt(&self);

    /// TODO: doc
    fn acked_unopt(&self) -> bool;

    /// TODO: doc
    fn owner_unopt(&self) -> &AtomicUsize;
}

/// TODO: doc
pub trait DeallocNode<T, N: Node> {
    /// TODO: doc
    fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle);
}

/// TODO(doc)
pub trait Invalid {
    /// TODO(doc)
    fn invalidate(&mut self);

    /// TODO(doc)
    fn is_invalid(&self) -> bool;
}

/// TODO(doc)
#[derive(Debug)]
pub struct Checkpoint<T: Invalid + Default + Clone + Collectable> {
    saved: T,
    _marker: PhantomData<*const T>,
}

unsafe impl<T: Invalid + Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Invalid + Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Invalid + Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        let mut t = T::default();
        t.invalidate();

        Self {
            saved: t,
            _marker: Default::default(),
        }
    }
}

impl<T: Invalid + Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<T> Memento for Checkpoint<T>
where
    T: 'static + Invalid + Default + Clone + Collectable,
{
    type Object<'o> = ();
    type Input<'o> = (T, fn(T));
    type Output<'o> = T;
    type Error<'o> = !;

    // #[inline]
    fn run<'o>(
        &'o mut self,
        (): Self::Object<'o>,
        (chk, if_exists): Self::Input<'o>,
        rec: bool,
        _: &'o Guard,
        _: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            if let Some(saved) = self.result() {
                if_exists(chk);
                return Ok(saved);
            }
        }

        // Normal run
        self.saved = chk.clone();
        persist_obj(&self.saved, true);
        Ok(chk)
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.saved.invalidate();
        persist_obj(&self.saved, true);
    }
}

impl<T: Invalid + Default + Clone + Collectable> Checkpoint<T> {
    #[inline]
    fn result<'g>(&self) -> Option<T> {
        if self.saved.is_invalid() {
            None
        } else {
            Some(self.saved.clone())
        }
    }
}

impl<T: Invalid + Default + Clone + Collectable> Drop for Checkpoint<T> {
    fn drop(&mut self) {
        assert!(self.saved.is_invalid(), "Checkpoint must be reset before dropped.")
    }
}
