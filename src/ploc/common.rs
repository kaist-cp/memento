//! Atomic Update Common

use std::{marker::PhantomData, sync::atomic::AtomicUsize};

use crossbeam_epoch::Guard;

use crate::{
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
    fn ack(&self);

    /// TODO(doc)
    fn acked(&self) -> bool;

    /// TODO(doc)
    fn owner(&self) -> &AtomicUsize;
}

/// TODO(doc)
pub trait NodeUnOpt: Sized {
    /// TODO(doc)
    fn ack_unopt(&self);

    /// TODO(doc)
    fn acked_unopt(&self) -> bool;

    /// TODO(doc)
    fn owner_unopt(&self) -> &AtomicUsize;
}

/// TODO(doc)
pub trait Checkpointable {
    /// TODO(doc)
    fn invalidate(&mut self);

    /// TODO(doc)
    fn is_invalid(&self) -> bool;
}

/// TODO(doc)
#[derive(Debug)]
pub struct Checkpoint<T: Checkpointable + Default + Clone + Collectable> {
    saved: T,
    _marker: PhantomData<*const T>,
}

unsafe impl<T: Checkpointable + Default + Clone + Collectable + Send + Sync> Send
    for Checkpoint<T>
{
}
unsafe impl<T: Checkpointable + Default + Clone + Collectable + Send + Sync> Sync
    for Checkpoint<T>
{
}

impl<T: Checkpointable + Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        let mut t = T::default();
        t.invalidate();

        Self {
            saved: t,
            _marker: Default::default(),
        }
    }
}

impl<T: Checkpointable + Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        T::filter(&mut s.saved, gc, pool);
    }
}

impl<T> Memento for Checkpoint<T>
where
    T: 'static + Checkpointable + Default + Clone + Collectable,
{
    type Object<'o> = ();
    type Input<'o> = (T, fn(T));
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        (): Self::Object<'o>,
        (chk, if_exists): Self::Input<'o>,
        rec: bool,
        _: &'o Guard,
        _: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            if let Some(saved) = self.peek() {
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
        persist_obj(&self.saved, false);
    }
}

impl<T: Checkpointable + Default + Clone + Collectable> Checkpoint<T> {
    /// TODO(doc)
    #[inline]
    pub fn peek(&self) -> Option<T> {
        if self.saved.is_invalid() {
            None
        } else {
            Some(self.saved.clone())
        }
    }
}

/// TODO(doc)
#[derive(Debug, Clone, Copy)]
pub struct CheckpointableUsize(pub usize);

impl CheckpointableUsize {
    const INVALID: usize = usize::MAX - u32::MAX as usize;
}

impl Default for CheckpointableUsize {
    fn default() -> Self {
        Self(Self::INVALID)
    }
}

impl Collectable for CheckpointableUsize {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl Checkpointable for CheckpointableUsize {
    fn invalidate(&mut self) {
        self.0 = CheckpointableUsize::INVALID;
    }

    fn is_invalid(&self) -> bool {
        self.0 == CheckpointableUsize::INVALID
    }
}

/// TODO(doc)
// TODO(@jeehoon.kang): move
#[derive(Debug)]
pub struct RetryLoop<M: Memento> {
    try_mmt: M,
}

unsafe impl<M: Memento + Send + Sync> Send for RetryLoop<M> {}
unsafe impl<M: Memento + Send + Sync> Sync for RetryLoop<M> {}

impl<M: Memento> Default for RetryLoop<M> {
    fn default() -> Self {
        Self {
            try_mmt: Default::default(),
        }
    }
}

impl<M: Memento> Collectable for RetryLoop<M> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        M::filter(&mut s.try_mmt, gc, pool);
    }
}

impl<M> Memento for RetryLoop<M>
where
    M: 'static + Memento,
{
    type Object<'o> = M::Object<'o>;
    type Input<'o> = M::Input<'o>;
    type Output<'o> = M::Output<'o>;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        obj: Self::Object<'o>,
        input: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Ok(ret) = self
            .try_mmt
            .run(obj.clone(), input.clone(), rec, guard, pool)
        {
            return Ok(ret);
        }

        loop {
            if let Ok(ret) = self
                .try_mmt
                .run(obj.clone(), input.clone(), false, guard, pool)
            {
                return Ok(ret);
            }
        }
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_mmt.reset(guard, pool);
    }
}
