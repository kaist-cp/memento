//! Atomic Update Common

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::Guard;

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// TODO: doc
pub trait Traversable<N> {
    /// TODO: doc
    fn search(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) -> bool;
}

/// TODO: doc
// TODO: node들 싹 통합: 각자의 node 안에 Node trait 구현된 걸 쓰도록
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
    /// Insert를 위한 atomic operation 전에 기각됨
    PrepareFail,

    /// CAS에 실패 (Strong fail)
    CASFail(PShared<'g, T>),

    /// Recovery run 때 fail임을 판단 (Weak fail)
    RecFail,
}

/// Empty를 표시하기 위한 태그
pub const EMPTY: usize = 2;

/// No owner를 표시하기 위함
#[inline]
pub fn no_owner() -> usize {
    let null = PShared::<()>::null();
    null.into_usize()
}

/// Input으로 주어지는 `save_loc`은 `no_read()`로 세팅되어 있어야 함
#[derive(Debug)]
pub struct Read<N: Node + Collectable> {
    _marker: PhantomData<*const N>,
}

unsafe impl<N: Node + Collectable + Send + Sync> Send for Read<N> {}
unsafe impl<N: Node + Collectable + Send + Sync> Sync for Read<N> {}

impl<N: Node + Collectable> Default for Read<N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<N: Node + Collectable> Collectable for Read<N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<N> Memento for Read<N>
where
    N: 'static + Node + Collectable,
{
    type Object<'o> = ();
    type Input<'o> = (&'o PAtomic<N>, &'o PAtomic<N>);
    type Output<'o> = Option<PShared<'o, N>>;
    // where
    //     N: 'o,
    // = ();
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        (): Self::Object<'o>,
        (save_loc, point): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        _: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return Ok(self.result(save_loc, guard));
        }

        // Normal run
        let p = point.load(Ordering::SeqCst, guard);
        save_loc.store(p, Ordering::Relaxed);
        persist_obj(save_loc, true);
        Ok(Some(p))
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<N: Node + Collectable> Read<N> {
    #[inline]
    fn result<'g>(&self, save_loc: &PAtomic<N>, guard: &'g Guard) -> Option<PShared<'g, N>> {
        let saved = save_loc.load(Ordering::Relaxed, guard);

        if saved == Self::no_read() {
            None
        } else {
            Some(saved)
        }
    }

    /// `Read`가 읽은 적이 없다는 걸 표시하기 위한 포인터
    #[inline]
    pub fn no_read<'g, T>() -> PShared<'g, T> {
        const NO_READ: usize = usize::MAX - u32::MAX as usize;
        unsafe { PShared::<T>::from_usize(NO_READ) }
    }
}
