//! Atomic Update Common

use std::sync::atomic::AtomicUsize;

use crossbeam_epoch::Guard;

use crate::{
    pepoch::{atomic::Pointer, PShared},
    plocation::PoolHandle,
};

/// TODO: doc
pub trait Traversable<T> {
    /// TODO: doc
    fn search(&self, target: PShared<'_, T>, guard: &Guard, pool: &PoolHandle) -> bool;
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
