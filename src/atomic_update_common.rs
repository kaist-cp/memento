//! Atomic Update Common

use std::sync::atomic::AtomicUsize;

use crossbeam_epoch::Guard;

use crate::{pepoch::PShared, plocation::PoolHandle};

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
    PrepareFail,

    /// TODO: doc
    CASFail(PShared<'g, T>),

    /// TODO: doc
    RecFail,
}

/// Empty를 표시하기 위한 태그
pub const EMPTY: usize = 2;
