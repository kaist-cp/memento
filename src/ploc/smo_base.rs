//! SMO Base

use crate::pepoch::{atomic::Pointer, PShared};

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
