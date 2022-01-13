//! SMO Base

use crate::pepoch::PShared;

/// TODO(doc)
#[derive(Debug)]
pub enum InsertErr<'g, T> {
    /// Insert를 위한 atomic operation 전에 기각됨
    NonNull,

    /// CAS에 실패 (Strong fail)
    CASFail(PShared<'g, T>),

    /// Recovery run 때 fail임을 판단 (Weak fail)
    RecFail,
}

/// No owner를 표시하기 위함
#[inline]
pub fn no_owner<'g, T>() -> PShared<'g, T> {
    PShared::null()
}
