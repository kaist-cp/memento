//! Default function of pepoch module

pub use super::guard::Guard;

/// TODO: doc, impl
pub fn pin() -> Guard {
    Guard {}
}

/// TODO: doc, impl
///
/// # Safety
///
/// TODO
pub unsafe fn unprotected() -> &'static Guard {
    static UNPROTECTED: Guard = Guard {};
    &UNPROTECTED
}
