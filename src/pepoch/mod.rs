//! Persistent epoch-based garbage collector

pub mod atomic;
pub mod default;
pub mod guard;

pub use atomic::{Atomic, Owned, Shared};
pub use default::*;
pub use guard::Guard;
