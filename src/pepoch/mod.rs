//! Persistent epoch-based garbage collector

pub mod atomic;
pub mod default;
pub mod guard;

pub use atomic::{PAtomic, POwned, PShared};
pub use default::*;
pub use guard::Guard;
