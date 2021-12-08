//! Structure Modification Operations

pub mod atomic_update;
pub mod atomic_update_common;
pub mod atomic_update_unopt;

pub use atomic_update::*;
pub use atomic_update_common::*;
pub use atomic_update_unopt::*;
