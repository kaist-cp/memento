//! Structure Modification Operations

pub mod common;
pub mod smo;
pub mod smo_unopt;

pub use common::*;
use crossbeam_epoch::Guard;
pub use smo::*;
pub use smo_unopt::*;

use crate::{pepoch::PShared, pmem::PoolHandle};

/// TODO: doc
pub trait Traversable<N> {
    /// TODO: doc
    fn search(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) -> bool;
}
