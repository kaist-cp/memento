//! Corundum Implementations (TODO: paper link)

mod pipe;
mod queue;

use corundum::default::Allocator;
pub type P = Allocator;

pub use pipe::CrndmPipe;
pub use queue::CrndmQueue;
