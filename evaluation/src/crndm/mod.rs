//! Corundum Implementations (TODO: paper link)

mod pipe;
mod queue;

use corundum::default::BuddyAlloc;
type P = BuddyAlloc;

pub use pipe::CrndmPipe;
pub use queue::CrndmQueue;
