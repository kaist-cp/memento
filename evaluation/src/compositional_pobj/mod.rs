//! Compositional PObj Implementations

// mod pipe;
// mod pipe_queue;
mod queue;
mod queue_general;
mod queue_lp;
mod queue_pbcomb;

// pub use pipe::GetOurPipeNOps;
// pub use pipe_queue::{MementoPipeQueueEnqDeqPair, MementoPipeQueueEnqDeqProb, TestPipeQueue};

pub use memento::ds::queue_pbcomb::NR_THREADS as MementoPBComb_NR_THREAD;
pub use queue::*;
pub use queue_general::*;
pub use queue_lp::*;
pub use queue_pbcomb::*;
