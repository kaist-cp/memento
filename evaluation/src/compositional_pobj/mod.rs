//! Compositional PObj Implementations

// mod pipe;
// mod pipe_queue;
mod queue;
mod queue_general;

// pub use pipe::GetOurPipeNOps;
// pub use pipe_queue::{MementoPipeQueueEnqDeqPair, MementoPipeQueueEnqDeqProb, TestPipeQueue};

pub use queue::*;
pub use queue_general::*;
