//! Compositional PObj Implementations

// mod pipe;
mod pipe_queue;
mod queue;

// pub use pipe::GetOurPipeNOps;
pub use pipe_queue::{MementoPipeQueueEnqDeqPair, MementoPipeQueueEnqDeqProb, TestPipeQueue};
pub use queue::{MementoQueueEnqDeqPair, MementoQueueEnqDeqProb, TestMementoQueue};
