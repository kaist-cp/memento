//! Compositional PObj Implementations

// mod pipe;
// mod pipe_queue;
mod queue;
mod queue_opt;

// pub use pipe::GetOurPipeNOps;
// pub use pipe_queue::{MementoPipeQueueEnqDeqPair, MementoPipeQueueEnqDeqProb, TestPipeQueue};
pub use queue::{MementoQueueEnqDeqPair, MementoQueueEnqDeqProb, TestMementoQueue};
pub use queue_opt::{MementoQueueOptEnqDeqPair, MementoQueueOptEnqDeqProb, TestMementoQueueOpt};
