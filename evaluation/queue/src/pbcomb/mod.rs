mod pipe_mmt;
mod queue;
mod queue_mmt;

pub use pipe_mmt::*;
pub use queue::NR_THREADS as PBComb_NR_THREAD;
pub use queue::*;
pub use queue_mmt::*;

type Data = usize;
