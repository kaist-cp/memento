mod crndm_pipe;
mod crndm_queue;

use corundum::default::BuddyAlloc;
type P = BuddyAlloc;

pub use crndm_pipe::CrndmPipe;
