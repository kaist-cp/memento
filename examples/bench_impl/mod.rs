//! benchmarking을 위한 구현들

mod compositional_pobj;
mod crndm;
mod dss;
mod friedman;

pub mod abstract_queue;
pub use compositional_pobj::{GetOurQueueNOps, GetOurPipeNOps};
pub use friedman::{GetDurableQueueNOps, GetLogQueueNOps};
pub use dss::GetDSSQueueNOps;
