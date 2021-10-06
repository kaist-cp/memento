//! benchmarking을 위한 구현들

mod compositional_pobj;
mod corundum;
mod dss;
mod friedman;

pub mod abstract_queue;
pub use compositional_pobj::GetOurQueueNOps;
pub use friedman::{GetDurableQueueNOps, GetLogQueueNOps};
