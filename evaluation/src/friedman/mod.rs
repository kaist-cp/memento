//! Friedman's persistent lock free queue implementations (TODO: paper link)

mod durable_queue;
mod log_queue;

pub use durable_queue::GetDurableQueueNOps;
pub use log_queue::GetLogQueueNOps;
