use std::time::{Duration, Instant};

use memento::pmem::{Collectable, GarbageCollection, PoolHandle};

pub mod cas;
pub mod mcas;
pub mod pcas;

struct Node;

impl Collectable for Node {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

pub trait TestNOps {
    // Count number of executions of `op` in `duration` seconds
    fn test_nops<'f, F: Fn(usize) -> bool>(&self, op: &'f F, tid: usize, duration: f64) -> usize
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
        while start.elapsed() < dur {
            if op(tid) {
                ops += 1;
            }
        }
        ops
    }
}
