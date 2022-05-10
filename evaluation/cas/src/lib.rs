use std::time::{Duration, Instant};

pub mod cas;

struct Node;

pub trait TestNOps {
    // Count number of executions of `op` in `duration` seconds
    fn test_nops<'f, F: Fn(usize)>(&self, op: &'f F, tid: usize, duration: f64) -> usize
    where
        &'f F: Send,
    {
        let mut ops = 0;
        let start = Instant::now();
        let dur = Duration::from_secs_f64(duration);
        while start.elapsed() < dur {
            op(tid);
            ops += 1;
        }
        ops
    }
}
