use super::{queue::CrndmQueue, P};
use crate::common::{TestKind, TestNOps, PIPE_INIT_SIZE};
use corundum::default::*;

/// Corundum Pipe
#[derive(Root, Debug)]
pub struct CrndmPipe {
    q1: CrndmQueue,
    q2: CrndmQueue,
}

impl TestNOps for CrndmPipe {}

impl CrndmPipe {
    pub fn get_nops(&self, kind: TestKind, nr_thread: usize, duration: f64) -> usize {
        // initialize
        for i in 0..PIPE_INIT_SIZE {
            self.q1.enqueue(i);
        }

        // run
        todo!()
        // let q1 = &self.q1;
        // let q2 = &self.q2;
        // match kind {
        //     TestKind::Pipe => self.test_nops(
        //         &|_tid| {
        //             P::transaction(|_| {
        //                 let v = loop {
        //                     if let Some(v) = q1.dequeue() {
        //                         break v;
        //                     }
        //                 };
        //                 q2.enqueue(v);
        //             })
        //             .unwrap();
        //         },
        //         nr_thread,
        //         duration,
        //     ),
        //     _ => unreachable!("Pipe를 위한 테스트만 해야함"),
        // }
    }
}
