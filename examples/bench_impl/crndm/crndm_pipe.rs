use super::{crndm_queue::CrndmQueue, P};
use crate::{TestKind, TestNOps, PIPE_INIT_SIZE};
use corundum::default::*;

#[derive(Root)]
pub struct CrndmPipe {
    q1: CrndmQueue,
    q2: CrndmQueue,
}

impl TestNOps for CrndmPipe {}

impl CrndmPipe {
    fn init(&self) {
        println!("initialize..");
        for i in 0..PIPE_INIT_SIZE {
            self.q1.enqueue(i);
        }
    }

    pub fn get_nops(&self, kind: TestKind, nr_thread: usize, duration: f64) -> usize {
        self.init();
        let q1 = &self.q1;
        let q2 = &self.q2;

        println!("run!");
        match kind {
            TestKind::Pipe => self.test_nops(
                &|tid| {
                    P::transaction(|_| {
                        let v = loop {
                            if let Some(v) = q1.dequeue() {
                                break v
                            }
                        };
                        q2.enqueue(v);
                    })
                    .unwrap();
                },
                nr_thread,
                duration,
            ),
            _ => unreachable!("Pipe를 위한 테스트만 해야함"),
        }
    }
}
