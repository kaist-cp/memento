//! Memento version of PBcombQueue
#![allow(warnings)]
use memento::pepoch::Guard;
use memento::ploc::Checkpoint;
use memento::pmem::persist_obj;
use memento::pmem::PoolHandle;

use super::Data;
use super::Func;
use super::PBCombQueue;

#[derive(Debug)]
pub struct MmtQueue {
    inner: PBCombQueue,
}

impl MmtQueue {
    pub fn enqueue<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let seq_old = enq.snapshot.checkpoint::<REC>(enq.seq, tid, pool).unwrap();

        if REC {
            // exactly-once { seq +=1 }
            if enq.seq == seq_old {
                enq.seq = seq_old + 1;
                persist_obj(enq, true);
            }
            // recover: if seq%2 == deactivate { already done. return same value } else { re-execute }
            let _ = self.inner.recover(Func::ENQUEUE, arg, enq.seq, tid, pool);
        } else {
            enq.seq = seq_old + 1; // e.g. seq: 0->1
            persist_obj(enq, true);
            let _ = self.inner.PBQueue(Func::ENQUEUE, arg, enq.seq, tid, pool); // e.g. 요청시 activate: 0->1, 완료시 deactivate: 0->1
        }
    }

    pub fn dequeue<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        let seq_old = deq.snapshot.checkpoint::<REC>(deq.seq, tid, pool).unwrap();

        if REC {
            // exactly-once { seq +=1 }
            if deq.seq == seq_old {
                deq.seq = seq_old + 1;
                persist_obj(deq, true);
            }

            return self
                .inner
                .recover(Func::DEQUEUE, tid, deq.seq, tid, pool)
                .deq_retval()
                .unwrap();
        } else {
            deq.seq = seq_old + 1;
            persist_obj(deq, true);
            return self
                .inner
                .PBQueue(Func::DEQUEUE, tid, deq.seq, tid, pool)
                .deq_retval()
                .unwrap();
        }
    }
}

#[derive(Debug)]
pub struct Enqueue {
    seq: u32,
    snapshot: Checkpoint<u32>,
}

#[derive(Debug)]
pub struct Dequeue {
    seq: u32,
    snapshot: Checkpoint<u32>,
}
