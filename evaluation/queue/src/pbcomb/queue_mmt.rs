//! Memento version of PBcombQueue
#![allow(warnings)]
use memento::pepoch::Guard;
use memento::ploc::Checkpoint;
use memento::pmem::persist_obj;
use memento::pmem::PoolHandle;

use super::Data;
use super::Func;
use super::PBCombQueue;

const MAX_THREADS: usize = 4;

#[derive(Debug)]
pub struct MmtQueue {
    inner: PBCombQueue,
    enq_seq: [u32; MAX_THREADS],
    deq_seq: [u32; MAX_THREADS],
}

impl MmtQueue {
    pub fn enqueue<const REC: bool>(
        &mut self,
        arg: Data,
        enq: &mut Enqueue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> () {
        let seq_old = enq
            .seq
            .checkpoint::<REC>(self.enq_seq[tid], tid, pool)
            .unwrap();

        // 이미 예전에 끝났던 seq라면 백업해둔 결과값 반환
        if self.enq_seq[tid] > seq_old + 1 {
            return enq.retval.unwrap();
        }

        // 최근 seq라면 exactly-once 실행
        let retval = {
            if self.enq_seq[tid] == seq_old {
                self.enq_seq[tid] = seq_old + 1;
                persist_obj(enq, true);

                // e.g. 요청시 activate: 0->1, 완료시 deactivate: 0->1
                self.inner
                    .PBQueue(Func::ENQUEUE, arg, self.enq_seq[tid], tid, pool)
                    .enq_retval()
                    .unwrap()
            } else {
                // seq+1인 상태로 남아있으면 recover로 (1) 실행 마무리 하던가 혹은 (2) 반환값 가져옴
                self.inner
                    .recover(Func::ENQUEUE, arg, self.enq_seq[tid], tid, pool)
                    .enq_retval()
                    .unwrap()
            }
        };
        enq.retval = Some(retval);
        return retval;
    }

    pub fn dequeue<const REC: bool>(
        &mut self,
        deq: &mut Dequeue,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> usize {
        let seq_old = deq
            .seq
            .checkpoint::<REC>(self.deq_seq[tid], tid, pool)
            .unwrap();

        // 이미 예전에 끝났던 seq라면 백업해둔 결과값 반환
        if self.deq_seq[tid] > seq_old + 1 {
            return deq.retval.unwrap();
        }

        // 최근 seq라면 exactly-once 실행
        let retval = {
            if self.deq_seq[tid] == seq_old {
                self.deq_seq[tid] = seq_old + 1;
                persist_obj(deq, true);
                self.inner
                    .PBQueue(Func::DEQUEUE, tid, self.deq_seq[tid], tid, pool)
                    .deq_retval()
                    .unwrap()
            } else {
                // seq+1인 상태로 남아있으면 recover로 (1) 실행 마무리 하던가 혹은 (2) 반환값 가져옴
                self.inner
                    .recover(Func::DEQUEUE, tid, self.deq_seq[tid], tid, pool)
                    .deq_retval()
                    .unwrap()
            }
        };
        deq.retval = Some(retval);
        return retval;
    }
}

#[derive(Debug)]
pub struct Enqueue {
    seq: Checkpoint<u32>,
    retval: Option<()>,
}

#[derive(Debug)]
pub struct Dequeue {
    seq: Checkpoint<u32>,
    retval: Option<usize>,
}
