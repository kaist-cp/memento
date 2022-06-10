//! Composition of PBcombQueue-mmt

use memento::pepoch::Guard;
use memento::pmem::PoolHandle;

use super::*;

struct TestObj {
    q1: MmtQueue,
    q2: MmtQueue,
}

impl TestObj {
    // Q) deq-enq composition 잘 되나?
    fn pipe<const REC: bool>(
        &mut self,
        pip: &mut Pipe,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let val = self.q1.dequeue::<REC>(&mut pip.deq, tid, guard, pool);
        let _ = self.q2.enqueue::<REC>(val, &mut pip.enq, tid, guard, pool);
    }

    // Q) 같은 op 연속 실행하는 composition도 잘 되나?
    fn deq_deq_enq<const REC: bool>(
        &mut self,
        dde: &mut DeqDeqEnq,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let v1 = self.q1.dequeue::<REC>(&mut dde.deq1, tid, guard, pool);
        let v2 = self.q1.dequeue::<REC>(&mut dde.deq2, tid, guard, pool);

        let _ = self
            .q1
            .enqueue::<REC>(v1 + v2, &mut dde.enq, tid, guard, pool);
    }
}

struct Pipe {
    deq: Dequeue,
    enq: Enqueue,
}

struct DeqDeqEnq {
    deq1: Dequeue,
    deq2: Dequeue,
    enq: Enqueue,
}
