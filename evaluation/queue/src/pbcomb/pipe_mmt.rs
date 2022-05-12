//! Composition of PBcombQueue-mmt

use memento::pepoch::Guard;
use memento::pmem::PoolHandle;

use super::*;

struct PipeObj {
    q1: MmtQueue,
    q2: MmtQueue,
}

impl PipeObj {
    fn pipe<const REC: bool>(
        &mut self,
        pip: &mut Pipe,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        // assume deq.seq=2, enq.seq=2

        let val = self.q1.dequeue::<REC>(&mut pip.deq, tid, guard, pool);
        // 여기서 crash나면, deq는 같은 값 가져오지만 enq는 재실행 못함
        // 즉 최대 1번 실행은 하지만 최소 1번 실행이 안됨
        //
        // e.g. 현재 값 => recover 실행시
        // <Dequeue>
        // deq.checkpoint=2
        // deq.seq=3            => checkpoint+1과 같으니 또 +1 하지 않음
        // deq.deactivate=3     => seq와 같으니 실행된것으로 봄
        // <Enqueue>
        // enq.checkpoint=1
        // enq.seq=2            => checkpoint+1과 같으니 또 +1 하지 않음
        // enq.deactivate=2     => seq와 같으니 실행된 것으로 봄

        let _ = self.q2.enqueue::<REC>(val, &mut pip.enq, tid, guard, pool);

        // exactly-once를 달성하기가 어렵다. 왜냐하면 이를 위한 counter(seq)를 사용자가 직접 잘 조정해야하기 때문.
        // 특히 composition시 inner obj를 위한 counter도 주의해야하므로 composition 할 수록 correct하게 만들기 더 어렵다.
    }

    // fn two_op<const REC: bool>(
    //     &mut self,
    //     pip: &mut Pipe,
    //     tid: usize,
    //     guard: &Guard,
    //     pool: &PoolHandle,
    // ) {
    //     let v1 = self.q1.dequeue::<REC>(&mut pip.deq, tid, guard, pool);
    //     let v2 = self.q1.dequeue::<REC>(&mut pip.deq, tid, guard, pool);
    //     let _ = self
    //         .q1
    //         .enqueue::<REC>(v1 + v2, &mut pip.enq, tid, guard, pool);

    //     let v = val.checkpoint(v1);
    //     v로 ~~ 이것저것 수행

    //     let v = val.checkpoint(v2);
    // }
}

struct Pipe {
    deq: Dequeue,
    enq: Enqueue,
}

// 결론:
// 1. seq를 내부에서 잘 쓰도록 포장해놓으면, 이후 다른 놈과 조립하기가 어렵다.
// 2. 같은 op을 여러 번 연속 실행하기가 어렵다.
//  - PBComb API로 가져올 수 있는 return 값은 가장 최근 op 뿐이기 때문에, 먼저 나왔던 return 값들은 따로 백업해둬야한다.
//  - 백업한 값을 가져올때, 값만 다시 가져오고 함수는 재실행하면 안된다.
