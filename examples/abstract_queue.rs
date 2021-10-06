use compositional_persistent_object::{persistent::POp, plocation::pool::PoolHandle};

use rand::Rng;

#[inline]
fn pick(prob: u32) -> bool {
    return rand::thread_rng().gen_ratio(prob, 100);
}

pub trait TestQueue {
    type EnqInput;
    type DeqInput;
    fn enqueue<O: POp>(&self, input: Self::EnqInput, pool: &PoolHandle<O>);
    fn dequeue<O: POp>(&self, input: Self::DeqInput, pool: &PoolHandle<O>);
}

pub fn enq_deq_prob<O: POp, Q: TestQueue>(
    q: &Q,
    enq: Q::EnqInput,
    deq: Q::DeqInput,
    prob: u32,
    pool: &PoolHandle<O>,
) {
    if pick(prob) {
        q.enqueue(enq, pool);
    } else {
        q.dequeue(deq, pool);
    }
}

pub fn enq_deq_pair<O: POp, Q: TestQueue>(
    q: &Q,
    enq: Q::EnqInput,
    deq: Q::DeqInput,
    pool: &PoolHandle<O>,
) {
    q.enqueue(enq, pool);
    q.dequeue(deq, pool);
}

// TODO: main이 모든 곳에 있다는 것은 조금 이상함
fn main() {
    // no-op
}
