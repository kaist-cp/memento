use compositional_persistent_object::{
    persistent::POp,
    plocation::pool::PoolHandle,
    queue::{Pop, Push, Queue},
};

use rand::Rng;

#[inline]
fn pick(probability: u32) -> bool {
    return rand::thread_rng().gen_ratio(probability, 100);
}

pub enum EnqInput<'a, T: Clone> {
    POpBased(&'a mut Push<T>, &'a Queue<T>, T), // POp, obj, input
    FriedmanDurableQ(T),                        // input
    FriedmanLogQ(T, usize, usize),              // input, tid, op_num
}

pub enum DeqInput<'a, T: Clone> {
    POpBased(&'a mut Pop<T>, &'a Queue<T>), // POp, obj
    FriedmanDurableQ(usize),                // tid
    FriedmanLogQ(usize, usize),             // tid, op_num
}

pub trait DurableQueue<T: Clone> {
    fn enqueue<O: POp>(&self, input: EnqInput<T>, pool: &PoolHandle<O>);
    fn dequeue<O: POp>(&self, input: DeqInput<T>, pool: &PoolHandle<O>);
}

pub fn enq_deq_either<O: POp, T: Clone, Q: DurableQueue<T>>(
    q: &Q,
    enq: EnqInput<T>,
    deq: DeqInput<T>,
    probability: u32,
    pool: &PoolHandle<O>,
) {
    if pick(probability) {
        q.enqueue(enq, pool);
    } else {
        q.dequeue(deq, pool);
    }
}

pub fn enq_deq_both<O: POp, T: Clone, Q: DurableQueue<T>>(
    q: &Q,
    enq: EnqInput<T>,
    deq: DeqInput<T>,
    pool: &PoolHandle<O>,
) {
    q.enqueue(enq, pool);
    q.dequeue(deq, pool);
}

fn main() {
    // no-op
}
