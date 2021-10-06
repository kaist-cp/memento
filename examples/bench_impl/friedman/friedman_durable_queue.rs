use crate::bench_impl::abstract_queue::*;
use crate::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::pool::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicIsize, Ordering};

struct Node<T: Clone> {
    val: MaybeUninit<T>,
    next: PAtomic<Node<T>>,
    deq_tid: AtomicIsize,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        Self {
            val: MaybeUninit::uninit(),
            next: PAtomic::null(),
            deq_tid: AtomicIsize::new(-1),
        }
    }
}

impl<T: Clone> Node<T> {
    fn new(val: T) -> Self {
        Self {
            val: MaybeUninit::new(val),
            next: PAtomic::null(),
            deq_tid: AtomicIsize::new(-1),
        }
    }
}

#[derive(Debug, Default)]
struct FriedmanDruableQueue<T: Clone> {
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
    returned_val: [PAtomic<Option<T>>; MAX_THREADS], // None: "EMPTY"
}

impl<T: Clone> FriedmanDruableQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> Self {
        let sentinel = Node::default();
        unsafe {
            let guard = pepoch::unprotected(pool);
            let sentinel = POwned::new(sentinel, pool).into_shared(guard);
            Self {
                head: PAtomic::from(sentinel),
                tail: PAtomic::from(sentinel),
                returned_val: Default::default(),
            }
        }
    }

    pub fn enqueue<O: POp>(&self, val: T, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let node = POwned::new(Node::new(val), pool).into_shared(&guard);
        // TODO: flush node
        loop {
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, &guard);

            if last == self.tail.load(Ordering::SeqCst, &guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(next, node, Ordering::SeqCst, Ordering::SeqCst, &guard)
                        .is_ok()
                    {
                        // TODO: flush(&last->next)
                        let _ = self.tail.compare_exchange(
                            last,
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                        return;
                    }
                } else {
                    // TODO: flush(&last->next)
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    );
                };
            }
        }
    }

    pub fn dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let mut new_returned_val = POwned::new(None, pool).into_shared(&guard); // TODO: PPtr?
                                                                                // TODO: flush `new_retunred_val`
        self.returned_val[tid].store(new_returned_val, Ordering::SeqCst);
        // TODO: flush `self.returned_val[tid]`

        let guard = pepoch::pin(pool);
        loop {
            let first = self.head.load(Ordering::SeqCst, &guard);
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, &guard);

            if first == self.head.load(Ordering::SeqCst, &guard) {
                if first == last {
                    if next.is_null() {
                        let new_returned_val_ref = unsafe { new_returned_val.deref_mut(pool) };
                        *new_returned_val_ref = None;
                        return;
                    }
                    // TODO: flush(first_ref.next);
                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    );
                } else {
                    let next_ref = unsafe { next.deref(pool) };
                    let val = Some(unsafe { (*next_ref.val.as_ptr()).clone() });

                    if next_ref
                        .deq_tid
                        .compare_exchange(-1, tid as isize, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        // TODO: flush(&first->next->deqTid);
                        let new_returned_val_ref = unsafe { new_returned_val.deref_mut(pool) };
                        *new_returned_val_ref = val;
                        // TODO: flush `self.returned_val[tid]`
                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                        return;
                    } else {
                        let deq_tid = next_ref.deq_tid.load(Ordering::SeqCst);
                        let mut returned_val =
                            self.returned_val[deq_tid as usize].load(Ordering::SeqCst, &guard);
                        // Same context
                        if self.head.load(Ordering::SeqCst, &guard) == first {
                            // TODO: flush(&first->next->deqTid);
                            let new_returned_val_ref = unsafe { returned_val.deref_mut(pool) };
                            *new_returned_val_ref = val;
                            // TODO: flush `self.returned_val[deq_tid]`
                            let _ = self.head.compare_exchange(
                                first,
                                next,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                &guard,
                            );
                        }
                    }
                }
            }
        }
    }
}

impl<T: Clone> TestQueue for FriedmanDruableQueue<T> {
    type EnqInput = T; // input
    type DeqInput = usize; // tid

    fn enqueue<O: POp>(&self, input: Self::EnqInput, pool: &PoolHandle<O>) {
        self.enqueue(input, pool);
    }
    fn dequeue<O: POp>(&self, tid: Self::DeqInput, pool: &PoolHandle<O>) {
        self.dequeue(tid, pool);
    }
}

#[derive(Default)]
pub struct GetDurableQueueNOps {
    queue: FriedmanDruableQueue<usize>,
}

impl GetDurableQueueNOps {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        self.queue = FriedmanDruableQueue::new(pool);
        for i in 0..QUEUE_INIT_SIZE {
            self.queue.enqueue(i, pool);
        }
    }
}

impl TestNOps for GetDurableQueueNOps {}

impl POp for GetDurableQueueNOps {
    type Object<'o> = ();
    type Input = (usize, f64, TestKind); // (n개 스레드로 m초 동안 테스트, 테스트 종류)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (nr_thread, duration, kind): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        self.init(pool);

        match kind {
            TestKind::QueuePair => self.test_nops(
                &|tid| {
                    let enq_input = tid;
                    let deq_input = tid;
                    enq_deq_pair(&self.queue, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ),
            TestKind::QueueProb(prob) => self.test_nops(
                &|tid| {
                    let enq_input = tid;
                    let deq_input = tid;
                    enq_deq_prob(&self.queue, enq_input, deq_input, prob, pool);
                },
                nr_thread,
                duration,
            ),
            _ => unreachable!("Queue를 위한 테스트만 해야함"),
        }
    }

    fn reset(&mut self, _: bool) {
        // no-op
    }
}
