use crate::bench_impl::abstract_queue::*;
use crate::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::{ll::*, pool::*};
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

#[derive(Debug)]
struct DurableQueue<T: Clone> {
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
    ret_val: [PAtomic<Option<T>>; MAX_THREADS], // None: "EMPTY"
}

impl<T: Clone> DurableQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> POwned<Self> {
        let guard = unsafe { pepoch::unprotected(pool) };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(Self {
            head: PAtomic::from(sentinel),
            tail: PAtomic::from(sentinel),
            ret_val: array_init::array_init(|_| PAtomic::null()),
        }, pool);
        persist_obj(unsafe { ret.deref(pool) }, true);

        ret
    }

    pub fn enqueue<O: POp>(&self, val: T, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let node = POwned::new(Node::new(val), pool).into_shared(&guard);
        persist_obj(unsafe { node.deref(pool) }, true);

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
                        persist_obj(&last_ref.next, true);
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
                    persist_obj(&last_ref.next, true);
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

        let mut new_ret_val = POwned::new(None, pool).into_shared(&guard); // TODO: PPtr?
        let new_ret_val_ref = unsafe { new_ret_val.deref_mut(pool) };
        persist_obj(new_ret_val_ref, true);

        self.ret_val[tid].store(new_ret_val, Ordering::SeqCst);
        persist_obj(&self.ret_val[tid], true);

        let new_ret_val_ref = unsafe { new_ret_val.deref_mut(pool) };
        loop {
            let first = self.head.load(Ordering::SeqCst, &guard);
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, &guard);

            if first == self.head.load(Ordering::SeqCst, &guard) {
                if first == last {
                    if next.is_null() {
                        // TODO: atomic data?
                        *new_ret_val_ref = None;
                        persist_obj(new_ret_val_ref, true);
                        return;
                    }
                    let last_ref = unsafe { last.deref(pool) };
                    persist_obj(&last_ref.next, true);
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
                        persist_obj(&next_ref.deq_tid, true);
                        *new_ret_val_ref = val;
                        persist_obj(new_ret_val_ref, true);
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
                        let mut addr =
                            self.ret_val[deq_tid as usize].load(Ordering::SeqCst, &guard);

                        // Same context
                        if self.head.load(Ordering::SeqCst, &guard) == first {
                            persist_obj(&next_ref.deq_tid, true);
                            let addr_ref = unsafe { addr.deref_mut(pool) };
                            *addr_ref = val;
                            persist_obj(addr_ref, true);
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

impl<T: Clone> TestQueue for DurableQueue<T> {
    type EnqInput = T; // input
    type DeqInput = usize; // tid

    fn enqueue<O: POp>(&self, input: Self::EnqInput, pool: &PoolHandle<O>) {
        self.enqueue(input, pool);
    }
    fn dequeue<O: POp>(&self, tid: Self::DeqInput, pool: &PoolHandle<O>) {
        self.dequeue(tid, pool);
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default)]
pub struct GetDurableQueueNOps;

impl TestNOps for GetDurableQueueNOps {}

impl POp for GetDurableQueueNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o, O: POp>(
        &mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        // Initialize Queue
        let q = DurableQueue::<usize>::new(pool);
        let q_ref = unsafe { q.deref(pool) };

        for i in 0..QUEUE_INIT_SIZE {
            q_ref.enqueue(i, pool);
        }

        match kind {
            TestKind::QueuePair => self.test_nops(
                &|tid| {
                    let enq_input = tid;
                    let deq_input = tid;
                    enq_deq_pair(q_ref, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ),
            TestKind::QueueProb(prob) => self.test_nops(
                &|tid| {
                    let enq_input = tid;
                    let deq_input = tid;
                    enq_deq_prob(q_ref, enq_input, deq_input, prob, pool);
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
