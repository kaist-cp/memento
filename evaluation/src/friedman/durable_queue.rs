use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned};
use compositional_persistent_object::persistent::*;
use compositional_persistent_object::plocation::{ll::*, pool::*};
use crossbeam_utils::CachePadded;
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
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    ret_val: [CachePadded<PAtomic<Option<T>>>; MAX_THREADS], // None: "EMPTY"
}

impl<T: Clone> DurableQueue<T> {
    fn new<O: POp>(pool: &PoolHandle) -> POwned<Self> {
        let guard = unsafe { pepoch::unprotected(pool) };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(
            Self {
                head: CachePadded::new(PAtomic::from(sentinel)),
                tail: CachePadded::new(PAtomic::from(sentinel)),
                ret_val: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
            },
            pool,
        );
        persist_obj(unsafe { ret.deref(pool) }, true);
        ret
    }

    fn enqueue<O: POp>(&self, val: T, pool: &PoolHandle) {
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

    fn dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle) {
        let guard = pepoch::pin(pool);

        // NOTE: Durable 큐의 하자 (1/1)
        // - 우리 큐: deq에서 새롭게 할당하는 것 없음
        // - Durable 큐: deq한 값을 가리킬 포인터 할당 및 persist
        //
        // ```
        let mut new_ret_val = POwned::new(None, pool).into_shared(&guard);
        let new_ret_val_ref = unsafe { new_ret_val.deref_mut(pool) };
        persist_obj(new_ret_val_ref, true);

        self.ret_val[tid].store(new_ret_val, Ordering::SeqCst);
        persist_obj(&self.ret_val[tid], true);
        // ```

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
                    // NOTE: 여기서 Durable 큐가 우리 큐랑 persist하는 시점은 다르지만 persist하는 총 횟수는 똑같음
                    // - 우리 큐:
                    //      - if/else문 진입 전에 persist 1번: "나는(deq POp) 이 노드를 pop 시도할거다"
                    //      - if/else문 진입 후에 각각 persist 1번: "이 노드를 pop해간 deq POp은 얘다"
                    // - Durable 큐:
                    //      - if/else문 진입 전에 persist 0번
                    //      - if/else문 진입 후에 각각 persist 2번: "이 노드를 pop해간 스레드는 `deq tid`다, "`deq tid` 스레드가 pop한 값는 이거다"
                    // TODO: 이게 성능 차이에 영향 미칠지?
                    //      - e.g. KSC 실험은 T를 고작 usize로 했지만, pop value의 사이즈가 커지면 유의미한 차이를 보일 것으로 기대

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

    fn enqueue<O: POp>(&self, input: Self::EnqInput, pool: &PoolHandle) {
        self.enqueue(input, pool);
    }
    fn dequeue<O: POp>(&self, tid: Self::DeqInput, pool: &PoolHandle) {
        self.dequeue(tid, pool);
    }
}

// TODO: 모든 큐의 실험 로직이 통합되어야 함
#[derive(Default, Debug)]
pub struct GetDurableQueueNOps;

impl TestNOps for GetDurableQueueNOps {}

impl POp for GetDurableQueueNOps {
    type Object<'o> = ();
    type Input = (TestKind, usize, f64); // (테스트 종류, n개 스레드로 m초 동안 테스트)
    type Output<'o> = usize; // 실행한 operation 수

    fn run<'o>(
        &mut self,
        _: Self::Object<'o>,
        (kind, nr_thread, duration): Self::Input,
        pool: &PoolHandle,
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
