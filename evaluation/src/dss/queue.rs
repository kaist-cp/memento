use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned, PShared};
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

/// TAGS
// TODO: x가 가리키는 포인터의 MSB에 저장하기. 현재는 patomic의 API를 쓰므로 LSB에 저장됨
// 태그로 5가지 상황이 구분돼야함
// version 1:
// - non-prepared: null with tag                            0b0000
// - enq-prepared: node with tag                            0b0001  (ENQ_PREP_TAG)
// - enq-prepared-and-completed: node with tag              0b0011  (ENQ_PREP_TAG | END_COMPL_TAG)
// - deq-prepared: null with tag                            0b0100  (DEQ_PREP_TAG)
// - deq-prepared-and-completed(empty): null wth tag        0b1100  (DEQ_PREP_TAG | EMPTY_TAG)
// - deq-prepared-and-completed?(not-empty): node with tag  0b0100  (DEQ_PREP_TAG) // 자기가 pop한게 맞는지 node에 기록된 deq_tid로 확인
//
//  0000 0000
//          ^(LSB 0) 1: enq_prepared
//         ^(LSB 1) 1: enq_completed
//        ^(LSB 2) 1: deq_prepared
//       ^(LSB 3) 1: deq_completed_empty

// (채택 중)version 2: END_COMPL_TAG, EMPTY_TAG에 같은 비트를 사용해서 1비트 절약가능
// - non-prepared: null with tag                            0b000
// - enq-prepared: node with tag                            0b001  (ENQ_PREP_TAG)
// - enq-prepared-and-completed: node with tag              0b101  (ENQ_PREP_TAG | END_COMPL_TAG)
// - deq-prepared: null with tag                            0b010  (DEQ_PREP_TAG)
// - deq-prepared-and-completed(empty): null wth tag        0b110  (DEQ_PREP_TAG | EMPTY_TAG)
// - deq-prepared-and-completed?(not-empty): node with tag  0b010  (DEQ_PREP_TAG) // 자기가 pop한게 맞는지 node에 기록된 deq_tid로 확인
//
//  0000 0000
//          ^(LSB 0) 1: enq_prepared
//         ^(LSB 1) 1: dnq_prepared
//        ^(LSB 2) 1: completed (END_COMPL_TAG if enq_prepared, EMPTY_TAG if dnq_prepared)
const ENQ_PREP_TAG: usize = 1;
const ENQ_COMPL_TAG: usize = 4;
const DEQ_PREP_TAG: usize = 2;
const EMPTY_TAG: usize = 4;

#[derive(Debug)]
struct DSSQueue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    x: [CachePadded<PAtomic<Node<T>>>; MAX_THREADS],
}

impl<T: Clone> DSSQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> POwned<Self> {
        let guard = unsafe { pepoch::unprotected(pool) };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(
            Self {
                head: CachePadded::new(PAtomic::from(sentinel)),
                tail: CachePadded::new(PAtomic::from(sentinel)),
                x: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
            },
            pool,
        );
        persist_obj(unsafe { ret.deref(pool) }, true);
        ret
    }

    fn prep_enqueue<O: POp>(&self, val: T, tid: usize, pool: &PoolHandle<O>) {
        let node = POwned::new(Node::new(val), pool);
        persist_obj(unsafe { node.deref(pool) }, true);
        self.x[tid].store(node.with_tag(ENQ_PREP_TAG), Ordering::SeqCst);
        persist_obj(&self.x[tid], true);
    }

    fn exec_enqueue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let node = self.x[tid].load(Ordering::SeqCst, &guard);

        loop {
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, &guard);

            if last == self.tail.load(Ordering::SeqCst, &guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(
                            PShared::null(),
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        )
                        .is_ok()
                    {
                        persist_obj(&last_ref.next, true);

                        // NOTE: DSS 큐의 하자 (1/1)
                        // - 우리 큐의 enq에는 이 write & persist 없음
                        // - 차이나는 이유:
                        //      - 우리 큐: enq 성공여부 구분을 direct tracking으로 함. 따라서 enq 성공이후 따로 write하는 것 없음
                        //      - DSS 큐: enq 성공여부 구분을 태그로 함(resolve_enq). 따라서 enq 성공이후 "성공했다"라는 태그를 write
                        // TODO: KSC 실험결과에서 우리 큐가 살짝 더 좋게 나온 이유는 이것 때문일 수도?
                        //
                        // ```
                        let _ = self.x[tid].fetch_or(ENQ_COMPL_TAG, Ordering::SeqCst, &guard);
                        persist_obj(&self.x[tid], true);
                        // ```

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

    fn _resolve_enqueue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let x_tid = self.x[tid].load(Ordering::SeqCst, &guard);
        if (x_tid.tag() & ENQ_COMPL_TAG) != 0 {
            // enqueue was prepared and took effect
            todo!("return (value, OK)")
        } else {
            // enqueue was prepared and did not take effect
            todo!("return (value, null")
        }
    }

    fn prep_dequeue(&self, tid: usize) {
        self.x[tid].store(PShared::null().with_tag(DEQ_PREP_TAG), Ordering::SeqCst);
        persist_obj(&self.x[tid], true);
    }

    fn exec_dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) -> Option<T> {
        let guard = pepoch::pin(pool);

        loop {
            let first = self.head.load(Ordering::SeqCst, &guard);
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, &guard);

            if first == self.head.load(Ordering::SeqCst, &guard) {
                if first == last {
                    // empty queue
                    if next.is_null() {
                        // nothing new appended at tail
                        let _ = self.x[tid].fetch_or(EMPTY_TAG, Ordering::SeqCst, &guard);
                        persist_obj(&self.x[tid], true);
                        return None; // EMPTY
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
                    // non-empty queue
                    self.x[tid].store(first.with_tag(DEQ_PREP_TAG), Ordering::SeqCst); // save predecessor of node to be dequeued
                    persist_obj(&self.x[tid], true);

                    let next_ref = unsafe { next.deref(pool) };
                    if next_ref
                        .deq_tid
                        .compare_exchange(-1, tid as isize, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        persist_obj(&next_ref.deq_tid, true);
                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            &guard,
                        );
                        return Some(unsafe { (*next_ref.val.as_ptr()).clone() });
                    } else if self.head.load(Ordering::SeqCst, &guard) == first {
                        // help another dequeueing thread
                        persist_obj(&next_ref.deq_tid, true);
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

    fn _resolve_dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let x_tid = self.x[tid].load(Ordering::SeqCst, &guard);
        if x_tid == PShared::null().with_tag(DEQ_PREP_TAG) {
            // dequeue was prepared but did not take effect
            todo!("return null");
        } else if x_tid == PShared::null().with_tag(DEQ_PREP_TAG | EMPTY_TAG) {
            // empty queue
            todo!("return EMPTY");
        } else {
            let x_tid_ref = unsafe { x_tid.deref(pool) };
            let next = x_tid_ref.next.load(Ordering::SeqCst, &guard);
            let next_ref = unsafe { next.deref(pool) };
            if next_ref.deq_tid.load(Ordering::SeqCst) == tid as isize {
                // non-empty queue
                todo!("return next_ref.value")
            } else {
                // X holds a node pointer, crashed before completing dequeue
                todo!("return null")
            }
        }
    }

    fn _resolve<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let x_tid = self.x[tid].load(Ordering::SeqCst, &guard);
        if (x_tid.tag() & ENQ_PREP_TAG) != 0 {
            todo!("resolve enq and return")
        } else if (x_tid.tag() & DEQ_PREP_TAG) != 0 {
            todo!("resolove deq and return")
        } else {
            // no operation was prepared
            todo!("return null")
        }
    }
}

impl<T: Clone> TestQueue for DSSQueue<T> {
    type EnqInput = (T, usize); // input, tid
    type DeqInput = usize; // tid

    fn enqueue<O: POp>(&self, (input, tid): Self::EnqInput, pool: &PoolHandle<O>) {
        self.prep_enqueue(input, tid, pool);
        self.exec_enqueue(tid, pool);
    }
    fn dequeue<O: POp>(&self, tid: Self::DeqInput, pool: &PoolHandle<O>) {
        self.prep_dequeue(tid);
        let _ = self.exec_dequeue(tid, pool);
    }
}

#[derive(Default, Debug)]
pub struct GetDSSQueueNOps;

impl TestNOps for GetDSSQueueNOps {}

impl POp for GetDSSQueueNOps {
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
        let q = DSSQueue::<usize>::new(pool);
        let q_ref = unsafe { q.deref(pool) };

        for i in 0..QUEUE_INIT_SIZE {
            q_ref.prep_enqueue(i, 0, pool);
            q_ref.exec_enqueue(0, pool);
        }

        match kind {
            TestKind::QueuePair => self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid);
                    let deq_input = tid;
                    enq_deq_pair(q_ref, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ),
            TestKind::QueueProb(prob) => self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid);
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
        // no-ops
    }
}
