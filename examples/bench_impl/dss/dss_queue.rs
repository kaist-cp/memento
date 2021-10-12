use crate::bench_impl::abstract_queue::*;
use crate::{TestKind, TestNOps, MAX_THREADS, QUEUE_INIT_SIZE};
use compositional_persistent_object::pepoch::{self as pepoch, PAtomic, POwned, PShared};
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
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
    x: [PAtomic<Node<T>>; MAX_THREADS],
}

impl<T: Clone> Default for DSSQueue<T> {
    fn default() -> Self {
        Self {
            head: Default::default(),
            tail: Default::default(),
            x: array_init::array_init(|_| PAtomic::null()),
        }
    }
}

impl<T: Clone> DSSQueue<T> {
    fn new<O: POp>(pool: &PoolHandle<O>) -> Self {
        let sentinel = Node::default();
        unsafe {
            let guard = pepoch::unprotected(pool);
            let sentinel = POwned::new(sentinel, pool).into_shared(guard);
            Self {
                head: PAtomic::from(sentinel),
                tail: PAtomic::from(sentinel),
                x: array_init::array_init(|_| PAtomic::null()),
            }
        }
    }

    pub fn prep_enqueue<O: POp>(&self, val: T, tid: usize, pool: &PoolHandle<O>) {
        let node = POwned::new(Node::new(val), pool);
        // TODO: flush node
        self.x[tid].store(node.with_tag(ENQ_PREP_TAG), Ordering::SeqCst);
        // TODO: flush (&x[tid])
    }

    pub fn exec_enqueue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        let guard = pepoch::pin(pool);
        let node = self.x[tid].load(Ordering::SeqCst, &guard);

        loop {
            let last = self.tail.load(Ordering::SeqCst, &guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, &guard);

            if last == self.tail.load(Ordering::SeqCst, &guard) {
                if next.is_null() {
                    // NOTE: DSS 논문의 구현에선 CAS(&last->next, NULL, node)지만 여기 구현은 durable queue와 같이 CAS(&last->next, next, node)로 함. 차이점은 없음
                    if last_ref
                        .next
                        .compare_exchange(next, node, Ordering::SeqCst, Ordering::SeqCst, &guard)
                        .is_ok()
                    {
                        // TODO: flush(&last->next)
                        self.x[tid].fetch_or(ENQ_COMPL_TAG, Ordering::SeqCst, &guard);
                        // TODO: flush(&x[tid]);
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

    pub fn resolve_enqueue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
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

    pub fn prep_dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
        self.x[tid].store(PShared::null().with_tag(DEQ_PREP_TAG), Ordering::SeqCst);
        // TODO: flush (&x[tid])
    }

    pub fn exec_dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) -> Option<T> {
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
                        self.x[tid].fetch_or(EMPTY_TAG, Ordering::SeqCst, &guard);
                        // TODO: flush &x[tid]
                        return None; // EMPTY
                    }
                    // TODO: flush(&last->next);
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

                    // TODO: flush (&x[tid])

                    let next_ref = unsafe { next.deref(pool) };
                    if next_ref
                        .deq_tid
                        .compare_exchange(-1, tid as isize, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        // TODO: flush(&next->deqTid);
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
                        // TODO: flush(&next->deqTid);
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

    pub fn resolve_dequeue<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
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

    pub fn resolve<O: POp>(&self, tid: usize, pool: &PoolHandle<O>) {
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
        self.prep_dequeue(tid, pool);
        self.exec_dequeue(tid, pool);
    }
}

#[derive(Default)]
pub struct GetDSSQueueNOps {
    queue: DSSQueue<usize>,
}

impl GetDSSQueueNOps {
    fn init<O: POp>(&mut self, pool: &PoolHandle<O>) {
        self.queue = DSSQueue::new(pool);
        for i in 0..QUEUE_INIT_SIZE {
            self.queue.prep_enqueue(i, 0, pool);
            self.queue.exec_enqueue(0, pool);
        }
    }
}

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
        self.init(pool);

        match kind {
            TestKind::QueuePair => self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid);
                    let deq_input = tid;
                    enq_deq_pair(&self.queue, enq_input, deq_input, pool);
                },
                nr_thread,
                duration,
            ),
            TestKind::QueueProb(prob) => self.test_nops(
                &|tid| {
                    let enq_input = (tid, tid);
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
        // no-ops
    }
}
