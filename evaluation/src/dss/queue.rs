use crate::common::queue::{enq_deq_pair, enq_deq_prob, TestQueue};
use crate::common::{TestNOps, DURATION, MAX_THREADS, PROB, QUEUE_INIT_SIZE, TOTAL_NOPS};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use epoch::Guard;
use memento::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use memento::*;
use memento::pmem::ralloc::{Collectable, GarbageCollection};
use memento::pmem::{ll::*, pool::*};
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

// resolve시 Op 타입
enum _OpResolved {
    Enqueue,
    Dequeue,
}

#[derive(Debug)]
struct DSSQueue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
    x: [CachePadded<PAtomic<Node<T>>>; MAX_THREADS],
}

impl<T: Clone> Collectable for DSSQueue<T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl<T: Clone> PDefault for DSSQueue<T> {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
            x: array_init::array_init(|_| CachePadded::new(PAtomic::null())),
        }
    }
}

impl<T: Clone> DSSQueue<T> {
    fn prep_enqueue(&self, val: T, tid: usize, pool: &'static PoolHandle) {
        let node = POwned::new(Node::new(val), pool);
        persist_obj(unsafe { node.deref(pool) }, true);
        self.x[tid].store(node.with_tag(ENQ_PREP_TAG), Ordering::Relaxed);
        persist_obj(&*self.x[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
    }

    fn exec_enqueue(&self, tid: usize, guard: &Guard, pool: &'static PoolHandle) {
        let node = self.x[tid].load(Ordering::Relaxed, guard);

        loop {
            let last = self.tail.load(Ordering::SeqCst, guard);
            let last_ref = unsafe { last.deref(pool) };
            let next = last_ref.next.load(Ordering::SeqCst, guard);

            if last == self.tail.load(Ordering::SeqCst, guard) {
                if next.is_null() {
                    if last_ref
                        .next
                        .compare_exchange(
                            PShared::null(),
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
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
                        self.x[tid]
                            .store(node.with_tag(node.tag() | ENQ_COMPL_TAG), Ordering::Relaxed);
                        persist_obj(&*self.x[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist

                        // ```

                        let _ = self.tail.compare_exchange(
                            last,
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
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
                        guard,
                    );
                };
            }
        }
    }

    fn _resolve_enqueue(&self, tid: usize, guard: &Guard, pool: &'static PoolHandle) -> (T, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        let node_ref = unsafe { x_tid.deref(pool) };
        let value = unsafe { (*node_ref.val.as_ptr()).clone() };
        if (x_tid.tag() & ENQ_COMPL_TAG) != 0 {
            // enqueue was prepared and took effect
            // "Enq 됨"
            (value, true)
        } else {
            // enqueue was prepared and did not take effect
            // "아직 Enq 되지 못함"
            (value, false)
        }
    }

    fn prep_dequeue(&self, tid: usize) {
        self.x[tid].store(PShared::null().with_tag(DEQ_PREP_TAG), Ordering::Relaxed);
        persist_obj(&*self.x[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
    }

    fn exec_dequeue(&self, tid: usize, guard: &Guard, pool: &'static PoolHandle) -> Option<T> {
        loop {
            let first = self.head.load(Ordering::SeqCst, guard);
            let last = self.tail.load(Ordering::SeqCst, guard);
            let first_ref = unsafe { first.deref(pool) };
            let next = first_ref.next.load(Ordering::SeqCst, guard);

            if first == self.head.load(Ordering::SeqCst, guard) {
                if first == last {
                    // empty queue
                    if next.is_null() {
                        // nothing new appended at tail
                        let node = self.x[tid].load(Ordering::Relaxed, guard);
                        self.x[tid].store(node.with_tag(node.tag() | EMPTY_TAG), Ordering::Relaxed);
                        persist_obj(&*self.x[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
                        return None; // EMPTY
                    }

                    let last_ref = unsafe { last.deref(pool) };
                    persist_obj(&last_ref.next, true);

                    let _ = self.tail.compare_exchange(
                        last,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                } else {
                    // non-empty queue
                    self.x[tid].store(first.with_tag(DEQ_PREP_TAG), Ordering::Relaxed); // save predecessor of node to be dequeued
                    persist_obj(&*self.x[tid], true); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist

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
                            guard,
                        );
                        guard.defer_persist(&*self.head); // 참조하는 이유: CachePadded 전체를 persist하면 손해이므로 안쪽 T만 persist
                        return Some(unsafe { (*next_ref.val.as_ptr()).clone() });
                    } else if self.head.load(Ordering::SeqCst, guard) == first {
                        // help another dequeueing thread
                        persist_obj(&next_ref.deq_tid, true);
                        let _ = self.head.compare_exchange(
                            first,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                    }
                }
            }
        }
    }

    fn _resolve_dequeue(
        &self,
        tid: usize,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> (Option<T>, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        if x_tid == PShared::null().with_tag(DEQ_PREP_TAG) {
            // dequeue was prepared but did not take effect
            // "준비는 했지만 실행을 안함"
            (None, false)
        } else if x_tid == PShared::null().with_tag(DEQ_PREP_TAG | EMPTY_TAG) {
            // empty queue
            // "EMPTY로 성공"
            (None, true)
        } else {
            let x_tid_ref = unsafe { x_tid.deref(pool) };
            let next = x_tid_ref.next.load(Ordering::SeqCst, guard);
            let next_ref = unsafe { next.deref(pool) };
            if next_ref.deq_tid.load(Ordering::SeqCst) == tid as isize {
                // non-empty queue
                // "Deq 성공"
                let value = unsafe { (*next_ref.val.as_ptr()).clone() };
                (Some(value), true)
            } else {
                // X holds a node pointer, crashed before completing dequeue
                // "포인팅했지만 내가 Deq 하지 못함"
                (None, false)
            }
        }
    }

    // return: ((op 종류, op에 관련된 값), op 성공여부)
    fn _resolve(
        &self,
        tid: usize,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> (Option<(_OpResolved, Option<T>)>, bool) {
        let x_tid = self.x[tid].load(Ordering::Relaxed, guard);
        if (x_tid.tag() & ENQ_PREP_TAG) != 0 {
            // Enq를 준비했었음. 성공했는지는 resolve_enqueue로 확인
            let (value, completed) = self._resolve_enqueue(tid, guard, pool);
            // ((Enq, Enq 하려던(혹은 이미 한) 값), Enq 성공여부)
            (Some((_OpResolved::Enqueue, Some(value))), completed)
        } else if (x_tid.tag() & DEQ_PREP_TAG) != 0 {
            // Deq를 준비했었음. 성공했는지는 resolve_deqqueue로 확인
            let (value, completed) = self._resolve_dequeue(tid, guard, pool);
            // ((Deq, Deq 한 값), Deq 성공여부)
            (Some((_OpResolved::Dequeue, value)), completed)
        } else {
            // no operation was prepared
            (None, false)
        }
    }
}

impl<T: Clone> TestQueue for DSSQueue<T> {
    type EnqInput = (T, usize); // input, tid
    type DeqInput = usize; // tid

    fn enqueue(&self, (input, tid): Self::EnqInput, guard: &Guard, pool: &'static PoolHandle) {
        // NOTE: 만약 crash를 고려한다면 새로 prep 하기전에 남아있는거 resolve로 확인 후 필요시 free 해야함
        self.prep_enqueue(input, tid, pool);
        self.exec_enqueue(tid, guard, pool);

        // 다음 prep으로 x[tid]를 덮어씌우더라도 여기서 x[tid]가 가리키는 노드는 free하면 안됨. 이미 enq된 노드임
    }

    fn dequeue(&self, tid: Self::DeqInput, guard: &Guard, pool: &'static PoolHandle) {
        // NOTE: 만약 crash를 고려한다면 새로 prep 하기전에 남아있는거 resolve로 확인 후 필요시 free 해야함
        self.prep_dequeue(tid);
        let val = self.exec_dequeue(tid, guard, pool);

        // 다음 prep으로 x[tid]를 덮어씌우기 전에 여기서 x[tid]가 가리키는 deq된 노드를 free
        //
        // `val`이 None이면 EMPTY로 끝난 것. free하면 안됨
        if val.is_some() {
            let node_tid = self.x[tid].load(Ordering::Relaxed, guard);
            unsafe { guard.defer_pdestroy(node_tid) };
        }
    }
}

#[derive(Debug)]
pub struct TestDSSQueue {
    queue: DSSQueue<usize>,
}

impl Collectable for TestDSSQueue {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl PDefault for TestDSSQueue {
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let queue = DSSQueue::pdefault(pool);
        let mut guard = epoch::pin();

        // 초기 노드 삽입
        for i in 0..QUEUE_INIT_SIZE {
            queue.prep_enqueue(i, 0, pool);
            queue.exec_enqueue(0, &guard, pool);
        }
        Self { queue }
    }
}

#[derive(Default, Debug)]
pub struct DSSQueueEnqDeqPair;

impl Collectable for DSSQueueEnqDeqPair {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for DSSQueueEnqDeqPair {}

impl Memento for DSSQueueEnqDeqPair {
    type Object<'o> = &'o TestDSSQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        _: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'_>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_input = (tid, tid);
                let deq_input = tid;
                enq_deq_pair(q, enq_input, deq_input, guard, pool);
            },
            tid,
            duration,
            guard,
        );
        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}

#[derive(Default, Debug)]
pub struct DSSQueueEnqDeqProb;

impl Collectable for DSSQueueEnqDeqProb {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
        todo!()
    }
}

impl TestNOps for DSSQueueEnqDeqProb {}

impl Memento for DSSQueueEnqDeqProb {
    type Object<'o> = &'o TestDSSQueue;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        tid: Self::Input<'o>,
        _: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'_>> {
        let q = &queue.queue;
        let duration = unsafe { DURATION };
        let prob = unsafe { PROB };

        let ops = self.test_nops(
            &|tid, guard| {
                let enq_input = (tid, tid);
                let deq_input = tid;
                enq_deq_prob(q, enq_input, deq_input, prob, guard, pool);
            },
            tid,
            duration,
            guard,
        );
        let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {
        // no-op
    }
}
