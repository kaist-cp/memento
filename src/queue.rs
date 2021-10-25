//! Persistent queue

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_utils::CachePadded;
use etrace::some_or;
use std::{mem::MaybeUninit, ptr};

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::persistent::*;
use crate::plocation::{ll::*, pool::*, ptr::*};

struct Node<T: Clone> {
    data: MaybeUninit<T>,
    next: PAtomic<Node<T>>,

    /// 누가 dequeue 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    dequeuer: AtomicUsize,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        let null: *const Dequeue<T> = ptr::null();
        Self {
            data: MaybeUninit::uninit(),
            next: PAtomic::null(),
            dequeuer: AtomicUsize::new(null as usize),
        }
    }
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: PAtomic::null(),
            dequeuer: AtomicUsize::new(Queue::<T>::no_dequeuer()),
        }
    }
}

/// `Queue`의 Enqueue operation
#[derive(Debug)]
pub struct Enqueue<T: Clone> {
    /// enqueue를 위해 할당된 node
    mine: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Enqueue<T> {
    fn default() -> Self {
        Self {
            mine: PAtomic::null(),
        }
    }
}

impl<T: 'static + Clone> POp for Enqueue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = T;
    type Output<'o> = ();
    type Error = !;

    fn run<'o, O: POp>(
        &mut self,
        queue: Self::Object<'o>,
        value: Self::Input,
        pool: &PoolHandle<O>,
    ) -> Result<Self::Output<'o>, Self::Error> {
        queue.enqueue(self, value, pool);
        Ok(())
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node (+ free가 반영되게끔 flush 해줘야함)
        self.mine.store(PShared::null(), Ordering::SeqCst);
        persist_obj(&self.mine, true)
    }
}

/// `Queue`의 Dequeue operation
#[derive(Debug)]
pub struct Dequeue<T: Clone> {
    /// dequeue의 타겟이 되는 node
    target: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Dequeue<T> {
    fn default() -> Self {
        Self {
            target: PAtomic::null(),
        }
    }
}

impl<T: 'static + Clone> POp for Dequeue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = ();
    type Output<'o> = Option<T>;
    type Error = !;

    fn run<'o, O: POp>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Result<Self::Output<'o>, Self::Error> {
        Ok(queue.dequeue(self, pool))
    }

    fn reset(&mut self, _: bool) {
        // TODO: if node has not been freed, check if the node is mine and free it
        self.target.store(PShared::null(), Ordering::SeqCst);
        persist_obj(&self.target, true)
    }
}

impl<T: Clone> Dequeue<T> {
    #[inline]
    fn id<O: POp>(&self, pool: &PoolHandle<O>) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }
}

/// empty가 아닐 때에*만* return 하는 dequeue operation
// TODO: 현재는 sub POp으로 Dequeue을 재사용하도록 구현되어 있음 (EMPTY 기록하는 오버헤드 발생)
//       필요하다면 빌트인 함수를 만들어서 최적화할 수 있음
#[derive(Debug)]
pub struct DequeueSome<T: Clone> {
    deq: Dequeue<T>,
}

impl<T: Clone> Default for DequeueSome<T> {
    fn default() -> Self {
        Self {
            deq: Default::default(),
        }
    }
}

impl<T: 'static + Clone> POp for DequeueSome<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = ();
    type Output<'o> = T;
    type Error = !;

    fn run<'o, O: POp>(
        &mut self,
        queue: Self::Object<'o>,
        (): Self::Input,
        pool: &PoolHandle<O>,
    ) -> Result<Self::Output<'o>, Self::Error> {
        loop {
            if let Ok(Some(v)) = self.deq.run(queue, (), pool) {
                return Ok(v);
            }
            self.deq.reset(false);
        }
    }

    fn reset(&mut self, nested: bool) {
        self.deq.reset(nested);
    }
}

/// Peristent queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone> Queue<T> {
    /// new
    // TODO: alloc, init 구상한 후 시그니처 변경
    pub fn new<O: POp>(pool: &PoolHandle<O>) -> POwned<Self> {
        let guard = unsafe { epoch::unprotected(pool) };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(Self {
            head: CachePadded::new(PAtomic::from(sentinel)),
            tail: CachePadded::new(PAtomic::from(sentinel)),
        }, pool);
        persist_obj(unsafe { ret.deref(pool) }, true);
        ret
    }

    fn enqueue<O: POp>(&self, client: &mut Enqueue<T>, value: T, pool: &PoolHandle<O>) {
        let guard = epoch::pin(pool);
        let node = some_or!(self.is_incomplete(client, value, &guard, pool), return);

        while self.try_enqueue(node, &guard, pool).is_err() {}
    }

    fn is_incomplete<'g, O: POp>(
        &self,
        client: &Enqueue<T>,
        value: T,
        guard: &'g Guard<'_>,
        pool: &PoolHandle<O>,
    ) -> Option<PShared<'g, Node<T>>> {
        let mine = client.mine.load(Ordering::SeqCst, guard);

        // (1) 첫 번째 실행
        if mine.is_null() {
            let n = POwned::new(Node::from(value), pool).into_shared(guard);
            persist_obj(unsafe { n.deref(pool) }, true);

            client.mine.store(n, Ordering::SeqCst);
            persist_obj(&client.mine, true);
            return Some(n);
        }

        // (2) stack 안에 있으면 enqueue된 것이다 (Direct tracking)
        if self.search(mine, guard, pool) {
            return None;
        }

        // (3) 이미 dequeue 되었다면 enqueue된 것이다
        let node_ref = unsafe { mine.deref(pool) };
        let null: *const Dequeue<T> = ptr::null();
        if node_ref.dequeuer.load(Ordering::SeqCst) != null as usize {
            return None;
        }

        Some(mine)
    }

    /// tail에 새 `node` 연결을 시도
    fn try_enqueue<O: POp>(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard<'_>,
        pool: &PoolHandle<O>,
    ) -> Result<(), ()> {
        let tail = self.tail.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.deref(pool) };
        let next = tail_ref.next.load(Ordering::SeqCst, guard);

        if tail == self.tail.load(Ordering::SeqCst, guard) {
            if next.is_null() {
                return tail_ref
                    .next
                    .compare_exchange(
                        PShared::null(),
                        node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    )
                    .map(|_| {
                        persist_obj(&tail_ref.next, true);
                        let _ = self.tail.compare_exchange(
                            tail,
                            node,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                    })
                    .map_err(|_| ());
            } else {
                persist_obj(&tail_ref.next, true);
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            }
        }

        Err(())
    }

    /// `node`가 Queue 안에 있는지 head부터 tail까지 순회하며 검색
    fn search<O: POp>(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard<'_>,
        pool: &PoolHandle<O>,
    ) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard);

        // TODO: null 나올 때까지 하지 않고 tail을 통해서 범위를 제한할 수 있을지?
        while !curr.is_null() {
            if curr == node {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    /// `dequeue()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;

    fn dequeue<O: POp>(&self, client: &mut Dequeue<T>, pool: &PoolHandle<O>) -> Option<T> {
        let guard = epoch::pin(pool);
        let target = client.target.load(Ordering::SeqCst, &guard);

        if target.tag() == Self::EMPTY {
            // post-crash execution (empty)
            return None;
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };

            // node가 정말 내가 dequeue한 게 맞는지 확인
            if target_ref.dequeuer.load(Ordering::SeqCst) == client.id(pool) {
                return Some(Self::finish_dequeue(target_ref));
            }
        }

        loop {
            if let Ok(v) = self.try_dequeue(client, &guard, pool) {
                return v;
            }
        }
    }

    /// head를 dequeue 시도
    fn try_dequeue<O: POp>(
        &self,
        client: &mut Dequeue<T>,
        guard: &Guard<'_>,
        pool: &PoolHandle<O>,
    ) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::SeqCst, guard);
        let tail = self.tail.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head.deref(pool) };
        let next = head_ref.next.load(Ordering::SeqCst, guard);

        if head == self.head.load(Ordering::SeqCst, guard) {
            if head == tail {
                // empty queue
                if next.is_null() {
                    client
                        .target
                        .store(PShared::null().with_tag(Self::EMPTY), Ordering::SeqCst);
                    persist_obj(&client.target, true);
                    return Ok(None);
                }

                let tail_ref = unsafe { tail.deref(pool) };
                persist_obj(&tail_ref.next, true);

                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                return Err(());
            } else {
                // 우선 내가 dequeue할 node를 가리킴
                client.target.store(next, Ordering::SeqCst);
                persist_obj(&client.target, true);

                // 실제로 dequeue 함
                let next_ref = unsafe { next.deref(pool) };
                return next_ref
                    .dequeuer
                    .compare_exchange(
                        Self::no_dequeuer(),
                        client.id(pool),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .map(|_| {
                        persist_obj(&next_ref.dequeuer, true);
                        let _ = self.head.compare_exchange(
                            head,
                            next,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        );
                        Some(Self::finish_dequeue(next_ref))
                    })
                    .map_err(|_| {
                        let h = self.head.load(Ordering::SeqCst, guard);
                        if h == head {
                            persist_obj(&next_ref.dequeuer, true);
                            let _ = self.head.compare_exchange(
                                head,
                                next,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            );
                        }
                    });
            }
        }

        Err(())
    }

    fn finish_dequeue(node: &Node<T>) -> T {
        unsafe { (*node.data.as_ptr()).clone() }
        // TODO: free node
    }

    #[inline]
    fn no_dequeuer() -> usize {
        let null: *const Dequeue<T> = ptr::null();
        null as usize
    }
}

#[cfg(test)]
mod test {
    use crossbeam_utils::thread;
    use serial_test::serial;

    use crate::utils::tests::*;

    use super::*;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    struct RootOp {
        // PAtomic인 이유
        // - Queue 초기화시 PoolHandle을 넘겨줘야하는데, Default로는 그게 안됌
        // - 따라서 일단 null로 초기화한 후 이후에 실제로 Queue 초기화
        //
        // TODO: 위처럼 adhoc한 방법 말고 더 나은 solution으로 바꾸기 (https://cp-git.kaist.ac.kr/persistent-mem/compositional-persistent-object/-/issues/74)
        queue: PAtomic<Queue<usize>>,

        enqs: [[Enqueue<usize>; COUNT]; NR_THREAD],
        deqs: [[Dequeue<usize>; COUNT]; NR_THREAD],
    }

    impl Default for RootOp {
        fn default() -> Self {
            Self {
                queue: PAtomic::null(),
                enqs: array_init::array_init(|_| {
                    array_init::array_init(|_| Enqueue::<usize>::default())
                }),
                deqs: array_init::array_init(|_| {
                    array_init::array_init(|_| Dequeue::<usize>::default())
                }),
            }
        }
    }

    impl RootOp {
        fn init<O: POp>(&self, pool: &PoolHandle<O>) {
            let guard = unsafe { epoch::unprotected(&pool) };
            let q = self.queue.load(Ordering::SeqCst, guard);

            // Initialize queue
            if q.is_null() {
                let q = Queue::<usize>::new(pool);
                // TODO: 여기서 crash나면 leak남
                self.queue.store(q, Ordering::SeqCst);
            }
        }
    }

    impl POp for RootOp {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        /// idempotent enq_deq
        fn run<'o, O: POp>(
            &mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &PoolHandle<O>,
        ) -> Result<Self::Output<'o>, Self::Error> {
            self.init(pool);

            // Alias
            let guard = unsafe { epoch::unprotected(&pool) };
            let (q, enqs, deqs) = (
                unsafe { self.queue.load(Ordering::SeqCst, guard).deref(pool) },
                &mut self.enqs,
                &mut self.deqs,
            );

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let enq_arr = unsafe {
                        (enqs.get_unchecked_mut(tid) as *mut [Enqueue<usize>])
                            .as_mut()
                            .unwrap()
                    };
                    let deq_arr = unsafe {
                        (deqs.get_unchecked_mut(tid) as *mut [Dequeue<usize>])
                            .as_mut()
                            .unwrap()
                    };

                    let _ = scope.spawn(move |_| {
                        for i in 0..COUNT {
                            let _ = enq_arr[i].run(q, tid, pool);
                            assert!(deq_arr[i].run(q, (), pool).unwrap().is_some());
                        }
                    });
                }
            })
            .unwrap();

            // Check empty
            assert!(q.dequeue(&mut Dequeue::default(), pool).is_none());

            // Check results
            let mut results = vec![0_usize; NR_THREAD];
            for deq_arr in deqs.iter_mut() {
                for deq in deq_arr.iter_mut() {
                    let ret = deq.run(&q, (), pool).unwrap().unwrap();
                    results[ret] += 1;
                }
            }

            assert!(results.iter().all(|r| *r == COUNT));
            Ok(())
        }

        fn reset(&mut self, _: bool) {
            // no-op
        }
    }

    impl TestRootOp for RootOp {}

    const FILE_NAME: &str = "enq_deq.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // TODO: stack의 enq_deq과 합치기
    // 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn enq_deq() {
        run_test::<RootOp, _>(FILE_NAME, FILE_SIZE)
    }
}
