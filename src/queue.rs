//! Persistent queue

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_utils::CachePadded;
use etrace::some_or;
use std::{mem::MaybeUninit, ptr};

use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{ll::*, pool::*, ptr::*};

// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
// - 문제: data가 uninit인지 init인지 구분불가
//      - uninit인데 init된 T로 취급하고 T::mark를 호출하면 leak 유발 (쓰레기 값을 mark하며 사용중이지 않은 블록도 mark될 수 있음)
// - 해결방안
//      1. data 타입을 Option<T>로 바꾸기
//      2. mark() 함수 인자에 flag를 추가하여, false면 Ralloc에게 넘기는 filter function을 no-op으로 넘기기
//          Queue::filter에서 head==tail이면(i.e. sentinel 노드만 있다면) false로 노드 mark
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

impl<T: Clone> Collectable for Node<T> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next = unsafe { next.deref_mut(pool) };
            Node::<T>::mark(next, gc);
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

impl<T: Clone> Collectable for Enqueue<T> {
    fn filter(enq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut mine = enq.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            let mine = unsafe { mine.deref_mut(pool) };
            Node::<T>::mark(mine, gc);
        }
    }
}

impl<T: 'static + Clone> Memento for Enqueue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = T;
    type Output<'o> = ();
    type Error = !;

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        value: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        queue.enqueue(self, value, guard, pool);
        Ok(())
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, _: &'static PoolHandle) {
        let mine = self.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            self.mine.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.mine, true);

            // crash-free execution이니 내가 가지고 있던 노드는 enq 되었음이 확실 => 내가 free하면 안됨
        }
    }
}

impl<T: Clone> Drop for Enqueue<T> {
    fn drop(&mut self) {
        let mine = self
            .mine
            .load(Ordering::SeqCst, unsafe { epoch::unprotected() });
        assert!(mine.is_null(), "reset 되어있지 않음.")
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

impl<T: Clone> Collectable for Dequeue<T> {
    fn filter(deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut target = deq.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target = unsafe { target.deref_mut(pool) };
            Node::<T>::mark(target, gc);
        }
    }
}

impl<T: 'static + Clone> Memento for Dequeue<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = ();
    type Output<'o> = Option<T>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        (): Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        Ok(queue.dequeue(self, guard, pool))
    }

    fn reset(&mut self, _: bool, guard: &mut Guard, _: &'static PoolHandle) {
        let target = self.target.load(Ordering::SeqCst, guard);

        if target.tag() == Queue::<T>::EMPTY {
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);
            return;
        }

        if !target.is_null() {
            // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
            // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
            self.target.store(PShared::null(), Ordering::SeqCst);
            persist_obj(&self.target, true);

            // crash-free execution이니 내가 deq 성공한 노드임이 확실 => 내가 free
            unsafe { guard.defer_pdestroy(target) };
        }
    }
}

impl<T: Clone> Dequeue<T> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        unsafe { self.as_pptr(pool).into_offset() }
    }
}

impl<T: Clone> Drop for Dequeue<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let target = self.target.load(Ordering::SeqCst, guard);
        assert!(target.is_null(), "reset 되어있지 않음.")
    }
}

/// empty가 아닐 때에*만* return 하는 dequeue operation
// TODO: 현재는 sub Memento으로 Dequeue을 재사용하도록 구현되어 있음 (EMPTY 기록하는 오버헤드 발생)
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

impl<T: Clone> Collectable for DequeueSome<T> {
    fn filter(deqsome: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr
        let mut target = deqsome.deq.target.load(Ordering::SeqCst, guard);
        if !target.is_null() {
            let target = unsafe { target.deref_mut(pool) };
            Node::<T>::mark(target, gc);
        }
    }
}

impl<T: 'static + Clone> Memento for DequeueSome<T> {
    type Object<'o> = &'o Queue<T>;
    type Input = ();
    type Output<'o> = T;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        queue: Self::Object<'o>,
        (): Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        loop {
            if let Ok(Some(v)) = self.deq.run(queue, (), guard, pool) {
                return Ok(v);
            }
            self.deq.reset(false, guard, pool);
        }
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        self.deq.reset(nested, guard, pool);
    }
}

impl<T: Clone> Drop for DequeueSome<T> {
    fn drop(&mut self) {
        todo!(
            "deq가 reset 되어있지 않으면 panic. is_reset API 파서 deq.is_reset()으로 확인해야 할듯"
        )
    }
}

/// Peristent queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: CachePadded<PAtomic<Node<T>>>,
    tail: CachePadded<PAtomic<Node<T>>>,
}

impl<T: Clone> Collectable for Queue<T> {
    fn filter(queue: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut head = queue.head.load(Ordering::SeqCst, guard);
        if !head.is_null() {
            let head = unsafe { head.deref_mut(pool) };
            Node::<T>::mark(head, gc);
        }
    }
}

impl<T: Clone> Queue<T> {
    /// new
    // TODO: alloc, init 구상한 후 시그니처 변경
    pub fn new(pool: &PoolHandle) -> POwned<Self> {
        let guard = unsafe { epoch::unprotected() };
        let sentinel = POwned::new(Node::default(), pool).into_shared(guard);
        persist_obj(unsafe { sentinel.deref(pool) }, true);

        let ret = POwned::new(
            Self {
                head: CachePadded::new(PAtomic::from(sentinel)),
                tail: CachePadded::new(PAtomic::from(sentinel)),
            },
            pool,
        );
        persist_obj(unsafe { ret.deref(pool) }, true);
        ret
    }

    fn enqueue(&self, client: &mut Enqueue<T>, value: T, guard: &Guard, pool: &PoolHandle) {
        let node = some_or!(self.is_incomplete(client, value, guard, pool), return);

        while self.try_enqueue(node, guard, pool).is_err() {}
    }

    fn is_incomplete<'g>(
        &self,
        client: &Enqueue<T>,
        value: T,
        guard: &'g Guard,
        pool: &PoolHandle,
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
    fn try_enqueue(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard,
        pool: &PoolHandle,
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
                        // TODO: 여기서 tail을 persist 하는 건 핵손해. offline-gc phase에서 tail align을 해줘야 할 듯
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
    fn search(&self, node: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
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

    fn dequeue(&self, client: &mut Dequeue<T>, guard: &Guard, pool: &PoolHandle) -> Option<T> {
        let target = client.target.load(Ordering::SeqCst, guard);

        if target.tag() == Self::EMPTY {
            // post-crash execution (empty)
            return None;
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };
            let next = target_ref.next.load(Ordering::SeqCst, guard);
            let next_ref = unsafe { next.deref(pool) };

            // node가 정말 내가 dequeue한 게 맞는지 확인
            if next_ref.dequeuer.load(Ordering::SeqCst) == client.id(pool) {
                return Some(Self::finish_dequeue(next_ref));
            }
        }

        loop {
            if let Ok(v) = self.try_dequeue(client, guard, pool) {
                return v;
            }
        }
    }

    /// head를 dequeue 시도
    fn try_dequeue(
        &self,
        client: &mut Dequeue<T>,
        guard: &Guard,
        pool: &PoolHandle,
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
                client.target.store(head, Ordering::SeqCst);
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
                        persist_obj(&self.head, true);
                        Some(Self::finish_dequeue(next_ref))
                    })
                    .map_err(|_| {
                        let h = self.head.load(Ordering::SeqCst, guard);
                        if h == head {
                            persist_obj(&next_ref.dequeuer, true); // enqueuer에게 enqueue 됐다는 확신을 주기 위해 head advance 전에 persist 해야 함
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

    use crate::{plocation::ralloc::Collectable, utils::tests::*};

    use super::*;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 1_000_000;

    /// 여러 스레드가 각각 enqueue; dequeue 순서로 반복
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
        fn init(&self, pool: &PoolHandle) {
            let guard = unsafe { epoch::unprotected() };
            let q = self.queue.load(Ordering::SeqCst, guard);

            // Initialize queue
            if q.is_null() {
                let q = Queue::<usize>::new(pool);
                // TODO: 여기서 crash나면 leak남
                self.queue.store(q, Ordering::SeqCst);
            }
        }
    }

    impl Collectable for RootOp {
        fn filter(root: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            let guard = unsafe { epoch::unprotected() };

            // Mark valid ptr to trace
            //
            // Ralloc에 null ptr 걸러내는 로직 있지만, 우리도 체크해야함. 왜냐하면 우리 로직에서 null ptr를 deref_mut하면 절대주소 구할 때 overflow나기때문
            // TODO: 우리는 null 검사 안해도 되게하기. Ralloc에서 null ptr 거르는데 우리도 null 검사하는 게 불편함
            let mut queue = root.queue.load(Ordering::SeqCst, guard);
            if !queue.is_null() {
                let queue = unsafe { queue.deref_mut(pool) };
                Queue::mark(queue, gc);
            }
            for enq_arr in root.enqs.as_mut() {
                for enq in enq_arr {
                    Enqueue::filter(enq, gc, pool);
                }
            }
            for deq_arr in root.deqs.as_mut() {
                for deq in deq_arr {
                    Dequeue::filter(deq, gc, pool);
                }
            }
        }
    }

    impl Memento for RootOp {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        /// idempotent enq_deq
        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            guard: &mut Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            self.init(pool);

            // Alias
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
                        let mut guard = epoch::pin();
                        for i in 0..COUNT {
                            let _ = enq_arr[i].run(q, tid, &mut guard, pool);
                            assert!(deq_arr[i].run(q, (), &mut guard, pool).unwrap().is_some());
                        }
                    });
                }
            })
            .unwrap();

            // Check empty
            let mut guard = epoch::pin();
            assert!(Dequeue::<usize>::default()
                .run(q, (), &mut guard, pool)
                .unwrap()
                .is_none());

            // Check results
            let mut results = vec![0_usize; NR_THREAD];
            for deq_arr in deqs.iter_mut() {
                for deq in deq_arr.iter_mut() {
                    let ret = deq.run(&q, (), &mut guard, pool).unwrap().unwrap();
                    results[ret] += 1;
                }
            }

            assert!(results.iter().all(|r| *r == COUNT));
            Ok(())
        }

        fn reset(&mut self, _: bool, _: &mut Guard, _: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootOp for RootOp {}

    // TODO: stack의 enq_deq과 합치기
    // - 테스트시 Enqueue/Dequeue 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // - pool을 2번째 열 때부터 gc 동작 확인가능:
    //      - 출력문으로 COUNT * NR_THREAD + 2개의 block이 reachable하다고 나옴
    //      - 여기서 +2는 Root, Queue를 가리키는 포인터
    //
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    // TODO: root op 실행 로직 고치기 https://cp-git.kaist.ac.kr/persistent-mem/memento/-/issues/95
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn enq_deq() {
        const FILE_NAME: &str = "enq_deq.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<RootOp, _>(FILE_NAME, FILE_SIZE)
    }
}
