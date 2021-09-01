//! Persistent queue

use core::sync::atomic::{AtomicUsize, Ordering};
use etrace::some_or;
use std::{mem::MaybeUninit, ptr};

use crate::pepoch::{self as epoch, Guard, PAtomic, POwned, PShared};
use crate::persistent::*;
use crate::plocation::pool::PoolHandle;

struct Node<T: Clone> {
    data: MaybeUninit<T>,
    next: PAtomic<Node<T>>,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        let null: *const Pop<T> = ptr::null();
        Self {
            data: MaybeUninit::uninit(),
            next: PAtomic::null(),
            popper: AtomicUsize::new(null as usize),
        }
    }
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: MaybeUninit::new(value),
            next: PAtomic::null(),
            popper: AtomicUsize::new(Queue::<T>::no_popper()),
        }
    }
}

/// `Queue`의 Push operation
#[derive(Debug)]
pub struct Push<T: Clone> {
    /// push를 위해 할당된 node
    mine: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Push<T> {
    fn default() -> Self {
        Self {
            mine: PAtomic::null(),
        }
    }
}

impl<T: Clone> POp for Push<T> {
    type Object = Queue<T>;
    type Input = T;
    type Output = ();

    fn run(&mut self, queue: &Self::Object, value: Self::Input, pool: &PoolHandle) -> Self::Output {
        queue.push(self, value, pool);
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node
        self.mine.store(PShared::null(), Ordering::SeqCst);
    }
}

/// `Queue`의 Pop operation
#[derive(Debug)]
pub struct Pop<T: Clone> {
    /// pop의 타겟이 되는 node
    target: PAtomic<Node<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            target: PAtomic::null(),
        }
    }
}

impl<T: Clone> POp for Pop<T> {
    type Object = Queue<T>;
    type Input = ();
    type Output = Option<T>;

    fn run(&mut self, queue: &Self::Object, _: Self::Input, pool: &PoolHandle) -> Self::Output {
        queue.pop(self, pool)
    }

    fn reset(&mut self, _: bool) {
        // TODO: if node has not been freed, check if the node is mine and free it
        self.target.store(PShared::null(), Ordering::SeqCst);
    }
}

impl<T: Clone> Pop<T> {
    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴때마다 주소바뀌니 상대주소로 식별해야함
        pool.get_persistent_addr(self as *const Self as usize)
            .unwrap()
    }
}

/// Peristent queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: PAtomic<Node<T>>,
    tail: PAtomic<Node<T>>,
}

impl<T: Clone> Queue<T> {
    /// new
    pub fn new(pool: &PoolHandle) -> Self {
        let sentinel = Node::default();
        unsafe {
            let guard = epoch::unprotected(pool);
            let sentinel = POwned::new(sentinel, pool).into_shared(guard);
            Self {
                head: PAtomic::from(sentinel),
                tail: PAtomic::from(sentinel),
            }
        }
    }

    // TODO: try mode
    fn push(&self, client: &mut Push<T>, value: T, pool: &PoolHandle) {
        let guard = epoch::pin(pool);
        let node = some_or!(self.is_incomplete(client, value, &guard, pool), return);

        while self.try_push(node, &guard, pool).is_err() {}
    }

    fn is_incomplete<'g>(
        &self,
        client: &Push<T>,
        value: T,
        guard: &'g Guard<'_>,
        pool: &PoolHandle,
    ) -> Option<PShared<'g, Node<T>>> {
        let mine = client.mine.load(Ordering::SeqCst, guard);

        // (1) 첫 번째 실행
        if mine.is_null() {
            let n = POwned::new(Node::from(value), pool).into_shared(guard);

            client.mine.store(n, Ordering::SeqCst);
            return Some(n);
        }

        // (2) stack 안에 있으면 push된 것이다 (Direct tracking)
        if self.search(mine, guard, pool) {
            return None;
        }

        // (3) 이미 pop 되었다면 push된 것이다
        let node_ref = unsafe { mine.deref(pool) };
        let null: *const Pop<T> = ptr::null();
        if node_ref.popper.load(Ordering::SeqCst) != null as usize {
            return None;
        }

        Some(mine)
    }

    /// tail에 새 `node` 연결을 시도
    fn try_push(
        &self,
        node: PShared<'_, Node<T>>,
        guard: &Guard<'_>,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        let tail = self.tail.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.deref(pool) };
        let next = tail_ref.next.load(Ordering::SeqCst, guard);

        if !next.is_null() {
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
            return Err(());
        }

        tail_ref
            .next
            .compare_exchange(
                PShared::null(),
                node,
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            )
            .map(|_| {
                let _ = self.tail.compare_exchange(
                    tail,
                    node,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
            .map_err(|_| ())
    }

    /// `node`가 Queue 안에 있는지 head부터 tail까지 순회하며 검색
    fn search(&self, node: PShared<'_, Node<T>>, guard: &Guard<'_>, pool: &PoolHandle) -> bool {
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

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;

    fn pop(&self, client: &mut Pop<T>, pool: &PoolHandle) -> Option<T> {
        let guard = epoch::pin(pool);
        let target = client.target.load(Ordering::SeqCst, &guard);

        if target.tag() == Self::EMPTY {
            // post-crash execution (empty)
            return None;
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref(pool) };

            // node가 정말 내가 pop한 게 맞는지 확인
            if target_ref.popper.load(Ordering::SeqCst) == client.id(pool) {
                return Some(Self::finish_pop(target_ref));
            }
        }

        loop {
            if let Ok(v) = self.try_pop(client, &guard, pool) {
                return v;
            }
        }
    }

    /// head를 pop 시도
    fn try_pop(
        &self,
        client: &mut Pop<T>,
        guard: &Guard<'_>,
        pool: &PoolHandle,
    ) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head.deref(pool) };
        let next = head_ref.next.load(Ordering::SeqCst, guard);
        let next_ref = some_or!(unsafe { next.as_ref(pool) }, {
            client
                .target
                .store(PShared::null().with_tag(Self::EMPTY), Ordering::SeqCst);
            return Ok(None);
        });

        let tail = self.tail.load(Ordering::SeqCst, guard);
        if tail == head {
            let _ =
                self.tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::SeqCst, guard);
        }

        // 우선 내가 pop할 node를 가리키고
        client.target.store(next, Ordering::SeqCst);

        next_ref
            .popper
            .compare_exchange(
                Self::no_popper(),
                client.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                let _ = self.head.compare_exchange(
                    head,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
                Some(Self::finish_pop(next_ref))
            })
            .map_err(|_| ())
    }

    fn finish_pop(node: &Node<T>) -> T {
        unsafe { node.data.as_ptr().as_ref().unwrap() }.clone()
        // free node
    }

    #[inline]
    fn no_popper() -> usize {
        let null: *const Pop<T> = ptr::null();
        null as usize
    }
}

#[cfg(test)]
mod test {
    use crossbeam_utils::thread;
    use serial_test::serial;

    use crate::plocation::pool::Pool;
    use crate::utils::tests::get_test_path;

    use super::*;

    #[derive(Default)]
    struct RootOp {
        // PAtomic인 이유
        // - Queue 초기화시 PoolHandle을 넘겨줘야하는데, Default로는 그게 안됌
        // - 따라서 일단 null로 초기화한 후 이후에 실제로 Queue 초기화
        queue: PAtomic<Queue<usize>>,

        // PAtomic인 이유
        // - NR_TRHEAD * COUNT 수만큼 전부 정적할당하려하니 stack 터지는 등 잘 안됐음
        // - 따라서 임시로 동적할당 사용
        pushes: [PAtomic<[MaybeUninit<Push<usize>>]>; NR_THREAD],
        pops: [PAtomic<[MaybeUninit<Pop<usize>>]>; NR_THREAD],
    }

    impl RootOp {
        fn init(&self, pool: &PoolHandle) {
            let guard = unsafe { epoch::unprotected(&pool) };

            // Initialize queue
            let q = self.queue.load(Ordering::SeqCst, guard);
            if q.is_null() {
                let q = POwned::new(Queue::<usize>::new(pool), pool);
                // TODO: 여기서 crash나면 leak남
                self.queue.store(q, Ordering::SeqCst);
            }

            // Initialize Push/Pop
            for i in 0..NR_THREAD {
                let (push, pop) = (
                    self.pushes[i].load(Ordering::SeqCst, guard),
                    self.pops[i].load(Ordering::SeqCst, guard),
                );
                if push.is_null() {
                    let mut owned = POwned::<[MaybeUninit<Push<usize>>]>::init(COUNT, &pool);
                    // TODO: 여기서 crash나면 leak남
                    let refs = unsafe { owned.deref_mut(pool) };
                    for i in 0..COUNT {
                        refs[i] = MaybeUninit::new(Push::default());
                    }
                    self.pushes[i].store(owned, Ordering::SeqCst);
                }
                if pop.is_null() {
                    let mut owned = POwned::<[MaybeUninit<Pop<usize>>]>::init(COUNT, &pool);
                    // TODO: 여기서 crash나면 leak남
                    let refs = unsafe { owned.deref_mut(pool) };
                    for i in 0..COUNT {
                        refs[i] = MaybeUninit::new(Pop::default());
                    }
                    self.pops[i].store(owned, Ordering::SeqCst);
                }
            }
        }
    }

    impl POp for RootOp {
        type Object = ();
        type Input = ();
        type Output = Result<(), ()>;

        /// idempotent push_pop
        fn run(&mut self, _: &Self::Object, _: Self::Input, pool: &PoolHandle) -> Self::Output {
            self.init(pool);
            let guard = unsafe { epoch::unprotected(&pool) };
            let q = unsafe { self.queue.load(Ordering::SeqCst, guard).deref(pool) };

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let (pushes, pops) = unsafe {
                        (
                            self.pushes[tid]
                                .load(Ordering::SeqCst, guard)
                                .deref_mut(pool),
                            self.pops[tid].load(Ordering::SeqCst, guard).deref_mut(pool),
                        )
                    };
                    let _ = scope.spawn(move |_| {
                        for i in 0..COUNT {
                            let (push, pop) = unsafe {
                                (
                                    pushes.get_unchecked_mut(i).as_mut_ptr().as_mut().unwrap(),
                                    pops.get_unchecked_mut(i).as_mut_ptr().as_mut().unwrap(),
                                )
                            };

                            push.run(q, tid, pool);
                            assert!(pop.run(q, (), pool).is_some());
                        }
                    });
                }
            })
            .unwrap();

            // Check empty
            let mut pop = POwned::new(Pop::default(), pool);
            assert!(q.pop(unsafe { pop.deref_mut(pool) }, pool).is_none());

            // Check results
            let mut results = vec![0_usize; NR_THREAD];
            for pops in self.pops.iter_mut() {
                let pops = unsafe { pops.load(Ordering::SeqCst, guard).deref_mut(pool) };
                for pop in pops {
                    let pop = unsafe { pop.as_mut_ptr().as_mut().unwrap() };
                    let ret = pop.run(&q, (), pool).unwrap();
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

    const FILE_NAME: &str = "push_pop.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    // TODO: stack의 push_pop과 합치기
    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn push_pop() {
        let filepath = get_test_path(FILE_NAME);

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = Pool::open(&filepath)
            .unwrap_or_else(|_| Pool::create::<RootOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 op 가져오기
        let mut root_ptr = pool_handle.get_root::<RootOp>().unwrap();
        let root_op = unsafe { root_ptr.deref_mut(&pool_handle) };

        // 루트 op 실행
        root_op.run(&(), (), &pool_handle).unwrap();
    }
}
