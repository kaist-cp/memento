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

impl<T: Clone> POp<&Queue<T>> for Push<T> {
    type Input = T;
    type Output = ();

    fn run(&mut self, queue: &Queue<T>, value: Self::Input, pool: &PoolHandle) -> Self::Output {
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

impl<T: Clone> POp<&Queue<T>> for Pop<T> {
    type Input = ();
    type Output = Option<T>;

    fn run(&mut self, queue: &Queue<T>, _: Self::Input, pool: &PoolHandle) -> Self::Output {
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

    struct RootOp {
        // PAtomic인 이유
        // - Queue 초기화시 PoolHandle을 넘겨줘야하는데, Default로는 그게 안됌
        // - 따라서 일단 null로 초기화한 후 이후에 실제로 Queue 초기화
        //
        // TODO: 위처럼 adhoc한 방법 말고 더 나은 solution으로 바꾸기 (https://cp-git.kaist.ac.kr/persistent-mem/compositional-persistent-object/-/issues/74)
        queue: PAtomic<Queue<usize>>,

        pushes: [[Push<usize>; COUNT]; NR_THREAD],
        pops: [[Pop<usize>; COUNT]; NR_THREAD],
    }

    impl Default for RootOp {
        fn default() -> Self {
            Self {
                queue: PAtomic::null(),
                pushes: array_init::array_init(|_| {
                    array_init::array_init(|_| Push::<usize>::default())
                }),
                pops: array_init::array_init(|_| {
                    array_init::array_init(|_| Pop::<usize>::default())
                }),
            }
        }
    }

    impl RootOp {
        fn init(&self, pool: &PoolHandle) {
            let guard = unsafe { epoch::unprotected(&pool) };
            let q = self.queue.load(Ordering::SeqCst, guard);

            // Initialize queue
            if q.is_null() {
                let q = POwned::new(Queue::<usize>::new(pool), pool);
                // TODO: 여기서 crash나면 leak남
                self.queue.store(q, Ordering::SeqCst);
            }
        }
    }

    impl POp<()> for RootOp {
        type Input = ();
        type Output = Result<(), ()>;

        /// idempotent push_pop
        fn run(&mut self, _: (), _: Self::Input, pool: &PoolHandle) -> Self::Output {
            self.init(pool);

            // Alias
            let guard = unsafe { epoch::unprotected(&pool) };
            let (q, pushes, pops) = (
                unsafe { self.queue.load(Ordering::SeqCst, guard).deref(pool) },
                &mut self.pushes,
                &mut self.pops,
            );

            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let push_arr = unsafe {
                        (pushes.get_unchecked_mut(tid) as *mut [Push<usize>])
                            .as_mut()
                            .unwrap()
                    };
                    let pop_arr = unsafe {
                        (pops.get_unchecked_mut(tid) as *mut [Pop<usize>])
                            .as_mut()
                            .unwrap()
                    };

                    let _ = scope.spawn(move |_| {
                        for i in 0..COUNT {
                            push_arr[i].run(q, tid, pool);
                            assert!(pop_arr[i].run(q, (), pool).is_some());
                        }
                    });
                }
            })
            .unwrap();

            // Check empty
            assert!(q.pop(&mut Pop::default(), pool).is_none());

            // Check results
            let mut results = vec![0_usize; NR_THREAD];
            for pop_arr in pops.iter_mut() {
                for pop in pop_arr.iter_mut() {
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
    // 테스트시 Push/Pop 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn push_pop() {
        let filepath = get_test_path(FILE_NAME);

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = Pool::open(&filepath)
            .unwrap_or_else(|_| Pool::create::<RootOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 op 가져오기
        let root_op = pool_handle.get_root::<RootOp>().unwrap();

        // 루트 op 실행
        root_op.run((), (), &pool_handle).unwrap();
    }
}
