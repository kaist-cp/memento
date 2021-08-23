//! Persistent queue

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use etrace::some_or;
use std::{mem::MaybeUninit, ptr};

use crate::persistent::*;

struct Node<T: Clone> {
    data: MaybeUninit<T>,
    next: Atomic<Node<T>>,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> Default for Node<T> {
    fn default() -> Self {
        let null: *const Pop<T> = ptr::null();
        Self {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
            popper: AtomicUsize::new(null as usize),
        }
    }
}

/// `Queue`의 Push operation
#[derive(Debug)]
pub struct Push<T: Clone> {
    /// push를 위해 할당된 node
    mine: Atomic<Node<T>>,
}

impl<T: Clone> Default for Push<T> {
    fn default() -> Self {
        Self {
            mine: Atomic::null(),
        }
    }
}

impl<T: Clone> PersistentOp for Push<T> {
    type Object = Queue<T>;
    type Input = T;
    type Output = ();

    fn run(&mut self, queue: &Self::Object, value: Self::Input) -> Self::Output {
        queue.push(self, value);
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node
        self.mine.store(Shared::null(), Ordering::SeqCst);
    }
}

/// `Queue`의 Pop operation
#[derive(Debug)]
pub struct Pop<T: Clone> {
    /// pop의 타겟이 되는 node
    target: Atomic<Node<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            target: Atomic::null(),
        }
    }
}

impl<T: Clone> PersistentOp for Pop<T> {
    type Object = Queue<T>;
    type Input = ();
    type Output = Option<T>;

    fn run(&mut self, queue: &Self::Object, _: Self::Input) -> Self::Output {
        queue.pop(self)
    }

    fn reset(&mut self, _: bool) {
        // TODO: if node has not been freed, check if the node is mine and free it
        self.target.store(Shared::null(), Ordering::SeqCst);
    }
}

/// Peristent queue
#[derive(Debug)]
pub struct Queue<T: Clone> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
}

impl<T: Clone> Default for Queue<T> {
    fn default() -> Self {
        let sentinel = Node::default();
        unsafe {
            let guard = epoch::unprotected();
            let sentinel = Owned::new(sentinel).into_shared(guard);
            Self {
                head: Atomic::from(sentinel),
                tail: Atomic::from(sentinel),
            }
        }
    }
}

impl<T: Clone> Queue<T> {
    // TODO: try mode
    fn push(&self, client: &mut Push<T>, value: T) {
        let guard = epoch::pin();
        let node = some_or!(self.is_incomplete(client, value, &guard), return);

        while self.try_push(node, &guard).is_err() {}
    }

    fn is_incomplete<'g>(
        &self,
        client: &Push<T>,
        value: T,
        guard: &'g Guard,
    ) -> Option<Shared<'g, Node<T>>> {
        let mine = client.mine.load(Ordering::SeqCst, guard);

        // (1) 첫 번째 실행
        if mine.is_null() {
            let null: *const Pop<T> = ptr::null();
            let n = Owned::new(Node {
                data: MaybeUninit::new(value),
                next: Atomic::null(),
                popper: AtomicUsize::new(null as usize),
            })
            .into_shared(guard);

            client.mine.store(n, Ordering::SeqCst);
            return Some(n);
        }

        // (2) stack 안에 있으면 push된 것이다 (Direct tracking)
        if self.search(mine, guard) {
            return None;
        }

        // (3) 이미 pop 되었다면 push된 것이다
        let node_ref = unsafe { mine.deref() };
        let null: *const Pop<T> = ptr::null();
        if node_ref.popper.load(Ordering::SeqCst) != null as usize {
            return None;
        }

        Some(mine)
    }

    /// tail에 새 `node` 연결을 시도
    fn try_push(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> Result<(), ()> {
        let tail = self.tail.load(Ordering::SeqCst, guard);
        let tail_ref = unsafe { tail.deref() };
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
                Shared::null(),
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
    fn search(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> bool {
        let mut curr = self.head.load(Ordering::SeqCst, guard);

        // TODO: null 나올 때까지 하지 않고 tail을 통해서 범위를 제한할 수 있을지?
        while !curr.is_null() {
            if curr == node {
                return true;
            }

            let curr_ref = unsafe { curr.deref() };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;

    fn pop(&self, client: &mut Pop<T>) -> Option<T> {
        let guard = epoch::pin();
        let target = client.target.load(Ordering::SeqCst, &guard);

        if target.tag() == Self::EMPTY {
            // post-crash execution (empty)
            return None;
        }

        let id = client as *const _ as usize;

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref() };

            // node가 정말 내가 pop한 게 맞는지 확인
            if target_ref.popper.load(Ordering::SeqCst) == id {
                return Some(Self::finish_pop(target_ref));
            }
        }

        loop {
            if let Ok(v) = self.try_pop(client, &guard) {
                return v;
            }
        }
    }

    /// head를 pop 시도
    fn try_pop(&self, client: &mut Pop<T>, guard: &Guard) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::SeqCst, guard);
        let head_ref = unsafe { head.deref() };
        let next = head_ref.next.load(Ordering::SeqCst, guard);
        let next_ref = some_or!(unsafe { next.as_ref() }, {
            client
                .target
                .store(Shared::null().with_tag(Self::EMPTY), Ordering::SeqCst);
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

        // pop할 node의 popper를 바꿈
        let null: *const Pop<T> = ptr::null();
        let id = client as *const _ as usize;
        next_ref
            .popper
            .compare_exchange(null as usize, id, Ordering::SeqCst, Ordering::SeqCst)
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
}

#[cfg(test)]
mod test {
    use crossbeam_utils::thread;
    use serial_test::serial;

    use super::*;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    // TODO: stack의 push_pop과 합치기
    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn push_pop() {
        let q = Queue::<usize>::default(); // TODO(persistent location)
        let mut pushes: Vec<Vec<Push<usize>>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| Push::default()).collect())
            .collect(); // TODO(persistent location)
        let mut pops: Vec<Vec<Pop<usize>>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| Pop::default()).collect())
            .collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let q = &q;
                let push_vec = unsafe {
                    (pushes.get_unchecked_mut(tid) as *mut Vec<Push<usize>>)
                        .as_mut()
                        .unwrap()
                };
                let pop_vec = unsafe {
                    (pops.get_unchecked_mut(tid) as *mut Vec<Pop<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..COUNT {
                        push_vec[i].run(q, tid);
                        assert!(pop_vec[i].run(q, ()).is_some());
                    }
                });
            }
        })
        .unwrap();

        // Check empty
        assert!(q.pop(&mut Pop::default()).is_none());

        // Check results
        let mut results = vec![0_usize; NR_THREAD];
        for pop_vec in pops.iter_mut() {
            for pop in pop_vec.iter_mut() {
                let ret = pop.run(&q, ()).unwrap();
                results[ret] += 1;
            }
        }

        assert!(results.iter().all(|r| *r == COUNT));
    }
}
