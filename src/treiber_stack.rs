//! Persistent stack based on Treiber stack

// TODO(SMR 적용):
// - SMR 만든 후 crossbeam 걷어내기
// - 현재는 persistent guard가 없어서 lifetime도 이상하게 박혀 있음

// TODO(pmem 사용(#31, #32)):
// - persist를 위해 flush/fence 추가
// - persistent location 위에서 동작

// TODO(Ordering):
// - Ordering 최적화

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use etrace::some_or;
use std::ptr;

use crate::persistent::*;

struct Node<T> {
    data: T,
    next: Atomic<Node<T>>,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

/// `TreiberStack::push()`를 호출할 때 쓰일 client
/// `TreiberStack::push(client, (val, is_try))`:
/// - Treiber stack에 `val`을 삽입함
/// - `is_try`가 `true`라면 1회만 시도
#[derive(Debug, Clone)]
pub struct PushClient<T> {
    /// push를 위해 할당된 node
    node: Atomic<Node<T>>,
}

impl<T> Default for PushClient<T> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<T> PersistentClient for PushClient<T> {
    fn reset(&mut self) {
        // TODO: if not finished -> free node
        self.node.store(Shared::null(), Ordering::SeqCst);
    }
}

/// `TreiberStack::pop()`를 호출할 때 쓰일 client
/// `TreiberStack::pop(client, is_try)`:
/// - Treiber stack에서 top node의 아이템을 반환함
/// - 비어 있을 경우 `Ok(None)`을 반환
/// - `is_try`가 `true`라면 1회만 시도 -> 실패시 `Err(())` 반환
#[derive(Debug, Clone)]
pub struct PopClient<T> {
    /// pup를 위해 할당된 node
    node: Atomic<Node<T>>,
}

impl<T> Default for PopClient<T> {
    fn default() -> Self {
        Self {
            node: Atomic::null(),
        }
    }
}

impl<T> PersistentClient for PopClient<T> {
    fn reset(&mut self) {
        self.node.store(Shared::null(), Ordering::SeqCst);
    }
}

/// Persistent Treiber stack
#[derive(Debug)]
pub struct TreiberStack<T> {
    top: Atomic<Node<T>>,
}

impl<T> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: Atomic::null(),
        }
    }
}

impl<T> TreiberStack<T> {
    /// `push()` 결과 중 trying/failure를 표시하기 위한 태그
    const TRYING: usize = 0; // default
    const FAIL: usize = 1;

    fn push(&self, client: &mut PushClient<T>, val: T, is_try: bool) -> Result<(), ()> {
        let guard = crossbeam_epoch::pin();
        let node = some_or!(self.is_incomplete(client, val, &guard), return Ok(()));

        if is_try {
            self.try_push_inner(node, &guard).map_err(|_| {
                // 재시도시 불필요한 search를 피하기 위한 FAIL 결과 태깅
                client
                    .node
                    .store(node.with_tag(Self::FAIL), Ordering::SeqCst);
            })
        } else {
            while self.try_push_inner(node, &guard).is_err() {}
            Ok(())
        }
    }

    /// client의 push 작업이 이미 끝났는지 체크
    /// 끝나지 않았다면 Some(`push 할 node_ptr`) 반환
    fn is_incomplete<'g>(
        &self,
        client: &PushClient<T>,
        val: T,
        guard: &'g Guard,
    ) -> Option<Shared<'g, Node<T>>> {
        let node = client.node.load(Ordering::SeqCst, guard);

        // (1) first execution
        if node.is_null() {
            let null: *const PopClient<T> = ptr::null();
            let n = Owned::new(Node {
                data: val,
                next: Atomic::null(),
                popper: AtomicUsize::new(null as usize),
            })
            .into_shared(guard);
            client.node.store(n, Ordering::SeqCst);
            return Some(n);
        }

        // (2) tag가 FAIL이면 `try_push()` 실패했던 것이다
        if node.tag() == 1 {
            client
                .node
                .store(node.with_tag(Self::TRYING), Ordering::SeqCst);
            return Some(node.with_tag(Self::TRYING));
        }

        // (3) stack 안에 있으면 push된 것이다 (Direct tracking)
        if self.search(node, guard) {
            return None;
        }

        // (4) 이미 pop 되었다면 push된 것이다
        let node_ref = unsafe { node.deref() };
        let null: *const PopClient<T> = ptr::null();
        if node_ref.popper.load(Ordering::SeqCst) != null as usize {
            return None;
        }

        Some(node)
    }

    /// top에 새 `node` 연결을 시도
    fn try_push_inner(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> Result<(), ()> {
        let node_ref = unsafe { node.deref() };
        let top = self.top.load(Ordering::SeqCst, guard);

        node_ref.next.store(top, Ordering::SeqCst);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|_| ())
    }

    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr.as_raw() == node.as_raw() {
                return true;
            }

            let curr_ref = unsafe { curr.deref() };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 1;

    fn pop(&self, client: &mut PopClient<T>, is_try: bool) -> Result<Option<T>, ()> {
        let guard = crossbeam_epoch::pin();

        let node = client.node.load(Ordering::SeqCst, &guard);

        if node.tag() == Self::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !node.is_null() {
            // post-crash execution (trying)
            let node_ref = unsafe { node.deref() };
            let my_id = client as *const PopClient<T> as usize;

            // node가 정말 내가 pop한 게 맞는지 확인
            if node_ref.popper.load(Ordering::SeqCst) == my_id {
                return Ok(Some(unsafe { ptr::read(&node_ref.data as *const _) }));
                // TODO: free node
            };
        }

        let mut top = self.top.load(Ordering::SeqCst, &guard);
        loop {
            match self.try_pop_inner(client as *const _ as usize, &client.node, top, &guard) {
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => return Ok(None),
                Err(_) if is_try => return Err(()),
                Err(Some(new_top)) => top = new_top,
                Err(None) => top = self.top.load(Ordering::SeqCst, &guard),
            }
        }
    }

    /// top node를 pop 시도
    fn try_pop_inner<'g>(
        &self,
        client_id: usize,
        client_node: &Atomic<Node<T>>,
        top: Shared<'_, Node<T>>,
        guard: &'g Guard,
    ) -> Result<Option<T>, Option<Shared<'g, Node<T>>>> {
        let top_ref = some_or!(unsafe { top.as_ref() }, {
            // empty
            client_node.store(Shared::null().with_tag(Self::EMPTY), Ordering::SeqCst);
            return Ok(None);
        });

        // 우선 내가 top node를 가리키고
        client_node.store(top, Ordering::SeqCst);

        // top node에 내 이름 새겨넣음
        let null: *const PopClient<T> = ptr::null();
        let pop_succ = top_ref
            .popper
            .compare_exchange(null as usize, client_id, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();

        // top node를 next로 바꿈
        let next = top_ref.next.load(Ordering::SeqCst, guard);
        let top_set_succ = self
            .top
            .compare_exchange(top, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            .is_ok();

        match (pop_succ, top_set_succ) {
            (true, _) => {
                // TODO: free node
                Ok(Some(unsafe { ptr::read(&top_ref.data as *const _) })) // popped
            }
            (false, true) => Err(Some(next)), // I changed top -> return the next top
            (false, false) => Err(None),      // Someone changed top
        }
    }
}

impl<T> PersistentOp<PushClient<T>> for TreiberStack<T> {
    type Input = (T, bool); // value, try mode
    type Output = Result<(), ()>;

    fn persistent_op(&self, client: &mut PushClient<T>, input: Self::Input) -> Self::Output {
        self.push(client, input.0, input.1)
    }
}

impl<T> PersistentOp<PopClient<T>> for TreiberStack<T> {
    type Input = bool; // try mode
    type Output = Result<Option<T>, ()>;

    fn persistent_op(&self, client: &mut PopClient<T>, input: Self::Input) -> Self::Output {
        self.pop(client, input)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_utils::thread;

    #[test]
    fn push_pop() {
        const NR_THREAD: usize = 4;
        const COUNT: usize = 1_000_000;

        let stack = TreiberStack::<usize>::default(); // TODO(persistent location)
        let mut push_clients = vec![vec!(PushClient::<usize>::default(); COUNT); NR_THREAD]; // TODO(persistent location)
        let mut pop_clients = vec![vec!(PopClient::<usize>::default(); COUNT); NR_THREAD]; // TODO(persistent location)

        // 아래 로직은 idempotent 함

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let stack_ref = &stack;
                let push_vec = unsafe {
                    (push_clients.get_unchecked_mut(tid) as *mut Vec<PushClient<usize>>)
                        .as_mut()
                        .unwrap()
                };
                let pop_vec = unsafe {
                    (pop_clients.get_unchecked_mut(tid) as *mut Vec<PopClient<usize>>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..COUNT {
                        let _ = stack_ref.persistent_op(&mut push_vec[i], (tid, false));
                        assert!(stack_ref
                            .persistent_op(&mut pop_vec[i], false)
                            .unwrap()
                            .is_some());
                    }
                });
            }
        })
        .unwrap();

        // Check empty
        assert!(stack
            .persistent_op(&mut PopClient::<usize>::default(), false)
            .unwrap()
            .is_none());

        // Check results
        let mut results = vec![0_usize; NR_THREAD];
        for client_vec in pop_clients.iter_mut() {
            for client in client_vec.iter_mut() {
                let ret = stack.persistent_op(client, false).unwrap().unwrap();
                results[ret] += 1;
            }
        }

        for &r in results.iter() {
            assert_eq!(r, COUNT);
        }
    }
}
