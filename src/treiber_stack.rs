//! Persistent stack based on Treiber stack

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use std::ptr;

use crate::persistent::*;

struct Node<T> {
    data: T,
    next: Atomic<Node<T>>,

    /// 누가 pop 했는지 식별 (popper를 통해 참조는 하지 않음)
    popper: AtomicUsize,
}

/// `Stack`의 `push()`를 호출할 때 쓰일 client
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
        self.node.store(Shared::null(), Ordering::SeqCst);
    }
}

/// `Stack`의 `pop()`를 호출할 때 쓰일 client
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
    /// Treiber stack에 `val`을 삽입함
    fn push(&self, client: &mut PushClient<T>, val: T) {
        let guard = &pin();

        let mut node = client.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            if self.is_finished(node, guard) {
                return;
            }
        } else {
            // Install a node for the first execution
            let null: *const PopClient<T> = ptr::null();
            node = Owned::new(Node {
                data: val,
                next: Atomic::null(),
                popper: AtomicUsize::new(null as usize), // CHECK: Does this guarantee uniqueness?
            })
            .into_shared(guard);
            client.node.store(node, Ordering::SeqCst);
        }

        while !self.try_push_inner(node, guard) {}
    }

    // TODO: pub try_push() 혹은 push에 count 파라미터 달기

    /// `node`의 push 작업이 이미 끝났는지 체크
    fn is_finished(&self, node: Shared<'_, Node<T>>, vguard: &Guard) -> bool {
        // (1) stack 안에 있으면 push된 것이다 (Direct tracking)
        if self.search(node, vguard) {
            return true;
        }

        // (2) 이미 pop 되었다면 push된 것이다
        let node_ref = unsafe { node.deref() };
        let null: *const PopClient<T> = ptr::null();
        if node_ref.popper.load(Ordering::SeqCst) != null as usize {
            return true;
        }

        false
    }

    /// top에 새 `node` 연결을 시도
    fn try_push_inner(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> bool {
        let node_ref = unsafe { node.deref() };
        let top = self.top.load(Ordering::SeqCst, guard);

        node_ref.next.store(top, Ordering::SeqCst);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .is_ok()
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

    /// Treiber stack에서 top node의 아이템을 반환함
    /// 비어 있을 경우 `None`을 반환
    fn pop(&self, client: &mut PopClient<T>) -> Option<T> {
        let guard = &pin();

        let node = client.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            // post-crash execution
            let node_ref = unsafe { node.deref() };
            let my_id = client as *const PopClient<T> as usize;

            // node가 정말 내가 pop한 게 맞는지 확인
            if node_ref.popper.load(Ordering::SeqCst) == my_id {
                return unsafe { Some(ptr::read(&node_ref.data as *const _)) };
                // TODO: free node
            };
        }

        let mut top = self.top.load(Ordering::SeqCst, guard);
        loop {
            match self.try_pop_inner(client as *const _ as usize, &client.node, top, guard) {
                Ok(Some(v)) => return Some(v),
                Ok(None) => return None,
                Err(Some(new_top)) => top = new_top,
                Err(None) => top = self.top.load(Ordering::SeqCst, guard),
            }
        }
    }

    // TODO: pub try_pop() 혹은 pop에 count 파라미터 달기

    /// top node를 pop 시도
    fn try_pop_inner<'g>(
        &self,
        client_id: usize,
        client_node: &Atomic<Node<T>>,
        top: Shared<'g, Node<T>>,
        guard: &'g Guard,
    ) -> Result<Option<T>, Option<Shared<'g, Node<T>>>> {
        let top_ref = some_or!(unsafe { top.as_ref() }, return Ok(None)); // empty

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
    type Input = T;
    type Output = ();

    fn persistent_op(&self, client: &mut PushClient<T>, input: T) {
        self.push(client, input)
    }
}

impl<T> PersistentOp<PopClient<T>> for TreiberStack<T> {
    type Input = ();
    type Output = Option<T>;

    fn persistent_op(&self, client: &mut PopClient<T>, _: ()) -> Option<T> {
        self.pop(client)
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
                        stack_ref.persistent_op(&mut push_vec[i], tid);
                        let _ = stack_ref.persistent_op(&mut pop_vec[i], ());
                    }
                });
            }
        })
        .unwrap();

        // Check empty
        assert!(stack
            .persistent_op(&mut PopClient::<usize>::default(), ())
            .is_none());

        // Check results
        let mut results = vec![0_usize; NR_THREAD];
        for client_vec in pop_clients.iter_mut() {
            for client in client_vec.iter_mut() {
                let ret = stack.persistent_op(client, ()).unwrap();
                results[ret] += 1;
            }
        }

        for &r in results.iter() {
            assert_eq!(r, COUNT);
        }
    }
}
