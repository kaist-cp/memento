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
#[derive(Debug)]
pub struct PushClient<'p, T> {
    /// push를 위해 할당된 node
    node: Shared<'p, Node<T>>,

    /// node 참조를 위한 persistent guard
    guard: Guard,
}

impl<T> Default for PushClient<'_, T> {
    fn default() -> Self {
        Self {
            node: Shared::null(),
            guard: pin(),
        }
    }
}

impl<T> PersistentClient for PushClient<'_, T> {
    fn reset(&mut self) {
        self.node = Shared::null();
        self.guard = pin();
    }
}

/// `Stack`의 `pop()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct PopClient<'p, T> {
    /// pup를 위해 할당된 node
    node: Shared<'p, Node<T>>,

    /// node 참조를 위한 persistent guard
    guard: Guard,
}

impl<T> Default for PopClient<'_, T> {
    fn default() -> Self {
        Self {
            node: Shared::null(),
            guard: pin(),
        }
    }
}

impl<T> PersistentClient for PopClient<'_, T> {
    fn reset(&mut self) {
        self.node = Shared::null();
        self.guard = pin();
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

impl<'p, T> TreiberStack<T> {
    /// Treiber stack에 `val`을 삽입함
    pub fn push(&self, client: &'p mut PushClient<'p, T>, val: T) {
        let vguard = &pin();

        if !client.node.is_null() {
            if self.is_finished(client.node, vguard) {
                return;
            }
        } else {
            // Install a node for the first execution
            let null: *const PopClient<'_, T> = ptr::null();
            let n = Owned::new(Node {
                data: val,
                next: Atomic::null(),
                popper: AtomicUsize::new(null as usize), // CHECK: Is this safe?
            });

            client.node = n.into_shared(&client.guard);
        }

        while !self.try_push_inner(client.node, vguard) {}
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
        let null: *const PopClient<'_, T> = ptr::null();
        if node_ref.popper.load(Ordering::SeqCst) != null as usize {
            return true;
        }

        false
    }

    /// top에 새 `node` 연결을 시도
    fn try_push_inner(&self, node: Shared<'_, Node<T>>, vguard: &Guard) -> bool {
        let node_ref = unsafe { node.deref() };
        let top = self.top.load(Ordering::SeqCst, vguard);

        node_ref.next.store(top, Ordering::SeqCst);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, vguard)
            .is_ok()
    }

    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, node: Shared<'_, Node<T>>, vguard: &Guard) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, vguard);

        while !curr.is_null() {
            if curr.as_raw() == node.as_raw() {
                return true;
            }

            let curr_ref = unsafe { curr.deref() };
            curr = curr_ref.next.load(Ordering::SeqCst, vguard);
        }

        false
    }

    /// Treiber stack에서 top node의 아이템을 반환함
    /// 비어 있을 경우 `None`을 반환
    pub fn pop(&self, client: &'p mut PopClient<'p, T>) -> Option<T> {
        if !client.node.is_null() {
            let node_ref = unsafe { client.node.deref() };
            let my_id = client as *const PopClient<'_, T> as usize;

            // node가 정말 내가 pop한 게 맞는지 확인
            if node_ref.popper.load(Ordering::SeqCst) == my_id {
                return unsafe { Some(ptr::read(&node_ref.data as *const _)) };
                // TODO: free node
            };
        }

        let vguard = &pin();
        let null: *const PopClient<'_, T> = ptr::null();

        loop {
            let top = self.top.load(Ordering::SeqCst, &client.guard);
            let top_ref = unsafe { top.as_ref()? };

            // 우선 내가 top node를 가리키고
            client.node = top;

            // top node에 내 이름 새겨넣음
            if top_ref
                .popper
                .compare_exchange(
                    null as usize,
                    &client as *const _ as usize,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return unsafe { Some(ptr::read(&top_ref.data as *const _)) };
                // TODO: free node
            }

            let next = top_ref.next.load(Ordering::SeqCst, vguard);
            let _ =
                self.top
                    .compare_exchange(top, next, Ordering::SeqCst, Ordering::SeqCst, vguard);
        }
    }

    // TODO: pub try_pop() 혹은 pop에 count 파라미터 달기
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn push_single() {
        let stack = TreiberStack::<usize>::default(); // TODO(persistent location)
                                                      // let mut push_client = PushClient::<usize>::default(); // TODO(persistent location)
        let mut pop_client = PopClient::<usize>::default(); // TODO(persistent location)

        // TODO: Fix "mut ref more than once" error
        // client 내의 shared pointer lifetime 때문에 error 발생
        // const COUNT: usize = 1_000_000;
        // for i in 0..COUNT {
        //     stack.push(&mut push_client, i);
        //     assert_eq!(stack.pop(&mut pop_client), Some(i));

        //     push_client.reset();
        //     pop_client.reset();
        // }

        assert!(stack.pop(&mut pop_client).is_none());
    }
}
