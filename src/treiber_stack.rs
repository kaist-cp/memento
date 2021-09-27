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
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use etrace::some_or;
use std::ptr;

use crate::persistent::*;
use crate::stack::*;

struct Node<T: Clone> {
    data: T,
    next: Atomic<Node<T>>,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: Atomic::null(),
            popper: AtomicUsize::new(TreiberStack::<T>::no_popper()),
        }
    }
}

trait PushType<T: Clone> {
    fn mine(&self) -> &Atomic<Node<T>>;
    fn is_try(&self) -> bool;
}

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    mine: Atomic<Node<T>>,
}

impl<T: Clone> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            mine: Atomic::null(),
        }
    }
}

impl<T: Clone> PushType<T> for TryPush<T> {
    #[inline]
    fn mine(&self) -> &Atomic<Node<T>> {
        &self.mine
    }

    #[inline]
    fn is_try(&self) -> bool {
        true
    }
}

unsafe impl<T: Clone> Send for TryPush<T> {}

impl<T: 'static + Clone> POp for TryPush<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = T;
    type Output<'s> = Result<(), TryFail>;

    fn run<'o>(&'o mut self, stack: Self::Object<'o>, value: Self::Input) -> Self::Output<'o> {
        stack.push(self, value)
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node
        self.mine.store(Shared::null(), Ordering::SeqCst);
    }
}

impl<T: Clone> TryPush<T> {
    /// 기본값 태그
    const DEFAULT: usize = 0;

    /// try push/pop 결과 실패를 표시하기 위한 태그
    const FAIL: usize = 1;

    /// 만약 input이 있다면 input은 남겨두고 try 실행을 안 한 것처럼 리셋함
    /// - 같은 input에 대해 exchange를 재시도하고 싶을 때 사용.
    ///   예를 들어, elimination stack의 push 구현 시 순서대로 (1) central stack try push/pop 실패, (2) exchange 실패, (3) central stack try push 재시도 하는 경우가 있음
    ///   이때 (1)에서 사용된 op을 `reset_weak()` 하여, `node`를 재할당 하지 않고 같은 input에 대해 exchange를 재시도할 수 있음.
    // TODO: 이미 push에 성공했으면 완전 reset 하기? exchanger::TryExchange의 reset_weak()과 일관성이 있어야 할 듯
    pub fn reset_weak(&self) {
        let guard = unsafe { epoch::unprotected() };
        let mine = self.mine.load(Ordering::SeqCst, guard);
        if mine.tag() == Self::FAIL {
            self.mine
                .store(mine.with_tag(Self::DEFAULT), Ordering::SeqCst);
        }
    }
}

/// TreiberStack의 push operation
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

impl<T: Clone> PushType<T> for Push<T> {
    #[inline]
    fn mine(&self) -> &Atomic<Node<T>> {
        &self.mine
    }

    #[inline]
    fn is_try(&self) -> bool {
        false
    }
}

unsafe impl<T: Clone> Send for Push<T> {}

impl<T: 'static + Clone> POp for Push<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = T;
    type Output<'s> = ();

    fn run<'o>(&'o mut self, stack: Self::Object<'o>, value: Self::Input) -> Self::Output<'o> {
        let pushed = stack.push(self, value);
        debug_assert!(pushed.is_ok())
    }

    fn reset(&mut self, _: bool) {
        // TODO: if not finished -> free node
        self.mine.store(Shared::null(), Ordering::SeqCst);
    }
}

trait PopType<T: Clone> {
    fn id(&self) -> usize;
    fn target(&self) -> &Atomic<Node<T>>;
    fn is_try(&self) -> bool;
}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    /// pup를 위해 할당된 node
    target: Atomic<Node<T>>,
}

impl<T: Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            target: Atomic::null(),
        }
    }
}

impl<T: Clone> PopType<T> for TryPop<T> {
    #[inline]
    fn id(&self) -> usize {
        self as *const Self as usize
    }

    #[inline]
    fn target(&self) -> &Atomic<Node<T>> {
        &self.target
    }

    #[inline]
    fn is_try(&self) -> bool {
        true
    }
}

unsafe impl<T: Clone> Send for TryPop<T> {}

impl<T: 'static + Clone> POp for TryPop<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = ();
    type Output<'s> = Result<Option<T>, TryFail>;

    fn run<'o>(&'o mut self, stack: Self::Object<'o>, _: Self::Input) -> Self::Output<'o> {
        stack.pop(self)
    }

    fn reset(&mut self, _: bool) {
        // TODO: if node has not been freed, check if the node is mine and free it
        // TODO: null이 아닐 때에만 store하는 게 더 빠를까?
        self.target.store(Shared::null(), Ordering::SeqCst);
    }
}

impl<T: Clone> TryPop<T> {
    /// try push/pop 결과 실패를 표시하기 위한 태그
    const FAIL: usize = 1;

    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;
}

/// TreiberStack의 pop operation
#[derive(Debug)]
pub struct Pop<T: Clone> {
    /// pup를 위해 할당된 node
    target: Atomic<Node<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            target: Atomic::null(),
        }
    }
}

impl<T: Clone> PopType<T> for Pop<T> {
    #[inline]
    fn id(&self) -> usize {
        self as *const Self as usize
    }

    #[inline]
    fn target(&self) -> &Atomic<Node<T>> {
        &self.target
    }

    #[inline]
    fn is_try(&self) -> bool {
        false
    }
}

unsafe impl<T: Clone> Send for Pop<T> {}

impl<T: 'static + Clone> POp for Pop<T> {
    type Object<'s> = &'s TreiberStack<T>;
    type Input = ();
    type Output<'s> = Option<T>;

    fn run<'o>(&'o mut self, stack: Self::Object<'o>, _: Self::Input) -> Self::Output<'o> {
        stack.pop(self).unwrap()
    }

    fn reset(&mut self, _: bool) {
        // TODO: if node has not been freed, check if the node is mine and free it
        self.target.store(Shared::null(), Ordering::SeqCst);
    }
}

/// Persistent Treiber stack
#[derive(Debug)]
pub struct TreiberStack<T: Clone> {
    top: Atomic<Node<T>>,
}

impl<T: Clone> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: Atomic::null(),
        }
    }
}

impl<T: Clone> TreiberStack<T> {
    fn push<C: PushType<T>>(&self, client: &C, value: T) -> Result<(), TryFail> {
        let guard = epoch::pin();
        let mut mine = client.mine().load(Ordering::SeqCst, &guard);

        if mine.is_null() {
            // (1) mine이 null이면 node 할당이 안 된 것이다
            let n = Owned::new(Node::from(value)).into_shared(&guard);

            client.mine().store(n, Ordering::SeqCst);
            mine = n;
        } else if mine.tag() == TryPush::<T>::FAIL {
            // (2) tag가 FAIL이면 try push 실패했던 것이다
            return Err(TryFail);
        } else if self.search(mine, &guard)
            || unsafe { mine.deref() }.popper.load(Ordering::SeqCst) != Self::no_popper()
        {
            // (3) stack 안에 mine이 있으면 push된 것이다 (Direct tracking)
            // (4) 이미 pop 되었다면 push된 것이다
            return Ok(());
        }

        if client.is_try() {
            self.try_push(mine, &guard).map_err(|e| {
                client
                    .mine()
                    .store(mine.with_tag(TryPush::<T>::FAIL), Ordering::SeqCst);
                e
            })
        } else {
            while self.try_push(mine, &guard).is_err() {}
            Ok(())
        }
    }

    /// top에 새 `node` 연결을 시도
    fn try_push(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> Result<(), TryFail> {
        let node_ref = unsafe { node.deref() };
        let top = self.top.load(Ordering::SeqCst, guard);

        node_ref.next.store(top, Ordering::SeqCst);
        self.top
            .compare_exchange(top, node, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|_| TryFail)
    }

    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, node: Shared<'_, Node<T>>, guard: &Guard) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == node {
                return true;
            }

            let curr_ref = unsafe { curr.deref() };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }

    fn pop<C: PopType<T>>(&self, client: &mut C) -> Result<Option<T>, TryFail> {
        let guard = epoch::pin();
        let target = client.target().load(Ordering::SeqCst, &guard);

        if target.tag() == TryPop::<T>::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            // post-crash execution (trying)
            let target_ref = unsafe { target.deref() };

            // target이 내가 pop한 게 맞는지 확인
            if target_ref.popper.load(Ordering::SeqCst) == client.id() {
                return Ok(Some(Self::finish_pop(target_ref)));
                // TODO: free node
            };

            // target이 stack에서 pop되긴 했는지 확인
            if !self.search(target, &guard) {
                // stack에서 나온 상태에서 crash 난 경우이므로 popper를 마저 기록해줌
                // cas인 이유: 다른 스레드도 같은 target을 노리던 중이었을 수도 있음
                if target_ref
                    .popper
                    .compare_exchange(
                        Self::no_popper(),
                        client.id(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return Ok(Some(Self::finish_pop(target_ref)));
                    // TODO: free node
                }
            }
        }

        if client.is_try() {
            return self.try_pop(client, &guard).map_err(|e| {
                client
                    .target()
                    .store(Shared::null().with_tag(TryPop::<T>::FAIL), Ordering::SeqCst);
                e
            });
        }

        loop {
            let result = self.try_pop(client, &guard);
            if result.is_ok() {
                return result;
            }
        }
    }

    /// top node를 pop 시도
    fn try_pop<C: PopType<T>>(&self, client: &C, guard: &Guard) -> Result<Option<T>, TryFail> {
        let top = self.top.load(Ordering::SeqCst, guard);
        let top_ref = some_or!(unsafe { top.as_ref() }, {
            // empty
            client.target().store(
                Shared::null().with_tag(TryPop::<T>::EMPTY),
                Ordering::SeqCst,
            );
            return Ok(None);
        });

        // 우선 내가 top node를 가리키고
        client.target().store(top, Ordering::SeqCst);

        // top node를 next로 바꿈
        let next = top_ref.next.load(Ordering::SeqCst, guard);
        self.top
            .compare_exchange(top, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| {
                // top node에 내 이름 새겨넣음
                top_ref.popper.store(client.id(), Ordering::SeqCst);
                // TODO: free node
                Some(Self::finish_pop(top_ref))
            })
            .map_err(|_| TryFail)
    }

    fn finish_pop(node: &Node<T>) -> T {
        node.data.clone()
        // free node
    }

    #[inline]
    fn no_popper() -> usize {
        let null: *const TryPop<T> = ptr::null();
        null as usize
    }
}

unsafe impl<T: Clone> Send for TreiberStack<T> {}

impl<T: 'static + Clone> Stack<T> for TreiberStack<T> {
    type TryPush = TryPush<T>;
    type Push = Push<T>;
    type TryPop = TryPop<T>;
    type Pop = Pop<T>;
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::stack::tests::*;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn push_pop() {
        test_push_pop::<TreiberStack<_>>(NR_THREAD, COUNT);
    }
}
