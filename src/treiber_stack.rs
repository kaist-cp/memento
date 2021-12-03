//! Persistent stack based on Treiber stack

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::AtomicBool;

use crate::atomic_update::{self, Delete, Insert, Traversable};
use crate::pepoch::{self as epoch, Guard, PAtomic, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{ll::*, pool::*};
use crate::stack::*;

// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
struct Node<T: Clone> {
    data: T,
    next: PAtomic<Node<T>>,

    /// push 되었는지 여부
    // 이게 없으면, pop()에서 node 뺀 후 popper 등록 전에 crash 났을 때, 노드가 이미 push 되었었다는 걸 알 수 없음
    pushed: AtomicBool,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    popper: AtomicUsize,
}

impl<T: Clone> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            pushed: AtomicBool::new(false),
            popper: AtomicUsize::new(Delete::<TreiberStack<T>, _>::no_owner()),
        }
    }
}

impl<T: Clone> atomic_update::Node for Node<T> {
    fn ack(&self) {
        self.pushed.store(true, Ordering::SeqCst);
    }

    fn acked(&self) -> bool {
        self.pushed.load(Ordering::SeqCst)
    }

    fn owner(&self) -> &AtomicUsize {
        &self.popper
    }

    fn next<'g>(&self, guard: &'g Guard) -> PShared<'g, Self> {
        self.next.load(Ordering::SeqCst, guard)
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Node<T> {}

impl<T: Clone> Collectable for Node<T> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next_ref = unsafe { next.deref_mut(pool) };
            Node::<T>::mark(next_ref, gc);
        }
    }
}

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    insert: Insert<TreiberStack<T>, Node<T>>,
}

impl<T: Clone> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            insert: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPush<T> {}

impl<T: Clone> Collectable for TryPush<T> {
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Insert::filter(&mut try_push.insert, gc, pool);
    }
}

impl<T: Clone> TryPush<T> {
    fn before_cas(mine: &mut Node<T>, oldtop: PShared<'_, Node<T>>) -> bool {
        mine.next.store(oldtop, Ordering::SeqCst);
        persist_obj(&mine.next, false);
        true
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = T;
    type Output<'o> = ();
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.insert
            .run(
                stack,
                (Node::from(value), &stack.top, Self::before_cas),
                guard,
                pool,
            )
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        // 원래 하위 memento를 reset할 경우 reset flag를 쓰는 게 도리에 맞으나
        // `Insert`의 `reset()`이 atomic 하므로 안 써도 됨
        self.insert.reset(nested, guard, pool);
    }

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        self.insert.set_recovery(pool);
    }
}

impl<T: Clone> Drop for TryPush<T> {
    fn drop(&mut self) {
        // TODO: "하위 메멘토의 `is_reset()`이 필요함"
    }
}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    /// pop를 위해 할당된 node
    delete: Delete<TreiberStack<T>, Node<T>>,
}

impl<T: Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPop<T> {}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Delete::filter(&mut try_pop.delete, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryPop<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        self.delete
            .run(stack, &stack.top, guard, pool)
            .map(|ret| ret.map(|popped| popped.data.clone()))
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        // 원래 하위 memento를 reset할 경우 reset flag를 쓰는 게 도리에 맞으나
        // `Delete`의 `reset()`이 atomic 하므로 안 써도 됨
        self.delete.reset(nested, guard, pool);
    }

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        self.delete.set_recovery(pool);
    }
}

impl<T: Clone> Drop for TryPop<T> {
    fn drop(&mut self) {
        // TODO: "하위 메멘토의 `is_reset()`이 필요함"
    }
}

/// Persistent Treiber stack
#[derive(Debug)]
pub struct TreiberStack<T: Clone> {
    top: PAtomic<Node<T>>,
}

impl<T: Clone> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: PAtomic::null(),
        }
    }
}

impl<T: Clone> Collectable for TreiberStack<T> {
    fn filter(stack: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut top = stack.top.load(Ordering::SeqCst, guard);
        if !top.is_null() {
            let top_ref = unsafe { top.deref_mut(pool) };
            Node::mark(top_ref, gc);
        }
    }
}

impl<T: Clone> PDefault for TreiberStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

impl<T: Clone> Traversable<Node<T>> for TreiberStack<T> {
    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let mut curr = self.top.load(Ordering::SeqCst, guard);

        while !curr.is_null() {
            if curr == target {
                return true;
            }

            let curr_ref = unsafe { curr.deref(pool) };
            curr = curr_ref.next.load(Ordering::SeqCst, guard);
        }

        false
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TreiberStack<T> {}

impl<T: 'static + Clone> Stack<T> for TreiberStack<T> {
    type TryPush = TryPush<T>;
    type TryPop = TryPop<T>;
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::{stack::tests::*, utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 1_000_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    impl TestRootObj for TreiberStack<usize> {}

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn push_pop() {
        const FILE_NAME: &str = "treiber_push_pop.pool";
        run_test::<TreiberStack<usize>, PushPop<TreiberStack<usize>, NR_THREAD, COUNT>, _>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 1,
        )
    }
}
