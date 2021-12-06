//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use crate::atomic_update_common::Traversable;
use crate::atomic_update_unopt::{DeleteUnOpt, InsertUnOpt};
use crate::pepoch::{self as epoch, Guard, PAtomic, PShared};
use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::{ll::*, pool::*};
use crate::stack::*;
use crate::unopt_node::{DeallocNode, NodeUnOpt};

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    insert: InsertUnOpt<TreiberStack<T>, NodeUnOpt<T, TreiberStack<T>>>,
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
        InsertUnOpt::filter(&mut try_push.insert, gc, pool);
    }
}

impl<T: Clone> TryPush<T> {
    #[inline]
    fn before_cas(
        mine: &mut NodeUnOpt<T, TreiberStack<T>>,
        old_top: PShared<'_, NodeUnOpt<T, TreiberStack<T>>>,
    ) -> bool {
        mine.next.store(old_top, Ordering::SeqCst);
        persist_obj(&mine.next, false);
        true
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = PShared<'o, NodeUnOpt<T, TreiberStack<T>>>;
    type Output<'o> = ();
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.insert
            .run(
                stack,
                (node, &stack.top, Self::before_cas),
                rec,
                guard,
                pool,
            )
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        self.insert.reset(nested, guard, pool);
    }
}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    /// pop를 위해 할당된 node
    delete: DeleteUnOpt<TreiberStack<T>, NodeUnOpt<T, TreiberStack<T>>>,
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
        DeleteUnOpt::filter(&mut try_pop.delete, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryPop<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = &'o PAtomic<NodeUnOpt<T, TreiberStack<T>>>;
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        mine_loc: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.delete
            .run(
                stack,
                (mine_loc, &stack.top, Self::get_next),
                rec,
                guard,
                pool,
            )
            .map(|ret| ret.map(|popped| unsafe { popped.deref(pool) }.data.clone()))
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        self.delete.reset(nested, guard, pool);
    }
}

impl<T: Clone> DeallocNode<T, NodeUnOpt<T, TreiberStack<T>>> for TryPop<T> {
    #[inline]
    fn dealloc(
        &self,
        target: PShared<'_, NodeUnOpt<T, TreiberStack<T>>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        self.delete.dealloc(target, guard, pool);
    }
}

impl<T: Clone> TryPop<T> {
    #[inline]
    fn get_next<'g>(
        target: PShared<'_, NodeUnOpt<T, TreiberStack<T>>>,
        _: &TreiberStack<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, NodeUnOpt<T, TreiberStack<T>>>>, ()> {
        if target.is_null() {
            return Ok(None);
        }

        let target_ref = unsafe { target.deref(pool) };
        let next = target_ref.next.load(Ordering::SeqCst, guard);
        Ok(Some(next))
    }
}

/// Persistent Treiber stack
#[derive(Debug)]
pub struct TreiberStack<T: Clone> {
    top: PAtomic<NodeUnOpt<T, TreiberStack<T>>>,
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
            NodeUnOpt::mark(top_ref, gc);
        }
    }
}

impl<T: Clone> PDefault for TreiberStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

impl<T: Clone> Traversable<NodeUnOpt<T, TreiberStack<T>>> for TreiberStack<T> {
    /// `node`가 Treiber stack 안에 있는지 top부터 bottom까지 순회하며 검색
    fn search(
        &self,
        target: PShared<'_, NodeUnOpt<T, TreiberStack<T>>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
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
