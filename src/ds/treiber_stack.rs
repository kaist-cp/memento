//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use super::stack::*;
use crate::node::Node;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::smo_unopt::{DeleteUnOpt, InsertUnOpt};
use crate::ploc::{Checkpoint, Traversable};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    insert: InsertUnOpt<TreiberStack<T>, Node<T>>,
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
    fn prepare(mine: &mut Node<T>, old_top: PShared<'_, Node<T>>) -> bool {
        mine.next.store(old_top, Ordering::SeqCst);
        persist_obj(&mine.next, false);
        true
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = PShared<'o, Node<T>>;
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
            .run(&stack.top, (node, stack, Self::prepare), rec, guard, pool)
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.insert.reset(guard, pool);
    }
}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    delete: DeleteUnOpt<TreiberStack<T>, Node<T>>,
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
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.delete
            .run(&stack.top, (stack, Self::get_next), rec, guard, pool)
            .map(|ret| {
                ret.map(|popped| unsafe {
                    guard.defer_pdestroy(popped);
                    popped.deref(pool).data.clone()
                })
            })
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.delete.reset(guard, pool);
    }
}

impl<T: Clone> TryPop<T> {
    #[inline]
    fn get_next<'g>(
        target: PShared<'_, Node<T>>,
        _: &TreiberStack<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<T>>>, ()> {
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
        PAtomic::filter(&mut stack.top, gc, pool);
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

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_push: TryPush<T>,
}

impl<T: Clone> Default for Push<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_push: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Push<T> {
    fn filter(push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut push.node, gc, pool);
        TryPush::filter(&mut push.try_push, gc, pool);
    }
}

impl<T: Clone> Memento for Push<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = T;
    type Output<'o>
    where
        T: 'o,
    = ();
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = self
            .node
            .run(
                (),
                (PAtomic::from(node), |aborted| {
                    let guard = unsafe { epoch::unprotected() };
                    let d = aborted.load(Ordering::Relaxed, guard);
                    unsafe { guard.defer_pdestroy(d) };
                }),
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        if self.try_push.run(stack, node, rec, guard, pool).is_ok() {
            return Ok(());
        }

        while self.try_push.run(stack, node, false, guard, pool).is_err() {}
        Ok(())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.node.reset(guard, pool);
        self.try_push.reset(guard, pool);
    }
}

unsafe impl<T: 'static + Clone> Send for Push<T> {}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: 'static + Clone> {
    try_pop: TryPop<T>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Pop<T> {
    fn filter(pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        TryPop::filter(&mut pop.try_pop, gc, pool);
    }
}

impl<T: Clone> Memento for Pop<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = ();
    type Output<'o>
    where
        T: 'o,
    = Option<T>;
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Ok(v) = self.try_pop.run(stack, (), rec, guard, pool) {
            return Ok(v);
        }

        loop {
            if let Ok(v) = self.try_pop.run(stack, (), false, guard, pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
    }
}

unsafe impl<T: Clone> Send for Pop<T> {}

impl<T: 'static + Clone> Stack<T> for TreiberStack<T> {
    type Push = Push<T>;
    type Pop = Pop<T>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};
    use rusty_fork::rusty_fork_test;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 10_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    impl TestRootObj for TreiberStack<usize> {}

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    rusty_fork_test! {
        #[test]
        fn push_pop() {
            const FILE_NAME: &str = "treiber_push_pop.pool";
            run_test::<TreiberStack<usize>, PushPop<TreiberStack<usize>, NR_THREAD, COUNT>, _>(
                FILE_NAME,
                FILE_SIZE,
                NR_THREAD + 1,
            )
        }
    }
}
