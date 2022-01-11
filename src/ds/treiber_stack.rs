//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use etrace::some_or;

use super::stack::*;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, RetryLoop};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// TODO(doc)
#[derive(Debug)]
pub struct Node<T> {
    /// TODO(doc)
    pub data: T,

    /// TODO(doc)
    pub next: PAtomic<Self>,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
        }
    }
}

// TODO(must): T should be collectable
impl<T> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next = unsafe { next.deref_mut(pool) };
            Node::<T>::mark(next, tid, gc);
        }
    }
}

/// TreiberStack의 try push operation
#[derive(Debug)]
pub struct TryPush<T: Clone> {
    /// push를 위해 할당된 node
    insert: Cas<Node<T>>,
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
    fn filter(try_push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Cas::filter(&mut try_push.insert, tid, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryPush<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = PShared<'o, Node<T>>;
    type Output<'o> = ();
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        stack: Self::Object<'o>,
        node: Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let top = stack.top.load(Ordering::SeqCst, guard);
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.store(top, Ordering::SeqCst);
        persist_obj(&node_ref.next, false); // we do CAS right after that

        self.insert
            .run(&stack.top, (top, node), tid, rec, guard, pool)
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.insert.reset(guard, pool);
    }
}

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_push: RetryLoop<TryPush<T>>,
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
    fn filter(push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut push.node, tid, gc, pool);
        RetryLoop::filter(&mut push.try_push, tid, gc, pool);
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
        &mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = self
            .node
            .run(
                (),
                (PAtomic::from(node), |aborted| {
                    drop(unsafe {
                        aborted
                            .load(Ordering::Relaxed, epoch::unprotected())
                            .into_owned()
                    });
                }),
                tid,
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        self.try_push.run(stack, node, tid, rec, guard, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.node.reset(guard, pool);
        self.try_push.reset(guard, pool);
    }
}

unsafe impl<T: 'static + Clone> Send for Push<T> {}

/// TreiberStack의 try pop operation
#[derive(Debug)]
pub struct TryPop<T: Clone> {
    delete: Cas<Node<T>>,
    top: Checkpoint<PAtomic<Node<T>>>,
}

impl<T: Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
            top: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TryPop<T> {}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Cas::filter(&mut try_pop.delete, tid, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryPop<T> {
    type Object<'o> = &'o TreiberStack<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let top = stack.top.load(Ordering::SeqCst, guard);
        let top = self
            .top
            .run((), (PAtomic::from(top), |_| ()), tid, rec, guard, pool)
            .unwrap()
            .load(Ordering::Relaxed, guard);

        let top_ref = some_or!(unsafe { top.as_ref(pool) }, return Ok(None));
        let next = top_ref.next.load(Ordering::SeqCst, guard);

        self.delete
            .run(&stack.top, (top, next), tid, rec, guard, pool)
            .map(|_| unsafe {
                guard.defer_pdestroy(top);
                Some(top_ref.data.clone())
            })
            .map_err(|_| TryFail)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.delete.reset(guard, pool);
        self.top.reset(guard, pool);
    }
}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: 'static + Clone> {
    try_pop: RetryLoop<TryPop<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Pop<T> {
    fn filter(pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        RetryLoop::filter(&mut pop.try_pop, tid, gc, pool);
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
        &mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.try_pop.run(stack, (), tid, rec, guard, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
    }
}

unsafe impl<T: Clone> Send for Pop<T> {}

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
    fn filter(stack: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        PAtomic::filter(&mut stack.top, tid, gc, pool);
    }
}

impl<T: Clone> PDefault for TreiberStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

unsafe impl<T: Clone + Send + Sync> Send for TreiberStack<T> {}

impl<T: 'static + Clone> Stack<T> for TreiberStack<T> {
    type Push = Push<T>;
    type Pop = Pop<T>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 1_000_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    impl TestRootObj for TreiberStack<usize> {}

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
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
