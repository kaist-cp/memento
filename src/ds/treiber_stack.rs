//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use etrace::some_or;

use super::stack::*;
use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, DetectableCASAtomic, Handle};
use crate::pmem::alloc::Collectable;
use crate::pmem::ll::*;
use crate::pmem::GarbageCollection;
use crate::pmem::PoolHandle;
use crate::*;
use mmt_derive::Collectable;

/// Treiber stack node
#[derive(Debug, Collectable)]
pub struct Node<T: Collectable> {
    /// Data
    pub(crate) data: T,

    /// Next node pointer
    pub(crate) next: PAtomic<Self>,
}

impl<T: Collectable> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
        }
    }
}

impl<T: Collectable> Drop for Node<T> {
    fn drop(&mut self) {
        let data = unsafe { *(&self.data as *const _ as *const usize) };
        println!("Node dropped: {{ data: {data}, next: {:?} }}", self.next);
    }
}

/// Try push memento
#[derive(Debug, Memento, Collectable)]
pub struct TryPush<T: Collectable + Clone> {
    top: Checkpoint<PAtomic<Node<T>>>,
    insert: Cas<Node<T>>,
}

unsafe impl<T: Collectable + Clone> Send for TryPush<T> {}

impl<T: Clone + Collectable> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            top: Default::default(),
            insert: Default::default(),
        }
    }
}

/// Push memento
#[derive(Debug, Memento, Collectable)]
pub struct Push<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_push: TryPush<T>,
}

impl<T: Clone + Collectable> Default for Push<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_push: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Push<T> {}

/// Try pop memento
#[derive(Debug, Memento, Collectable)]
pub struct TryPop<T: Clone + Collectable> {
    delete: Cas<Node<T>>,
    top: Checkpoint<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
            top: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TryPop<T> {}

/// Pop memento
#[derive(Debug, Memento, Collectable)]
pub struct Pop<T: Clone + Collectable> {
    try_pop: TryPop<T>,
}

impl<T: Clone + Collectable> Default for Pop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Pop<T> {}

/// Persistent Treiber stack
#[derive(Debug, Collectable)]
pub struct TreiberStack<T: Clone + Collectable> {
    top: DetectableCASAtomic<Node<T>>,
}

impl<T: Clone + Collectable> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: DetectableCASAtomic::default(),
        }
    }
}

impl<T: Clone + Collectable> PDefault for TreiberStack<T> {
    fn pdefault(_: &Handle) -> Self {
        Self::default()
    }
}

impl<T: Clone + Collectable> TreiberStack<T> {
    /// Try push
    pub fn try_push(
        &self,
        node: PShared<'_, Node<T>>,
        try_push: &mut TryPush<T>,
        handle: &Handle,
    ) -> Result<(), TryFail> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let top = try_push
            .top
            .checkpoint(
                || {
                    let top = self.top.load(Ordering::SeqCst, handle);
                    let node_ref = unsafe { node.deref(pool) };
                    // TODO: check if same & otherwise store/flush
                    node_ref.next.store(top, Ordering::SeqCst);
                    persist_obj(&node_ref.next, true);
                    PAtomic::from(top)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);

        self.top
            .cas(top, node, &mut try_push.insert, handle)
            .map_err(|_| TryFail)
    }

    /// Push
    pub fn push(&self, value: T, push: &mut Push<T>, handle: &Handle) {
        let (guard, pool) = (&handle.guard, handle.pool);
        let node = push
            .node
            .checkpoint(
                || {
                    let node = POwned::new(Node::from(value), pool);
                    persist_obj(unsafe { node.deref(pool) }, true);
                    PAtomic::from(node)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);

        while self.try_push(node, &mut push.try_push, handle).is_err() {}
    }

    /// Try pop
    pub fn try_pop(&self, try_pop: &mut TryPop<T>, handle: &Handle) -> Result<Option<T>, TryFail> {
        let (guard, pool) = (&handle.guard, handle.pool);
        let top = try_pop
            .top
            .checkpoint(
                || {
                    let top = self.top.load(Ordering::SeqCst, handle);
                    PAtomic::from(top)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);

        let top_ref = some_or!(unsafe { top.as_ref(pool) }, return Ok(None));
        let next = top_ref.next.load(Ordering::SeqCst, guard); // next is stable because top is stable here (invariant of stack)

        self.top
            .cas(top, next, &mut try_pop.delete, handle)
            .map(|_| unsafe {
                guard.defer_pdestroy(top);
                Some(top_ref.data.clone())
            })
            .map_err(|_| TryFail)
    }

    /// Pop
    pub fn pop(&self, pop: &mut Pop<T>, handle: &Handle) -> Option<T> {
        loop {
            if let Ok(ret) = self.try_pop(&mut pop.try_pop, handle) {
                return ret;
            }
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for TreiberStack<T> {}

impl<T: Clone + Collectable> Stack<T> for TreiberStack<T> {
    type Push = Push<T>;
    type Pop = Pop<T>;

    #[inline]
    fn push(&self, value: T, push: &mut Self::Push, handle: &Handle) {
        self.push(value, push, handle)
    }

    #[inline]
    fn pop(&self, pop: &mut Self::Pop, handle: &Handle) -> Option<T> {
        self.pop(pop, handle)
    }
}

#[allow(warnings)]
pub(crate) mod test {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};

    const NR_THREAD: usize = 2;
    const NR_COUNT: usize = 10_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    fn push_pop() {
        const FILE_NAME: &str = "treiber_stack";
        run_test::<TestRootObj<TreiberStack<TestValue>>, PushPop<_, NR_THREAD, NR_COUNT>>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        )
    }

    /// Test function for psan
    #[cfg(feature = "pmcheck")]
    pub(crate) fn pushpop(thread: usize, count: usize) {
        assert!(thread == 2 && count == 2);
        const FILE_NAME: &str = "treiber_stack";
        run_test::<TestRootObj<TreiberStack<TestValue>>, PushPop<_, 2, 2>>(
            FILE_NAME, FILE_SIZE, thread, count,
        )
    }
}
