//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use etrace::{ok_or, some_or};

use super::stack::*;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, DetectableCASAtomic};
use crate::pmem::ralloc::{Collectable, GarbageCollection};
use crate::pmem::{ll::*, pool::*};
use crate::*;

/// Treiber stack node
#[derive(Debug)]
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

impl<T: Collectable> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut node.next, tid, gc, pool);
        T::filter(&mut node.data, tid, gc, pool);
    }
}

/// Try push memento
#[derive(Debug, Default)]
pub struct TryPush {
    insert: Cas,
}

unsafe impl Send for TryPush {}

impl Collectable for TryPush {
    fn filter(try_push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_push.insert, tid, gc, pool);
    }
}

impl TryPush {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.insert.clear();
    }
}

/// Push memento
#[derive(Debug)]
pub struct Push<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_push: TryPush,
}

impl<T: Clone + Collectable> Default for Push<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_push: Default::default(),
        }
    }
}

impl<T: Clone + Collectable> Collectable for Push<T> {
    fn filter(push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut push.node, tid, gc, pool);
        TryPush::filter(&mut push.try_push, tid, gc, pool);
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Push<T> {}

impl<T: Clone + Collectable> Push<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.node.clear();
        self.try_push.clear();
    }
}

/// Try pop memento
#[derive(Debug)]
pub struct TryPop<T: Clone + Collectable> {
    delete: Cas,
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

impl<T: Clone + Collectable> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Cas::filter(&mut try_pop.delete, tid, gc, pool);
        Checkpoint::filter(&mut try_pop.top, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> TryPop<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.delete.clear();
        self.top.clear();
    }
}

/// Pop memento
#[derive(Debug)]
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

impl<T: Clone + Collectable> Collectable for Pop<T> {
    fn filter(pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        TryPop::filter(&mut pop.try_pop, tid, gc, pool);
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Pop<T> {}

impl<T: Clone + Collectable> Pop<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.try_pop.clear();
    }
}

/// Persistent Treiber stack
#[derive(Debug)]
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

impl<T: Clone + Collectable> Collectable for TreiberStack<T> {
    fn filter(stack: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut stack.top, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> PDefault for TreiberStack<T> {
    fn pdefault(_: &PoolHandle) -> Self {
        Self::default()
    }
}

impl<T: Clone + Collectable> TreiberStack<T> {
    /// Try push
    pub fn try_push<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        try_push: &mut TryPush,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let top = self.top.load(Ordering::SeqCst, guard, pool);
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.store(top, Ordering::SeqCst);
        persist_obj(&node_ref.next, false); // we do CAS right after that

        self.top
            .cas::<REC>(top, node, &mut try_push.insert, tid, guard, pool)
            .map_err(|_| TryFail)
    }

    /// Push
    pub fn push<const REC: bool>(
        &self,
        value: T,
        push: &mut Push<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = ok_or!(
            push.node.checkpoint::<REC>(PAtomic::from(node), tid, pool),
            e,
            unsafe {
                drop(
                    e.new
                        .load(Ordering::Relaxed, epoch::unprotected())
                        .into_owned(),
                );
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        if self
            .try_push::<REC>(node, &mut push.try_push, tid, guard, pool)
            .is_ok()
        {
            return;
        }

        loop {
            if self
                .try_push::<false>(node, &mut push.try_push, tid, guard, pool)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Try pop
    pub fn try_pop<const REC: bool>(
        &self,
        try_pop: &mut TryPop<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        let top = self.top.load(Ordering::SeqCst, guard, pool);
        let top = ok_or!(
            try_pop.top.checkpoint::<REC>(PAtomic::from(top), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        let top_ref = some_or!(unsafe { top.as_ref(pool) }, return Ok(None));
        let next = top_ref.next.load(Ordering::SeqCst, guard);

        self.top
            .cas::<REC>(top, next, &mut try_pop.delete, tid, guard, pool)
            .map(|_| unsafe {
                guard.defer_pdestroy(top);
                Some(top_ref.data.clone())
            })
            .map_err(|_| TryFail)
    }

    /// Pop
    pub fn pop<const REC: bool>(
        &self,
        pop: &mut Pop<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Option<T> {
        if let Ok(ret) = self.try_pop::<REC>(&mut pop.try_pop, tid, guard, pool) {
            return ret;
        }

        loop {
            if let Ok(ret) = self.try_pop::<false>(&mut pop.try_pop, tid, guard, pool) {
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
    fn push<const REC: bool>(
        &self,
        value: T,
        push: &mut Self::Push,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        self.push::<REC>(value, push, tid, guard, pool)
    }

    #[inline]
    fn pop<const REC: bool>(
        &self,
        pop: &mut Self::Pop,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Option<T> {
        self.pop::<REC>(pop, tid, guard, pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};

    const NR_THREAD: usize = 12;
    const COUNT: usize = 20_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    fn push_pop() {
        const FILE_NAME: &str = "treiber_stack";
        run_test::<TestRootObj<TreiberStack<usize>>, PushPop<_, NR_THREAD, COUNT>>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 1,
        )
    }
}
