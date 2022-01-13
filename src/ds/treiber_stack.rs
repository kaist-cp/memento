//! Persistent stack based on Treiber stack

use core::sync::atomic::Ordering;

use crossbeam_utils::Backoff;
use etrace::{ok_or, some_or};

use super::stack::*;
use crate::pepoch::{self as epoch, Guard, PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, GeneralSMOAtomic};
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
        PAtomic::filter(&mut node.next, tid, gc, pool);
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

impl<T: Clone> TryPush<T> {
    /// Reset TryPush memento
    #[inline]
    pub fn reset(&mut self) {
        self.insert.reset();
    }
}

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: Clone> {
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
    fn filter(push: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut push.node, tid, gc, pool);
        TryPush::filter(&mut push.try_push, tid, gc, pool);
    }
}

impl<T: Clone> Push<T> {
    /// Reset push memento
    #[inline]
    pub fn reset(&mut self) {
        self.node.reset();
        self.try_push.reset();
    }
}

unsafe impl<T: Clone> Send for Push<T> {}

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
        Checkpoint::filter(&mut try_pop.top, tid, gc, pool);
    }
}

impl<T: Clone> TryPop<T> {
    /// Reset TryPop memento
    #[inline]
    pub fn reset(&mut self) {
        self.delete.reset();
        self.top.reset();
    }
}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: Clone> {
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
    fn filter(pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        TryPop::filter(&mut pop.try_pop, tid, gc, pool);
    }
}

impl<T: Clone> Pop<T> {
    /// Reset Pop memento
    #[inline]
    pub fn reset(&mut self) {
        self.try_pop.reset();
    }
}

unsafe impl<T: Clone> Send for Pop<T> {}

/// Persistent Treiber stack
#[derive(Debug)]
pub struct TreiberStack<T: Clone> {
    top: GeneralSMOAtomic<Node<T>>,
}

impl<T: Clone> Default for TreiberStack<T> {
    fn default() -> Self {
        Self {
            top: GeneralSMOAtomic::default(),
        }
    }
}

impl<T: Clone> Collectable for TreiberStack<T> {
    fn filter(stack: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        GeneralSMOAtomic::filter(&mut stack.top, tid, gc, pool);
    }
}

impl<T: Clone> PDefault for TreiberStack<T> {
    fn pdefault(_: &PoolHandle) -> Self {
        Self::default()
    }
}

impl<T: Clone> TreiberStack<T> {
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
            push.node.checkpoint::<REC>(PAtomic::from(node)),
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
        .load(Ordering::Relaxed, guard); // TODO(opt): usize를 checkpoint 해보기 (using `PShared::from_usize()`)

        if self
            .try_push::<REC>(node, &mut push.try_push, tid, guard, pool)
            .is_ok()
        {
            return;
        }

        let backoff = Backoff::default();
        loop {
            backoff.snooze();
            if self
                .try_push::<false>(node, &mut push.try_push, tid, guard, pool)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Try push
    pub fn try_push<const REC: bool>(
        &self,
        node: PShared<'_, Node<T>>,
        try_push: &mut TryPush<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), TryFail> {
        let top = self.top.load(Ordering::SeqCst, guard);
        let node_ref = unsafe { node.deref(pool) };
        node_ref.next.store(top, Ordering::SeqCst);
        persist_obj(&node_ref.next, false); // we do CAS right after that

        self.top
            .cas::<REC>(top, node, &mut try_push.insert, tid, guard, pool)
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

        let backoff = Backoff::default();
        loop {
            backoff.snooze();
            if let Ok(ret) = self.try_pop::<false>(&mut pop.try_pop, tid, guard, pool) {
                return ret;
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
        let top = self.top.load(Ordering::SeqCst, guard);
        let top = ok_or!(
            try_pop.top.checkpoint::<REC>(PAtomic::from(top)),
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
}

unsafe impl<T: Clone + Send + Sync> Send for TreiberStack<T> {}

impl<T: Clone> Stack<T> for TreiberStack<T> {
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
    const COUNT: usize = 100_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    fn push_pop() {
        const FILE_NAME: &str = "treiber_push_pop.pool";
        run_test::<
            TestRootObj<TreiberStack<usize>>,
            PushPop<TreiberStack<usize>, NR_THREAD, COUNT>,
            _,
        >(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }
}
