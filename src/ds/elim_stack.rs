//! Persistent stack based on Elimination backoff stack

use std::sync::atomic::Ordering;

use mmt_derive::Collectable;
use rand::{thread_rng, Rng};

use crate::{
    pepoch::{PAtomic, POwned, PShared},
    ploc::{Checkpoint, Handle},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    Memento, PDefault,
};

use super::{
    exchanger::{Exchanger, TryExchange},
    stack::{Stack, TryFail},
    treiber_stack::{self, Node, TreiberStack},
};

const ELIM_SIZE: usize = 1;

#[inline]
#[allow(clippy::modulo_one)]
fn get_random_elim_index() -> usize {
    thread_rng().gen::<usize>() % ELIM_SIZE
}

#[derive(Debug, Clone)]
enum Request<T> {
    Push(T),
    Pop,
}

impl<T: Collectable> Collectable for Request<T> {
    fn filter(req: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        if let Self::Push(v) = req {
            T::filter(v, tid, gc, pool);
        }
    }
}

/// Try push memento
#[derive(Debug, Memento, Collectable)]
pub struct TryPush<T: Clone + Collectable> {
    /// try push memento for inner stack
    try_push: treiber_stack::TryPush<Request<T>>,

    /// elimination exchanger's exchange op
    try_xchg: TryExchange<Request<T>>,

    /// elimination exchange index
    elim_idx: usize,
}

impl<T: Clone + Collectable> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            try_push: Default::default(),
            try_xchg: Default::default(),
            elim_idx: get_random_elim_index(),
        }
    }
}

/// Push memento
#[derive(Debug, Memento, Collectable)]
pub struct Push<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<Request<T>>>>,
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
    /// try pop memento for inner stack
    try_pop: treiber_stack::TryPop<Request<T>>,

    /// exchanger node
    pop_node: Checkpoint<PAtomic<Node<Request<T>>>>,

    /// try exchange memento
    try_xchg: TryExchange<Request<T>>,

    /// elimination exchange index
    elim_idx: usize,
}

impl<T: Clone + Collectable> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
            pop_node: Default::default(),
            try_xchg: Default::default(),
            elim_idx: get_random_elim_index(),
        }
    }
}

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

/// Persistent Elimination backoff stack
#[derive(Debug)]
pub struct ElimStack<T: Clone + Collectable> {
    inner: TreiberStack<Request<T>>,
    slots: [Exchanger<Request<T>>; ELIM_SIZE],
}

impl<T: Clone + Collectable> Default for ElimStack<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            slots: array_init::array_init(|_| Exchanger::<Request<T>>::default()),
        }
    }
}

impl<T: Clone + Collectable> Collectable for ElimStack<T> {
    fn filter(
        elim_stack: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        TreiberStack::filter(&mut elim_stack.inner, tid, gc, pool);
        for slot in elim_stack.slots.as_mut() {
            Exchanger::filter(slot, tid, gc, pool);
        }
    }
}

impl<T: Clone + Collectable> PDefault for ElimStack<T> {
    fn pdefault(_: &PoolHandle) -> Self {
        Self::default()
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for ElimStack<T> {}
unsafe impl<T: Clone + Collectable> Sync for ElimStack<T> {}

impl<T: Clone + Collectable> ElimStack<T> {
    /// Try push
    fn try_push(
        &self,
        node: PShared<'_, Node<Request<T>>>,
        try_push: &mut TryPush<T>,
        handle: &Handle,
    ) -> Result<(), TryFail> {
        if self
            .inner
            .try_push(node, &mut try_push.try_push, handle)
            .is_ok()
        {
            return Ok(());
        }

        let value = unsafe { node.deref(handle.pool) }.data.clone();

        self.slots[try_push.elim_idx]
            .try_exchange(
                value,
                |req| matches!(req, Request::Pop),
                &mut try_push.try_xchg,
                handle,
            )
            .map(|_| ())
            .map_err(|_| TryFail)
    }

    /// Push
    pub fn push(&self, value: T, push: &mut Push<T>, handle: &Handle) {
        let (guard, pool) = (&handle.guard, handle.pool);
        let node = push
            .node
            .checkpoint(
                || {
                    let node = POwned::new(Node::from(Request::Push(value)), pool);
                    persist_obj(unsafe { node.deref(pool) }, true);
                    PAtomic::from(node)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);

        while self.try_push(node, &mut push.try_push, handle).is_err() {}
    }

    /// Try pop
    fn try_pop(&self, try_pop: &mut TryPop<T>, handle: &Handle) -> Result<Option<T>, TryFail> {
        if let Ok(popped) = self.inner.try_pop(&mut try_pop.try_pop, handle) {
            let ret = popped.map(|req| {
                if let Request::Push(v) = req {
                    v
                } else {
                    panic!("Pop req is not in the stack")
                }
            });
            return Ok(ret);
        }

        let req = self.slots[try_pop.elim_idx]
            .try_exchange(
                Request::Pop,
                |req| matches!(req, Request::Push(_)),
                &mut try_pop.try_xchg,
                handle,
            )
            .map_err(|_| TryFail)?;

        if let Request::Push(v) = req {
            Ok(Some(v))
        } else {
            panic!("Exchanged only with Push due to exchange conditions")
        }
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

impl<T: Clone + Collectable> Stack<T> for ElimStack<T> {
    type Push = Push<T>;
    type Pop = Pop<T>;

    fn push(&self, value: T, push: &mut Self::Push, handle: &Handle) {
        self.push(value, push, handle)
    }

    fn pop(&self, pop: &mut Self::Pop, handle: &Handle) -> Option<T> {
        self.pop(pop, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};

    const NR_THREAD: usize = 3;
    const NR_COUNT: usize = 10_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // We should enlarge stack size for the test (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    #[test]
    fn push_pop() {
        const FILE_NAME: &str = "elim_stack";
        run_test::<TestRootObj<ElimStack<TestValue>>, PushPop<_, NR_THREAD, NR_COUNT>>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        )
    }
}
