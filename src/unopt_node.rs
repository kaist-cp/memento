//! UnOpt Node

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    atomic_update_common::{self, Traversable},
    atomic_update_unopt::DeleteUnOpt,
    pepoch::{self as epoch, PAtomic, PShared},
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// TODO: doc
// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
pub struct NodeUnOpt<T, O: Traversable<Self>> {
    /// TODO: doc
    pub(crate) data: T,

    /// TODO: doc
    pub(crate) next: PAtomic<NodeUnOpt<T, O>>,

    /// push 되었는지 여부
    // 이게 없으면, pop()에서 node 뺀 후 popper 등록 전에 crash 났을 때, 노드가 이미 push 되었었다는 걸 알 수 없음
    pub(crate) pushed: AtomicBool,

    /// 누가 pop 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    pub(crate) popper: AtomicUsize,
}

impl<T, O: Traversable<Self>> From<T> for NodeUnOpt<T, O> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            pushed: AtomicBool::new(false),
            popper: AtomicUsize::new(DeleteUnOpt::<O, _>::no_owner()),
        }
    }
}

impl<T, O: Traversable<Self>> atomic_update_common::Node for NodeUnOpt<T, O> {
    #[inline]
    fn ack(&self) {
        self.pushed.store(true, Ordering::SeqCst);
        persist_obj(&self.pushed, true);
    }

    #[inline]
    fn acked(&self) -> bool {
        self.pushed.load(Ordering::SeqCst)
    }

    #[inline]
    fn owner(&self) -> &AtomicUsize {
        &self.popper
    }
}

unsafe impl<T: Send + Sync, O: Traversable<Self>> Send for NodeUnOpt<T, O> {}

impl<T, O: Traversable<Self>> Collectable for NodeUnOpt<T, O> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next_ref = unsafe { next.deref_mut(pool) };
            NodeUnOpt::<T, O>::mark(next_ref, gc);
        }
    }
}

/// TODO: doc
pub trait DeallocNode<T, N: atomic_update_common::Node> {
    /// TODO: doc
    fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle);
}
