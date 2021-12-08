//! Node

use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};

use crate::{
    atomic_update_common::{self, no_owner},
    pepoch::{self as epoch, PAtomic},
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle, ll::persist_obj,
    },
};

/// TODO: doc
// TODO: T가 포인터일 수 있으니 T도 Collectable이여야함
#[derive(Debug)]
pub struct Node<T> {
    /// TODO: doc
    pub(crate) data: T,

    /// TODO: doc
    pub(crate) next: PAtomic<Self>,

    pub(crate) acked_unopt: AtomicBool,

    /// 누가 delete 했는지 식별 (unopt op에서만 사용 e.g. treiber stack)
    pub(crate) owner_unopt: AtomicUsize,

    /// 누가 delete/update 했는지 식별
    pub(crate) owner: AtomicUsize,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            acked_unopt: AtomicBool::new(false),
            owner_unopt: AtomicUsize::new(no_owner()),
            owner: AtomicUsize::new(no_owner()),
        }
    }
}

impl<T> Collectable for Node<T> {
    fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark valid ptr to trace
        let mut next = node.next.load(Ordering::SeqCst, guard);
        if !next.is_null() {
            let next = unsafe { next.deref_mut(pool) };
            Node::<T>::mark(next, gc);
        }
    }
}

impl<T> atomic_update_common::Node for Node<T> {
    #[inline]
    fn ack(&self) {}

    #[inline]
    fn acked(&self) -> bool {
        self.owner().load(Ordering::SeqCst) != no_owner()
    }

    #[inline]
    fn owner(&self) -> &AtomicUsize {
        &self.owner
    }
}

impl<T> atomic_update_common::NodeUnOpt for Node<T> {
    #[inline]
    fn ack_unopt(&self) {
        self.acked_unopt.store(true, Ordering::SeqCst);
        persist_obj(&self.acked_unopt, true);
    }

    #[inline]
    fn acked_unopt(&self) -> bool {
        self.acked_unopt.load(Ordering::SeqCst)
    }

    #[inline]
    fn owner_unopt(&self) -> &AtomicUsize {
        &self.owner_unopt
    }
}
