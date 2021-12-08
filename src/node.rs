//! Node

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    atomic_update_common::{self, no_owner},
    pepoch::{self as epoch, PAtomic},
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
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

    /// 누가 dequeue 했는지 식별
    // usize인 이유: AtomicPtr이 될 경우 불필요한 SMR 발생
    pub(crate) dequeuer: AtomicUsize, // TODO: 이름 바꾸기
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            next: PAtomic::null(),
            dequeuer: AtomicUsize::new(no_owner()),
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
        &self.dequeuer
    }
}
