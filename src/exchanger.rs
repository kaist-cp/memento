//! Persistent Exchanger

use std::marker::PhantomData;

use crossbeam_epoch::Guard;

use crate::{
    atomic_update::{Delete, DeleteHelper, Insert, SMOAtomic, Update},
    atomic_update_common::Traversable,
    node::Node,
    pepoch::PShared,
    persistent::Memento,
    plocation::{ralloc::Collectable, PoolHandle},
};

/// Exchanger의 try exchange 실패
#[derive(Debug)]
pub struct TryFail;

/// TODO: doc
#[derive(Debug)]
pub struct ExchangeNode<T> {
    _marker: PhantomData<T>,
}

/// Exchanger의 try exchange
#[derive(Debug)]
pub struct TryExchange<T: Clone> {
    insert: Insert<Exchanger<T>, Node<ExchangeNode<T>, Exchanger<T>>>,
    update: Update<Exchanger<T>, Node<ExchangeNode<T>, Exchanger<T>>, Self>,
    delete: Delete<Exchanger<T>, Node<ExchangeNode<T>, Exchanger<T>>, Self>,
}

impl<T: Clone> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            insert: Default::default(),
            update: Default::default(),
            delete: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryExchange<T> {
    fn filter(
        s: &mut Self,
        gc: &mut crate::plocation::ralloc::GarbageCollection,
        pool: &PoolHandle,
    ) {
        todo!()
    }
}

impl<T: Clone> Memento for TryExchange<T> {
    type Object<'o> = Exchanger<T>;
    type Input<'o> = Node<ExchangeNode<T>, Exchanger<T>>;
    type Output<'o>
    where
        T: 'o,
    = &'o Node<ExchangeNode<T>, Exchanger<T>>;
    type Error<'o>
    where
        T: 'o,
    = TryFail;

    fn run<'o>(
        &'o mut self,
        object: Self::Object<'o>,
        input: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        todo!()
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

impl<T: Clone> DeleteHelper<Exchanger<T>, Node<ExchangeNode<T>, Exchanger<T>>> for TryExchange<T> {
    fn prepare<'g>(
        cur: PShared<'_, Node<ExchangeNode<T>, Exchanger<T>>>,
        obj: &Exchanger<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<ExchangeNode<T>, Exchanger<T>>>>, ()> {
        todo!()
    }

    fn node_when_deleted<'g>(
        deleted: PShared<'_, Node<ExchangeNode<T>, Exchanger<T>>>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, Node<ExchangeNode<T>, Exchanger<T>>> {
        todo!()
    }
}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<T: Clone> {
    slot: SMOAtomic<Self, Node<ExchangeNode<T>, Self>, TryExchange<T>>,
}

impl<T: Clone> Traversable<Node<ExchangeNode<T>, Exchanger<T>>> for Exchanger<T> {
    fn search(
        &self,
        target: PShared<'_, Node<ExchangeNode<T>, Exchanger<T>>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        todo!()
    }
}
