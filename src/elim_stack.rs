//! Persistent stack based on Elimination backoff stack

use std::sync::atomic::Ordering;

use crossbeam_epoch::Guard;
use rand::{thread_rng, Rng};

use crate::{
    exchanger::{Exchanger, TryExchange},
    node::Node,
    pepoch::{PAtomic, POwned, PShared},
    persistent::{Memento, PDefault, AtomicReset},
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    stack::{Stack, TryFail},
    treiber_stack::{self},
};

const ELIM_SIZE: usize = 16;

#[inline]
fn get_random_elim_index() -> usize {
    thread_rng().gen::<usize>() % ELIM_SIZE
}

#[derive(Debug, Clone)]
enum Request<T> {
    Push(T),
    Pop,
}

/// ElimStack의 push operation
#[derive(Debug)]
struct TryPush<T: 'static + Clone> {
    /// inner stack의 push op
    try_push: treiber_stack::TryPush<Request<T>>,

    /// elimination exchange를 위해 할당된 index
    elim_idx: usize,

    /// elimination exchanger의 exchange op
    try_exchange: TryExchange<Request<T>>,
}

impl<T: Clone> Default for TryPush<T> {
    fn default() -> Self {
        Self {
            try_push: Default::default(),
            elim_idx: get_random_elim_index(), // TODO: Fixed index vs online random index 성능 비교
            try_exchange: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryPush<T> {
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        treiber_stack::TryPush::filter(&mut try_push.try_push, gc, pool);
        TryExchange::filter(&mut try_push.try_exchange, gc, pool);
    }
}

impl<T> Memento for TryPush<T>
where
    T: 'static + Clone,
{
    type Object<'o> = &'o ElimStack<T>;
    type Input<'o> = PShared<'o, Node<Request<T>>>;
    type Output<'o> = ();
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        elim: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if self
            .try_push
            .run(&elim.inner, node, rec, guard, pool)
            .is_ok()
        {
            return Ok(());
        }

        self.try_exchange
            .run(&elim.slots[self.elim_idx], node, rec, guard, pool)
            .map(|_| ())
            .map_err(|_| TryFail)

        // TODO: exchanger가 교환 조건 받도록 해야 함
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_push.reset(guard, pool);
        self.try_exchange.reset(guard, pool);
    }
}

/// `ElimStack::pop()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct TryPop<T: 'static + Clone> {
    /// inner stack의 pop client
    try_pop: treiber_stack::TryPop<Request<T>>,

    /// elimination exchange를 위해 할당된 index
    elim_idx: usize,

    /// exchanger에 들어갈 node
    exchange_pop_node: PAtomic<Node<Request<T>>>,

    /// elimination exchanger의 exchange client
    try_exchange: AtomicReset<TryExchange<Request<T>>>,
}

impl<T: 'static + Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
            elim_idx: get_random_elim_index(), // TODO: Fixed index vs online random index 성능 비교
            exchange_pop_node: PAtomic::null(),
            try_exchange: AtomicReset::default(),
        }
    }
}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        treiber_stack::TryPop::filter(&mut try_pop.try_pop, gc, pool);
        AtomicReset::filter(&mut try_pop.try_exchange, gc, pool);
    }
}

impl<T> Memento for TryPop<T>
where
    T: 'static + Clone,
{
    type Object<'o> = &'o ElimStack<T>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        elim: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Ok(popped) = self.try_pop.run(&elim.inner, (), rec, guard, pool) {
            let ret = popped.map(|req| {
                if let Request::Push(v) = req {
                    v
                } else {
                    unreachable!("stack에 Pop req가 들어가진 않음")
                }
            });
            return Ok(ret);
        }

        // exchanger에 pop req를 담은 node를 넣어줘야 됨
        let node = if rec {
            let node = self.exchange_pop_node.load(Ordering::Relaxed, guard);
            if node.is_null() {
                self.new_pop_node(guard, pool)
            } else {
                node
            }
        } else {
            self.new_pop_node(guard, pool)
        };

        let req = self
            .try_exchange
            .run(&elim.slots[self.elim_idx], node, rec, guard, pool)
            .map_err(|_| {
                self.try_exchange.reset(guard, pool);
                TryFail
            })?;

        if let Request::Push(v) = req {
            Ok(Some(v))
        } else {
            unreachable!("exchange 조건으로 인해 Push랑만 교환함")
        }

        // TODO: exchanger가 교환 조건 받도록 해야 함
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
        self.try_exchange.reset(guard, pool);
    }
}

impl<T: Clone> TryPop<T> {
    #[inline]
    fn new_pop_node<'g>(
        &self,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> PShared<'g, Node<Request<T>>> {
        let pop_node = POwned::new(Node::from(Request::Pop), pool).into_shared(guard);
        self.exchange_pop_node.store(pop_node, Ordering::Relaxed);
        persist_obj(&self.exchange_pop_node, true);
        pop_node
    }
}

/// Persistent Elimination backoff stack
/// - ELIM_SIZE: size of elimination array
#[derive(Debug)]
pub struct ElimStack<T: 'static + Clone> {
    inner: treiber_stack::TreiberStack<Request<T>>,
    slots: [Exchanger<Request<T>>; ELIM_SIZE],
}

impl<T: Clone> Default for ElimStack<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            slots: array_init::array_init(|_| Exchanger::<Request<T>>::default()),
        }
    }
}

impl<T: Clone> Collectable for ElimStack<T> {
    fn filter(elim_stack: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        // TODO

        // S::filter(&mut elim_stack.inner, gc, pool);
        // for slot in elim_stack.slots.as_mut() {
        //     Exchanger::filter(slot, gc, pool);
        // }
    }
}

impl<T: Clone> PDefault for ElimStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

unsafe impl<T: Clone + Send + Sync> Send for ElimStack<T> {}
unsafe impl<T: Clone> Sync for ElimStack<T> {}
