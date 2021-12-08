//! Persistent stack based on Elimination backoff stack

use crossbeam_epoch::Guard;
use rand::{thread_rng, Rng};

use crate::{
    exchanger::{Exchanger, TryExchange},
    persistent::{PDefault, Memento},
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    stack::{Stack, TryFail},
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
pub struct TryPush<T: 'static + Clone, S: Stack<T>> {
    /// inner stack의 push op
    try_push: S::TryPush,

    /// elimination exchange를 위해 할당된 index
    elim_idx: usize,

    /// elimination exchanger의 exchange op
    try_exchange: TryExchange<Request<T>>,
}

impl<T: Clone, S: Stack<T>> Default for TryPush<T, S> {
    fn default() -> Self {
        Self {
            try_push: Default::default(),
            elim_idx: get_random_elim_index(), // TODO: Fixed index vs online random index 성능 비교
            try_exchange: Default::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for TryPush<T, S> {
    fn filter(try_push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        S::TryPush::filter(&mut try_push.try_push, gc, pool);
        TryExchange::filter(&mut try_push.try_exchange, gc, pool);
    }
}

impl<T, S> Memento for TryPush<T, S>
where
    T: 'static + Clone,
    S: 'static + Stack<T>,
{
    type Object<'o> = &'o ElimStack<T, S>;
    type Input<'o> = T;
    type Output<'o> = ();
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        todo!();
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_push.reset(guard, pool);
        self.try_exchange.reset(guard, pool);
    }
}

/// `ElimStack::pop()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct TryPop<T: 'static + Clone, S: Stack<T>> {
    /// inner stack의 pop client
    try_pop: S::TryPop,

    /// elimination exchange를 위해 할당된 index
    elim_idx: usize,

    /// elimination exchanger의 exchange client
    try_exchange: TryExchange<Request<T>>,
}

impl<T: 'static + Clone, S: Stack<T>> Default for TryPop<T, S> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
            elim_idx: get_random_elim_index(), // TODO: Fixed index vs online random index 성능 비교
            try_exchange: TryExchange::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for TryPop<T, S> {
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        S::TryPop::filter(&mut try_pop.try_pop, gc, pool);
        TryExchange::filter(&mut try_pop.try_exchange, gc, pool);
    }
}

impl<T, S> Memento for TryPop<T, S>
where
    T: 'static + Clone,
    S: 'static + Stack<T>,
{
    type Object<'o> = &'o ElimStack<T, S>;
    type Input<'o> = ();
    type Output<'o> = Option<T>;
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        todo!()
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
        self.try_exchange.reset(guard, pool);
    }
}

/// Persistent Elimination backoff stack
/// - ELIM_SIZE: size of elimination array
#[derive(Debug)]
pub struct ElimStack<T: 'static + Clone, S: Stack<T>> {
    inner: S,
    slots: [Exchanger<Request<T>>; ELIM_SIZE],
}

impl<T: Clone, S: Stack<T>> Default for ElimStack<T, S> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            slots: array_init::array_init(|_| Exchanger::<Request<T>>::default()),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for ElimStack<T, S> {
    fn filter(elim_stack: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        S::filter(&mut elim_stack.inner, gc, pool);
        for slot in elim_stack.slots.as_mut() {
            Exchanger::filter(slot, gc, pool);
        }
    }
}

impl<T: Clone, S: Stack<T>> PDefault for ElimStack<T, S> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

unsafe impl<T: Clone + Send + Sync, S: Send + Stack<T>> Send for ElimStack<T, S> {}
unsafe impl<T: Clone, S: Stack<T>> Sync for ElimStack<T, S> {}
