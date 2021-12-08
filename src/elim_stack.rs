//! Persistent stack based on Elimination backoff stack

use crossbeam_epoch::Guard;
use rand::{thread_rng, Rng};

use crate::{
    exchanger::{Exchanger, TryExchange},
    node::Node,
    pepoch::PShared,
    persistent::{Memento, PDefault},
    plocation::{
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
        let inner_res = self.try_push.run(&elim.inner, node, rec, guard, pool);
        // inner_res.or(self
        //     .try_exchange
        //     .run(&elim.slots[self.elim_idx], node, rec, guard, pool));
        todo!()

        // TODO: exchanger가 교환 조건 받도록
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

    /// elimination exchanger의 exchange client
    try_exchange: TryExchange<Request<T>>,
}

impl<T: 'static + Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
            elim_idx: get_random_elim_index(), // TODO: Fixed index vs online random index 성능 비교
            try_exchange: TryExchange::default(),
        }
    }
}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        treiber_stack::TryPop::filter(&mut try_pop.try_pop, gc, pool);
        TryExchange::filter(&mut try_pop.try_exchange, gc, pool);
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
