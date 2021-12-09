//! Persistent stack based on Elimination backoff stack

use std::sync::atomic::Ordering;

use crossbeam_epoch::{self as epoch, Guard};
use rand::{thread_rng, Rng};

use crate::{
    node::Node,
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    ploc::{Checkpoint, RetryLoop},
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
    treiber_stack::{self, TreiberStack},
};

const ELIM_SIZE: usize = 2;

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
            elim_idx: get_random_elim_index(), // TODO(opt): Fixed index vs online random index 성능 비교
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
        &mut self,
        elim: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
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
            .run(
                &elim.slots[self.elim_idx],
                (node, |req| matches!(req, Request::Pop)),
                rec,
                guard,
                pool,
            )
            .map(|_| ())
            .map_err(|_| TryFail)
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
    pop_node: Checkpoint<PAtomic<Node<Request<T>>>>,

    /// elimination exchanger의 exchange client
    try_exchange: TryExchange<Request<T>>,
}

impl<T: 'static + Clone> Default for TryPop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
            elim_idx: get_random_elim_index(), // TODO(opt): Fixed index vs online random index 성능 비교
            pop_node: Default::default(),
            try_exchange: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryPop<T> {
    fn filter(try_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        treiber_stack::TryPop::filter(&mut try_pop.try_pop, gc, pool);
        Checkpoint::filter(&mut try_pop.pop_node, gc, pool);
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
        &mut self,
        elim: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
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
        // TODO(must): (1) try_pop이 pop_node를 인풋으로 받고 (2) pop이 뭐로 성공했는지 인식해서 해제해줌?
        let pop_node = POwned::new(Node::from(Request::Pop), pool);
        persist_obj(unsafe { pop_node.deref(pool) }, true);

        let pop_node = self
            .pop_node
            .run(
                (),
                (PAtomic::from(pop_node), |aborted| {
                    let guard = unsafe { epoch::unprotected() };
                    let d = aborted.load(Ordering::Relaxed, guard);
                    unsafe { guard.defer_pdestroy(d) };
                }),
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        let req = self
            .try_exchange
            .run(
                &elim.slots[self.elim_idx],
                (pop_node, |req| matches!(req, Request::Push(_))),
                rec,
                guard,
                pool,
            )
            .map_err(|_| TryFail)?;

        if let Request::Push(v) = req {
            Ok(Some(v))
        } else {
            unreachable!("exchange 조건으로 인해 Push랑만 교환함")
        }
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
        self.pop_node.reset(guard, pool);
        self.try_exchange.reset(guard, pool);
    }
}

/// Persistent Elimination backoff stack
/// - ELIM_SIZE: size of elimination array
#[derive(Debug)]
pub struct ElimStack<T: 'static + Clone> {
    inner: TreiberStack<Request<T>>,
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
        TreiberStack::filter(&mut elim_stack.inner, gc, pool);
        for slot in elim_stack.slots.as_mut() {
            Exchanger::filter(slot, gc, pool);
        }
    }
}

impl<T: Clone> PDefault for ElimStack<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Self::default()
    }
}

unsafe impl<T: Clone + Send + Sync> Send for ElimStack<T> {}
unsafe impl<T: Clone> Sync for ElimStack<T> {}

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<Request<T>>>>,
    try_push: RetryLoop<TryPush<T>>,
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
    fn filter(push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut push.node, gc, pool);
        RetryLoop::filter(&mut push.try_push, gc, pool);
    }
}

impl<T: Clone> Memento for Push<T> {
    type Object<'o> = &'o ElimStack<T>;
    type Input<'o> = T;
    type Output<'o>
    where
        T: 'o,
    = ();
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = POwned::new(Node::from(Request::Push(value)), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = self
            .node
            .run(
                (),
                (PAtomic::from(node), |aborted| {
                    let guard = unsafe { epoch::unprotected() };
                    let d = aborted.load(Ordering::Relaxed, guard);
                    unsafe { guard.defer_pdestroy(d) };
                }),
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        self.try_push
            .run(stack, node, rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.node.reset(guard, pool);
        self.try_push.reset(guard, pool);
    }
}

unsafe impl<T: 'static + Clone> Send for Push<T> {}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: 'static + Clone> {
    try_pop: RetryLoop<TryPop<T>>,
}

impl<T: Clone> Default for Pop<T> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for Pop<T> {
    fn filter(pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        RetryLoop::filter(&mut pop.try_pop, gc, pool);
    }
}

impl<T: Clone> Memento for Pop<T> {
    type Object<'o> = &'o ElimStack<T>;
    type Input<'o> = ();
    type Output<'o>
    where
        T: 'o,
    = Option<T>;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.try_pop
            .run(stack, (), rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.try_pop.reset(guard, pool);
    }
}

unsafe impl<T: Clone> Send for Pop<T> {}

impl<T: 'static + Clone> Stack<T> for ElimStack<T> {
    type Push = Push<T>;
    type Pop = Pop<T>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ds::stack::tests::PushPop, test_utils::tests::*};
    use rusty_fork::rusty_fork_test;

    const NR_THREAD: usize = 2;
    const COUNT: usize = 10_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    impl TestRootObj for ElimStack<usize> {}

    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    rusty_fork_test! {
        #[test]
        fn push_pop() {
            const FILE_NAME: &str = "elim_push_pop.pool";
            run_test::<ElimStack<usize>, PushPop<ElimStack<usize>, NR_THREAD, COUNT>, _>(
                FILE_NAME,
                FILE_SIZE,
                NR_THREAD + 1,
            )
        }
    }
}
