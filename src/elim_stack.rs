//! Persistent stack based on Elimination backoff stack

// TODO: treiber 보다 느림...
//       - 느린 이유 의심: `push(value)` 시 inner stack node와 exchanger node에 각각 value를 clone 함
//       - 밝혀진 느린 이유: exchange 하게 되면 느려짐. exchager의 helping 메커니즘이 문제일 수도 있음

use chrono::Duration;
use crossbeam_epoch::Guard;
use rand::{thread_rng, Rng};

use crate::exchanger::{Exchanger, TryExchange};
use crate::persistent::*;
use crate::plocation::ll::persist_obj;
use crate::plocation::pool::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::stack::*;

const ELIM_SIZE: usize = 16;
const ELIM_DELAY: i64 = 10; // 10ms

#[inline]
fn get_random_elim_index() -> usize {
    thread_rng().gen::<usize>() % ELIM_SIZE
}

#[derive(Debug, Clone)]
enum Request<T> {
    Push(T),
    Pop,
}

unsafe impl<T: Send> Send for Request<T> {}

/// ElimStack op의 상태를 나타냄
#[derive(Debug, Clone)]
enum State {
    /// Inner stack에 push/pop 시도 중 (default)
    TryingInner,

    /// elimination array에 push/pop 시도 중
    Eliminating,

    /// reset을 atomic하게 하기 위한 플래그 -> reset이 끝나면 UsingStack으로 변경
    Resetting,
}

/// ElimStack의 push operation
#[derive(Debug)]
pub struct TryPush<T: 'static + Clone, S: Stack<T>> {
    /// client의 push 시도 상태
    state: State,

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
            state: State::TryingInner,
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
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        stack.try_push(self, value, guard, pool)
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        if nested {
            self.state = State::Resetting;
            persist_obj(&self.state, true);
        }

        self.try_exchange.reset(true, guard, pool);
        self.try_push.reset(true, guard, pool);

        if nested {
            self.state = State::TryingInner;
            persist_obj(&self.state, true);
        }
    }

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        self.try_push.set_recovery(pool);
        self.try_exchange.set_recovery(pool);

        // TODO: reset 중이었다가 crash난 애의 reset을 끝내줄 수 있음.
        //       그러면 run에서 reset 중인지 검사 불필요.
    }
}

/// `ElimStack::pop()`를 호출할 때 쓰일 client
#[derive(Debug)]
pub struct TryPop<T: 'static + Clone, S: Stack<T>> {
    /// client의 pop 시도 상태
    state: State,

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
            state: State::TryingInner,
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
    type Error = TryFail;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        stack.try_pop(self, guard, pool)
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        if nested {
            self.state = State::Resetting;
            persist_obj(&self.state, true);
        }

        self.try_pop.reset(true, guard, pool);
        self.try_exchange.reset(true, guard, pool);

        if nested {
            self.state = State::TryingInner;
            persist_obj(&self.state, true);
        }
    }

    fn set_recovery(&mut self, pool: &'static PoolHandle) {
        self.try_pop.set_recovery(pool);
        self.try_exchange.set_recovery(pool);

        // TODO: reset 중이었다가 crash난 애의 reset을 끝내줄 수 있음.
        //       그러면 run에서 reset 중인지 검사 불필요.
    }
}

/// Persistent Elimination backoff stack
/// - ELIM_SIZE: size of elimination array
/// - ELIM_DELAY: elimination waiting time (milliseconds)
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

impl<T, S> ElimStack<T, S>
where
    T: 'static + Clone,
    S: 'static + Stack<T>,
{
    /// elimination stack의 push를 시도
    ///
    /// 1. inner stack에 push를 시도
    /// 2. 실패시 elimination exchanger에서 pop request와 exchange 시도
    fn try_push(
        &self,
        client: &mut TryPush<T, S>,
        value: T,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), TryFail> {
        if let State::Resetting = client.state {
            // TODO: recovery 중에만 검사하도록
            client.reset(false, guard, pool);
        }

        // TODO 느린 이유 의심: `push(value)` 시 inner stack node와 exchanger node에 각각 value를 clone 함
        if let State::TryingInner = client.state {
            if client
                .try_push
                .run(&self.inner, value.clone(), guard, pool)
                .is_ok()
            {
                return Ok(());
            }

            // Trying push was fail, now try elimination backoff
            client.state = State::Eliminating;
            persist_obj(&client.state, true);
        }

        client
            .try_exchange
            .run(
                &self.slots[client.elim_idx],
                (
                    Request::Push(value),
                    Duration::milliseconds(ELIM_DELAY),
                    |req| matches!(req, Request::Pop),
                ),
                guard,
                pool,
            )
            .map(|_| ())
            .map_err(|_| {
                client.state = State::TryingInner;
                persist_obj(&client.state, true);
                TryFail
            })
    }

    /// elimination stack의 pop를 시도
    ///
    /// 1. inner stack에 pop를 시도
    /// 2. 실패시 elimination exchanger에서 push request와 exchange 시도
    fn try_pop(
        &self,
        client: &mut TryPop<T, S>,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<T>, TryFail> {
        if let State::Resetting = client.state {
            // TODO: recovery 중에만 검사하도록
            client.reset(false, guard, pool);
        }

        if let State::TryingInner = client.state {
            if let Ok(v) = client.try_pop.run(&self.inner, (), guard, pool) {
                return Ok(v);
            }

            // Trying pop was fail, now try elimination backoff
            client.state = State::Eliminating;
            persist_obj(&client.state, true);
        }

        client
            .try_exchange
            .run(
                &self.slots[client.elim_idx],
                (Request::Pop, Duration::milliseconds(ELIM_DELAY), |req| {
                    matches!(req, Request::Push(_))
                }),
                guard,
                pool,
            )
            .map(|req| {
                if let Request::Push(v) = req {
                    Some(v)
                } else {
                    unreachable!("No exchange between pops")
                }
            })
            .map_err(|_| {
                client.state = State::TryingInner;
                persist_obj(&client.state, true);
                TryFail
            })
    }
}

unsafe impl<T: Clone + Send + Sync, S: Send + Stack<T>> Send for ElimStack<T, S> {}
unsafe impl<T: Clone, S: Stack<T>> Sync for ElimStack<T, S> {}

impl<T, S> Stack<T> for ElimStack<T, S>
where
    T: 'static + Clone,
    S: 'static + Stack<T>,
{
    type TryPush = TryPush<T, S>;
    type TryPop = TryPop<T, S>;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::stack::tests::*;
    use crate::treiber_stack::TreiberStack;
    use crate::utils::tests::*;
    use serial_test::serial;

    const NR_THREAD: usize = 12;
    const COUNT: usize = 100_000;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    type ElimTreiber = ElimStack<usize, TreiberStack<usize>>;
    impl TestRootObj for ElimTreiber {}
    impl TestRootObj for ElimStack<usize, ElimTreiber> {}

    /// treiber stack을 inner stack으로 하는 elim stack의 push-pop 테스트
    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn push_pop() {
        type O = ElimTreiber;
        type M = PushPop<O, NR_THREAD, COUNT>;

        const FILE_NAME: &str = "elim_push_pop.pool";
        run_test::<O, M, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1)
    }

    /// "treiber stack을 inner stack으로 하는 elim stack"을 inner stack으로 하는 elim stack의 push-pop 테스트
    // 테스트시 정적할당을 위해 스택 크기를 늘려줘야함 (e.g. `RUST_MIN_STACK=1073741824 cargo test`)
    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn push_pop_double() {
        type O = ElimStack<usize, ElimTreiber>;
        type M = PushPop<O, NR_THREAD, COUNT>;

        const FILE_NAME: &str = "elim_push_pop_double.pool";
        run_test::<O, M, _>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }
}
