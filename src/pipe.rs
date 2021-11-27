//! Persistent pipe

use crossbeam_epoch::Guard;

use crate::{
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

/// `from` op과 `to` op을 failure-atomic하게 실행하는 pipe operation
///
/// - `'o`: 연결되는 두 Op(i.e. `Op1` 및 `Op2`)의 lifetime
#[derive(Debug)]
pub struct Pipe<Op1, Op2>
where
    for<'o> Op1: 'static + Memento<Output<'o> = Op2::Input>,
    Op2: Memento,
{
    /// 먼저 실행될 op. `Pipe` op의 input은 `from` op의 input과 같음
    from: Op1,

    /// 다음에 실행될 op. `Pipe` op의 output은 `to` op의 output과 같음
    to: Op2,

    /// reset 중인지 나타내는 flag
    resetting: bool,
}

impl<Op1, Op2> Default for Pipe<Op1, Op2>
where
    for<'o> Op1: 'static + Memento<Output<'o> = Op2::Input>,
    Op2: Memento,
{
    fn default() -> Self {
        Self {
            from: Default::default(),
            to: Default::default(),
            resetting: false,
        }
    }
}

impl<Op1, Op2> Collectable for Pipe<Op1, Op2>
where
    for<'o> Op1: 'static + Memento<Output<'o> = Op2::Input>,
    Op2: Memento,
{
    fn filter(pipe: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Op1::filter(&mut pipe.from, gc, pool);
        Op2::filter(&mut pipe.to, gc, pool);
    }
}

impl<Op1, Op2> Memento for Pipe<Op1, Op2>
where
    for<'o> Op1: Memento<Output<'o> = Op2::Input>,
    Op2: Memento,
{
    type Object<'o> = (Op1::Object<'o>, Op2::Object<'o>);
    type Input = Op1::Input;
    type Output<'o>
    where
        Op2: 'o,
    = Op2::Output<'o>;
    type Error = ();

    fn run<'o>(
        &'o mut self,
        (from_obj, to_obj): Self::Object<'o>,
        init: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        if self.resetting {
            // TODO: recovery 중에만 검사하도록
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false, guard, pool);
        }

        let v = self.from.run(from_obj, init, guard, pool).map_err(|_| ())?;
        self.to.run(to_obj, v, guard, pool).map_err(|_| ())
    }

    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle) {
        if !nested {
            self.resetting = true;
            persist_obj(&self.resetting, true);
        }

        self.from.reset(true, guard, pool);
        self.to.reset(true, guard, pool);

        if !nested {
            self.resetting = false;
            persist_obj(&self.resetting, true);
        }
    }
}

impl<Op1, Op2> Drop for Pipe<Op1, Op2>
where
    for<'o> Op1: Memento<Output<'o> = Op2::Input>,
    Op2: Memento,
{
    fn drop(&mut self) {
        todo!("하위 Memento들이 reset 되어있지않으면 panic")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistent::PDefault;
    use crate::plocation::ralloc::{Collectable, GarbageCollection};
    use crate::queue::*;
    use crate::utils::tests::*;
    use serial_test::serial;

    const COUNT: usize = 1_000_000;

    impl Collectable for [Queue<usize>; 2] {
        fn filter(q_arr: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Queue::<usize>::filter(&mut q_arr[0], gc, pool);
            Queue::<usize>::filter(&mut q_arr[1], gc, pool);
        }
    }

    impl PDefault for [Queue<usize>; 2] {
        fn pdefault(pool: &'static PoolHandle) -> Self {
            [Queue::pdefault(pool), Queue::pdefault(pool)]
        }
    }

    struct Transfer {
        pipes: [Pipe<DequeueSome<usize>, Enqueue<usize>>; COUNT],
        suppliers: [Enqueue<usize>; COUNT],
        consumers: [DequeueSome<usize>; COUNT],
    }

    impl Default for Transfer {
        fn default() -> Self {
            Self {
                pipes: array_init::array_init(|_| Pipe::default()),
                suppliers: array_init::array_init(|_| Enqueue::default()),
                consumers: array_init::array_init(|_| DequeueSome::default()),
            }
        }
    }

    impl Collectable for Transfer {
        fn filter(transfer: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            // Call filter of inner struct
            for pipe in transfer.pipes.as_mut() {
                Pipe::filter(pipe, gc, pool);
            }
            for enq in transfer.suppliers.as_mut() {
                Enqueue::filter(enq, gc, pool);
            }
            for deqsome in transfer.consumers.as_mut() {
                DequeueSome::filter(deqsome, gc, pool);
            }
        }
    }

    impl Memento for Transfer {
        type Object<'o> = &'o [Queue<usize>; 2];
        type Input = usize; // tid(mid)
        type Output<'o> = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            q_arr: Self::Object<'o>,
            tid: Self::Input,
            guard: &mut Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            let (q1, q2) = (&q_arr[0], &q_arr[1]);

            match tid {
                // T0: Supply q1
                0 => {
                    for (i, enq) in self.suppliers.iter_mut().enumerate() {
                        let _ = enq.run(q1, i, guard, pool);
                    }
                }
                // T1: Transfer q1->q2
                1 => {
                    for pipe in self.pipes.iter_mut() {
                        let _ = pipe.run((q1, q2), (), guard, pool);
                    }
                }
                // T2: Consume q2
                2 => {
                    for (i, deq) in self.consumers.iter_mut().enumerate() {
                        let v = deq.run(&q2, (), guard, pool).unwrap();
                        assert_eq!(v, i);
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }

        fn reset(&mut self, _: bool, _: &mut Guard, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootObj for [Queue<usize>; 2] {}
    impl TestRootMemento<[Queue<usize>; 2]> for Transfer {}

    const FILE_NAME: &str = "pipe_concur.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn pipe_concur() {
        run_test::<[Queue<usize>; 2], Transfer, _>(FILE_NAME, FILE_SIZE, 3)
    }
}
