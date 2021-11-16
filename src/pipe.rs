//! Persistent pipe

use crate::{
    persistent::POp,
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
    for<'o> Op1: 'static + POp<Output<'o> = Op2::Input>,
    Op2: POp,
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
    for<'o> Op1: 'static + POp<Output<'o> = Op2::Input>,
    Op2: POp,
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
    for<'o> Op1: 'static + POp<Output<'o> = Op2::Input>,
    Op2: POp,
{
    fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
        todo!()
    }
}

impl<Op1, Op2> POp for Pipe<Op1, Op2>
where
    for<'o> Op1: POp<Output<'o> = Op2::Input>,
    Op2: POp,
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
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        if self.resetting {
            // TODO: recovery 중에만 검사하도록
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false, pool);
        }

        let v = self.from.run(from_obj, init, pool).map_err(|_| ())?;
        self.to.run(to_obj, v, pool).map_err(|_| ())
    }

    fn reset(&mut self, nested: bool, pool: &'static PoolHandle) {
        if !nested {
            self.resetting = true;
            persist_obj(&self.resetting, true);
        }

        self.from.reset(true, pool);
        self.to.reset(true, pool);

        if !nested {
            self.resetting = false;
            persist_obj(&self.resetting, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;
    use serial_test::serial;
    use std::sync::atomic::Ordering;

    use crate::pepoch::{self, PAtomic};
    use crate::persistent::*;
    use crate::plocation::ralloc::{Collectable, GarbageCollection};
    use crate::queue::*;
    use crate::utils::tests::*;

    use super::*;

    const COUNT: usize = 1_000_000;

    struct Transfer {
        q1: PAtomic<Queue<usize>>,
        q2: PAtomic<Queue<usize>>,
        pipes: [Pipe<DequeueSome<usize>, Enqueue<usize>>; COUNT],
        suppliers: [Enqueue<usize>; COUNT],
        consumers: [DequeueSome<usize>; COUNT],
    }

    impl Default for Transfer {
        fn default() -> Self {
            Self {
                q1: Default::default(),
                q2: Default::default(),
                pipes: array_init::array_init(|_| Pipe::default()),
                suppliers: array_init::array_init(|_| Enqueue::default()),
                consumers: array_init::array_init(|_| DequeueSome::default()),
            }
        }
    }

    impl Transfer {
        fn init(&self, pool: &PoolHandle) {
            let guard = unsafe { pepoch::unprotected() };
            let q1 = self.q1.load(Ordering::SeqCst, guard);
            let q2 = self.q2.load(Ordering::SeqCst, guard);

            // Initialize q1
            if q1.is_null() {
                let q = Queue::<usize>::new(pool);
                // TODO: 여기서 crash나면 leak남
                self.q1.store(q, Ordering::SeqCst);
            }

            // Initialize q2
            if q2.is_null() {
                let q = Queue::<usize>::new(pool);
                // TODO: 여기서 crash나면 leak남
                self.q2.store(q, Ordering::SeqCst);
            }
        }
    }

    impl POp for Transfer {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            self.init(pool);

            // Alias
            let guard = unsafe { pepoch::unprotected() };
            let q1 = unsafe { self.q1.load(Ordering::SeqCst, guard).deref(pool) };
            let q2 = unsafe { self.q2.load(Ordering::SeqCst, guard).deref(pool) };
            let pipes = &mut self.pipes;
            let suppliers = &mut self.suppliers;
            let consumers = &mut self.consumers;

            #[allow(box_pointers)]
            thread::scope(|scope| {
                // T0: Supply q1
                let _ = scope.spawn(move |_| {
                    for (i, enq) in suppliers.iter_mut().enumerate() {
                        let _ = enq.run(q1, i, pool);
                    }
                });

                // T1: Transfer q1->q2
                let _ = scope.spawn(move |_| {
                    for pipe in pipes.iter_mut() {
                        let _ = pipe.run((q1, q2), (), pool);
                    }
                });

                // T2: Consume q2
                let _ = scope.spawn(move |_| {
                    for (i, deq) in consumers.iter_mut().enumerate() {
                        let v = deq.run(&q2, (), pool).unwrap();
                        assert_eq!(v, i);
                    }
                });
            })
            .unwrap();

            Ok(())
        }

        fn reset(&mut self, _nested: bool, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl Collectable for Transfer {
        fn filter(_s: &mut Self, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
            todo!()
        }
    }

    impl TestRootOp for Transfer {}

    const FILE_NAME: &str = "pipe_concur.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn pipe_concur() {
        run_test::<Transfer, _>(FILE_NAME, FILE_SIZE)
    }
}
