//! Persistent pipe

use crate::{persistent::POp, plocation::{PoolHandle, ll::persist_obj}};

/// `from` op과 `to` op을 failure-atomic하게 실행하는 pipe operation
///
/// - `'o`: 연결되는 두 Op(i.e. `Op1` 및 `Op2`)의 lifetime
#[derive(Debug)]
pub struct Pipe<Op1, Op2>
where
    for<'o> Op1: POp<Output<'o> = Op2::Input>,
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
    for<'o> Op1: POp<Output<'o> = Op2::Input>,
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

impl<Op1, Op2> POp for Pipe<Op1, Op2>
where
    for<'o> Op1: POp<Output<'o> = Op2::Input>,
    Op2: POp,
{
    type Object<'o> = (Op1::Object<'o>, Op2::Object<'o>);
    type Input = Op1::Input;
    type Output<'o> = Op2::Output<'o>;
    type Error = ();

    fn run<'o, O: POp>(
        &mut self,
        (from_obj, to_obj): Self::Object<'o>,
        init: Self::Input,
        pool: &PoolHandle<O>,
    ) -> Result<Self::Output<'o>, Self::Error> {
        if self.resetting { // TODO: recovery 중에만 검사하도록
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false);
        }

        let v = self.from.run(from_obj, init, pool).map_err(|_| ())?;
        self.to.run(to_obj, v, pool).map_err(|_| ())
    }

    fn reset(&mut self, nested: bool) {
        if !nested {
            self.resetting = true;
            persist_obj(&self.resetting, true);
        }

        self.from.reset(true);
        self.to.reset(true);

        if !nested {
            self.resetting = false;
            persist_obj(&self.resetting, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use crossbeam_utils::thread;
    use std::sync::atomic::Ordering;

    use crate::pepoch::{self, PAtomic};
    use crate::persistent::*;
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
        fn init<O: POp>(&self, pool: &PoolHandle<O>) {
            let guard = unsafe { pepoch::unprotected(&pool) };
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

        fn run<'o, O: POp>(
            &mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &PoolHandle<O>,
        ) -> Result<Self::Output<'o>, Self::Error> {
            self.init(pool);

            // Alias
            let guard = unsafe { pepoch::unprotected(pool) };
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

        fn reset(&mut self, _nested: bool) {
            todo!("reset test")
        }
    }

    impl TestRootOp for Transfer {}

    const FILE_NAME: &str = "pipe_concur.pool";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn pipe_concur() {
        run_test::<Transfer, _>(FILE_NAME, FILE_SIZE)
    }
}
