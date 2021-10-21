//! Persistent pipe

use crate::{persistent::POp, plocation::{PoolHandle, ll::persist_obj}};

/// `from` op과 `to` op을 failure-atomic하게 실행하는 pipe operation
///
/// - `'p`: 연결되는 두 Op(i.e. `Op1` 및 `Op2`)의 lifetime
/// - `O#`: `Op#`이 실행되는 object
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

    fn run<'o, O: POp>(
        &mut self,
        (from_obj, to_obj): Self::Object<'o>,
        init: Self::Input,
        pool: &PoolHandle<O>,
    ) -> Self::Output<'o> {
        if self.resetting {
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false);
        }

        let v = self.from.run(from_obj, init, pool);
        self.to.run(to_obj, v, pool)
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
    use std::sync::atomic::Ordering;

    use crate::pepoch::{self, PAtomic};
    use crate::persistent::*;
    use crate::plocation::Pool;
    use crate::queue::*;
    use crate::utils::tests::get_test_path;

    use super::*;

    use crossbeam_utils::thread;
    use serial_test::serial;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
    const COUNT: usize = 1_000_000;

    /// empty가 아닐 때에*만* return 하는 pop operation
    struct MustPop<T: Clone> {
        pop: Pop<T>,
    }

    impl<T: Clone> Default for MustPop<T> {
        fn default() -> Self {
            Self {
                pop: Default::default(),
            }
        }
    }

    impl<T: 'static + Clone> POp for MustPop<T> {
        type Object<'q> = &'q Queue<T>;
        type Input = ();
        type Output<'q> = T;

        fn run<'o, O: POp>(
            &mut self,
            queue: Self::Object<'o>,
            _: Self::Input,
            pool: &PoolHandle<O>,
        ) -> Self::Output<'o> {
            loop {
                if let Some(v) = self.pop.run(queue, (), pool) {
                    return v;
                }
                self.pop.reset(false);
            }
        }

        fn reset(&mut self, _: bool) {
            self.pop.reset(true);
        }
    }

    struct TestPipeOp {
        // TODO
        q1: PAtomic<Queue<usize>>,
        q2: PAtomic<Queue<usize>>,
        pipes: [Pipe<MustPop<usize>, Push<usize>>; COUNT],
        suppliers: [Push<usize>; COUNT],
        consumers: [MustPop<usize>; COUNT],
    }

    impl Default for TestPipeOp {
        fn default() -> Self {
            Self {
                q1: Default::default(),
                q2: Default::default(),
                pipes: array_init::array_init(|_| Pipe::default()),
                suppliers: array_init::array_init(|_| Push::default()),
                consumers: array_init::array_init(|_| MustPop::default()),
            }
        }
    }

    impl TestPipeOp {
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

    impl POp for TestPipeOp {
        type Object<'o> = ();
        type Input = bool; // false: test sequentially, true: test concurrently
        type Output<'o> = Result<(), ()>;

        fn run<'o, O: POp>(
            &mut self,
            _: Self::Object<'o>,
            concurrent: Self::Input,
            pool: &PoolHandle<O>,
        ) -> Self::Output<'o> {
            self.init(pool);

            // Alias
            let guard = unsafe { pepoch::unprotected(pool) };
            let q1 = unsafe { self.q1.load(Ordering::SeqCst, guard).deref(pool) };
            let q2 = unsafe { self.q2.load(Ordering::SeqCst, guard).deref(pool) };
            let pipes = &mut self.pipes;
            let suppliers = &mut self.suppliers;
            let consumers = &mut self.consumers;

            if !concurrent {
                // 1. Supply q1
                for (i, push) in suppliers.iter_mut().enumerate() {
                    push.run(&q1, i, pool);
                }

                // 2. Transfer q1->q2
                for pipe in pipes.iter_mut() {
                    pipe.run((&q1, &q2), (), pool);
                }

                // 3. Consume q2
                for (i, pop) in self.consumers.iter_mut().enumerate() {
                    let v = pop.run(&q2, (), pool);
                    assert_eq!(v, i);
                }
            } else {
                #[allow(box_pointers)]
                thread::scope(|scope| {
                    // T0: Supply q1
                    let _ = scope.spawn(move |_| {
                        for (i, push) in suppliers.iter_mut().enumerate() {
                            push.run(q1, i, pool);
                        }
                    });

                    // T1: Transfer q1->q2
                    let _ = scope.spawn(move |_| {
                        for pipe in pipes.iter_mut() {
                            pipe.run((q1, q2), (), pool);
                        }
                    });

                    // T2: Consume q2
                    let _ = scope.spawn(move |_| {
                        for (i, pop) in consumers.iter_mut().enumerate() {
                            let v = pop.run(&q2, (), pool);
                            assert_eq!(v, i);
                        }
                    });
                })
                .unwrap();
            }

            Ok(())
        }

        fn reset(&mut self, _nested: bool) {
            // no-op
        }
    }

    #[test]
    fn pipe_seq() {
        let filepath = get_test_path("pipe_seq.pool");

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = unsafe { Pool::open(&filepath) }
            .unwrap_or_else(|_| Pool::create::<TestPipeOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 op 가져오기
        let root_op = pool_handle.get_root();

        // 루트 op 실행
        root_op.run((), false, &pool_handle).unwrap();
    }

    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn pipe_concur() {
        let filepath = get_test_path("pipe_concur.pool");

        // 풀 열기 (없으면 새로 만듦)
        let pool_handle = unsafe { Pool::open(&filepath) }
            .unwrap_or_else(|_| Pool::create::<TestPipeOp>(&filepath, FILE_SIZE).unwrap());

        // 루트 op 가져오기
        let root_op = pool_handle.get_root();

        // 루트 op 실행
        root_op.run((), true, &pool_handle).unwrap();
    }
}
