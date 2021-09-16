//! Persistent pipe

use std::marker::PhantomData;

use crate::persistent::POp;

/// `from` op과 `to` op을 failure-atomic하게 실행하는 pipe operation
///
/// - `'p`: 연결되는 두 Op(i.e. `Op1` 및 `Op2`)의 lifetime
/// - `O#`: `Op#`이 실행되는 object
#[derive(Debug)]
pub struct Pipe<'p, Op1, O1, Op2, O2>
where
    Op1: POp<'p, O1, Output = Op2::Input>,
    Op2: POp<'p, O2>,
{
    /// 먼저 실행될 op. `Pipe` op의 input은 `from` op의 input과 같음
    from: Op1,

    /// 다음에 실행될 op. `Pipe` op의 output은 `to` op의 output과 같음
    to: Op2,

    /// reset 중인지 나타내는 flag
    resetting: bool,
    _marker: PhantomData<&'p (O1, O2)>,
}

impl<'p, Op1, O1, Op2, O2> Default for Pipe<'p, Op1, O1, Op2, O2>
where
    Op1: POp<'p, O1, Output = Op2::Input>,
    Op2: POp<'p, O2>,
{
    fn default() -> Self {
        Self {
            from: Default::default(),
            to: Default::default(),
            resetting: false,
            _marker: Default::default(),
        }
    }
}

impl<'p, Op1, O1, Op2, O2> POp<'p, ()> for Pipe<'p, Op1, O1, Op2, O2>
where
    Op1: POp<'p, O1, Output = Op2::Input>,
    Op2: POp<'p, O2>,
{
    type Input = (Op1::Input, O1, O2);
    type Output = Op2::Output;

    fn run(&'p mut self, _: (), (init, from_obj, to_obj): Self::Input) -> Self::Output {
        if self.resetting {
            // TODO: This is unlikely. Use unstable `std::intrinsics::unlikely()`?
            self.reset(false);
        }

        let v = self.from.run(from_obj, init);
        self.to.run(to_obj, v)
    }

    fn reset(&mut self, nested: bool) {
        if !nested {
            self.resetting = true;
        }

        self.from.reset(true);
        self.to.reset(true);

        if !nested {
            self.resetting = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::persistent::*;
    use crate::queue::*;

    use super::*;

    use crossbeam_utils::thread;
    use serial_test::serial;

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

    impl<'q, T: Clone> POp<'q, &'q Queue<T>> for MustPop<T> {
        type Input = ();
        type Output = T;

        fn run(&mut self, queue: &Queue<T>, _: Self::Input) -> Self::Output {
            loop {
                if let Some(v) = self.pop.run(queue, ()) {
                    return v;
                }
                self.pop.reset(false);
            }
        }

        fn reset(&mut self, _: bool) {
            self.pop.reset(true);
        }
    }

    #[test]
    fn pipe_seq() {
        let q1 = Queue::<usize>::default(); // TODO(persistent location)
        let q2 = Queue::<usize>::default(); // TODO(persistent location)

        let mut suppliers: Vec<Push<usize>> = (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)
        let mut pipes: Vec<Pipe<'_, MustPop<usize>, _, Push<usize>, _>> =
            (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)
        let mut consumers: Vec<Pop<usize>> = (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        for (i, push) in suppliers.iter_mut().enumerate() {
            push.run(&q1, i);
        }

        for pipe in pipes.iter_mut() {
            pipe.run((), ((), &q1, &q2));
        }

        for (i, pop) in consumers.iter_mut().enumerate() {
            let v = pop.run(&q2, ());
            assert_eq!(v.unwrap(), i);
        }
    }

    #[test]
    #[serial] // Multi-threaded test의 속도 저하 방지
    fn pipe_concur() {
        let q1 = Queue::<usize>::default(); // TODO(persistent location)
        let q2 = Queue::<usize>::default(); // TODO(persistent location)

        let mut suppliers: Vec<Push<usize>> = (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)
        let mut pipes: Vec<Pipe<'_, MustPop<usize>, _, Push<usize>, _>> =
            (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)
        let mut consumers: Vec<MustPop<usize>> = (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        #[allow(box_pointers)]
        thread::scope(|scope| {
            let q1 = &q1;
            let q2 = &q2;
            let pipes = &mut pipes;

            let _ = scope.spawn(move |_| {
                for (i, push) in suppliers.iter_mut().enumerate() {
                    push.run(q1, i);
                }
            });

            let _ = scope.spawn(move |_| {
                for pipe in pipes.iter_mut() {
                    pipe.run((), ((), q1, q2));
                }
            });

            let _ = scope.spawn(move |_| {
                for (i, pop) in consumers.iter_mut().enumerate() {
                    assert_eq!(pop.run(&q2, ()), i);
                }
            });
        })
        .unwrap();
    }
}
