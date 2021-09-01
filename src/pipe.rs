//! Persistent pipe

use std::marker::PhantomData;

use crate::persistent::POp;

/// `from` op과 `to` op을 failure-atomic하게 실행하는 pipe operation
#[derive(Debug)]
pub struct Pipe<O1, T1, O2, T2>
where
    O1: POp<T1, Output = O2::Input>,
    O2: POp<T2>,
{
    /// 먼저 실행될 op. `Pipe` op의 input은 `from` op의 input과 같음
    from: O1,

    /// 다음에 실행될 op. `Pipe` op의 output은 `to` op의 output과 같음
    to: O2,

    /// reset 중인지 나타내는 flag
    resetting: bool,
    _marker: PhantomData<(T1, T2)>,
}

impl<O1, T1, O2, T2> Default for Pipe<O1, T1, O2, T2>
where
    O1: POp<T1, Output = O2::Input>,
    O2: POp<T2>,
{
    fn default() -> Self {
        Self {
            from: Default::default(),
            to: Default::default(),
            resetting: false,
            _marker: Default::default()
        }

    }
}

impl<O1, T1, O2, T2> POp<()> for Pipe<O1, T1, O2, T2>
where
    O1: POp<T1, Output = O2::Input>,
    O2: POp<T2>,
{
    type Input = (O1::Input, T1, T2);
    type Output = O2::Output;

    fn run(&mut self, _: (), (init, from_obj, to_obj): Self::Input) -> Self::Output {
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

    impl<T: Clone> POp<&Queue<T>> for MustPop<T> {
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
        let mut pipes: Vec<Pipe<MustPop<usize>, _, Push<usize>, _>> =
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
        let mut pipes: Vec<Pipe<MustPop<usize>, _, Push<usize>, _>> =
            (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)
        let mut consumers: Vec<MustPop<usize>> = (0..COUNT).map(|_| Default::default()).collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        #[allow(box_pointers)]
        thread::scope(|scope| {
            let q1 = &q1;
            let q2 = &q2;

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
