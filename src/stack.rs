//! Persistent Stack

use crate::persistent::*;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::PoolHandle;

/// Stack의 try push/pop 실패
#[derive(Debug, Clone)]
pub struct TryFail;

/// Persistent stack trait
pub trait Stack<T: 'static + Clone>: 'static + Default + Collectable {
    /// Try push 연산을 위한 Persistent op.
    /// Try push의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    type TryPush: for<'o> Memento<
        Object<'o> = &'o Self,
        Input = T,
        Output<'o> = (),
        Error = TryFail,
    >;

    /// Push 연산을 위한 Persistent op.
    /// 반드시 push에 성공함.
    type Push: for<'o> Memento<Object<'o> = &'o Self, Input = T, Output<'o> = (), Error = !> =
        Push<T, Self>;

    /// Try pop 연산을 위한 Persistent op.
    /// Try pop의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    /// Try pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type TryPop: for<'o> Memento<
        Object<'o> = &'o Self,
        Input = (),
        Output<'o> = Option<T>,
        Error = TryFail,
    >;

    /// Pop 연산을 위한 Persistent op.
    /// 반드시 pop에 성공함.
    /// pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type Pop: for<'o> Memento<Object<'o> = &'o Self, Input = (), Output<'o> = Option<T>, Error = !> =
        Pop<T, Self>;
}

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: 'static + Clone, S: Stack<T>> {
    try_push: S::TryPush,
}

impl<T: Clone, S: Stack<T>> Default for Push<T, S> {
    fn default() -> Self {
        Self {
            try_push: Default::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for Push<T, S> {
    fn filter(push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        S::TryPush::filter(&mut push.try_push, gc, pool);
    }
}

impl<T: Clone, S: Stack<T>> Memento for Push<T, S> {
    type Object<'o> = &'o S;
    type Input = T;
    type Output<'o>
    where
        T: 'o,
    = ();
    type Error = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        while self.try_push.run(stack, value.clone(), pool).is_err() {}
        Ok(())
    }

    fn reset(&mut self, _: bool, pool: &'static PoolHandle) {
        self.try_push.reset(true, pool);
    }
}

unsafe impl<T: 'static + Clone, S: Stack<T>> Send for Push<T, S> where S::TryPush: Send {}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: 'static + Clone, S: Stack<T>> {
    try_pop: S::TryPop,
}

impl<T: Clone, S: Stack<T>> Default for Pop<T, S> {
    fn default() -> Self {
        Self {
            try_pop: Default::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for Pop<T, S> {
    fn filter(pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        S::TryPop::filter(&mut pop.try_pop, gc, pool);
    }
}

impl<T: Clone, S: Stack<T>> Memento for Pop<T, S> {
    type Object<'o> = &'o S;
    type Input = ();
    type Output<'o>
    where
        T: 'o,
    = Option<T>;
    type Error = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error> {
        loop {
            if let Ok(v) = self.try_pop.run(stack, (), pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, _: bool, pool: &'static PoolHandle) {
        self.try_pop.reset(true, pool);
    }
}

unsafe impl<T: Clone, S: Stack<T>> Send for Pop<T, S> where S::TryPop: Send {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crossbeam_utils::thread;

    use crate::{plocation::PoolHandle, utils::tests::TestRootOp};

    pub(crate) struct PushPop<S: Stack<usize>, const NR_THREAD: usize, const COUNT: usize> {
        stack: S,
        pushes: [[S::Push; COUNT]; NR_THREAD],
        pops: [[S::Pop; COUNT]; NR_THREAD],
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Default for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize>,
    {
        fn default() -> Self {
            Self {
                stack: Default::default(),
                pushes: array_init::array_init(|_| array_init::array_init(|_| S::Push::default())),
                pops: array_init::array_init(|_| array_init::array_init(|_| S::Pop::default())),
            }
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Collectable for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize> + Sync + 'static,
        S::Push: Send,
        S::Pop: Send,
    {
        fn filter(push_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            S::filter(&mut push_pop.stack, gc, pool);
            for pushes in push_pop.pushes.as_mut() {
                for push in pushes {
                    S::Push::filter(push, gc, pool);
                }
            }
            for pops in push_pop.pops.as_mut() {
                for pop in pops {
                    S::Pop::filter(pop, gc, pool);
                }
            }
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Memento for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize> + Sync + 'static,
        S::Push: Send,
        S::Pop: Send,
    {
        type Object<'o> = ();
        type Input = ();
        type Output<'o> = ();
        type Error = !;

        /// push_pop을 반복하는 Concurrent stack test
        ///
        /// - Job: 자신의 tid로 1회 push하고 그 뒤 1회 pop을 함
        /// - 여러 스레드가 Job을 반복
        /// - 마지막에 지금까지의 모든 pop의 결과물이 각 tid값의 정확한 누적 횟수를 가지는지 체크
        fn run<'o>(
            &'o mut self,
            (): Self::Object<'o>,
            (): Self::Input,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error> {
            #[allow(box_pointers)]
            thread::scope(|scope| {
                for tid in 0..NR_THREAD {
                    let s = &self.stack;
                    let pushes = unsafe {
                        (self.pushes.get_unchecked_mut(tid) as *mut [S::Push; COUNT])
                            .as_mut()
                            .unwrap()
                    };
                    let pops = unsafe {
                        (self.pops.get_unchecked_mut(tid) as *mut [S::Pop; COUNT])
                            .as_mut()
                            .unwrap()
                    };

                    let _ = scope.spawn(move |_| {
                        for i in 0..COUNT {
                            let _ = pushes[i].run(s, tid, pool);
                            assert!(pops[i].run(s, (), pool).unwrap().is_some());
                        }
                    });
                }
            })
            .unwrap();

            // Check empty
            assert!(S::Pop::default()
                .run(&self.stack, (), pool)
                .unwrap()
                .is_none());

            // Check results
            let mut results = vec![0_usize; NR_THREAD];
            for pops in self.pops.iter_mut() {
                for pop in pops.iter_mut() {
                    let ret = pop.run(&self.stack, (), pool).unwrap().unwrap();
                    results[ret] += 1;
                }
            }

            assert!(results.iter().all(|r| *r == COUNT));
            Ok(())
        }

        fn reset(&mut self, _: bool, _: &PoolHandle) {
            todo!("reset test")
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> TestRootOp for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize> + Sync + 'static,
        S::Push: Send,
        S::Pop: Send,
    {
    }
}
