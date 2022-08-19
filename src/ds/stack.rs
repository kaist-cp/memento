//! Persistent Stack

use crate::pmem::ralloc::Collectable;
use crate::*;

/// Failure of stack operations
#[derive(Debug)]
pub struct TryFail;

/// Persistent stack trait
pub trait Stack<T>: PDefault + Collectable
where
    T: Clone,
{
    /// Push memento
    type Push: Default + Collectable;

    /// Pop memento
    type Pop: Default + Collectable;

    /// Push
    fn push(
        &self,
        value: T,
        mmt: &mut Self::Push,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
        rec: &mut bool,
    );

    /// Pop
    fn pop(
        &self,
        mmt: &mut Self::Pop,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
        rec: &mut bool,
    ) -> Option<T>;
}

#[cfg(test)]
pub(crate) mod tests {
    use crossbeam_epoch::Guard;

    use super::*;
    use crate::pmem::ralloc::GarbageCollection;
    use crate::pmem::*;
    use crate::test_utils::tests::*;

    pub(crate) struct PushPop<S, const NR_THREAD: usize, const COUNT: usize>
    where
        S: Stack<TestValue>,
    {
        pushes: [S::Push; COUNT],
        pops: [S::Pop; COUNT],
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Default for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<TestValue>,
    {
        fn default() -> Self {
            Self {
                pushes: array_init::array_init(|_| S::Push::default()),
                pops: array_init::array_init(|_| S::Pop::default()),
            }
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Collectable for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<TestValue>,
    {
        fn filter(
            push_pop: &mut Self,
            tid: usize,
            gc: &mut GarbageCollection,
            pool: &mut PoolHandle,
        ) {
            for push in push_pop.pushes.as_mut() {
                S::Push::filter(push, tid, gc, pool);
            }
            for pop in push_pop.pops.as_mut() {
                S::Pop::filter(pop, tid, gc, pool);
            }
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> RootObj<PushPop<S, NR_THREAD, COUNT>>
        for TestRootObj<S>
    where
        S: Stack<TestValue>,
    {
        /// Concurrent stack test that repeats push and pop
        ///
        /// - Job: Push 1 time with thread's id, then pop 1 time
        /// - All threads repeat the job
        /// - Finally, it is checked whether the results of all pops so far have the correct cumulative count of each tid value.
        fn run(
            &self,
            push_pop: &mut PushPop<S, NR_THREAD, COUNT>,
            tid: usize,
            guard: &Guard,
            pool: &PoolHandle,
        ) {
            let mut rec = true; // TODO: generalize
            let testee = unsafe { TESTER.as_ref().unwrap().testee(tid, true) };

            // push; pop;
            for seq in 0..COUNT {
                let _ = self.obj.push(
                    TestValue::new(tid, seq),
                    &mut push_pop.pushes[seq],
                    tid,
                    guard,
                    pool,
                    &mut rec,
                );
                let res = self
                    .obj
                    .pop(&mut push_pop.pops[seq], tid, guard, pool, &mut rec);

                assert!(res.is_some(), "{tid} {seq}");
                testee.report(seq, res.unwrap());
            }
        }
    }
}
