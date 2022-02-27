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
    fn push<const REC: bool>(
        &self,
        value: T,
        mmt: &mut Self::Push,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    );

    /// Pop
    fn pop<const REC: bool>(
        &self,
        mmt: &mut Self::Pop,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Option<T>;
}

#[cfg(test)]
pub(crate) mod tests {

    use std::sync::atomic::Ordering;

    use crossbeam_epoch::Guard;

    use super::*;
    use crate::pmem::ralloc::GarbageCollection;
    use crate::pmem::{PoolHandle, RootObj};
    use crate::test_utils::tests::*;

    pub(crate) struct PushPop<S, const NR_THREAD: usize, const COUNT: usize>
    where
        S: Stack<usize>,
    {
        pushes: [S::Push; COUNT],
        pops: [S::Pop; COUNT],
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Default for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize>,
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
        S: Stack<usize>,
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
        S: Stack<usize>,
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
            match tid {
                // T1: Check the execution results of other threads
                1 => {
                    // Wait for all other threads to finish
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check empty
                    let mut tmp_pop = S::Pop::default();
                    let must_none = self.obj.pop::<true>(&mut tmp_pop, tid, guard, pool);
                    assert!(must_none.is_none());

                    // Check results
                    assert!(RESULTS[1].load(Ordering::SeqCst) == 0);
                    assert!((2..NR_THREAD + 2)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // Threads other than T1 perform { push; pop; }
                _ => {
                    // push; pop;
                    for i in 0..COUNT {
                        let _ =
                            self.obj
                                .push::<true>(tid, &mut push_pop.pushes[i], tid, guard, pool);
                        let res = self
                            .obj
                            .pop::<true>(&mut push_pop.pops[i], tid, guard, pool);
                        assert!(res.is_some());

                        // Transfer the pop result to the result array
                        let _ = RESULTS[res.unwrap()].fetch_add(1, Ordering::SeqCst);
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
}
