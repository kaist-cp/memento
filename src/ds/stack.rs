//! Persistent Stack

use crate::ploc::Handle;
use crate::pmem::alloc::Collectable;
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
    type Push: Memento;

    /// Pop memento
    type Pop: Memento;

    /// Push
    fn push(&self, value: T, mmt: &mut Self::Push, handle: &Handle);

    /// Pop
    fn pop(&self, mmt: &mut Self::Pop, handle: &Handle) -> Option<T>;
}

#[cfg(test)]
pub(crate) mod tests {

    use super::*;
    use crate::ploc::Handle;
    use crate::pmem::alloc::GarbageCollection;
    use crate::pmem::*;
    use crate::test_utils::tests::*;

    pub(crate) struct PushPop<S, const NR_THREAD: usize, const COUNT: usize>
    where
        S: Stack<TestValue>,
    {
        pushes: [S::Push; COUNT],
        pops: [S::Pop; COUNT],
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Memento for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<TestValue>,
    {
        fn clear(&mut self) {
            for push in self.pushes.as_mut() {
                push.clear();
            }
            for pop in self.pops.as_mut() {
                pop.clear();
            }
        }
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
        fn run(&self, push_pop: &mut PushPop<S, NR_THREAD, COUNT>, handle: &Handle) {
            let tid = handle.tid;
            let testee = unsafe { TESTER.as_ref().unwrap().testee(true, handle) };

            // push; pop;
            for seq in 0..COUNT {
                let _ = self
                    .obj
                    .push(TestValue::new(tid, seq), &mut push_pop.pushes[seq], handle);
                let res = self.obj.pop(&mut push_pop.pops[seq], handle);

                assert!(res.is_some(), "{tid} {seq}");
                testee.report(seq, res.unwrap());
            }
        }
    }
}
