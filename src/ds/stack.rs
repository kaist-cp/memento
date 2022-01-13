//! Persistent Stack

use crate::pmem::ralloc::Collectable;
use crate::*;

/// TODO(doc)
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
        fn filter(push_pop: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
        S: Stack<usize> + Sync,
    {
        /// push_pop을 반복하는 Concurrent stack test
        ///
        /// - Job: 자신의 tid로 1회 push하고 그 뒤 1회 pop을 함
        /// - 여러 스레드가 Job을 반복
        /// - 마지막에 지금까지의 모든 pop의 결과물이 각 tid값의 정확한 누적 횟수를 가지는지 체크
        fn run(
            &self,
            push_pop: &mut PushPop<S, NR_THREAD, COUNT>,
            tid: usize,
            guard: &Guard,
            pool: &PoolHandle,
        ) {
            match tid {
                // T0: 다른 스레드들의 실행결과를 확인
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check empty
                    let mut tmp_pop = S::Pop::default();
                    let must_none = self.obj.pop::<true>(&mut tmp_pop, tid, guard, pool);
                    assert!(must_none.is_none());

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    assert!((1..NR_THREAD + 1)
                        .all(|tid| { RESULTS[tid].load(Ordering::SeqCst) == COUNT }));
                }
                // T0이 아닌 다른 스레드들은 stack에 { push; pop; } 수행
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

                        // deq 결과를 실험결과에 전달
                        let _ = RESULTS[res.unwrap()].fetch_add(1, Ordering::SeqCst);
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
}
