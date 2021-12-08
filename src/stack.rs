//! Persistent Stack

use crate::atomic_update_common::Traversable;
use crate::node::Node;
use crate::persistent::*;
use crate::plocation::ralloc::Collectable;

/// TODO: doc
#[derive(Debug)]
pub struct TryFail;

/// Persistent stack trait
pub trait Stack<T: 'static + Clone>:
    'static + Default + Collectable
{
    /// Push 연산을 위한 Persistent op.
    /// 반드시 push에 성공함.
    type Push: for<'o> Memento<Object<'o> = &'o Self, Input<'o> = T, Output<'o> = (), Error<'o> = !>;

    /// Pop 연산을 위한 Persistent op.
    /// 반드시 pop에 성공함.
    /// pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type Pop: for<'o> Memento<
        Object<'o> = &'o Self,
        Input<'o> = (),
        Output<'o> = Option<T>,
        Error<'o> = !,
    >;
}

#[cfg(test)]
pub(crate) mod tests {

    use std::sync::atomic::Ordering;

    use crossbeam_epoch::Guard;

    use super::*;
    use crate::plocation::PoolHandle;
    use crate::plocation::ralloc::GarbageCollection;
    use crate::utils::tests::*;

    pub(crate) struct PushPop<S: Stack<usize>, const NR_THREAD: usize, const COUNT: usize> {
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
        S: Stack<usize> + Sync + 'static,
        S::Push: Send,
        S::Pop: Send,
    {
        fn filter(push_pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            for push in push_pop.pushes.as_mut() {
                S::Push::filter(push, gc, pool);
            }
            for pop in push_pop.pops.as_mut() {
                S::Pop::filter(pop, gc, pool);
            }
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> Memento for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize> + Sync + 'static,
        S::Push: Send,
        S::Pop: Send,
    {
        type Object<'o> = &'o S;
        type Input<'o> = usize; // tid(mid)
        type Output<'o> = ();
        type Error<'o> = !;

        /// push_pop을 반복하는 Concurrent stack test
        ///
        /// - Job: 자신의 tid로 1회 push하고 그 뒤 1회 pop을 함
        /// - 여러 스레드가 Job을 반복
        /// - 마지막에 지금까지의 모든 pop의 결과물이 각 tid값의 정확한 누적 횟수를 가지는지 체크
        fn run<'o>(
            &'o mut self,
            stack: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            match tid {
                // T0: 다른 스레드들의 실행결과를 확인
                0 => {
                    // 다른 스레드들이 다 끝날때까지 기다림
                    while JOB_FINISHED.load(Ordering::SeqCst) != NR_THREAD {}

                    // Check empty
                    assert!(S::Pop::default()
                        .run(stack, (), rec, guard, pool)
                        .unwrap()
                        .is_none());

                    // Check results
                    assert!(RESULTS[0].load(Ordering::SeqCst) == 0);
                    for tid in 1..NR_THREAD + 1 {
                        assert!(RESULTS[tid].load(Ordering::SeqCst) == COUNT);
                    }
                }
                // T0이 아닌 다른 스레드들은 stack에 { push; pop; } 수행
                _ => {
                    // push; pop;
                    for i in 0..COUNT {
                        let _ = self.pushes[i].run(stack, tid, rec, guard, pool);
                        assert!(self.pops[i]
                            .run(stack, (), rec, guard, pool)
                            .unwrap()
                            .is_some());
                    }

                    // pop 결과를 실험결과에 전달
                    for pop in self.pops.as_mut() {
                        let ret = pop.run(stack, (), rec, guard, pool).unwrap().unwrap();
                        let _ = RESULTS[ret].fetch_add(1, Ordering::SeqCst);
                    }

                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        }

        fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl<S, const NR_THREAD: usize, const COUNT: usize> TestRootMemento<S>
        for PushPop<S, NR_THREAD, COUNT>
    where
        S: Stack<usize> + Sync + 'static + TestRootObj,
        S::Push: Send,
        S::Pop: Send,
    {
    }
}
