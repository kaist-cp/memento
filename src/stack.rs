//! Persistent Stack

use std::sync::atomic::Ordering;

use crossbeam_epoch::Guard;

use crate::atomic_update_common::Traversable;
use crate::pepoch::{self as epoch, PAtomic, POwned, PShared};
use crate::persistent::*;
use crate::plocation::ll::persist_obj;
use crate::plocation::ralloc::{Collectable, GarbageCollection};
use crate::plocation::PoolHandle;
use crate::unopt_node::{DeallocNode, NodeUnOpt};

/// TODO: doc
#[derive(Debug)]
pub struct TryFail;

/// Persistent stack trait
pub trait Stack<T: 'static + Clone>:
    'static + Default + Collectable + Traversable<NodeUnOpt<T, Self>>
{
    /// Try push 연산을 위한 Persistent op.
    /// Try push의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    type TryPush: for<'o> Memento<
        Object<'o> = &'o Self,
        Input<'o> = PShared<'o, NodeUnOpt<T, Self>>,
        Output<'o> = (),
        Error<'o> = TryFail,
    >;

    /// Push 연산을 위한 Persistent op.
    /// 반드시 push에 성공함.
    type Push: for<'o> Memento<Object<'o> = &'o Self, Input<'o> = T, Output<'o> = (), Error<'o> = !> =
        Push<T, Self>;

    /// Try pop 연산을 위한 Persistent op.
    /// Try pop의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    /// Try pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type TryPop: for<'o> Memento<
            Object<'o> = &'o Self,
            Input<'o> = &'o PAtomic<NodeUnOpt<T, Self>>,
            Output<'o> = Option<T>,
            Error<'o> = TryFail,
        > + DeallocNode<T, NodeUnOpt<T, Self>>;

    /// Pop 연산을 위한 Persistent op.
    /// 반드시 pop에 성공함.
    /// pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type Pop: for<'o> Memento<
        Object<'o> = &'o Self,
        Input<'o> = (),
        Output<'o> = Option<T>,
        Error<'o> = !,
    > = Pop<T, Self>;
}

/// Stack의 try push를 이용하는 push op.
#[derive(Debug)]
pub struct Push<T: 'static + Clone, S: Stack<T> + Traversable<NodeUnOpt<T, S>>> {
    node: PAtomic<NodeUnOpt<T, S>>,
    try_push: S::TryPush,
}

impl<T: Clone, S: Stack<T>> Default for Push<T, S> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_push: Default::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for Push<T, S> {
    fn filter(push: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut node = push.node.load(Ordering::Relaxed, guard);
        if !node.is_null() {
            let node_ref = unsafe { node.deref_mut(pool) };
            NodeUnOpt::<T, S>::mark(node_ref, gc);
        }

        S::TryPush::filter(&mut push.try_push, gc, pool);
    }
}

impl<T: Clone, S: Stack<T>> Drop for Push<T, S> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let node = self.node.load(Ordering::Relaxed, guard);
        assert!(node.is_null(), "reset 되어있지 않음.")
        // TODO: trypush의 리셋여부 파악?
    }
}

impl<T: Clone, S: Stack<T>> Memento for Push<T, S> {
    type Object<'o> = &'o S;
    type Input<'o> = T;
    type Output<'o>
    where
        T: 'o,
    = ();
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        value: Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = if rec {
            let node = self.node.load(Ordering::Relaxed, guard);
            if node.is_null() {
                self.new_node(value, guard, pool)
            } else {
                node
            }
        } else {
            self.new_node(value, guard, pool)
        };

        if self.try_push.run(stack, node, rec, guard, pool).is_ok() {
            return Ok(());
        }

        while self.try_push.run(stack, node, false, guard, pool).is_err() {}
        Ok(())
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        self.try_push.reset(nested, guard, pool);
    }
}

impl<T: Clone, S: Stack<T>> Push<T, S> {
    #[inline]
    fn new_node<'g>(
        &self,
        value: T,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> PShared<'g, NodeUnOpt<T, S>> {
        let node = POwned::new(NodeUnOpt::from(value), pool).into_shared(guard);
        self.node.store(node, Ordering::Relaxed);
        persist_obj(&self.node, true);
        node
    }
}

unsafe impl<T: 'static + Clone, S: Stack<T>> Send for Push<T, S> where S::TryPush: Send {}

/// Stack의 try pop을 이용하는 pop op.
#[derive(Debug)]
pub struct Pop<T: 'static + Clone, S: Stack<T>> {
    mine: PAtomic<NodeUnOpt<T, S>>,
    try_pop: S::TryPop,
}

impl<T: Clone, S: Stack<T>> Default for Pop<T, S> {
    fn default() -> Self {
        Self {
            mine: Default::default(),
            try_pop: Default::default(),
        }
    }
}

impl<T: Clone, S: Stack<T>> Collectable for Pop<T, S> {
    fn filter(pop: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut mine = pop.mine.load(Ordering::SeqCst, guard);
        if !mine.is_null() {
            let mine_ref = unsafe { mine.deref_mut(pool) };
            NodeUnOpt::<T, S>::mark(mine_ref, gc);
        }

        S::TryPop::filter(&mut pop.try_pop, gc, pool);
    }
}

impl<T: Clone, S: Stack<T>> Memento for Pop<T, S> {
    type Object<'o> = &'o S;
    type Input<'o> = ();
    type Output<'o>
    where
        T: 'o,
    = Option<T>;
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        stack: Self::Object<'o>,
        (): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if let Ok(v) = self.try_pop.run(stack, &self.mine, rec, guard, pool) {
            return Ok(v);
        }

        loop {
            if let Ok(v) = self.try_pop.run(stack, &self.mine, false, guard, pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        let mine = self.mine.load(Ordering::Relaxed, guard);

        // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
        // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
        self.mine.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.mine, true);
        self.try_pop.dealloc(mine, guard, pool);

        self.try_pop.reset(nested, guard, pool);
    }
}

impl<T: Clone, S: Stack<T>> Drop for Pop<T, S> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let mine = self.mine.load(Ordering::Relaxed, guard);
        assert!(mine.is_null(), "reset 되어있지 않음.")
        // TODO: trypop의 리셋여부 파악?
    }
}

unsafe impl<T: Clone, S: Stack<T>> Send for Pop<T, S> where S::TryPop: Send {}

#[cfg(test)]
pub(crate) mod tests {

    use std::sync::atomic::Ordering;

    use super::*;
    use crate::plocation::PoolHandle;
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

        fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {
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
