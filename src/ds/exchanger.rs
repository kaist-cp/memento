//! Persistent Exchanger

// TODO(must): 2-byte high tagging to resolve ABA problem

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    node::Node,
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    ploc::{
        common::Checkpoint,
        smo::{clear_owner, Delete, DeleteHelper, Insert, SMOAtomic, Update},
        NeedRetry, RetryLoop, Traversable,
    },
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    {Memento, PDefault},
};

// WAITING Tag
const WAITING: usize = 1;

#[inline]
fn opposite_tag(t: usize) -> usize {
    1 - t
}

/// Exchanger의 try exchange 실패
#[derive(Debug)]
pub enum TryFail {
    /// 시간 초과
    Timeout,

    /// 컨텐션
    Busy,
}

/// Exchanger의 try exchange
#[derive(Debug)]
pub struct TryExchange<T: Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    init_slot: Checkpoint<PAtomic<Node<T>>>,
    wait_slot: Checkpoint<PAtomic<Node<T>>>,

    insert: Insert<Exchanger<T>, Node<T>>,
    update: Update<Exchanger<T>, Node<T>, Self>,
    delete: Delete<Exchanger<T>, Node<T>, Self>,
}

impl<T: Clone> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            init_slot: Default::default(),
            wait_slot: Default::default(),
            insert: Default::default(),
            update: Default::default(),
            delete: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryExchange<T> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut s.init_slot, gc, pool);
        Checkpoint::filter(&mut s.wait_slot, gc, pool);
        Insert::filter(&mut s.insert, gc, pool);
        Update::filter(&mut s.update, gc, pool);
        Delete::filter(&mut s.delete, gc, pool);
    }
}

type ExchangeCond<T> = fn(&T) -> bool;

impl<T: 'static + Clone> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (T, ExchangeCond<T>);
    type Output<'o> = T; // TODO(opt): input과의 대구를 고려해서 node reference가 나을지?
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = self
            .node
            .run(
                (),
                (PAtomic::from(node), |aborted| {
                    let guard = unsafe { epoch::unprotected() };
                    let d = aborted.load(Ordering::Relaxed, guard);
                    unsafe { guard.defer_pdestroy(d) };
                }),
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // 예전에 읽었던 slot을 불러오거나 새로 읽음
        let slot = xchg.slot.load(Ordering::SeqCst, guard);
        let slot = self
            .init_slot
            .run((), (PAtomic::from(slot), |_| {}), rec, guard, pool)
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // slot이 null 이면 insert해서 기다림
        // - 실패하면 페일 리턴
        if slot.is_null() {
            let mine = node.with_tag(WAITING); // 비어있으므로 내가 WAITING으로 선언

            let inserted = self.insert.run(
                &xchg.slot,
                (mine, xchg, Self::prepare_insert),
                rec,
                guard,
                pool,
            );

            if inserted.is_err() {
                // contention
                unsafe { guard.defer_pdestroy(node) }; // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                return Err(TryFail::Busy);
            }

            return self.wait(mine, xchg, rec, guard, pool);
        }

        // slot이 null이 아니면 tag를 확인하고 반대껄 장착하고 update
        // - 내가 WAITING으로 성공하면 기다림
        // - 내가 non WAITING으로 성공하면 성공 리턴
        // - 실패하면 contention으로 인한 fail 리턴
        let my_tag = opposite_tag(slot.tag());
        let mine = node.with_tag(my_tag);

        // 상대가 기다리는 입장인 경우
        if my_tag != WAITING {
            let slot_ref = unsafe { slot.deref(pool) }; // SAFE: free되지 않은 node임. 왜냐하면 WAITING 하던 애가 그냥 나갈 때는 반드시 slot을 비우고 나감.
            if !cond(&slot_ref.data) {
                return Err(TryFail::Busy);
            }
        }

        // (1) cond를 통과한 적합한 상대가 기다리고 있거나
        // (2) 이미 교환 끝난 애가 slot에 들어 있음
        let updated = self
            .update
            .run(&xchg.slot, (mine, slot, xchg), rec, guard, pool)
            .map_err(|_| {
                // 실패하면 contention으로 인한 fail 리턴
                unsafe { guard.defer_pdestroy(node) }; // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                TryFail::Busy
            })?;

        // 내가 기다린다고 선언한 거면 기다림
        if my_tag == WAITING {
            return self.wait(mine, xchg, rec, guard, pool);
        }

        // even으로 성공하면 성공 리턴
        let partner = updated;
        let partner_ref = unsafe { partner.deref(pool) };
        unsafe { guard.defer_pdestroy(mine) };
        Ok(partner_ref.data.clone())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.init_slot.reset(guard, pool);
        self.wait_slot.reset(guard, pool);
        self.insert.reset(guard, pool);
        self.update.reset(guard, pool);
        self.delete.reset(guard, pool);
    }
}

impl<T: 'static + Clone> TryExchange<T> {
    fn wait<'g>(
        &mut self,
        mine: PShared<'_, Node<T>>,
        xchg: &Exchanger<T>,
        rec: bool,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<T, TryFail> {
        // TODO(opt): timeout을 받고 loop을 쓰자

        if !rec {
            // 누군가 update 해주길 기다림
            // (복구 아닐 때에만 기다림)
            std::thread::sleep(Duration::from_nanos(100));
        }

        let slot = xchg.slot.load(Ordering::SeqCst, guard);
        let slot = self
            .wait_slot
            .run((), (PAtomic::from(slot), |_| {}), rec, guard, pool)
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // slot이 나에서 다른 애로 바뀌었다면 내 파트너의 value 갖고 나감
        if slot != mine {
            return Ok(Self::succ_after_wait(mine, guard, pool));
        }

        // 기다리다 지치면 delete 함
        // delete 실패하면 그 사이에 매칭 성사된 거임
        let deleted = self.delete.run(&xchg.slot, (mine, xchg), rec, guard, pool);

        if let Ok(res) = deleted {
            match res {
                Some(d) => {
                    unsafe { guard.defer_pdestroy(d) }; // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                    return Err(TryFail::Timeout);
                }
                None => {
                    unreachable!(
                        "Delete is successful only if there is a node s.t. the node is mine."
                    )
                }
            }
        }

        Ok(Self::succ_after_wait(mine, guard, pool))
    }

    #[inline]
    fn succ_after_wait(mine: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> T {
        // 내 파트너는 나의 owner()임
        let mine_ref = unsafe { mine.deref(pool) };
        let partner = unsafe { Update::<Exchanger<T>, Node<T>, Self>::next_updated_node(mine_ref) };
        let partner_ref = unsafe { partner.deref(pool) };
        unsafe { guard.defer_pdestroy(mine) };
        partner_ref.data.clone()
    }

    #[inline]
    fn prepare_insert(_: &mut Node<T>) -> bool {
        true
    }
}

impl<T: Clone> DeleteHelper<Exchanger<T>, Node<T>> for TryExchange<T> {
    fn prepare_delete<'g>(
        cur: PShared<'_, Node<T>>,
        mine: PShared<'_, Node<T>>,
        _: &Exchanger<T>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<T>>>, NeedRetry> {
        if cur == mine {
            return Ok(Some(PShared::<_>::null()));
        }

        Err(NeedRetry)
    }

    fn prepare_update<'g>(
        cur: PShared<'_, Node<T>>,
        expected: PShared<'_, Node<T>>,
        _: &Exchanger<T>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> bool {
        cur == expected
    }

    fn node_when_deleted<'g>(
        _: PShared<'_, Node<T>>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> PShared<'g, Node<T>> {
        PShared::<_>::null()
    }
}

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_xchg: RetryLoop<TryExchange<T>>,
}

impl<T: Clone> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_xchg: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Exchange<T> {}

impl<T: Clone> Collectable for Exchange<T> {
    fn filter(xchg: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut xchg.node, gc, pool);
        RetryLoop::filter(&mut xchg.try_xchg, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for Exchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (T, ExchangeCond<T>);
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // let node = POwned::new(Node::from(value), pool);
        // persist_obj(unsafe { node.deref(pool) }, true);

        // let node = self
        //     .node
        //     .run(
        //         (),
        //         (PAtomic::from(node), |aborted| {
        //             let guard = unsafe { epoch::unprotected() };
        //             let d = aborted.load(Ordering::Relaxed, guard);
        //             unsafe { guard.defer_pdestroy(d) };
        //         }),
        //         rec,
        //         guard,
        //         pool,
        //     )
        //     .unwrap()
        //     .load(Ordering::Relaxed, guard);

        self.try_xchg
            .run(xchg, (value, cond), rec, guard, pool)
            .map_err(|_| unreachable!("Retry never fails."))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.node.reset(guard, pool);
        self.try_xchg.reset(guard, pool);
    }
}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<T: Clone> {
    slot: SMOAtomic<Self, Node<T>, TryExchange<T>>,
}

impl<T: Clone> Default for Exchanger<T> {
    fn default() -> Self {
        Self {
            slot: SMOAtomic::default(),
        }
    }
}

impl<T: Clone> PDefault for Exchanger<T> {
    fn pdefault(_: &'static PoolHandle) -> Self {
        Default::default()
    }
}

impl<T: Clone> Traversable<Node<T>> for Exchanger<T> {
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, _: &PoolHandle) -> bool {
        let slot = self.slot.load(Ordering::SeqCst, guard);
        slot == target
    }
}

impl<T: Clone> Collectable for Exchanger<T> {
    fn filter(xchg: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        SMOAtomic::filter(&mut xchg.slot, gc, pool);
    }
}

#[cfg(test)]
mod tests {
    use rusty_fork::rusty_fork_test;

    use crate::{
        pmem::ralloc::{Collectable, GarbageCollection},
        test_utils::tests::{run_test, TestRootMemento, TestRootObj},
    };

    use super::*;

    /// 두 스레드가 한 exchanger를 두고 잘 교환하는지 (1회) 테스트
    #[derive(Default)]
    struct ExchangeOnce {
        exchange: Exchange<usize>,
    }

    impl Collectable for ExchangeOnce {
        fn filter(xchg_once: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchange::filter(&mut xchg_once.exchange, gc, pool);
        }
    }

    impl Memento for ExchangeOnce {
        type Object<'o> = &'o Exchanger<usize>;
        type Input<'o> = usize; // tid(mid)
        type Output<'o> = ();
        type Error<'o> = !;

        fn run<'o>(
            &mut self,
            xchg: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            assert!(tid == 0 || tid == 1);

            for _ in 0..100 {
                // `move` for `tid`
                let ret = self
                    .exchange
                    .run(xchg, (tid, |_| true), rec, guard, pool)
                    .unwrap();
                assert_eq!(ret, 1 - tid);
            }

            Ok(())
        }

        fn reset(&mut self, _guard: &Guard, _pool: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl TestRootObj for Exchanger<usize> {}
    impl TestRootMemento<Exchanger<usize>> for ExchangeOnce {}

    rusty_fork_test! {
        #[test]
        fn exchange_once() {
            const FILE_NAME: &str = "exchange_once.pool";
            const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

            run_test::<Exchanger<usize>, ExchangeOnce, _>(FILE_NAME, FILE_SIZE, 2)
        }
    }

    /// 세 스레드가 인접한 스레드와 아이템을 교환하여 전체적으로 rotation 되는지 테스트
    ///
    ///   ---T0---                   -------T1-------                   ---T2---
    ///  |        |                 |                |                 |        |
    ///     (exchange0)        (exchange0)     (exchange2)        (exchange2)
    /// [item]    <-----lxchg----->       [item]       <-----rxchg----->     [item]
    #[derive(Default)]
    struct RotateLeft {
        item: usize,
        exchange0: Exchange<usize>,
        exchange2: Exchange<usize>,
    }

    impl Collectable for RotateLeft {
        fn filter(rleft: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchange::filter(&mut rleft.exchange0, gc, pool);
            Exchange::filter(&mut rleft.exchange2, gc, pool);
        }
    }

    impl Memento for RotateLeft {
        type Object<'o> = &'o [Exchanger<usize>; 2];
        type Input<'o> = usize;
        type Output<'o> = ();
        type Error<'o> = !;

        /// Before rotation : [0]  [1]  [2]
        /// After rotation  : [1]  [2]  [0]
        fn run<'o>(
            &mut self,
            xchgs: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            // Alias
            let lxchg = &xchgs[0];
            let rxchg = &xchgs[1];
            let item = &mut self.item;

            *item = tid;

            match tid {
                // T0: [0] -> [1]    [2]
                0 => {
                    *item = self
                        .exchange0
                        .run(lxchg, (*item, |_| true), rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 1);
                }
                // T1: Composition in the middle
                1 => {
                    // Step1: [0] <- [1]    [2]

                    *item = self
                        .exchange0
                        .run(lxchg, (*item, |_| true), rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 0);

                    // Step2: [1]    [0] -> [2]
                    *item = self
                        .exchange2
                        .run(rxchg, (*item, |_| true), rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 2);
                }
                // T2: [0]    [1] <- [2]
                2 => {
                    *item = self
                        .exchange2
                        .run(rxchg, (*item, |_| true), rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 0);
                }
                _ => unreachable!(),
            }
            Ok(())
        }

        fn reset(&mut self, _guard: &Guard, _pool: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl Collectable for [Exchanger<usize>; 2] {
        fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchanger::filter(&mut s[0], gc, pool);
            Exchanger::filter(&mut s[1], gc, pool);
        }
    }
    impl PDefault for [Exchanger<usize>; 2] {
        fn pdefault(pool: &'static PoolHandle) -> Self {
            [Exchanger::pdefault(pool), Exchanger::pdefault(pool)]
        }
    }
    impl TestRootObj for [Exchanger<usize>; 2] {}
    impl TestRootMemento<[Exchanger<usize>; 2]> for RotateLeft {}

    rusty_fork_test! {
        #[test]
        fn rotate_left() {
            const FILE_NAME: &str = "rotate_left.pool";
            const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

            run_test::<[Exchanger<usize>; 2], RotateLeft, _>(FILE_NAME, FILE_SIZE, 3);
        }
    }
}
