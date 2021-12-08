//! Persistent Exchanger

// TODO: 2-byte high tagging to resolve ABA problem + unsafe owner clear

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    atomic_update::{Delete, DeleteHelper, Insert, SMOAtomic, Update},
    atomic_update_common::{Load, Traversable},
    node::Node,
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    persistent::{Memento, PDefault},
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
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
    init_ld_param: PAtomic<Node<T>>,
    wait_ld_param: PAtomic<Node<T>>,
    load: Load<Node<T>>,

    insert: Insert<Exchanger<T>, Node<T>>,

    update_param: PAtomic<Node<T>>,
    update: Update<Exchanger<T>, Node<T>, Self>,

    delete_param: PAtomic<Node<T>>,
    delete: Delete<Exchanger<T>, Node<T>, Self>,
}

impl<T: Clone> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            init_ld_param: PAtomic::from(Load::<Node<T>>::no_read()),
            wait_ld_param: PAtomic::from(Load::<Node<T>>::no_read()),
            load: Default::default(),
            insert: Default::default(),
            update_param: PAtomic::null(),
            update: Default::default(),
            delete_param: PAtomic::null(),
            delete: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryExchange<T> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut node = s.init_ld_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<T>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<T>::mark(node_ref, gc);
        }

        let mut node = s.wait_ld_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<T>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<T>::mark(node_ref, gc);
        }

        let mut node = s.update_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<T>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<T>::mark(node_ref, gc);
        }

        let mut node = s.delete_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<T>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<T>::mark(node_ref, gc);
        }

        Load::filter(&mut s.load, gc, pool);
        Insert::filter(&mut s.insert, gc, pool);
        Update::filter(&mut s.update, gc, pool);
        Delete::filter(&mut s.delete, gc, pool);
    }
}

type ExchangeCond<T> = fn(&T) -> bool;

impl<T: 'static + Clone> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (PShared<'o, Node<T>>, ExchangeCond<T>);
    type Output<'o> = T; // TODO: input과의 대구를 고려해서 node reference가 나을지?
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        (node, cond): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // 예전에 읽었던 slot을 불러오거나 새로 읽음
        let slot = self
            .load
            .run((), (&self.init_ld_param, &xchg.slot), rec, guard, pool)
            .unwrap();

        // slot이 null 이면 insert해서 기다림
        // - 실패하면 페일 리턴
        if slot.is_null() {
            let mine = node.with_tag(WAITING); // 비어있으므로 내가 WAITING으로 선언

            let inserted = self.insert.run(
                xchg,
                (mine, &xchg.slot, Self::prepare_insert),
                rec,
                guard,
                pool,
            );
            if inserted.is_err() {
                // contention
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
        let updated = self.update.run(
            xchg,
            (mine, &self.update_param, &xchg.slot),
            rec,
            guard,
            pool,
        );

        // 실패하면 contention으로 인한 fail 리턴
        if updated.is_err() {
            return Err(TryFail::Busy);
        }

        // 내가 기다린다고 선언한 거면 기다림
        if my_tag == WAITING {
            return self.wait(mine, xchg, rec, guard, pool);
        }

        // even으로 성공하면 성공 리턴
        let partner = updated.unwrap();
        let partner_ref = unsafe { partner.deref(pool) };
        Ok(partner_ref.data.clone())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        self.init_ld_param
            .store(Load::<Node<T>>::no_read(), Ordering::Relaxed);
        self.wait_ld_param
            .store(Load::<Node<T>>::no_read(), Ordering::Relaxed);
        self.update_param.store(PShared::null(), Ordering::Relaxed);
        self.delete_param.store(PShared::null(), Ordering::Relaxed);

        self.load.reset(guard, pool);
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
        // TODO: timeout을 받고 loop을 쓰자
        // 누군가 update 해주길 기다림
        let slot = self
            .load
            .run((), (&self.wait_ld_param, &xchg.slot), rec, guard, pool)
            .unwrap();

        // slot이 나에서 다른 애로 바뀌었다면 내 파트너의 value 갖고 나감
        if slot != mine {
            return Ok(Self::wait_succ(mine, pool));
        }

        std::thread::sleep(Duration::from_millis(1)); // TODO: timeout 받으면 이제 이건 backoff로 바뀜

        // 기다리다 지치면 delete 함
        // delete 실패하면 그 사이에 매칭 성사된 거임
        let deleted = self.delete.run(
            xchg,
            (&self.delete_param, mine, &xchg.slot),
            rec,
            guard,
            pool,
        );

        if deleted.is_ok() {
            // TODO: 소유권 청소해야 함
            return Err(TryFail::Timeout);
        }

        return Ok(Self::wait_succ(mine, pool));
    }

    #[inline]
    fn wait_succ(mine: PShared<'_, Node<T>>, pool: &PoolHandle) -> T {
        // 내 파트너는 나의 owner()임
        let mine_ref = unsafe { mine.deref(pool) };
        let partner = unsafe { Update::<Exchanger<T>, Node<T>, Self>::next_updated_node(mine_ref) };
        let partner_ref = unsafe { partner.deref(pool) };
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
    ) -> Result<Option<PShared<'g, Node<T>>>, ()> {
        if cur == mine {
            return Ok(Some(PShared::<_>::null()));
        }

        Err(())
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
pub struct Exchange<T: Clone> {
    node: PAtomic<Node<T>>,
    try_xchg: TryExchange<T>,
}

impl<T: Clone> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: PAtomic::null(),
            try_xchg: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync> Send for Exchange<T> {}

impl<T: Clone> Collectable for Exchange<T> {
    fn filter(xchg: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut node = xchg.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<T>::mark(node_ref, gc);
        }

        TryExchange::<T>::filter(&mut xchg.try_xchg, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for Exchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (T, ExchangeCond<T>);
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input<'o>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let node = if rec {
            let node = self.node.load(Ordering::Relaxed, guard);
            if node.is_null() {
                self.new_node(value.clone(), guard, pool)
            } else {
                node
            }
        } else {
            self.new_node(value.clone(), guard, pool)
        };

        if let Ok(v) = self.try_xchg.run(xchg, (node, cond), rec, guard, pool) {
            return Ok(v);
        }

        loop {
            let node = self.new_node(value.clone(), guard, pool); // TODO: alloc 문제 해결하고 clone 다 떼주기
            if let Ok(v) = self.try_xchg.run(xchg, (node, cond), false, guard, pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, guard: &Guard, _: &'static PoolHandle) {
        let node = self.node.load(Ordering::SeqCst, guard);
        if !node.is_null() {
            self.node.store(PShared::null(), Ordering::SeqCst);
            // TODO: 이 사이에 죽으면 partner의 포인터에 의해 gc가 수거하지 못해 leak 발생
            unsafe { guard.defer_pdestroy(node) };
        }
    }
}

impl<T: Clone> Exchange<T> {
    #[inline]
    fn new_node<'g>(
        &self,
        value: T,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> PShared<'g, Node<T>> {
        let node = POwned::new(Node::from(value), pool).into_shared(guard);
        self.node.store(node, Ordering::Relaxed);
        persist_obj(&self.node, true);
        node
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
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        // Mark ptr if valid
        let mut slot = s.slot.load(Ordering::SeqCst, guard);
        if !slot.is_null() {
            let slot_ref = unsafe { slot.deref_mut(pool) };
            Node::mark(slot_ref, gc);
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        plocation::ralloc::{Collectable, GarbageCollection},
        utils::tests::{run_test, TestRootMemento, TestRootObj},
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
            &'o mut self,
            xchg: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            assert!(tid == 0 || tid == 1);

            for _ in 0..100 {
                // `move` for `tid`
                let ret = self.exchange.run(xchg, (tid, |_| true), rec, guard, pool).unwrap();
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

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn exchange_once() {
        const FILE_NAME: &str = "exchange_once.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<Exchanger<usize>, ExchangeOnce, _>(FILE_NAME, FILE_SIZE, 2)
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
            &'o mut self,
            xchgs: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &Guard,
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
                    *item = self.exchange0.run(lxchg, (*item, |_| true), rec, guard, pool).unwrap();
                    assert_eq!(*item, 1);
                }
                // T1: Composition in the middle
                1 => {
                    // Step1: [0] <- [1]    [2]

                    *item = self.exchange0.run(lxchg, (*item, |_| true), rec, guard, pool).unwrap();
                    assert_eq!(*item, 0);

                    // Step2: [1]    [0] -> [2]
                    *item = self.exchange2.run(rxchg, (*item, |_| true), rec, guard, pool).unwrap();
                    assert_eq!(*item, 2);
                }
                // T2: [0]    [1] <- [2]
                2 => {
                    *item = self.exchange2.run(rxchg, (*item, |_| true), rec, guard, pool).unwrap();
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

    // TODO: #[serial] 대신 https://crates.io/crates/rusty-fork 사용
    #[test]
    #[serial] // Ralloc은 동시에 두 개의 pool 사용할 수 없기 때문에 테스트를 병렬적으로 실행하면 안됨 (Ralloc은 global pool 하나로 관리)
    fn rotate_left() {
        const FILE_NAME: &str = "rotate_left.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<[Exchanger<usize>; 2], RotateLeft, _>(FILE_NAME, FILE_SIZE, 3);
    }
}
