//! Persistent Exchanger

// TODO(must): 2-byte high tagging to resolve ABA problem

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};
use etrace::*;

use crate::{
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    ploc::{
        common::Checkpoint,
        no_owner,
        smo::{Delete, Insert, Node as SMONode, SMOAtomic},
        RetryLoop, Traversable, DeleteMode,
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

/// TODO(doc)
#[derive(Debug)]
pub struct Node<T> {
    data: T,
    owner: PAtomic<Self>,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            owner: PAtomic::from(no_owner()),
        }
    }
}

// TODO(must): T should be collectable
impl<T> Collectable for Node<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<T> SMONode for Node<T> {
    #[inline]
    fn owner(&self) -> &PAtomic<Self> {
        &self.owner
    }
}

/// Exchanger의 try exchange
#[derive(Debug)]
pub struct TryExchange<T: Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    init_slot: Checkpoint<PAtomic<Node<T>>>,
    wait_slot: Checkpoint<PAtomic<Node<T>>>,

    insert: Insert<Exchanger<T>, Node<T>>,
    update: Delete<Node<T>>,
    delete: Delete<Node<T>>,
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
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut s.init_slot, tid, gc, pool);
        Checkpoint::filter(&mut s.wait_slot, tid, gc, pool);
        Insert::filter(&mut s.insert, tid, gc, pool);
        Delete::filter(&mut s.update, tid, gc, pool);
        Delete::filter(&mut s.delete, tid, gc, pool);
    }
}

type ExchangeCond<T> = fn(&T) -> bool;

impl<T: 'static + Clone + std::fmt::Debug> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (T, ExchangeCond<T>);
    type Output<'o> = T;
    type Error<'o> = TryFail;

    fn run<'o>(
        &mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input<'o>,
        tid: usize,
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
                    drop(unsafe { aborted.load(Ordering::Relaxed, guard).into_owned() });
                }),
                tid,
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // 예전에 읽었던 slot을 불러오거나 새로 읽음
        let init_slot = xchg.slot.load_helping(guard, pool);
        let init_slot = self
            .init_slot
            .run(
                (),
                (PAtomic::from(init_slot), |_| {}),
                tid,
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // slot이 null 이면 insert해서 기다림
        // - 실패하면 fail 리턴
        if init_slot.is_null() {
            let mine = node.with_high_tag(WAITING); // 비어있으므로 내가 WAITING으로 선언

            let inserted =
                self.insert
                    .run(&xchg.slot, (mine, xchg, |_| true), tid, rec, guard, pool);

            // If insert failed, return error.
            if inserted.is_err() {
                unsafe { guard.defer_pdestroy(node) }; // TODO(must): crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                return Err(TryFail::Busy);
            }

            return self.wait(mine, xchg, rec, guard, pool, tid);
        }

        // slot이 null이 아니면 tag를 확인하고 반대껄 장착하고 update
        // - 내가 WAITING으로 성공하면 기다림
        // - 내가 non WAITING으로 성공하면 성공 리턴
        // - 실패하면 contention으로 인한 fail 리턴
        let my_tag = opposite_tag(init_slot.high_tag());
        let mine = node.with_high_tag(my_tag);

        // 상대가 기다리는 입장인 경우
        if my_tag != WAITING {
            let slot_ref = unsafe { init_slot.deref(pool) }; // SAFE: free되지 않은 node임. 왜냐하면 WAITING 하던 애가 그냥 나갈 때는 반드시 slot을 비우고 나감.
            if !cond(&slot_ref.data) {
                return Err(TryFail::Busy);
            }
        }

        // (1) cond를 통과한 적합한 상대가 기다리고 있거나
        // (2) 이미 교환 끝난 애가 slot에 들어 있음
        let updated = self
            .update
            .run(&xchg.slot, (init_slot, mine, DeleteMode::Drop), tid, rec, guard, pool)
            .map_err(|_| {
                // 실패하면 contention으로 인한 fail 리턴
                unsafe { guard.defer_pdestroy(node) }; // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                TryFail::Busy
            })?;

        // 내가 기다린다고 선언한 거면 기다림
        if my_tag == WAITING {
            return self.wait(mine, xchg, rec, guard, pool, tid);
        }

        // 내가 안기다리는거로 성공
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
        tid: usize,
    ) -> Result<T, TryFail> {
        // TODO(opt): timeout을 받고 loop을 쓰자

        if !rec {
            // 누군가 update 해주길 기다림
            // (복구 아닐 때에만 기다림)
            std::thread::sleep(Duration::from_nanos(100));
        }

        let wait_slot = xchg.slot.load_helping(guard, pool);

        let wait_slot = self
            .wait_slot
            .run(
                (),
                (PAtomic::from(wait_slot), |_| {}),
                tid,
                rec,
                guard,
                pool,
            )
            .unwrap()
            .load(Ordering::Relaxed, guard);

        // wait_slot이 나에서 다른 애로 바뀌었다면 내 파트너의 value 갖고 나감
        if wait_slot != mine {
            return Ok(Self::succ_after_wait(mine, guard, pool));
        }

        // 기다리다 지치면 delete 함
        // delete 실패하면 그 사이에 매칭 성사된 거임
        let deleted = ok_or!(
            self.delete
                .run(&xchg.slot, (mine, PShared::null(), DeleteMode::Drop), tid, rec, guard, pool),
            return Ok(Self::succ_after_wait(mine, guard, pool))
        );

        unsafe { guard.defer_pdestroy(deleted) }; // TODO: crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
        Err(TryFail::Timeout)
    }

    #[inline]
    fn succ_after_wait(mine: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> T {
        // 내 파트너는 나의 owner()임
        let mine_ref = unsafe { mine.deref(pool) };
        let partner = mine_ref.owner().load(Ordering::SeqCst, guard);
        let partner_ref = unsafe { partner.deref(pool) };
        unsafe { guard.defer_pdestroy(mine) };
        partner_ref.data.clone()
    }
}

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T: 'static + Clone + std::fmt::Debug> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_xchg: RetryLoop<TryExchange<T>>,
}

impl<T: Clone + std::fmt::Debug> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_xchg: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Send + Sync + std::fmt::Debug> Send for Exchange<T> {}

impl<T: Clone + std::fmt::Debug> Collectable for Exchange<T> {
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        Checkpoint::filter(&mut xchg.node, tid, gc, pool);
        RetryLoop::filter(&mut xchg.try_xchg, tid, gc, pool);
    }
}

impl<T: 'static + Clone + std::fmt::Debug> Memento for Exchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (T, ExchangeCond<T>);
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        xchg: Self::Object<'o>,
        (value, cond): Self::Input<'o>,
        tid: usize,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        self.try_xchg
            .run(xchg, (value, cond), tid, rec, guard, pool)
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
    slot: SMOAtomic<Node<T>>,
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
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> bool {
        let slot = self.slot.load_helping(guard, pool);
        slot == target
    }
}

impl<T: Clone> Collectable for Exchanger<T> {
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        SMOAtomic::filter(&mut xchg.slot, tid, gc, pool);
    }
}

#[cfg(test)]
mod tests {
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
        fn filter(xchg_once: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchange::filter(&mut xchg_once.exchange, tid, gc, pool);
        }
    }

    impl Memento for ExchangeOnce {
        type Object<'o> = &'o Exchanger<usize>;
        type Input<'o> = ();
        type Output<'o> = ();
        type Error<'o> = !;

        fn run<'o>(
            &mut self,
            xchg: Self::Object<'o>,
            (): Self::Input<'o>,
            tid: usize,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            assert!(tid == 0 || tid == 1);

            for _ in 0..100 {
                // `move` for `tid`
                let ret = self
                    .exchange
                    .run(xchg, (tid, |_| true), tid, rec, guard, pool)
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

    #[test]
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
        fn filter(rleft: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchange::filter(&mut rleft.exchange0, tid, gc, pool);
            Exchange::filter(&mut rleft.exchange2, tid, gc, pool);
        }
    }

    impl Memento for RotateLeft {
        type Object<'o> = &'o [Exchanger<usize>; 2];
        type Input<'o> = ();
        type Output<'o> = ();
        type Error<'o> = !;

        /// Before rotation : [0]  [1]  [2]
        /// After rotation  : [1]  [2]  [0]
        fn run<'o>(
            &mut self,
            xchgs: Self::Object<'o>,
            (): Self::Input<'o>,
            tid: usize,
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
                        .run(lxchg, (*item, |_| true), tid, rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 1);
                }
                // T1: Composition in the middle
                1 => {
                    // Step1: [0] <- [1]    [2]

                    *item = self
                        .exchange0
                        .run(lxchg, (*item, |_| true), tid, rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 0);

                    // Step2: [1]    [0] -> [2]
                    *item = self
                        .exchange2
                        .run(rxchg, (*item, |_| true), tid, rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 2);
                }
                // T2: [0]    [1] <- [2]
                2 => {
                    *item = self
                        .exchange2
                        .run(rxchg, (*item, |_| true), tid, rec, guard, pool)
                        .unwrap();
                    assert_eq!(*item, 0);
                }
                _ => panic!(),
            }
            Ok(())
        }

        fn reset(&mut self, _guard: &Guard, _pool: &'static PoolHandle) {
            todo!("reset test")
        }
    }

    impl Collectable for [Exchanger<usize>; 2] {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
            Exchanger::filter(&mut s[0], tid, gc, pool);
            Exchanger::filter(&mut s[1], tid, gc, pool);
        }
    }
    impl PDefault for [Exchanger<usize>; 2] {
        fn pdefault(pool: &'static PoolHandle) -> Self {
            [Exchanger::pdefault(pool), Exchanger::pdefault(pool)]
        }
    }
    impl TestRootObj for [Exchanger<usize>; 2] {}
    impl TestRootMemento<[Exchanger<usize>; 2]> for RotateLeft {}

    #[test]
    fn rotate_left() {
        const FILE_NAME: &str = "rotate_left.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<[Exchanger<usize>; 2], RotateLeft, _>(FILE_NAME, FILE_SIZE, 3);
    }
}
