//! Persistent Exchanger

// TODO(must): 2-byte high tagging to resolve ABA problem

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};
use etrace::ok_or;

use crate::{
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    ploc::{
        common::Checkpoint,
        insert_delete::{Delete, Insert, Node as SMONode, SMOAtomic},
        not_deleted, Traversable,
    },
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    PDefault,
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

/// Exchanger node
#[derive(Debug)]
pub struct Node<T> {
    data: T,
    owner: PAtomic<Self>,
}

impl<T> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            owner: PAtomic::from(not_deleted()),
        }
    }
}

// TODO(must): T should be collectable
impl<T> Collectable for Node<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl<T> SMONode for Node<T> {
    #[inline]
    fn replacement(&self) -> &PAtomic<Self> {
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
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut s.init_slot, tid, gc, pool);
        Checkpoint::filter(&mut s.wait_slot, tid, gc, pool);
        Insert::filter(&mut s.insert, tid, gc, pool);
        Delete::filter(&mut s.update, tid, gc, pool);
        Delete::filter(&mut s.delete, tid, gc, pool);
    }
}

type ExchangeCond<T> = fn(&T) -> bool;

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T: 'static + Clone> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_xchg: TryExchange<T>,
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
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut xchg.node, tid, gc, pool);
        TryExchange::filter(&mut xchg.try_xchg, tid, gc, pool);
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
    fn pdefault(_: &PoolHandle) -> Self {
        Default::default()
    }
}

impl<T: Clone> Traversable<Node<T>> for Exchanger<T> {
    fn search(&self, target: PShared<'_, Node<T>>, guard: &Guard, _: &PoolHandle) -> bool {
        let slot = self.slot.load(true, Ordering::SeqCst, guard);
        slot == target
    }
}

impl<T: Clone> Collectable for Exchanger<T> {
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        SMOAtomic::filter(&mut xchg.slot, tid, gc, pool);
    }
}

impl<T: Clone> Exchanger<T> {
    /// Try Exchange
    pub fn try_exchange<const REC: bool>(
        &self,
        value: T,
        cond: ExchangeCond<T>,
        try_xchg: &mut TryExchange<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<T, TryFail> {
        let node = POwned::new(Node::from(value), pool);
        persist_obj(unsafe { node.deref(pool) }, true);

        let node = ok_or!(
            try_xchg
                .node
                .checkpoint::<REC>(PAtomic::from(node), tid, pool),
            e,
            unsafe {
                drop(
                    e.new
                        .load(Ordering::Relaxed, epoch::unprotected())
                        .into_owned(),
                );
                e.current
            }
        )
        .load(Ordering::Relaxed, guard);

        // 예전에 읽었던 slot을 불러오거나 새로 읽음
        let init_slot = self.slot.load(true, Ordering::SeqCst, guard);
        let init_slot = ok_or!(
            try_xchg
                .init_slot
                .checkpoint::<REC>(PAtomic::from(init_slot), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        // slot이 null 이면 insert해서 기다림
        // - 실패하면 fail 리턴
        if init_slot.is_null() {
            let mine = node.with_high_tag(WAITING); // 비어있으므로 내가 WAITING으로 선언

            let inserted =
                self.slot
                    .insert::<_, REC>(mine, self, &mut try_xchg.insert, guard, pool);

            // If insert failed, return error.
            if inserted.is_err() {
                unsafe { guard.defer_pdestroy(node) }; // TODO(must): crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                return Err(TryFail::Busy);
            }

            return self.wait::<REC>(mine, try_xchg, tid, guard, pool);
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
            .slot
            .delete::<REC>(init_slot, mine, &mut try_xchg.update, tid, guard, pool)
            .map_err(|_| {
                // 실패하면 contention으로 인한 fail 리턴
                unsafe { guard.defer_pdestroy(node) }; // TODO(must): crossbeam 패치 이전에는 test 끝날 때 double free 날 수 있음
                TryFail::Busy
            })?;

        // 내가 기다린다고 선언한 거면 기다림
        if my_tag == WAITING {
            return self.wait::<REC>(mine, try_xchg, tid, guard, pool);
        }

        // 내가 안 기다리고 성공
        let partner = updated;
        let partner_ref = unsafe { partner.deref(pool) };
        Ok(partner_ref.data.clone())
    }

    /// Exchange
    pub fn exchange<const REC: bool>(
        &self,
        value: T,
        cond: ExchangeCond<T>,
        xchg: &mut Exchange<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> T {
        if let Ok(ret) =
            self.try_exchange::<REC>(value.clone(), cond, &mut xchg.try_xchg, tid, guard, pool)
        {
            return ret;
        }

        loop {
            if let Ok(ret) = self.try_exchange::<false>(
                value.clone(),
                cond,
                &mut xchg.try_xchg,
                tid,
                guard,
                pool,
            ) {
                return ret;
            }
        }
    }

    fn wait<'g, const REC: bool>(
        &self,
        mine: PShared<'_, Node<T>>,
        try_xchg: &mut TryExchange<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<T, TryFail> {
        // TODO(opt): timeout을 받고 loop을 쓰자

        if !REC {
            // 누군가 update 해주길 기다림
            // (복구 아닐 때에만 기다림)
            std::thread::sleep(Duration::from_nanos(100));
        }

        let wait_slot = self.slot.load(true, Ordering::SeqCst, guard);
        let wait_slot = ok_or!(
            try_xchg
                .wait_slot
                .checkpoint::<REC>(PAtomic::from(wait_slot), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        // wait_slot이 나에서 다른 애로 바뀌었다면 내 파트너의 value 갖고 나감
        if wait_slot != mine {
            return Ok(Self::succ_after_wait(mine, guard, pool));
        }

        // 기다리다 지치면 delete 함
        // delete 실패하면 그 사이에 매칭 성사된 거임
        if self
            .slot
            .delete::<REC>(
                mine,
                PShared::null(),
                &mut try_xchg.delete,
                tid,
                guard,
                pool,
            )
            .is_ok()
        {
            Err(TryFail::Timeout)
        } else {
            Ok(Self::succ_after_wait(mine, guard, pool))
        }
    }

    #[inline]
    fn succ_after_wait(mine: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> T {
        // 내 파트너는 나의 owner()임
        let mine_ref = unsafe { mine.deref(pool) };
        let partner = mine_ref.replacement().load(Ordering::SeqCst, guard);
        let partner_ref = unsafe { partner.deref(pool) };
        partner_ref.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pmem::{
            ralloc::{Collectable, GarbageCollection},
            RootObj,
        },
        test_utils::tests::{run_test, TestRootObj},
    };

    use super::*;

    /// 두 스레드가 한 exchanger를 두고 잘 교환하는지 (1회) 테스트
    #[derive(Default)]
    struct ExchangeOnce {
        xchg: Exchange<usize>,
    }

    impl Collectable for ExchangeOnce {
        fn filter(
            xchg_once: &mut Self,
            tid: usize,
            gc: &mut GarbageCollection,
            pool: &mut PoolHandle,
        ) {
            Exchange::filter(&mut xchg_once.xchg, tid, gc, pool);
        }
    }

    impl RootObj<ExchangeOnce> for TestRootObj<Exchanger<usize>> {
        fn run(&self, xchg_once: &mut ExchangeOnce, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let tid = tid + 1;
            assert!(tid == 1 || tid == 2);

            for _ in 0..100 {
                // `move` for `tid`
                let ret =
                    self.obj
                        .exchange::<true>(tid, |_| true, &mut xchg_once.xchg, tid, guard, pool);
                assert_eq!(ret, tid ^ 3);
            }
        }
    }

    #[test]
    fn exchange_once() {
        const FILE_NAME: &str = "exchange_once.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<Exchanger<usize>>, ExchangeOnce, _>(FILE_NAME, FILE_SIZE, 2)
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
        fn filter(rleft: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Exchange::filter(&mut rleft.exchange0, tid, gc, pool);
            Exchange::filter(&mut rleft.exchange2, tid, gc, pool);
        }
    }

    impl Collectable for [Exchanger<usize>; 2] {
        fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Exchanger::filter(&mut s[0], tid, gc, pool);
            Exchanger::filter(&mut s[1], tid, gc, pool);
        }
    }

    impl PDefault for [Exchanger<usize>; 2] {
        fn pdefault(pool: &PoolHandle) -> Self {
            [Exchanger::pdefault(pool), Exchanger::pdefault(pool)]
        }
    }

    impl RootObj<RotateLeft> for TestRootObj<[Exchanger<usize>; 2]> {
        /// Before rotation : [1]  [2]  [3]
        /// After rotation  : [2]  [3]  [1]
        fn run(&self, rotl: &mut RotateLeft, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let tid = tid + 1;

            // Alias
            let lxchg = &self.obj[0];
            let rxchg = &self.obj[1];
            let item = &mut rotl.item;

            *item = tid;

            match tid {
                // T1: [1] -> [2]    [3]
                1 => {
                    *item = lxchg.exchange::<true>(
                        *item,
                        |_| true,
                        &mut rotl.exchange0,
                        tid,
                        guard,
                        pool,
                    );
                    assert_eq!(*item, 2);
                }
                // T2: Composition in the middle
                2 => {
                    // Step1: [1] <- [2]    [3]
                    *item = lxchg.exchange::<true>(
                        *item,
                        |_| true,
                        &mut rotl.exchange0,
                        tid,
                        guard,
                        pool,
                    );
                    assert_eq!(*item, 1);

                    // Step2: [2]    [1] -> [3]
                    *item = rxchg.exchange::<true>(
                        *item,
                        |_| true,
                        &mut rotl.exchange2,
                        tid,
                        guard,
                        pool,
                    );
                    assert_eq!(*item, 3);
                }
                // T3: [1]    [2] <- [3]
                3 => {
                    *item = rxchg.exchange::<true>(
                        *item,
                        |_| true,
                        &mut rotl.exchange2,
                        tid,
                        guard,
                        pool,
                    );
                    assert_eq!(*item, 1);
                }
                _ => unreachable!("The maximum number of threads is 3"),
            }
        }
    }

    #[test]
    fn rotate_left() {
        const FILE_NAME: &str = "rotate_left.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<[Exchanger<usize>; 2]>, RotateLeft, _>(FILE_NAME, FILE_SIZE, 3);
    }
}
