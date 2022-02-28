//! Persistent Exchanger

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};
use etrace::ok_or;

use crate::{
    pepoch::{PAtomic, PDestroyable, POwned, PShared},
    ploc::{
        common::Checkpoint,
        insert_delete::{Node as SMONode, SMOAtomic},
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

/// try exchange failure
#[derive(Debug)]
pub enum TryFail {
    /// Time out
    Timeout,

    /// Busy due to contention
    Busy,
}

/// Exchanger node
#[derive(Debug)]
pub struct Node<T: Collectable> {
    data: T,
    repl: PAtomic<Self>,
}

impl<T: Collectable> From<T> for Node<T> {
    fn from(value: T) -> Self {
        Self {
            data: value,
            repl: PAtomic::from(not_deleted()),
        }
    }
}

impl<T: Collectable> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut node.data, tid, gc, pool);
        PAtomic::filter(&mut node.repl, tid, gc, pool);
    }
}

impl<T: Collectable> SMONode for Node<T> {
    #[inline]
    fn replacement(&self) -> &PAtomic<Self> {
        &self.repl
    }
}

/// Try exchange memento
#[derive(Debug)]
pub struct TryExchange<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    init_slot: Checkpoint<PAtomic<Node<T>>>,
    wait_slot: Checkpoint<PAtomic<Node<T>>>,
}

impl<T: Clone + Collectable> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            init_slot: Default::default(),
            wait_slot: Default::default(),
        }
    }
}

impl<T: Clone + Collectable> Collectable for TryExchange<T> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut s.init_slot, tid, gc, pool);
        Checkpoint::filter(&mut s.wait_slot, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> TryExchange<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.node.clear();
        self.init_slot.clear();
        self.wait_slot.clear();
    }
}

type ExchangeCond<T> = fn(&T) -> bool;

/// Exchanger's exchange operation.
#[derive(Debug)]
pub struct Exchange<T: Clone + Collectable> {
    node: Checkpoint<PAtomic<Node<T>>>,
    try_xchg: TryExchange<T>,
}

impl<T: Clone + Collectable> Default for Exchange<T> {
    fn default() -> Self {
        Self {
            node: Default::default(),
            try_xchg: Default::default(),
        }
    }
}

unsafe impl<T: Clone + Collectable + Send + Sync> Send for Exchange<T> {}

impl<T: Clone + Collectable> Collectable for Exchange<T> {
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut xchg.node, tid, gc, pool);
        TryExchange::filter(&mut xchg.try_xchg, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> Exchange<T> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.node.clear();
        self.try_xchg.clear();
    }
}

/// Exchanger
/// Values are exchanged between threads through internal slots.
#[derive(Debug)]
pub struct Exchanger<T: Clone + Collectable> {
    slot: SMOAtomic<Node<T>>,
}

impl<T: Clone + Collectable> Default for Exchanger<T> {
    fn default() -> Self {
        Self {
            slot: SMOAtomic::default(),
        }
    }
}

impl<T: Clone + Collectable> PDefault for Exchanger<T> {
    fn pdefault(_: &PoolHandle) -> Self {
        Default::default()
    }
}

impl<T: Clone + Collectable> Traversable<Node<T>> for Exchanger<T> {
    fn contains(&self, target: PShared<'_, Node<T>>, guard: &Guard, _: &PoolHandle) -> bool {
        let slot = self.slot.load(true, Ordering::SeqCst, guard);
        slot == target
    }
}

impl<T: Clone + Collectable> Collectable for Exchanger<T> {
    fn filter(xchg: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        SMOAtomic::filter(&mut xchg.slot, tid, gc, pool);
    }
}

impl<T: Clone + Collectable> Exchanger<T> {
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

        // Loads previously read slots or reads new ones
        let init_slot = self.slot.load(true, Ordering::SeqCst, guard);
        let init_slot = ok_or!(
            try_xchg
                .init_slot
                .checkpoint::<REC>(PAtomic::from(init_slot), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        // If slot is null, insert and wait
        // - return fail on failure
        if init_slot.is_null() {
            let mine = node.with_high_tag(WAITING); // It's empty, so I declare WAITING

            let inserted = self.slot.insert::<_, REC>(mine, self, guard, pool);

            // If insert failed, return error.
            if inserted.is_err() {
                unsafe { guard.defer_pdestroy(node) };
                return Err(TryFail::Busy);
            }

            return self.wait::<REC>(mine, try_xchg, tid, guard, pool);
        }

        // If the slot is not null, check the tag and install the opposite one and update
        // - Wait if I succeed with WAITING
        // - Return success if I succeed with non-WAITING
        // - If it fails, it returns fail due to contention.
        let my_tag = opposite_tag(init_slot.high_tag());
        let mine = node.with_high_tag(my_tag);

        // Case where the partner is in a waiting position
        if my_tag != WAITING {
            let slot_ref = unsafe { init_slot.deref(pool) }; // SAFE: It is a node that is not freed.
                                                             // Because, when the thread that was waiting for exits without exchange, it must empty the slot.
            if !cond(&slot_ref.data) {
                return Err(TryFail::Busy);
            }
        }

        // (1) A suitable partner who has passed cond is waiting or
        // (2) a node that has already been exchanged is in the slot
        let updated = self
            .slot
            .delete::<REC>(init_slot, mine, tid, guard, pool)
            .map_err(|_| {
                // If it fails, it returns fail due to contention.
                unsafe { guard.defer_pdestroy(node) };
                TryFail::Busy
            })?;

        // Wait if I declared I'm waiting
        if my_tag == WAITING {
            return self.wait::<REC>(mine, try_xchg, tid, guard, pool);
        }

        // Case where I succeeded right away without waiting.
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

    fn wait<const REC: bool>(
        &self,
        mine: PShared<'_, Node<T>>,
        try_xchg: &mut TryExchange<T>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<T, TryFail> {
        std::thread::sleep(Duration::from_nanos(100));

        let wait_slot = self.slot.load(true, Ordering::SeqCst, guard);
        let wait_slot = ok_or!(
            try_xchg
                .wait_slot
                .checkpoint::<REC>(PAtomic::from(wait_slot), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        // If wait_slot is changed from me to another node, I take my partner's value
        if wait_slot != mine {
            return Ok(Self::succ_after_wait(mine, guard, pool));
        }

        // If I get tired of waiting, I empty the slot.
        // If delete fails, matching has been completed.
        if self
            .slot
            .delete::<REC>(mine, PShared::null(), tid, guard, pool)
            .is_ok()
        {
            Err(TryFail::Timeout)
        } else {
            Ok(Self::succ_after_wait(mine, guard, pool))
        }
    }

    #[inline]
    fn succ_after_wait(mine: PShared<'_, Node<T>>, guard: &Guard, pool: &PoolHandle) -> T {
        // My partner is my replacement()
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

    /// Test whether two threads exchange well with one exchanger (one time)
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

    /// Test whether three threads rotate as a whole by exchanging items with adjacent threads
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
