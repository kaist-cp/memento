//! Persistent Exchanger

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    atomic_update::{Delete, DeleteHelper, Insert, SMOAtomic, Update},
    atomic_update_common::Traversable,
    node::Node,
    pepoch::{PAtomic, PShared},
    persistent::{Memento, PDefault},
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
};

// WAITING Tag
const WAITING: usize = 0;

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

/// TODO: doc
#[derive(Debug)]
pub struct ExchangeNode<T> {
    value: T,
}

impl<T> From<T> for ExchangeNode<T> {
    fn from(value: T) -> Self {
        Self { value }
    }
}

/// Exchanger의 try exchange
#[derive(Debug)]
pub struct TryExchange<T: Clone> {
    insert: Insert<Exchanger<T>, Node<ExchangeNode<T>>>,
    update: Update<Exchanger<T>, Node<ExchangeNode<T>>, Self>,
    delete: Delete<Exchanger<T>, Node<ExchangeNode<T>>, Self>,
}

impl<T: Clone> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            insert: Default::default(),
            update: Default::default(),
            delete: Default::default(),
        }
    }
}

impl<T: Clone> Collectable for TryExchange<T> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = (
        PShared<'o, Node<ExchangeNode<T>>>,
        &'o PAtomic<Node<ExchangeNode<T>>>,
        &'o PAtomic<Node<ExchangeNode<T>>>,
    );
    type Output<'o> = T; // TODO: input과의 대구를 고려해서 node reference가 나을지?
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        (node, update_target_loc, delete_target_loc): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let slot = xchg.slot.load(Ordering::SeqCst, guard);

        if rec {
            // TODO: crash 후 slot에 내가 들어가 있는 경우 등....
            // TODO: read memento를 만들자
        }

        // Normal run

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

            return self.wait(mine, delete_target_loc, xchg, rec, guard, pool);
        }

        // slot이 null이 아니면 tag를 확인하고 반대껄 장착하고 update
        // - 내가 WAITING으로 성공하면 기다림
        // - 내가 non WAITING으로 성공하면 성공 리턴
        // - 실패하면 contention으로 인한 fail 리턴
        let my_tag = opposite_tag(slot.tag());
        let mine = node.with_tag(my_tag);

        let updated = self.update.run(
            xchg,
            (mine, update_target_loc, &xchg.slot),
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
            return self.wait(mine, delete_target_loc, xchg, rec, guard, pool);
        }

        // even으로 성공하면 성공 리턴
        let partner = updated.unwrap();
        let partner_ref = unsafe { partner.deref(pool) };
        Ok(partner_ref.data.value.clone())
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

impl<T: 'static + Clone> TryExchange<T> {
    fn wait<'g>(
        &mut self,
        mine: PShared<'_, Node<ExchangeNode<T>>>,
        delete_target_loc: &PAtomic<Node<ExchangeNode<T>>>,
        xchg: &Exchanger<T>,
        rec: bool,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<T, TryFail> {
        // TODO: timeout을 받고 loop을 쓰자
        // 누군가 update 해주길 기다림
        let slot = xchg.slot.load(Ordering::SeqCst, guard);

        // slot이 나에서 다른 애로 바뀌었다면 내 파트너의 value 갖고 나감 ( TODO: 파트너? owner를 봐야 한다고?)
        if slot != mine {
            return Ok(Self::wait_succ(mine, pool));
        }

        std::thread::sleep(Duration::from_millis(1)); // TODO: timeout 받으면 이제 이건 backoff로 바뀜

        // 기다리다 지치면 delete 함
        // delete 실패하면 그 사이에 매칭 성사된 거임
        let deleted = self
            .delete
            .run(xchg, (delete_target_loc, &xchg.slot), rec, guard, pool);

        if deleted.is_ok() {
            return Err(TryFail::Timeout);
        }

        return Ok(Self::wait_succ(mine, pool));
    }

    #[inline]
    fn wait_succ(mine: PShared<'_, Node<ExchangeNode<T>>>, pool: &PoolHandle) -> T {
        // 내 파트너는 나의 owner()임
        let mine_ref = unsafe { mine.deref(pool) };
        let partner = unsafe {
            Update::<Exchanger<T>, Node<ExchangeNode<T>>, Self>::next_updated_node(mine_ref)
        };
        let partner_ref = unsafe { partner.deref(pool) };
        partner_ref.data.value.clone()
    }

    #[inline]
    fn prepare_insert(_: &mut Node<ExchangeNode<T>>) -> bool {
        // TODO: 모든 opt insert는 prepare가 필요 없는 것 같음
        true
    }
}

impl<T: Clone> DeleteHelper<Exchanger<T>, Node<ExchangeNode<T>>> for TryExchange<T> {
    fn prepare_delete<'g>(
        cur: PShared<'_, Node<ExchangeNode<T>>>,
        obj: &Exchanger<T>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<ExchangeNode<T>>>>, ()> {
        todo!()
    }

    fn node_when_deleted<'g>(
        deleted: PShared<'_, Node<ExchangeNode<T>>>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, Node<ExchangeNode<T>>> {
        PShared::<_>::null()
    }
}

/// 스레드 간의 exchanger
/// 내부에 마련된 slot을 통해 스레드들끼리 값을 교환함
#[derive(Debug)]
pub struct Exchanger<T: Clone> {
    slot: SMOAtomic<Self, Node<ExchangeNode<T>>, TryExchange<T>>,
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

impl<T: Clone> Traversable<Node<ExchangeNode<T>>> for Exchanger<T> {
    fn search(
        &self,
        target: PShared<'_, Node<ExchangeNode<T>>>,
        guard: &Guard,
        _: &PoolHandle,
    ) -> bool {
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
