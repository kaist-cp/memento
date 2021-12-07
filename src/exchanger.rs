//! Persistent Exchanger

// TODO: 2-byte high tagging to resolve ABA problem

use std::{sync::atomic::Ordering, time::Duration};

use crossbeam_epoch::{self as epoch, Guard};

use crate::{
    atomic_update::{Delete, DeleteHelper, Insert, SMOAtomic, Update},
    atomic_update_common::{Load, Traversable},
    node::Node,
    pepoch::{PAtomic, PShared, POwned, PDestroyable},
    persistent::{Memento, PDefault},
    plocation::{
        ralloc::{Collectable, GarbageCollection},
        PoolHandle, ll::persist_obj,
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
    init_ld_param: PAtomic<Node<ExchangeNode<T>>>,
    wait_ld_param: PAtomic<Node<ExchangeNode<T>>>,
    load: Load<Node<ExchangeNode<T>>>,

    insert: Insert<Exchanger<T>, Node<ExchangeNode<T>>>,

    update_param: PAtomic<Node<ExchangeNode<T>>>,
    update: Update<Exchanger<T>, Node<ExchangeNode<T>>, Self>,

    delete_param: PAtomic<Node<ExchangeNode<T>>>,
    delete: Delete<Exchanger<T>, Node<ExchangeNode<T>>, Self>,
}

impl<T: Clone> Default for TryExchange<T> {
    fn default() -> Self {
        Self {
            init_ld_param: PAtomic::from(Load::<Node<ExchangeNode<T>>>::no_read()),
            wait_ld_param: PAtomic::from(Load::<Node<ExchangeNode<T>>>::no_read()),
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
        if !node.is_null() && node != Load::<Node<ExchangeNode<T>>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<ExchangeNode<T>>::mark(node_ref, gc);
        }

        let mut node = s.wait_ld_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<ExchangeNode<T>>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<ExchangeNode<T>>::mark(node_ref, gc);
        }

        let mut node = s.update_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<ExchangeNode<T>>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<ExchangeNode<T>>::mark(node_ref, gc);
        }

        let mut node = s.delete_param.load(Ordering::Relaxed, guard);
        if !node.is_null() && node != Load::<Node<ExchangeNode<T>>>::no_read() {
            let node_ref = unsafe { node.deref_mut(pool) };
            Node::<ExchangeNode<T>>::mark(node_ref, gc);
        }

        Load::filter(&mut s.load, gc, pool);
        Insert::filter(&mut s.insert, gc, pool);
        Update::filter(&mut s.update, gc, pool);
        Delete::filter(&mut s.delete, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for TryExchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = PShared<'o, Node<ExchangeNode<T>>>;
    type Output<'o> = T; // TODO: input과의 대구를 고려해서 node reference가 나을지?
    type Error<'o> = TryFail;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
        node: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // 예전에 읽었던 slot을 불러오거나 새로 읽음
        let saved_slot = self
            .load
            .run((), (&self.init_ld_param, &xchg.slot), rec, guard, pool)
            .unwrap();
        let slot = if let Some(s) = saved_slot {
            s
        } else {
            self.load
                .run((), (&self.init_ld_param, &xchg.slot), false, guard, pool)
                .unwrap()
                .unwrap()
        };

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
        Ok(partner_ref.data.value.clone())
    }

    fn reset(&mut self, nested: bool, guard: &Guard, pool: &'static PoolHandle) {
        self.init_ld_param
            .store(Load::<Node<ExchangeNode<T>>>::no_read(), Ordering::Relaxed);
        self.wait_ld_param
            .store(Load::<Node<ExchangeNode<T>>>::no_read(), Ordering::Relaxed);
        self.update_param.store(PShared::null(), Ordering::Relaxed);
        self.delete_param.store(PShared::null(), Ordering::Relaxed);

        self.load.reset(nested, guard, pool);
        self.insert.reset(nested, guard, pool);
        self.update.reset(nested, guard, pool);
        self.delete.reset(nested, guard, pool);
    }
}

impl<T: 'static + Clone> TryExchange<T> {
    fn wait<'g>(
        &mut self,
        mine: PShared<'_, Node<ExchangeNode<T>>>,
        xchg: &Exchanger<T>,
        rec: bool,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<T, TryFail> {
        // TODO: timeout을 받고 loop을 쓰자
        // 누군가 update 해주길 기다림
        let saved_slot = self
            .load
            .run((), (&self.wait_ld_param, &xchg.slot), rec, guard, pool)
            .unwrap();
        let slot = if let Some(s) = saved_slot {
            s
        } else {
            self.load
                .run((), (&self.wait_ld_param, &xchg.slot), false, guard, pool)
                .unwrap()
                .unwrap()
        };

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
        true
    }
}

impl<T: Clone> DeleteHelper<Exchanger<T>, Node<ExchangeNode<T>>> for TryExchange<T> {
    fn prepare_delete<'g>(
        cur: PShared<'_, Node<ExchangeNode<T>>>,
        mine: PShared<'_, Node<ExchangeNode<T>>>,
        _: &Exchanger<T>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<ExchangeNode<T>>>>, ()> {
        if cur == mine {
            return Err(());
        }

        Ok(Some(PShared::<_>::null()))
    }

    fn node_when_deleted<'g>(
        _: PShared<'_, Node<ExchangeNode<T>>>,
        _: &'g Guard,
        _: &PoolHandle,
    ) -> PShared<'g, Node<ExchangeNode<T>>> {
        PShared::<_>::null()
    }
}

/// Exchanger의 exchange operation.
/// 반드시 exchange에 성공함.
#[derive(Debug)]
pub struct Exchange<T: Clone> {
    node: PAtomic<Node<ExchangeNode<T>>>,
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
            Node::<ExchangeNode<T>>::mark(node_ref, gc);
        }

        TryExchange::<T>::filter(&mut xchg.try_xchg, gc, pool);
    }
}

impl<T: 'static + Clone> Memento for Exchange<T> {
    type Object<'o> = &'o Exchanger<T>;
    type Input<'o> = T;
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        xchg: Self::Object<'o>,
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

        if let Ok(v) = self.try_xchg.run(xchg, node, rec, guard, pool) {
            return Ok(v);
        }

        loop {
            if let Ok(v) = self.try_xchg.run(xchg, node, false, guard, pool) {
                return Ok(v);
            }
        }
    }

    fn reset(&mut self, _: bool, guard: &Guard, _: &'static PoolHandle) {
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
    ) -> PShared<'g, Node<ExchangeNode<T>>> {
        let node = POwned::new(Node::from(ExchangeNode::from(value)), pool).into_shared(guard);
        self.node.store(node, Ordering::Relaxed);
        persist_obj(&self.node, true);
        node
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
