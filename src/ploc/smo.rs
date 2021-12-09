//! Atomic update memento collections

use std::{marker::PhantomData, ops::Deref, sync::atomic::Ordering};

use crossbeam_epoch::Guard;

use super::{common::Node, no_owner, InsertErr, Traversable};

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PDestroyable, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
    Memento,
};

/// TODO: doc
///
/// 빠졌던 노드를 다시 넣으려 하면 안 됨
#[derive(Debug)]
pub struct Insert<O, N: Node + Collectable> {
    _marker: PhantomData<*const (O, N)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync> Send for Insert<O, N> {}
unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for Insert<O, N> {}

impl<O, N: Node + Collectable> Default for Insert<O, N> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable> Collectable for Insert<O, N> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N> Memento for Insert<O, N>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
{
    type Object<'o> = &'o PAtomic<N>;
    type Input<'o> = (
        PShared<'o, N>,
        &'o O,
        fn(&mut N) -> bool, // cas 전에 할 일 (bool 리턴값은 계속 진행할지 여부)
    );
    type Output<'o>
    where
        O: 'o,
        N: 'o,
    = ();
    type Error<'o> = InsertErr<'o, N>;

    fn run<'o>(
        &'o mut self,
        point: Self::Object<'o>,
        (mut new, obj, prepare): Self::Input<'o>, // TODO: prepare도 그냥 Prepare trait으로 할 수 있을 듯
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(obj, new, guard, pool);
        }

        // Normal run
        let new_ref = unsafe { new.deref_mut(pool) };
        let old = point.load(Ordering::SeqCst, guard);

        if !old.is_null() || !prepare(new_ref) {
            return Err(InsertErr::PrepareFail);
        }

        let ret = point
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
            .map(|_| ())
            .map_err(|e| InsertErr::CASFail(e.current));

        persist_obj(point, true);
        ret
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
}

impl<O: Traversable<N>, N: Node + Collectable> Insert<O, N> {
    #[inline]
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if unsafe { new.deref(pool) }.acked()
            || obj.search(new, guard, pool)
            || unsafe { new.deref(pool) }.acked()
        {
            return Ok(());
        }

        Err(InsertErr::RecFail) // Fail이 crash 이후 달라질 수 있음. Insert는 weak 함
    }
}

// TODO: How to use union type for this purpose?
struct DeleteOrNode;

impl DeleteOrNode {
    /// Delete client id임을 표시하기 위한 태그
    const DELETE_CLIENT: usize = 1;

    #[inline]
    fn is_node<'g, N>(checked: usize) -> Option<PShared<'g, N>> {
        if checked & Self::DELETE_CLIENT == Self::DELETE_CLIENT {
            return None;
        }

        unsafe { Some(PShared::<_>::from_usize(checked)) }
    }

    #[inline]
    fn set_delete(x: usize) -> usize {
        (x & (!0 << 1)) | Self::DELETE_CLIENT // TODO: client가 align 되어있다는 확신이 없음. 일단 LSB를 그냥 맘대로 사용함.
    }
}

/// TODO: doc
// TODO: 이거 나중에 unopt랑도 같이 쓸 수 있을 듯
pub trait DeleteHelper<O, N> {
    /// OK(Some or None): next or empty, Err: need retry
    fn prepare_delete<'g>(
        cur: PShared<'_, N>,
        forbidden: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()>;

    /// 계속 진행 여부를 리턴
    fn prepare_update<'g>(
        cur: PShared<'_, N>,
        expected: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> bool;

    /// A pointer that should be next after a node is deleted
    fn node_when_deleted<'g>(
        deleted: PShared<'_, N>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, N>;
}

/// TODO: doc
#[derive(Debug)]
pub struct SMOAtomic<O, N: Collectable, G: DeleteHelper<O, N>> {
    ptr: PAtomic<N>,
    _marker: PhantomData<*const (O, G)>,
}

impl<O, N: Collectable, G: DeleteHelper<O, N>> Collectable for SMOAtomic<O, N, G> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        PAtomic::filter(&mut s.ptr, gc, pool);
    }
}

impl<O, N: Collectable, G: DeleteHelper<O, N>> Default for SMOAtomic<O, N, G> {
    fn default() -> Self {
        Self {
            ptr: PAtomic::null(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Collectable, G: DeleteHelper<O, N>> From<PShared<'_, N>> for SMOAtomic<O, N, G> {
    fn from(node: PShared<'_, N>) -> Self {
        Self {
            ptr: PAtomic::from(node),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Collectable, G: DeleteHelper<O, N>> Deref for SMOAtomic<O, N, G> {
    type Target = PAtomic<N>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

unsafe impl<O, N: Collectable, G: DeleteHelper<O, N>> Send for SMOAtomic<O, N, G> {}
unsafe impl<O, N: Collectable, G: DeleteHelper<O, N>> Sync for SMOAtomic<O, N, G> {}

/// TODO: doc
// TODO: 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
#[derive(Debug)]
pub struct Delete<O, N: Node + Collectable, G: DeleteHelper<O, N>> {
    target_loc: PAtomic<N>,
    _marker: PhantomData<*const (O, N, G)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync, G: DeleteHelper<O, N>> Send
    for Delete<O, N, G>
{
}
unsafe impl<O, N: Node + Collectable + Send + Sync, G: DeleteHelper<O, N>> Sync
    for Delete<O, N, G>
{
}

impl<O, N: Node + Collectable, G: DeleteHelper<O, N>> Default for Delete<O, N, G> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: DeleteHelper<O, N>> Collectable for Delete<O, N, G> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N, G> Memento for Delete<O, N, G>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
    G: 'static + DeleteHelper<O, N>,
{
    type Object<'o> = &'o SMOAtomic<O, N, G>;
    type Input<'o> = (PShared<'o, N>, &'o O);
    type Output<'o>
    where
        O: 'o,
        N: 'o,
        G: 'o,
    = Option<PShared<'o, N>>;
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        point: Self::Object<'o>,
        (forbidden, obj): Self::Input<'o>, // TODO: forbidden은 general하게 사용될까? 사용하는 좋은 방법은? prepare에 넘기지 말고 그냥 여기서 eq check로 사용해버리기?
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(guard, pool);
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);

        let next = match G::prepare_delete(target, forbidden, obj, guard, pool) {
            Ok(Some(n)) => n,
            Ok(None) => {
                self.target_loc
                    .store(PShared::null().with_tag(Self::EMPTY), Ordering::Relaxed);
                persist_obj(&self.target_loc, true);
                return Ok(None);
            }
            Err(()) => return Err(()),
        };

        // 우선 내가 target을 가리키고
        self.target_loc.store(target, Ordering::Relaxed);
        persist_obj(&self.target_loc, false);

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { target.deref(pool) };
        let owner = target_ref.owner();
        owner
            .compare_exchange(
                no_owner(),
                self.id(pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(owner, false);

                // 주인을 정했으니 이제 point를 바꿔줌
                let _ =
                    point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);

                // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
                guard.defer_persist(point);

                Some(target)
            })
            .map_err(|cur| {
                let p = point.load(Ordering::SeqCst, guard);

                if p != target {
                    return;
                }

                // same context
                persist_obj(owner, false); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist

                // 승리한 애가 (1) update면 걔의 node, (2) delete면 그냥 next(= node_when_delete(target))
                let real_next = DeleteOrNode::is_node(cur).unwrap_or(next);

                // point를 승리한 애와 관련된 것으로 바꿔주
                let _ = point.compare_exchange(
                    target,
                    real_next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                );
            })
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        let target = self.target_loc.load(Ordering::Relaxed, guard);

        // null로 바꾼 후, free 하기 전에 crash 나도 상관없음.
        // root로부터 도달 불가능해졌다면 GC가 수거해갈 것임.
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, true);
        self.dealloc(target, guard, pool); // TODO(must): elim stack의 경우 fail일 때도 reset이 불릴 수 있음
    }
}

impl<O, N, G> Delete<O, N, G>
where
    O: Traversable<N>,
    N: Node + Collectable,
    G: DeleteHelper<O, N>,
{
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    #[inline]
    fn result<'g>(
        &self,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);

        if target.tag() & Self::EMPTY == Self::EMPTY {
            // post-crash execution (empty)
            return Ok(None);
        }

        if !target.is_null() {
            let target_ref = unsafe { target.deref(pool) };
            let owner = target_ref.owner().load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if owner == self.id(pool) {
                return Ok(Some(target));
            };
        }

        Err(()) // 찜한 게 아무 의미가 없을 때는 실패로 간주 (Weak fail)
    }

    /// TODO: doc
    pub fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) {
        if target.is_null() || target.tag() == Self::EMPTY {
            return;
        }

        // owner가 내가 아닐 수 있음
        // 따라서 owner를 확인 후 내가 delete한게 맞는다면 free
        unsafe {
            if target.deref(pool).owner().load(Ordering::SeqCst) == self.id(pool) {
                guard.defer_pdestroy(target);
            }
        }
    }

    #[inline]
    fn id(&self, pool: &PoolHandle) -> usize {
        // 풀 열릴 때마다 주소 바뀌니 상대주소로 식별해야 함
        let off = unsafe { self.as_pptr(pool).into_offset() };
        DeleteOrNode::set_delete(off)
    }
}

/// TODO: doc
///
/// # Safety
///
/// 내가 Insert/Update로 넣은 node를 내가 Delete 해서 뺐을 때 사용
/// - 남이 넣었던 건 하면 안 되는 이유: 넣었던 애가 `acked()`를 호출하기 때문에 owner를 건드리면 안 됨
/// - 내가 Update로 뺐을 때 하면 안 되는 이유: point CAS를 helping 하는 애들이 next node를 owner를 통해 알게 되므로 건드리면 안 됨
pub unsafe fn clear_owner<N: Node>(deleted_node: &N) {
    let owner = deleted_node.owner();
    owner.store(no_owner(), Ordering::SeqCst);
    persist_obj(owner, true);
}

/// TODO: doc
///
/// 빠졌던 노드를 다시 넣으려 하면 안 됨
// TODO: 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
// TODO: update는 O 필요 없는 것 같음
#[derive(Debug)]
pub struct Update<O, N: Node + Collectable, G: DeleteHelper<O, N>> {
    _marker: PhantomData<*const (O, N, G)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync, G: DeleteHelper<O, N>> Send
    for Update<O, N, G>
{
}
unsafe impl<O, N: Node + Collectable + Send + Sync, G: DeleteHelper<O, N>> Sync
    for Update<O, N, G>
{
}

impl<O, N: Node + Collectable, G: DeleteHelper<O, N>> Default for Update<O, N, G> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: DeleteHelper<O, N>> Collectable for Update<O, N, G> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N, G: DeleteHelper<O, N>> Memento for Update<O, N, G>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
    G: 'static,
{
    type Object<'o> = &'o SMOAtomic<O, N, G>;
    type Input<'o> = (PShared<'o, N>, &'o PAtomic<N>, PShared<'o, N>, &'o O);
    type Output<'o>
    where
        O: 'o,
        N: 'o,
    = PShared<'o, N>;
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        point: Self::Object<'o>,
        (new, save_loc, expected, obj): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(new, save_loc, guard, pool);
        }

        // Normal run

        // 포인트의 현재 타겟 불러옴
        let target = point.load(Ordering::SeqCst, guard);

        if target.is_null() {
            return Err(());
        }

        if !G::prepare_update(target, expected, obj, guard, pool) {
            return Err(());
        }

        let target_ref = unsafe { target.deref(pool) };

        // owner의 주인이 없는지 확인
        let owner = target_ref.owner();
        let o = owner.load(Ordering::SeqCst);

        // 이미 주인이 있다면 point를 바꿔주고 페일 리턴
        // TODO: 찜하기 전에 load 먼저 해보는 건데, 그 순서는 실험을 하고 나서 정하자
        if o != no_owner() {
            persist_obj(owner, false);
            let next =
                DeleteOrNode::is_node(o).unwrap_or(G::node_when_deleted(target, guard, pool));
            let _ = point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
            // 빠졌던 노드를 다시 들어오게 되는 경우는 없어야 함
            return Err(());
        }

        // 우선 내가 target을 가리키고
        save_loc.store(target, Ordering::Relaxed);
        persist_obj(save_loc, false);

        // 빼려는 node가 내가 넣을 노드 가리키게 함
        owner
            .compare_exchange(
                no_owner(),
                new.into_usize(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| {
                persist_obj(owner, false);

                // 주인을 정했으니 이제 point를 바꿔줌
                let _ =
                    point.compare_exchange(target, new, Ordering::SeqCst, Ordering::SeqCst, guard);

                // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
                guard.defer_persist(point);
                target
            })
            .map_err(|cur| {
                let p = point.load(Ordering::SeqCst, guard);

                if p != target {
                    return;
                }

                // same context
                persist_obj(owner, false); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist

                // point가 바뀌어야 할 next를 설정
                let next =
                    DeleteOrNode::is_node(cur).unwrap_or(G::node_when_deleted(target, guard, pool));

                // point를 승자가 원하는 node로 바꿔줌
                let _ =
                    point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);
            })
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, N, G> Update<O, N, G>
where
    O: Traversable<N>,
    N: Node + Collectable,
    G: DeleteHelper<O, N>,
{
    #[inline]
    fn result<'g>(
        &self,
        new: PShared<'_, N>,
        save_loc: &PAtomic<N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        let target = save_loc.load(Ordering::Relaxed, guard);

        if !target.is_null() {
            let target_ref = unsafe { target.deref(pool) };
            let owner = target_ref.owner().load(Ordering::SeqCst);

            // target이 내가 pop한 게 맞는지 확인
            if owner == new.into_usize() {
                return Ok(target);
            };
        }

        Err(())
    }

    /// TODO: doc
    pub fn dealloc(
        &self,
        target: PShared<'_, N>,
        new: PShared<'_, N>,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        // owner가 내가 아닐 수 있음
        // 따라서 owner를 확인 후 내가 update한 게 맞는다면 free
        unsafe {
            let owner = target.deref(pool).owner().load(Ordering::SeqCst);

            if owner == new.into_usize() {
                guard.defer_pdestroy(target);
            }
        }
    }

    /// # Safety
    ///
    /// Update되어 owner가 node일 때만 사용해야 함
    pub unsafe fn next_updated_node<'g>(old: &N) -> PShared<'g, N> {
        let u = old.owner().load(Ordering::SeqCst);
        assert_ne!(u, no_owner()); // TODO: ABA 문제가 터지는지 확인하기 위해 달아놓은 assert
        PShared::from_usize(u)
    }
}
