//! Atomic update memento collections

use std::{marker::PhantomData, ops::Deref, sync::atomic::Ordering};

use crossbeam_epoch::Guard;
use etrace::*;

use super::{common::Node, no_owner, InsertErr, Traversable};

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
    Memento,
};

/// TODO(doc)
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
    type Object<'o> = &'o PAtomic<N>; // TODO(must): SMOAtomic?
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
        &mut self,
        point: Self::Object<'o>,
        (mut new, obj, prepare): Self::Input<'o>, // TODO(opt): prepare도 그냥 Prepare trait으로 할 수 있을 듯
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
            // TODO: prepare(new_ref, old)? for tags
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

// TODO(opt): go_to_utils
#[inline]
fn with_high_tag(htag: u16, data: usize) -> usize {
    let high_bits = !(usize::MAX >> 16);
    (high_bits & ((htag as usize).rotate_right(16))) | (!high_bits & data)
}

#[inline]
fn get_high_tag(data: usize) -> u16 {
    let high_bits = !(usize::MAX >> 16);
    (data & high_bits).rotate_left(16) as u16
}

struct DeleteOrNode;

impl DeleteOrNode {
    const UPDATED_NODE: usize = 0;
    const DELETE_CLIENT: usize = 1;

    /// Ok(node_ptr) if node, otherwise Err(del_type)
    #[inline]
    fn get_node<'g, N>(checked: usize) -> Result<PShared<'g, N>, u16> {
        if checked & Self::DELETE_CLIENT == Self::DELETE_CLIENT {
            return Err(get_high_tag(checked));
        }

        unsafe { Ok(PShared::<_>::from_usize(checked)) }
    }

    #[inline]
    fn delete(x: usize, del_type: u16) -> usize {
        with_high_tag(del_type, x) & (!0 << 1) | Self::DELETE_CLIENT
    }

    #[inline]
    fn updated_node<N>(n: PShared<'_, N>) -> usize {
        n.with_tag(Self::UPDATED_NODE).into_usize()
    }
}

/// TODO(doc)
#[derive(Debug)]
pub struct NeedRetry;

/// TODO(doc)
// TODO(opt): 이거 나중에 unopt랑도 같이 쓸 수 있을 듯
pub trait UpdateDeleteInfo<O, N> {
    /// OK(Some or None): next or empty, Err: need retry
    fn prepare_delete<'g>(
        del_type: u16,
        cur: PShared<'_, N>,
        aux: PShared<'g, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, NeedRetry>;

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
        del_type: u16,
        deleted: PShared<'g, N>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Option<PShared<'g, N>>;
}

/// TODO(doc)
#[derive(Debug)]
pub struct SMOAtomic<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> {
    inner: PAtomic<N>,
    _marker: PhantomData<*const (O, G)>,
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Collectable for SMOAtomic<O, N, G> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        PAtomic::filter(&mut s.inner, gc, pool);
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Default for SMOAtomic<O, N, G> {
    fn default() -> Self {
        Self {
            inner: PAtomic::null(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> From<PShared<'_, N>>
    for SMOAtomic<O, N, G>
{
    fn from(node: PShared<'_, N>) -> Self {
        Self {
            inner: PAtomic::from(node),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Deref for SMOAtomic<O, N, G> {
    type Target = PAtomic<N>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> SMOAtomic<O, N, G> {
    pub fn load_helping<'g>(&self, guard: &'g Guard, pool: &PoolHandle) -> PShared<'g, N> {
        let mut p = self.inner.load(Ordering::SeqCst, guard);
        loop {
            let p_ref = some_or!(unsafe { p.as_ref(pool) }, return p);

            let owner = p_ref.owner();
            let o = owner.load(Ordering::SeqCst);
            if o == no_owner() {
                return p;
            }

            persist_obj(owner, true); // TODO(opt): async reset
            p = ok_or!(self.help(p, o, None, guard, pool), e, return e);
        }
    }

    /// Ok(ptr): required to be checked if the node is owned by someone
    /// Err(ptr): No need to help anymore
    #[inline]
    fn help<'g>(
        &self,
        old: PShared<'g, N>,
        owner: usize,
        helper_del_type_next: Option<(u16, PShared<'g, N>)>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<PShared<'g, N>, PShared<'g, N>> {
        // self가 바뀌어야 할 next를 설정
        let next = ok_or!(DeleteOrNode::get_node(owner), d, {
            match helper_del_type_next {
                Some((helper_del_type, next)) => {
                    if helper_del_type == d {
                        next
                    } else {
                        some_or!(G::node_when_deleted(d, old, guard, pool), return Err(old))
                    }
                }
                None => some_or!(G::node_when_deleted(d, old, guard, pool), return Err(old)),
            }
        });

        // self를 승자가 원하는 node로 바꿔줌
        let ret =
            match self
                .inner
                .compare_exchange(old, next, Ordering::SeqCst, Ordering::SeqCst, guard)
            {
                Ok(n) => n,
                Err(e) => e.current,
            };
        Ok(ret)
    }
}

unsafe impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Send for SMOAtomic<O, N, G> {}
unsafe impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Sync for SMOAtomic<O, N, G> {}

/// TODO(doc)
/// Do not use LSB while using `Delete` or `Update`.
/// It's reserved for them.
/// 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
#[derive(Debug)]
pub struct Delete<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> {
    target_loc: PAtomic<N>,
    _marker: PhantomData<*const (O, N, G)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync, G: UpdateDeleteInfo<O, N>> Send
    for Delete<O, N, G>
{
}
unsafe impl<O, N: Node + Collectable + Send + Sync, G: UpdateDeleteInfo<O, N>> Sync
    for Delete<O, N, G>
{
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Default for Delete<O, N, G> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Collectable for Delete<O, N, G> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N, G> Memento for Delete<O, N, G>
where
    O: 'static,
    N: 'static + Node + Collectable,
    G: 'static + UpdateDeleteInfo<O, N>,
{
    type Object<'o> = &'o SMOAtomic<O, N, G>;
    type Input<'o> = (u16, PShared<'o, N>, &'o O, usize);
    type Output<'o>
    where
        O: 'o,
        N: 'o,
        G: 'o,
    = Option<PShared<'o, N>>;
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (del_type, forbidden, obj, tid): Self::Input<'o>, // TODO(must): forbidden은 general하게 사용될까? 사용하는 좋은 방법은? prepare에 넘기지 말고 그냥 여기서 eq check로 사용해버리기?
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(del_type, guard, pool);
        }

        // Normal run
        let target = point.load_helping(guard, pool);
        let next = ok_or!(
            G::prepare_delete(del_type, target, forbidden, obj, guard, pool),
            return Err(())
        );
        let next = some_or!(next, {
            self.target_loc
                .store(PShared::null().with_tag(Self::EMPTY), Ordering::Relaxed);
            persist_obj(&self.target_loc, true);
            return Ok(None);
        });

        // 우선 내가 target을 가리키고
        self.target_loc.store(target, Ordering::Relaxed);
        persist_obj(&self.target_loc, false); // we're doing CAS soon.

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { target.deref(pool) };
        let owner = target_ref.owner();
        let _ = owner
            .compare_exchange(
                no_owner(),
                self.id(del_type, pool),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map_err(|_| ())?;

        // Now I own the location. flush the owner.
        persist_obj(owner, false);

        // 주인을 정했으니 이제 point를 바꿔줌
        let _ = point.compare_exchange(target, next, Ordering::SeqCst, Ordering::SeqCst, guard);

        // 바뀐 point는 내가 뽑은 node를 free하기 전에 persist 될 거임
        // post-crash에서 history가 끊기진 않음: 다음 접근자가 `Insert`라면, 그는 point를 persist 무조건 할 거임.
        guard.defer_persist(point);

        Ok(Some(target))
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, false);
    }
}

impl<O, N, G> Delete<O, N, G>
where
    N: Node + Collectable,
    G: UpdateDeleteInfo<O, N>,
{
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    #[inline]
    fn result<'g>(
        &self,
        del_type: u16,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);
        let target_ref = some_or!(unsafe { target.as_ref(pool) }, {
            return if target.tag() & Self::EMPTY != 0 {
                // empty라고 명시됨
                Ok(None)
            } else {
                // 내가 찜한게 없으면 실패
                Err(())
            };
        });

        // 내가 찜한 target의 owner가 나이면 성공, 아니면 실패
        let owner = target_ref.owner().load(Ordering::SeqCst);
        if owner == self.id(del_type, pool) {
            Ok(Some(target))
        } else {
            Err(())
        }
    }

    #[inline]
    fn id(&self, del_type: u16, pool: &PoolHandle) -> usize {
        // 풀 열릴 때마다 주소 바뀌니 상대주소로 식별해야 함
        let off = unsafe { self.as_pptr(pool).into_offset() };
        DeleteOrNode::delete(off, del_type)
    }
}

/// TODO(doc)
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

/// TODO(doc)
/// Do not use LSB while using `Delete` or `Update`.
/// It's reserved for them.
/// 빠졌던 노드를 다시 넣으려 하면 안 됨
/// 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
// TODO(opt): update는 O 필요 없는 것 같음
#[derive(Debug)]
pub struct Update<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> {
    target_loc: PAtomic<N>,
    _marker: PhantomData<*const (O, N, G)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync, G: UpdateDeleteInfo<O, N>> Send
    for Update<O, N, G>
{
}
unsafe impl<O, N: Node + Collectable + Send + Sync, G: UpdateDeleteInfo<O, N>> Sync
    for Update<O, N, G>
{
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Default for Update<O, N, G> {
    fn default() -> Self {
        Self {
            target_loc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: UpdateDeleteInfo<O, N>> Collectable for Update<O, N, G> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N, G: UpdateDeleteInfo<O, N>> Memento for Update<O, N, G>
where
    O: 'static,
    N: 'static + Node + Collectable,
    G: 'static,
{
    type Object<'o> = &'o SMOAtomic<O, N, G>;
    type Input<'o> = (PShared<'o, N>, PShared<'o, N>, &'o O);
    type Output<'o>
    where
        O: 'o,
        N: 'o,
    = PShared<'o, N>;
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        point: Self::Object<'o>,
        (expected, new, obj): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(new, guard, pool);
        }

        // Normal run

        // 포인트의 현재 타겟 불러옴
        let target = point.load_helping(guard, pool);
        if target.is_null() || !G::prepare_update(target, expected, obj, guard, pool) {
            return Err(());
        }

        // println!("update zzim {:?}", target);
        let target_ref = unsafe { target.deref(pool) };

        // 우선 내가 target을 가리키고
        self.target_loc.store(target, Ordering::Relaxed);
        persist_obj(&self.target_loc, false);

        // 빼려는 node가 내가 넣을 노드 가리키게 함
        let owner = target_ref.owner();
        owner
            .compare_exchange(
                no_owner(),
                DeleteOrNode::updated_node(new),
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
                // TODO(opt): 헬핑하지 말고 리턴
                let p = point.load_helping(guard, pool);

                if p != target {
                    return;
                }

                // same context
                persist_obj(owner, false); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist

                let _ = point.help(target, cur, None, guard, pool);
            })
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.target_loc.store(PShared::null(), Ordering::Relaxed);
        persist_obj(&self.target_loc, false);
    }
}

impl<O, N, G> Update<O, N, G>
where
    N: Node + Collectable,
    G: UpdateDeleteInfo<O, N>,
{
    #[inline]
    fn result<'g>(
        &self,
        new: PShared<'_, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<PShared<'g, N>, ()> {
        let target = self.target_loc.load(Ordering::Relaxed, guard);
        let target_ref = some_or!(unsafe { target.as_ref(pool) }, return Err(()));

        // 내가 찜한 target의 owner가 new이면 성공, 아니면 실패
        let owner = target_ref.owner().load(Ordering::SeqCst);
        if owner == DeleteOrNode::updated_node(new) {
            Ok(target)
        } else {
            Err(())
        }
    }

    pub fn next_updated_node<'g>(old: &N) -> Option<PShared<'g, N>> {
        let u = old.owner().load(Ordering::SeqCst);
        assert_ne!(u, no_owner()); // TODO(must): ABA 문제가 터지는지 확인하기 위해 달아놓은 assert

        let n = ok_or!(DeleteOrNode::get_node(u), return None);
        Some(n)
    }
}
