//! Atomic update memento collections

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::Guard;

use crate::{
    atomic_update_common::{InsertErr, Node, Traversable},
    pepoch::{atomic::Pointer, PAtomic, PDestroyable, PShared},
    persistent::Memento,
    plocation::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        AsPPtr, PoolHandle,
    },
};

/// TODO: doc
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
    type Object<'o> = &'o O;
    type Input<'o> = (
        PShared<'o, N>,
        &'o PAtomic<N>,
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
        obj: Self::Object<'o>,
        (mut new, point, prepare): Self::Input<'o>, // TODO: prepare도 그냥 Prepare trait으로 할 수 있을 듯
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

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O: Traversable<N>, N: Node + Collectable> Insert<O, N> {
    fn result<'g>(
        &self,
        obj: &O,
        new: PShared<'g, N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertErr<'g, N>> {
        if obj.search(new, guard, pool) || unsafe { new.deref(pool) }.acked() {
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
        x | Self::DELETE_CLIENT // TODO: client id의 LSB에 trailing zero가 있나? (node ptr과 구분)
    }
}

/// TODO: doc
// TODO: 이거 나중에 unopt랑도 같이 쓸 수 있을 듯
pub trait GetNext<O, N> {
    /// OK(Some or None): next or empty, Err: need retry
    fn get_next<'g>(
        cur: PShared<'_, N>,
        obj: &O,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()>;
}

/// TODO: doc
#[derive(Debug)]
pub struct SMOAtomic<O, N, G: GetNext<O, N>> {
    ptr: PAtomic<N>,
    _marker: PhantomData<*const (O, G)>,
}

impl<O, N, G: GetNext<O, N>> Deref for SMOAtomic<O, N, G> {
    type Target = PAtomic<N>;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

/// TODO: doc
// TODO: 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
#[derive(Debug)]
pub struct Delete<O, N: Node + Collectable, G: GetNext<O, N>> {
    _marker: PhantomData<*const (O, N, G)>,
}

unsafe impl<O, N: Node + Collectable + Send + Sync, G: GetNext<O, N>> Send for Delete<O, N, G> {}
unsafe impl<O, N: Node + Collectable + Send + Sync, G: GetNext<O, N>> Sync for Delete<O, N, G> {}

impl<O, N: Node + Collectable, G: GetNext<O, N>> Default for Delete<O, N, G> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<O, N: Node + Collectable, G: GetNext<O, N>> Collectable for Delete<O, N, G> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<O, N, G> Memento for Delete<O, N, G>
where
    O: 'static + Traversable<N>,
    N: 'static + Node + Collectable,
    G: 'static + GetNext<O, N>,
{
    type Object<'o> = &'o O;
    type Input<'o> = (&'o PAtomic<N>, &'o SMOAtomic<O, N, G>);
    type Output<'o>
    where
        O: 'o,
        N: 'o,
        G: 'o,
    = Option<PShared<'o, N>>;
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        obj: Self::Object<'o>,
        (target_loc, point): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            return self.result(target_loc, guard, pool);
        }

        // Normal run
        let target = point.load(Ordering::SeqCst, guard);

        // TODO: history 따라감, 마지막 지점 찾아서 처리
        // TODO: track_history에서 empty 상황까지 알려줄 듯

        let next = match G::get_next(target, obj, guard, pool) {
            Ok(Some(n)) => n,
            Ok(None) => {
                target_loc.store(PShared::null().with_tag(Self::EMPTY), Ordering::Relaxed);
                persist_obj(&target_loc, true);
                return Ok(None);
            }
            Err(()) => return Err(()),
        };

        // 우선 내가 target을 가리키고
        target_loc.store(target, Ordering::Relaxed);
        persist_obj(target_loc, false);

        // 빼려는 node에 내 이름 새겨넣음
        let target_ref = unsafe { target.deref(pool) };
        let owner = target_ref.owner();
        owner
            .compare_exchange(
                Self::no_owner(),
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
                if p == target {
                    // same context
                    persist_obj(owner, false); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist

                    // 승리한 애가 (1) update면 걔의 node, (2) delete면 그냥 next
                    let real_next = DeleteOrNode::is_node(cur).unwrap_or(next);

                    // point를 승리한 애와 관련된 것으로 바꿔주
                    let _ = point.compare_exchange(
                        target,
                        real_next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    );
                }
            })
    }

    fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
}

impl<O, N, G> Delete<O, N, G>
where
    O: Traversable<N>,
    N: Node + Collectable,
    G: GetNext<O, N>,
{
    /// `pop()` 결과 중 Empty를 표시하기 위한 태그
    const EMPTY: usize = 2;

    fn result<'g>(
        &self,
        target_loc: &PAtomic<N>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<Option<PShared<'g, N>>, ()> {
        let target = target_loc.load(Ordering::Relaxed, guard);

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

        Err(())
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

    /// TODO: doc
    // TODO: 공통 함수로 빼기
    #[inline]
    pub fn no_owner() -> usize {
        let null = PShared::<Self>::null();
        null.into_usize()
    }
}

// fn track_history<'g, O, N: Node, G: GetNext<O, N>>(
//     point: &SMOAtomic<O, N, G>,
//     guard: &'g Guard,
//     pool: &PoolHandle,
// ) -> Vec<PShared<'g, N>> {
//     let mut history = vec![]; // TODO: tiny vector

//     // 마지막으로 update 결과 노드 찾음
//     // TODO: 순회하면서 persist 해야 할 듯?
//     let mut cur = point.load(Ordering::SeqCst, guard);
//     loop {
//         history.push(cur);

//         if cur.is_null() {
//             return history;
//         }

//         let cur_ref = unsafe { cur.deref(pool) };
//         let owner = cur_ref.owner().load(Ordering::SeqCst);
//         persist_obj(cur_ref.owner(), true);

//         if owner & DELETE_CLIENT == DELETE_CLIENT {
//             // Owner is a delete client
//             // get next from G
//             cur = G::get_next(cur, guard, pool);
//         } else {
//             // Owner is a node
//             // The node is next
//             cur = unsafe { PShared::from_usize(owner) };
//         }
//     }
// }

// fn update_point<'g, N>(
//     point: &AtomicUsize,
//     mut old: usize,
//     new: usize,
//     history: &Vec<usize>,
//     guard: &'g Guard,
// ) {
//     loop {
//         // point를 new로 갱신 시도
//         let res = point.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst);

//         match res {
//             Ok(_) => break,
//             Err(e) => {
//                 // 현재 point가 지금까지 거쳐 온 history에 있는 애를 가리키는지 확인
//                 let found = history.iter().find(|&&x| x == e).is_some();

//                 if found {
//                     // 있으면 다시 카스
//                     old = e;
//                 }

//                 // history에 없으면 카스 안 해도 됨 (point가 이미 new를 지나감)
//                 break;
//             }
//         }
//     }
// }

// /// TODO: doc
// TODO: 이걸 사용하는 Node의 `acked()`는 owner가 `no_owner()`가 아닌지를 판단해야 함
// #[derive(Debug)]
// pub struct Update<O, N: Node + Collectable> {
//     _marker: PhantomData<*const (O, N)>,
// }

// unsafe impl<O, N: Node + Collectable + Send + Sync> Send for Update<O, N> {}
// unsafe impl<O, N: Node + Collectable + Send + Sync> Sync for Update<O, N> {}

// impl<O, N: Node + Collectable> Default for Update<O, N> {
//     fn default() -> Self {
//         Self {
//             _marker: Default::default(),
//         }
//     }
// }

// impl<O, N: Node + Collectable> Collectable for Update<O, N> {
//     fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
// }

// impl<O, N> Memento for Update<O, N>
// where
//     O: 'static + Traversable<N>,
//     N: 'static + Node + Collectable,
// {
//     type Object<'o> = &'o O;
//     type Input<'o> = (PShared<'o, N>, &'o PAtomic<N>, &'o AtomicUsize);
//     type Output<'o>
//     where
//         O: 'o,
//         N: 'o,
//     = Option<PShared<'o, N>>;
//     type Error<'o> = ();

//     fn run<'o>(
//         &'o mut self,
//         obj: Self::Object<'o>,
//         (new, save_loc, point): Self::Input<'o>,
//         rec: bool,
//         guard: &'o Guard,
//         pool: &'static PoolHandle,
//     ) -> Result<Self::Output<'o>, Self::Error<'o>> {
//         if rec {
//             return self.result(new, save_loc, guard, pool);
//         }

//         // Normal run

//         // TODO: use history chain
//         // let mut history = track_history(point, guard, pool);
//         // let target = *history.last().unwrap();

//         // 우선 내가 target을 가리키고
//         save_loc.store(target, Ordering::Relaxed);
//         persist_obj(save_loc, false);

//         let target_ref = unsafe { target.deref(pool) };

//         // 빼려는 node가 내가 넣을 노드 가리키게 함
//         let owner = target_ref.owner(); // TODO: owner를 포인터로, insert랑 대구가 맞아야 함
//         owner
//             .compare_exchange(
//                 PShared::<N>::null().into_usize(),
//                 new.into_usize(), // TODO: 나중엔 delete랑 구분하는 태그를 넣어줌, filter에서 노드는 잘 살릴 수 있게 해줘야 함
//                 Ordering::SeqCst,
//                 Ordering::SeqCst,
//             )
//             .map(|_| {
//                 persist_obj(owner, true);
//                 update_point(point, target, new.into_usize(), &history, guard);
//                 guard.defer_persist(point);
//                 Some(target)
//             })
//             .map_err(|e| {
//                 let cur = point.load(Ordering::SeqCst, guard);
//                 if cur == target {
//                     // same context
//                     persist_obj(owner, true); // insert한 애에게 insert 되었다는 확신을 주기 위해서 struct advanve 시키기 전에 반드시 persist
//                     update_point(point, target, e, &history, guard);
//                 }
//             })
//     }

//     fn reset(&mut self, _: bool, _: &Guard, _: &'static PoolHandle) {}
// }

// impl<O, N> Update<O, N>
// where
//     O: Traversable<N>,
//     N: Node + Collectable,
// {
//     /// `pop()` 결과 중 Empty를 표시하기 위한 태그
//     const EMPTY: usize = 2;

//     fn result<'g>(
//         &self,
//         new: PShared<'_, N>,
//         save_loc: &PAtomic<N>,
//         guard: &'g Guard,
//         pool: &'static PoolHandle,
//     ) -> Result<Option<PShared<'g, N>>, ()> {
//         let target = save_loc.load(Ordering::Relaxed, guard);

//         if target.tag() & Self::EMPTY == Self::EMPTY {
//             // post-crash execution (empty)
//             return Ok(None);
//         }

//         if !target.is_null() {
//             let target_ref = unsafe { target.deref(pool) };
//             let owner = target_ref.owner().load(Ordering::SeqCst);

//             // target이 내가 pop한 게 맞는지 확인
//             if owner == new.into_usize() {
//                 return Ok(Some(target));
//             };
//         }

//         Err(())
//     }

//     /// TODO: doc
//     pub fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle) {
//         // TODO: 내가 넣었던 `new` 포인터와 비교해봐야 함

//         // if target.is_null() || target.tag() == Self::EMPTY {
//         //     return;
//         // }

//         // // owner가 내가 아닐 수 있음
//         // // 따라서 owner를 확인 후 내가 delete한 게 맞는다면 free
//         // unsafe {
//         //     let owner = target.deref(pool).owner().load(Ordering::SeqCst);

//         //     if owner.as_ptr().into_offset() == self.id(pool) {
//         //         guard.defer_pdestroy(target);
//         //     }
//         // }
//     }

//     // TODO: 쓸 일 없을 듯?
//     #[inline]
//     fn id(&self, pool: &PoolHandle) -> usize {
//         // 풀 열릴 때마다 주소 바뀌니 상대주소로 식별해야 함
//         unsafe { self.as_pptr(pool).into_offset() }
//     }

//     /// TODO: doc
//     #[inline]
//     pub fn no_owner() -> usize {
//         let null = PShared::<Self>::null();
//         null.into_usize()
//     }
// }
