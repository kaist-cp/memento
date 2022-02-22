//! Concurrent Level Hash Table.
#![allow(missing_docs)]
#![allow(box_pointers)]
#![allow(unreachable_pub)]
use core::cmp;
use core::fmt::{Debug, Display};
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use std::sync::mpsc;

use cfg_if::cfg_if;
use crossbeam_epoch::{unprotected, Guard};
use derivative::Derivative;
use etrace::*;
use fasthash::Murmur3HasherExt;
use itertools::*;
use libc::c_void;
use parking_lot::{lock_api::RawMutex, RawMutex as RawMutexImpl};
use tinyvec::*;

use crate::pepoch::atomic::cut_as_high_tag_len;
use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, DetectableCASAtomic};
use crate::pmem::{
    global_pool, persist_obj, sfence, AsPPtr, Collectable, GarbageCollection, PPtr, PoolHandle,
};
use crate::PDefault;

const TINY_VEC_CAPACITY: usize = 8;

/// Insert client
#[derive(Debug)]
pub struct Insert<K, V> {
    occupied: Checkpoint<bool>,
    node: Checkpoint<PAtomic<Slot<K, V>>>,
    insert_inner: InsertInner<K, V>,
    move_done: Checkpoint<bool>,
    tag_cas: Cas<Slot<K, V>>,
}

impl<K, V> Default for Insert<K, V> {
    fn default() -> Self {
        Self {
            occupied: Default::default(),
            node: Default::default(),
            insert_inner: Default::default(),
            move_done: Default::default(),
            tag_cas: Default::default(),
        }
    }
}

impl<K, V> Collectable for Insert<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

/// Insert inner client
#[derive(Debug)]
pub struct InsertInner<K, V> {
    insert_chk: Checkpoint<(usize, PPtr<DetectableCASAtomic<Slot<K, V>>>)>,
    insert_cas: Cas<Slot<K, V>>,
}

impl<K, V> Default for InsertInner<K, V> {
    fn default() -> Self {
        Self {
            insert_chk: Default::default(),
            insert_cas: Default::default(),
        }
    }
}

impl<K, V> Collectable for InsertInner<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

/// Resize client
#[derive(Debug)]
pub struct Resize<K, V> {
    delete_chk: Checkpoint<(PPtr<DetectableCASAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
    delete_cas: Cas<Slot<K, V>>,
    insert_chk: Checkpoint<PPtr<DetectableCASAtomic<Slot<K, V>>>>,
    insert_cas: Cas<Slot<K, V>>,
}

impl<K, V> Default for Resize<K, V> {
    fn default() -> Self {
        Self {
            delete_chk: Default::default(),
            delete_cas: Default::default(),
            insert_chk: Default::default(),
            insert_cas: Default::default(),
        }
    }
}

impl<K, V> Collectable for Resize<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

/// Delete client
#[derive(Debug)]
pub struct TryDelete<K, V> {
    delete_cas: Cas<Slot<K, V>>,
    find_result_chk: Checkpoint<(PPtr<DetectableCASAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
}

impl<K, V> Default for TryDelete<K, V> {
    fn default() -> Self {
        Self {
            delete_cas: Default::default(),
            find_result_chk: Default::default(),
        }
    }
}

impl<K, V> Collectable for TryDelete<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

/// Delete client
#[derive(Debug)]
pub struct Delete<K, V> {
    try_delete: TryDelete<K, V>,
}

impl<K, V> Default for Delete<K, V> {
    fn default() -> Self {
        Self {
            try_delete: Default::default(),
        }
    }
}

impl<K, V> Collectable for Delete<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

cfg_if! {
    if #[cfg(feature = "stress")] {
        // For stress test.

        const SLOTS_IN_BUCKET: usize = 1;
        const LEVEL_DIFF: usize = 2;
        const MIN_SIZE: usize = 1;

        const fn level_size_next(size: usize) -> usize {
            size + LEVEL_DIFF
        }

        const fn level_size_prev(size: usize) -> usize {
            size - LEVEL_DIFF
        }
    } else {
        // For real workload.

        // 해시 크기: MIN_SIZE * SLOTS_IN_BUCKET * (1+LEVEL_RATIO)
        const SLOTS_IN_BUCKET: usize = 8; // 고정
        const LEVEL_RATIO: usize = 2; // 고정
        const MIN_SIZE: usize = 786432; // 이걸로 해시 크기 조절

        const fn level_size_next(size: usize) -> usize {
            size * LEVEL_RATIO
        }

        const fn level_size_prev(size: usize) -> usize {
            size / LEVEL_RATIO
        }
    }
}

fn hashes<T: Hash>(t: &T) -> (u16, [u32; 2]) {
    let mut hasher = Murmur3HasherExt::default();
    t.hash(&mut hasher);
    let hash = hasher.finish() as usize;

    let tag = hash.rotate_left(16) as u16;
    let tag = cut_as_high_tag_len(tag as usize) as u16;
    let left = hash as u32;
    let right = hash.rotate_right(32) as u32;

    (tag, [left, if left != right { right } else { right + 1 }])
}

#[derive(Debug, Default)]
struct Slot<K, V> {
    key: K,
    value: V,
}

impl<K, V> From<(K, V)> for Slot<K, V> {
    #[inline]
    fn from((key, value): (K, V)) -> Self {
        Self { key, value }
    }
}

// TODO(must): V도 collectable 해야 함
impl<K, V> Collectable for Slot<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

#[derive(Debug)]
#[repr(align(64))]
struct Bucket<K, V> {
    slots: [DetectableCASAtomic<Slot<K, V>>; SLOTS_IN_BUCKET],
}

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: PAtomic<Node<T>>,
}

impl<T> From<T> for Node<T> {
    fn from(val: T) -> Self {
        Self {
            data: val,
            next: PAtomic::null(),
        }
    }
}

#[derive(Debug)]
struct NodeIter<'g, T> {
    inner: PShared<'g, Node<PAtomic<[MaybeUninit<T>]>>>,
    last: PShared<'g, Node<PAtomic<[MaybeUninit<T>]>>>,
    guard: &'g Guard,
}

#[derive(Debug)]
struct Context<K, V> {
    first_level: PAtomic<Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>>,
    last_level: PAtomic<Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>>,

    /// Should resize until the last level's size > resize_size
    ///
    /// invariant: resize_size = first_level_size / 2 / 2
    resize_size: usize,
}

impl<K, V> Collectable for Context<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

/// TODO(doc)
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ClevelInner<K, V> {
    context: PAtomic<Context<K, V>>,

    #[derivative(Debug = "ignore")]
    add_level_lock: RawMutexImpl,
}

impl<K, V> Collectable for ClevelInner<K, V> {
    fn filter(_s: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &mut PoolHandle) {
        todo!()
    }
}

impl<K, V> PDefault for ClevelInner<K, V> {
    fn pdefault(pool: &PoolHandle) -> Self {
        let guard = unsafe { unprotected() }; // SAFE for initialization

        let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
        let last_level = new_node(MIN_SIZE, pool);
        let last_level_ref = unsafe { last_level.deref(pool) };
        last_level_ref.next.store(first_level, Ordering::Relaxed);
        // persist_obj(&last_level_ref.next, true); // TODO(opt): false

        ClevelInner {
            context: PAtomic::new(
                Context {
                    first_level: first_level.into(),
                    last_level: last_level.into(),
                    resize_size: 0,
                },
                pool,
            ),
            add_level_lock: RawMutexImpl::INIT, // TODO: use our spinlock
        }
    }
}

/// Resize loop
pub fn resize_loop<K: Debug + Display + PartialEq + Hash, V: Debug, const REC: bool>(
    clevel: &ClevelInner<K, V>,
    recv: &mpsc::Receiver<()>,
    resize: &mut Resize<K, V>,
    tid: usize,
    guard: &mut Guard,
    pool: &PoolHandle,
) {
    if REC {
        clevel.resize::<REC>(resize, tid, guard, pool);
        guard.repin_after(|| {});
    }

    while let Ok(()) = recv.recv() {
        println!("[resize_loop] do resize!");
        clevel.resize::<false>(resize, tid, guard, pool);
        guard.repin_after(|| {});
    }
}

#[derive(Debug)]
struct FindResult<'g, K, V> {
    /// level's size
    size: usize,
    slot: &'g DetectableCASAtomic<Slot<K, V>>,
    slot_ptr: PShared<'g, Slot<K, V>>,
}

impl<'g, K, V> Default for FindResult<'g, K, V> {
    #[allow(deref_nullptr)]
    fn default() -> Self {
        Self {
            size: 0,
            slot: unsafe { &*ptr::null() },
            slot_ptr: PShared::null(),
        }
    }
}

impl<'g, T: Debug> Iterator for NodeIter<'g, T> {
    type Item = &'g [MaybeUninit<T>];

    fn next(&mut self) -> Option<Self::Item> {
        let pool = global_pool().unwrap(); // TODO: global pool 안쓰기
        let inner_ref = unsafe { self.inner.as_ref(pool) }?;
        self.inner = if self.inner == self.last {
            PShared::null()
        } else {
            inner_ref.next.load(Ordering::Acquire, self.guard)
        };
        Some(unsafe {
            inner_ref
                .data
                .load(Ordering::Relaxed, self.guard)
                .deref(pool)
        })
    }
}

impl<K: PartialEq + Hash, V> Context<K, V> {
    pub fn level_iter<'g>(&'g self, guard: &'g Guard) -> NodeIter<'g, Bucket<K, V>> {
        NodeIter {
            inner: self.last_level.load(Ordering::Acquire, guard),
            last: self.first_level.load(Ordering::Acquire, guard),
            guard,
        }
    }
}

impl<K: Debug + Display + PartialEq + Hash, V: Debug> Context<K, V> {
    /// `Ok`: found something (may not be unique)
    ///
    /// `Err` means contention
    fn find_fast<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<FindResult<'g, K, V>>, ()> {
        let mut found_moved = false;

        // level_iter: from last (small) to first (large)
        for array in self.level_iter(guard) {
            let size = array.len();
            for key_hash in key_hashes
                .into_iter()
                .map(|key_hash| key_hash as usize % size)
                .sorted()
                .dedup()
            {
                for slot in unsafe { array[key_hash].assume_init_ref().slots.iter() } {
                    let slot_ptr = slot.load(Ordering::Acquire, guard, pool);

                    // check 2-byte tag
                    if slot_ptr.high_tag() != key_tag as usize {
                        continue;
                    }

                    let slot_ref = some_or!(unsafe { slot_ptr.as_ref(pool) }, continue);
                    if *key != slot_ref.key {
                        continue;
                    }

                    // `tag = 1` means the slot is being moved or already moved.
                    //
                    // CAUTION: we should use another bit for tagging in Memento. The LSB is for SMO.
                    if slot_ptr.tag() == 1 {
                        found_moved = true;
                        continue;
                    }

                    return Ok(Some(FindResult {
                        size,
                        slot,
                        slot_ptr,
                    }));
                }
            }
        }

        if found_moved {
            // We cannot conclude whether we the moved item is in the hash table. On the one hand,
            // the moved item may already have been removed by another thread. On the other hand,
            // the being moved item may not yet been added again.
            Err(())
        } else {
            Ok(None)
        }
    }

    /// `Ok`: found a unique item (by deduplication)
    ///
    /// `Err` means contention
    fn find<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<Option<FindResult<'g, K, V>>, ()> {
        let mut found = tiny_vec!([_; TINY_VEC_CAPACITY]);

        // "bottom-to-top" or "last-to-first"
        for array in self.level_iter(guard) {
            let size = array.len();
            for key_hash in key_hashes
                .into_iter()
                .map(|key_hash| key_hash as usize % size)
                .sorted()
                .dedup()
            {
                for slot in unsafe { array[key_hash].assume_init_ref().slots.iter() } {
                    let slot_ptr = slot.load(Ordering::Acquire, guard, pool);

                    // check 2-byte tag
                    if slot_ptr.high_tag() != key_tag as usize {
                        continue;
                    }

                    let slot_ref = some_or!(unsafe { slot_ptr.as_ref(pool) }, continue);
                    if *key != slot_ref.key {
                        continue;
                    }

                    found.push(FindResult {
                        size,
                        slot,
                        slot_ptr,
                    });
                }
            }
        }

        // find result nearest to the top.
        // CAUTION: tag conflicts with Memento SMO.
        let last = some_or!(found.pop(), return Ok(None));
        if last.slot_ptr.tag() == 1 {
            return Err(());
        }

        // ptrs to delete.
        let mut owned_found = tiny_vec!([FindResult<'g, K, V>; TINY_VEC_CAPACITY]);
        for find_result in found.into_iter().rev() {
            if find_result.slot_ptr.tag() == 1 {
                // The item is moved.
                let slot_ptr = find_result.slot_ptr.with_tag(0);

                if last.slot_ptr == slot_ptr || owned_found.iter().any(|x| x.slot_ptr == slot_ptr) {
                    // If the moved item is found again, help moving.
                    find_result
                        .slot
                        .inner
                        .store(PShared::null().with_tag(1), Ordering::Release);
                } else {
                    // If the moved item is not found again, retry.
                    return Err(());
                }
            } else {
                owned_found.push(find_result);
            }
        }

        let mut fence = false;

        // last is the find result to return.
        // remove everything else.
        for find_result in owned_found.into_iter() {
            // caution: we need **strong** CAS to guarantee uniqueness. maybe next time...

            match find_result.slot.inner.compare_exchange(
                find_result.slot_ptr,
                PShared::null(),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => unsafe {
                    persist_obj(&find_result.slot.inner, false);
                    fence = true;
                    guard.defer_pdestroy(find_result.slot_ptr);
                },
                Err(e) => {
                    if e.current == find_result.slot_ptr.with_tag(1) {
                        // If the item is moved, retry.
                        return Err(());
                    }
                }
            }
        }

        if fence {
            sfence();
        }
        Ok(Some(last))
    }
}

fn new_node<K, V>(
    size: usize,
    pool: &PoolHandle,
) -> POwned<Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>> {
    let data = POwned::<[MaybeUninit<Bucket<K, V>>]>::init(size, pool);
    let data_ref = unsafe { data.deref(pool) };
    unsafe {
        let _ = libc::memset(
            data_ref as *const _ as *mut c_void,
            0x0,
            size * std::mem::size_of::<Bucket<K, V>>(),
        );
    }
    // TODO: persist_obj(&data_ref, true);

    // TODO: pallocation maybeuninit 잘 동작하나?
    POwned::new(Node::from(PAtomic::from(data)), pool)
    // TODO: persist_obj(&above, true);
}

impl<K, V> Drop for ClevelInner<K, V> {
    fn drop(&mut self) {
        let pool = global_pool().unwrap(); // TODO: global pool 안쓰기?
        let guard = unsafe { unprotected() };
        let context = self.context.load(Ordering::Relaxed, guard);
        let context_ref = unsafe { context.deref(pool) };

        let mut node = context_ref.last_level.load(Ordering::Relaxed, guard);
        while let Some(node_ref) = unsafe { node.as_ref(pool) } {
            let next = node_ref.next.load(Ordering::Relaxed, guard);
            let data = unsafe { node_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
            for bucket in data.iter() {
                for slot in unsafe { bucket.assume_init_ref().slots.iter() } {
                    let slot_ptr = slot.load(Ordering::Relaxed, guard, pool);
                    if !slot_ptr.is_null() {
                        unsafe {
                            guard.defer_pdestroy(slot_ptr);
                        }
                    }
                }
            }
            unsafe {
                guard.defer_pdestroy(node);
            }
            node = next;
        }
    }
}

#[derive(Debug, Clone)]
pub enum InsertError {
    Occupied,
}

impl<K: Debug + Display + PartialEq + Hash, V: Debug> ClevelInner<K, V> {
    // pub fn new(pool: &PoolHandle) -> (Self, ClevelResize<K, V>) {
    //     let guard = unsafe { unprotected() };

    //     let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
    //     let last_level = new_node(MIN_SIZE, pool);
    //     let last_level_ref = unsafe { last_level.deref(pool) };
    //     last_level_ref.next.store(first_level, Ordering::Relaxed);
    //     let inner = Arc::new(ClevelInner {
    //         context: PAtomic::new(
    //             Context {
    //                 first_level: first_level.into(),
    //                 last_level: last_level.into(),
    //                 resize_size: 0,
    //             },
    //             pool,
    //         ),
    //         add_level_lock: RawMutexImpl::INIT,
    //     });
    //     let (resize_send, resize_recv) = mpsc::channel();
    //     (
    //         Self {
    //             inner: inner.clone(),
    //             resize_send,
    //         },
    //         ClevelResize { inner, resize_recv },
    //     )
    // }

    pub fn get_capacity(&self, guard: &Guard, pool: &PoolHandle) -> usize {
        let context = self.context.load(Ordering::Acquire, guard);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);
        let first_level = context_ref.first_level.load(Ordering::Relaxed, guard);

        let first_level_data = unsafe {
            first_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
        };
        let last_level_data = unsafe {
            last_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
        };

        (first_level_data.len() * 2 - last_level_data.len()) * SLOTS_IN_BUCKET
    }

    fn add_level<'g>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>,
        first_level: &'g Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, bool) {
        let first_level_data =
            unsafe { first_level.data.load(Ordering::Relaxed, guard).deref(pool) };
        let next_level_size = level_size_next(first_level_data.len());

        // insert a new level to the next of the first level.
        let next_level = first_level.next.load(Ordering::Acquire, guard);
        let next_level = if !next_level.is_null() {
            next_level
        } else {
            self.add_level_lock.lock(); // TODO(must): volatile lock을 쓴다해도 여기에 도달했을 때 다시 lock을 얻어야 함
            let next_level = first_level.next.load(Ordering::Acquire, guard);
            let next_level = if !next_level.is_null() {
                next_level
            } else {
                let next_node = new_node(next_level_size, pool);
                // TODO(must): CAS 후 persist가 되어야 함 (general CAS까진 쓸 거 없이 이쪽+윗쪽에서만 하면 될 듯)
                first_level
                    .next
                    .compare_exchange(
                        PShared::null(),
                        next_node,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        guard,
                    )
                    .unwrap_or_else(|err| err.current)
            };
            unsafe {
                self.add_level_lock.unlock();
            }
            next_level
        };

        // update context.
        let context_ref = unsafe { context.deref(pool) };
        let mut context_new = POwned::new(
            Context {
                first_level: PAtomic::from(next_level),
                last_level: context_ref.last_level.clone(),
                resize_size: level_size_prev(level_size_prev(next_level_size)),
            },
            pool,
        );
        loop {
            context = ok_or!(
                self.context.compare_exchange(
                    context,
                    context_new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard
                ),
                e,
                {
                    context = e.current;
                    context_new = e.new;
                    let context_ref = unsafe { e.current.deref(pool) };

                    if unsafe {
                        context_ref
                            .first_level
                            .load(Ordering::Acquire, guard)
                            .deref(pool)
                            .data
                            .load(Ordering::Relaxed, guard)
                            .deref(pool)
                    }
                    .len()
                        >= next_level_size
                    {
                        return (context, false);
                    }

                    // We thought this is unreachable but indeed reachable...
                    let context_new_ref = unsafe { context_new.deref(pool) };
                    context_new_ref.last_level.store(
                        context_ref.last_level.load(Ordering::Acquire, guard),
                        Ordering::Relaxed,
                    );
                    continue;
                }
            );

            break;
        }

        fence(Ordering::SeqCst);
        (context, true)
    }

    fn invalidate_and_move<'g, const REC: bool>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>,
        mut first_level: PShared<'g, Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>>,
        slot_slot_ptr: Option<(&DetectableCASAtomic<Slot<K, V>>, PShared<'_, Slot<K, V>>)>,
        client: &mut Resize<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (
        PShared<'g, Context<K, V>>,
        PShared<'g, Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>>,
    ) {
        let (slot, slot_ptr) = if REC {
            some_or!(
                client.delete_chk.peek(tid, pool),
                return (context, first_level)
            )
        } else {
            let s = slot_slot_ptr.unwrap();
            ok_or!(
                client.delete_chk.checkpoint::<REC>(
                    (PPtr::from(unsafe { s.0.as_pptr(pool) }), PAtomic::from(s.1),),
                    tid,
                    pool,
                ),
                e,
                e.current
            )
        };

        let slot = unsafe { slot.deref(pool) };
        let slot_ptr = slot_ptr.load(Ordering::Relaxed, guard);

        if slot
            .cas::<REC>(
                slot_ptr,
                slot_ptr.with_tag(1),
                &mut client.delete_cas,
                tid,
                guard,
                pool,
            )
            .is_err()
        {
            return (context, first_level);
        }

        if REC {
            // TODO(opt): normal run에서 timestamp 증가로 인해 `REC` 분기 안 따도 되긴 함.
            if let Some(ins_slot) = client.insert_chk.peek(tid, pool) {
                let ins_slot = unsafe { ins_slot.deref(pool) };
                if ins_slot
                    .cas::<REC>(
                        PShared::null(),
                        slot_ptr,
                        &mut client.insert_cas,
                        tid,
                        guard,
                        pool,
                    )
                    .is_ok()
                {
                    return (context, first_level);
                }
            }
        }

        // 넣을 곳 찾고 넣기
        loop {
            let context_ref = unsafe { context.deref(pool) };
            first_level = context_ref.first_level.load(Ordering::Acquire, guard);
            let first_level_ref = unsafe { first_level.deref(pool) };
            let first_level_data = unsafe {
                first_level_ref
                    .data
                    .load(Ordering::Relaxed, guard)
                    .deref(pool)
            };
            let first_level_size = first_level_data.len();

            let (key_tag, key_hashes) = hashes(&unsafe { slot_ptr.deref(pool) }.key);
            let key_hashes = key_hashes
                .into_iter()
                .map(|key_hash| key_hash as usize % first_level_size)
                .sorted()
                .dedup();
            for i in 0..SLOTS_IN_BUCKET {
                for key_hash in key_hashes.clone() {
                    let slot = unsafe {
                        first_level_data[key_hash]
                            .assume_init_ref()
                            .slots
                            .get_unchecked(i)
                    };

                    let slot_first_level = slot.load(Ordering::Acquire, guard, pool);
                    if let Some(slot) = unsafe { slot_first_level.as_ref(pool) } {
                        // 2-byte tag checking
                        if slot_first_level.high_tag() != key_tag as usize {
                            continue;
                        }

                        if slot.key != unsafe { slot_ptr.deref(pool) }.key {
                            continue;
                        }

                        return (context, first_level);
                    }

                    let _ = client.insert_chk.checkpoint::<REC>(
                        PPtr::from(unsafe { slot.as_pptr(pool) }),
                        tid,
                        pool,
                    );

                    if slot
                        .cas::<REC>(
                            PShared::null(),
                            slot_ptr,
                            &mut client.insert_cas,
                            tid,
                            guard,
                            pool,
                        )
                        .is_ok()
                    {
                        return (context, first_level);
                    }
                }
            }

            // The first level is full. Resize and retry.
            let (context_new, _) = self.add_level(context, first_level_ref, guard, pool);
            context = context_new;
        }
    }

    pub fn resize<const REC: bool>(
        &self,
        client: &mut Resize<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let mut context = self.context.load(Ordering::Acquire, guard);
        let mut context_ref = unsafe { context.deref(pool) };
        let mut first_level = context_ref.first_level.load(Ordering::Acquire, guard);

        if REC {
            let res = self.invalidate_and_move::<REC>(
                context,
                first_level,
                None,
                client,
                tid,
                guard,
                pool,
            );

            context = res.0;
            first_level = res.1;
        }

        loop {
            context_ref = unsafe { context.deref(pool) };
            let last_level = context_ref.last_level.load(Ordering::Acquire, guard);
            let last_level_ref = unsafe { last_level.deref(pool) };
            let last_level_data = unsafe {
                last_level_ref
                    .data
                    .load(Ordering::Relaxed, guard)
                    .deref(pool)
            };
            let last_level_size = last_level_data.len();

            // if we don't need to resize, break out.
            if context_ref.resize_size < last_level_size {
                break;
            }

            for (_bid, bucket) in last_level_data.iter().enumerate() {
                for (_sid, slot) in unsafe { bucket.assume_init_ref().slots.iter().enumerate() } {
                    let mut slot_ptr = slot.load(Ordering::Acquire, guard, pool);
                    loop {
                        if slot_ptr.is_null() {
                            break;
                        }

                        // tagged with 1 by concurrent move_if_resized(). we should wait for the item to be moved before changing context.
                        // example: insert || lookup (1); lookup (2), maybe lookup (1) can see the insert while lookup (2) doesn't.
                        // TODO: should we do it...?
                        if slot_ptr.tag() == 1 {
                            slot_ptr = slot.load(Ordering::Acquire, guard, pool);
                            continue;
                        }

                        let res = self.invalidate_and_move::<false>(
                            context,
                            first_level,
                            Some((slot, slot_ptr)),
                            client,
                            tid,
                            guard,
                            pool,
                        );

                        context = res.0;
                        first_level = res.1;
                        break;
                    }
                }
            }

            context_ref = unsafe { context.deref(pool) };
            let next_level = last_level_ref.next.load(Ordering::Acquire, guard);
            let mut context_new = POwned::new(
                Context {
                    first_level: first_level.into(),
                    last_level: next_level.into(),
                    resize_size: context_ref.resize_size,
                },
                pool,
            );

            loop {
                context = ok_or!(
                    self.context.compare_exchange(
                        context,
                        context_new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        guard
                    ),
                    e,
                    {
                        context = e.current;
                        context_new = e.new;
                        let context_ref = unsafe { e.current.deref(pool) };
                        let context_new_ref = unsafe { context_new.deref_mut(pool) };
                        context_new_ref.first_level.store(
                            context_ref.first_level.load(Ordering::Acquire, guard),
                            Ordering::Relaxed,
                        );
                        context_new_ref.resize_size =
                            cmp::max(context_new_ref.resize_size, context_ref.resize_size);
                        continue;
                    }
                );

                unsafe {
                    guard.defer_pdestroy(last_level);
                }
                break;
            }
        }
    }

    pub fn is_resizing<'g>(&self, guard: &'g Guard, pool: &PoolHandle) -> bool {
        let context = self.context.load(Ordering::Acquire, guard);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);

        (unsafe {
            last_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
                .len()
        }) <= context_ref.resize_size
    }

    fn find_fast<'g>(
        &self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find_fast(key, key_tag, key_hashes, guard, pool);
            let find_result = ok_or!(find_result, {
                context = self.context.load(Ordering::Acquire, guard);
                continue;
            });
            let find_result = some_or!(find_result, {
                let context_new = self.context.load(Ordering::Acquire, guard);

                // However, a rare case for missing is: after a search operation starts, other
                // threads add a new level through expansion and rehashing threads move the item
                // that matches the key of the search to the new level. To fix this missing, clevel
                // hashing leverages the atomicity of context.  Specifically, when no matched item
                // is found after b2t search, clevel hashing checks the global context pointer with
                // the previous local copy. If the two pointers are different, redo the search.
                //
                // our algorithm
                // - resize doesn't remove 1-tag items.
                // - find, move_if_resized removes 1-tag items.
                if context != context_new {
                    context = context_new;
                    continue;
                }
                return (context, None);
            });
            return (context, Some(find_result));
        }
    }

    fn find<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find(key, key_tag, key_hashes, guard, pool);
            let find_result = ok_or!(find_result, {
                context = self.context.load(Ordering::Acquire, guard);
                continue;
            });
            let find_result = some_or!(find_result, {
                let context_new = self.context.load(Ordering::Acquire, guard);

                // the same possible corner case as `find_fast`
                if context != context_new {
                    context = context_new;
                    continue;
                }
                return (context, None);
            });
            return (context, Some(find_result));
        }
    }

    pub fn search<'g>(&'g self, key: &K, guard: &'g Guard, pool: &'g PoolHandle) -> Option<&'g V> {
        let (key_tag, key_hashes) = hashes(key);
        let (_, find_result) = self.find_fast(key, key_tag, key_hashes, guard, pool);
        Some(&unsafe { find_result?.slot_ptr.deref(pool) }.value)
    }

    #[inline]
    fn try_slot_insert_inner<'g, const REC: bool>(
        &self,
        slot_p: PPtr<DetectableCASAtomic<Slot<K, V>>>,
        slot_new: PShared<'g, Slot<K, V>>,
        size: usize,
        cas: &mut Cas<Slot<K, V>>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<FindResult<'g, K, V>, ()> {
        let slot = unsafe { slot_p.deref(pool) };
        let _ = slot
            .cas::<REC>(PShared::null(), slot_new, cas, tid, guard, pool)
            .map_err(|_| ())?;

        Ok(FindResult {
            size,
            slot,
            slot_ptr: slot_new,
        })
    }

    fn try_slot_insert<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot_new: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        client: &mut InsertInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<FindResult<'g, K, V>, ()> {
        if REC {
            if let Some((size, slot_p)) = client.insert_chk.peek(tid, pool) {
                let res = self.try_slot_insert_inner::<REC>(
                    slot_p,
                    slot_new,
                    size,
                    &mut client.insert_cas,
                    tid,
                    guard,
                    pool,
                );
                if res.is_ok() {
                    return res;
                }
            }
        }

        let context_ref = unsafe { context.deref(pool) };
        let mut arrays = tiny_vec!([_; TINY_VEC_CAPACITY]);
        for array in context_ref.level_iter(guard) {
            arrays.push(array);
        }

        // top-to-bottom search
        for array in arrays.into_iter().rev() {
            let size = array.len();
            if context_ref.resize_size >= size {
                break;
            }

            // i and then key_hash: for load factor... let's insert to less crowded bucket... (fuzzy)
            let key_hashes = key_hashes
                .into_iter()
                .map(|key_hash| key_hash as usize % size)
                .sorted()
                .dedup();
            for i in 0..SLOTS_IN_BUCKET {
                for key_hash in key_hashes.clone() {
                    let slot = unsafe { array[key_hash].assume_init_ref().slots.get_unchecked(i) };

                    if !slot.load(Ordering::Acquire, guard, pool).is_null() {
                        continue;
                    }

                    // Before
                    // if let Ok(slot_ptr) = slot.compare_exchange(
                    //     PShared::null(),
                    //     slot_new,
                    //     Ordering::AcqRel,
                    //     Ordering::Relaxed,
                    //     guard,
                    // ) {
                    //     return Ok(FindResult {
                    //         size,
                    //         _bucket_index: key_hash,
                    //         slot,
                    //         slot_ptr,
                    //     });
                    // }

                    // After(general)
                    let (size, slot_p) = client
                        .insert_chk
                        .checkpoint::<false>(
                            (size, PPtr::from(unsafe { slot.as_pptr(pool) })),
                            tid,
                            pool,
                        )
                        .unwrap();

                    let res = self.try_slot_insert_inner::<false>(
                        slot_p,
                        slot_new,
                        size,
                        &mut client.insert_cas,
                        tid,
                        guard,
                        pool,
                    );
                    if res.is_ok() {
                        return res;
                    }
                }
            }
        }

        Err(())
    }

    #[inline]
    fn insert_inner_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        sender: &mpsc::Sender<()>,
        client: &mut InsertInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(PShared<'g, Context<K, V>>, FindResult<'g, K, V>), PShared<'g, Context<K, V>>>
    {
        if let Ok(result) =
            self.try_slot_insert::<REC>(context, slot, key_hashes, client, tid, guard, pool)
        {
            return Ok((context, result));
        }

        // No remaining slots. Resize.
        let context_ref = unsafe { context.deref(pool) };
        let first_level = context_ref.first_level.load(Ordering::Acquire, guard);
        let first_level_ref = unsafe { first_level.deref(pool) };
        let (context_new, added) = self.add_level(context, first_level_ref, guard, pool);
        if added {
            let _ = sender.send(());
        }

        Err(context_new)
    }

    fn insert_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        sender: &mpsc::Sender<()>,
        insert_inner: &mut InsertInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        let mut res = self.insert_inner_inner::<REC>(
            context,
            slot,
            key_hashes,
            sender,
            insert_inner,
            tid,
            guard,
            pool,
        );

        while let Err(context_new) = res {
            res = self.insert_inner_inner::<false>(
                context_new,
                slot,
                key_hashes,
                sender,
                insert_inner,
                tid,
                guard,
                pool,
            );
        }

        res.unwrap()
    }

    fn insert_loop<'g, const REC: bool>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        sender: &mpsc::Sender<()>,
        move_done: &mut Checkpoint<bool>,
        tag_cas: &mut Cas<Slot<K, V>>,
        insert_inner: &mut InsertInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) {
        let (_, result) = self.insert_inner::<REC>(
            context,
            slot,
            key_hashes,
            sender,
            insert_inner,
            tid,
            guard,
            pool,
        );

        // If the inserted slot is being resized, try again.
        fence(Ordering::SeqCst);

        // If the context remains the same, it's done.
        context = self.context.load(Ordering::Acquire, guard);

        // If the inserted array is not being resized, it's done.
        let context_ref = unsafe { context.deref(pool) };

        let done = ok_or!(
            move_done.checkpoint::<REC>(context_ref.resize_size < result.size, tid, pool),
            e,
            e.current
        );
        if done {
            return;
        }

        // Move the slot if the slot is not already (being) moved.
        //
        // the resize thread may already have passed the slot. I need to move it.

        if result
            .slot
            .cas::<REC>(
                result.slot_ptr,
                result.slot_ptr.with_tag(1),
                tag_cas,
                tid,
                guard,
                pool,
            )
            .is_err()
        {
            return;
        }

        let mut prev_result = result;
        loop {
            let (_, result) = self.insert_inner::<false>(
                context,
                slot,
                key_hashes,
                sender,
                insert_inner,
                tid,
                guard,
                pool,
            );

            // TODO(must): 이거 rec에서도 해줘야 함. 안 그러면 resize가 한없이 기다릴 수 있음
            prev_result
                .slot
                .inner
                .store(PShared::null().with_tag(1), Ordering::Release);

            // If the inserted slot is being resized, try again.
            fence(Ordering::SeqCst);

            // If the context remains the same, it's done.
            context = self.context.load(Ordering::Acquire, guard);

            // If the inserted array is not being resized, it's done.
            let context_ref = unsafe { context.deref(pool) };

            let done = move_done
                .checkpoint::<false>(context_ref.resize_size < result.size, tid, pool)
                .unwrap();
            if done {
                return;
            }

            // Move the slot if the slot is not already (being) moved.
            //
            // the resize thread may already have passed the slot. I need to move it.

            if result
                .slot
                .cas::<false>(
                    result.slot_ptr,
                    result.slot_ptr.with_tag(1),
                    tag_cas,
                    tid,
                    guard,
                    pool,
                )
                .is_err()
            {
                break;
            }

            prev_result = result;
        }
    }

    pub fn insert<const REC: bool>(
        &self,
        key: K,
        value: V,
        sender: &mpsc::Sender<()>,
        client: &mut Insert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        let (key_tag, key_hashes) = hashes(&key);
        let (context, find_result) = self.find(&key, key_tag, key_hashes, guard, pool);

        let occupied = find_result.is_some();
        let occupied = ok_or!(
            client.occupied.checkpoint::<REC>(occupied, tid, pool),
            e,
            e.current
        );
        if occupied {
            // occupied is true if `find_result` is `Some`
            return Err(InsertError::Occupied);
        }

        let slot = POwned::new(Slot::from((key, value)), pool)
            .with_high_tag(key_tag as usize)
            .into_shared(guard);
        let slot = ok_or!(
            client
                .node
                .checkpoint::<REC>(PAtomic::from(slot), tid, pool),
            e,
            e.current
        )
        .load(Ordering::Relaxed, guard);

        self.insert_loop::<REC>(
            context,
            slot,
            key_hashes,
            sender,
            &mut client.move_done,
            &mut client.tag_cas,
            &mut client.insert_inner,
            tid,
            guard,
            pool,
        );
        Ok(())
    }

    // pub fn update(
    //     &self,
    //     tid: usize,
    //     key: K,
    //     value: V,
    //     sender: &mpsc::Sender<()>,
    //     guard: &Guard,
    //     pool: &PoolHandle,
    // ) -> Result<(), (K, V)>
    // where
    //     K: Clone,
    // {
    //     let (key_tag, key_hashes) = hashes(&key);
    //     let mut slot_new =
    //         POwned::new(Slot::from((key.clone(), value)), pool).with_high_tag(key_tag as usize);

    //     loop {
    //         let (context, find_result) = self.find(&key, key_tag, key_hashes, guard, pool);
    //         let find_result = some_or!(find_result, {
    //             let slot_ref = unsafe { slot_new.deref(pool) };
    //             // TODO: 이렇게 k,v 리턴하면 안됨. 그냥 update 실패 리턴값 없애자
    //             let (k, v) = (slot_ref.key.clone(), unsafe { ptr::read(&slot_ref.value) });
    //             // TODO(must): free new slot
    //             return Err((k, v));
    //         });

    //         if let Err(e) = find_result.slot.compare_exchange(
    //             find_result.slot_ptr,
    //             slot_new,
    //             Ordering::AcqRel,
    //             Ordering::Acquire,
    //             guard,
    //         ) {
    //             slot_new = e.new;
    //             continue;
    //         }

    //         unsafe {
    //             guard.defer_pdestroy(find_result.slot_ptr);
    //         }
    //         self.move_if_resized(tid, context, find_result, key_hashes, sender, guard, pool);
    //         return Ok(());
    //     }
    // }

    fn try_delete<const REC: bool>(
        &self,
        key: &K,
        try_delete: &mut TryDelete<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<bool, ()> {
        let (key_tag, key_hashes) = hashes(&key);
        let (_, find_result) = self.find(key, key_tag, key_hashes, guard, pool);

        // Before
        // let find_result = some_or!(find_result, {
        //     println!("[delete] None");
        //     return Ok(false);
        // });
        //
        // if find_result
        //     .slot
        //     .compare_exchange(
        //         find_result.slot_ptr,
        //         PShared::null(),
        //         Ordering::AcqRel,
        //         Ordering::Relaxed,
        //         guard,
        //     )
        //     .is_err()
        // {
        //     continue;
        // }
        // unsafe {
        //     guard.defer_pdestroy(find_result.slot_ptr);
        // }

        // After(general)
        let (slot, slot_ptr) = match find_result {
            Some(res) => (
                unsafe { res.slot.as_pptr(pool) },
                PAtomic::from(res.slot_ptr),
            ),
            None => (PPtr::null(), PAtomic::null()),
        };

        let chk = ok_or!(
            try_delete
                .find_result_chk
                .checkpoint::<REC>((slot, slot_ptr), tid, pool),
            e,
            e.current
        );

        if chk.0.is_null() {
            // slot is null if find result is none
            return Ok(false);
        }

        let slot = unsafe { chk.0.deref(pool) };
        let slot_ptr = chk.1.load(Ordering::Relaxed, guard);

        slot.cas::<REC>(
            slot_ptr,
            PShared::null(),
            &mut try_delete.delete_cas,
            tid,
            guard,
            pool,
        )
        .map(|_| unsafe {
            guard.defer_pdestroy(slot_ptr);
            true
        })
        .map_err(|_| ())
    }

    pub fn delete<const REC: bool>(
        &self,
        key: &K,
        delete: &mut Delete<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        if let Ok(ret) = self.try_delete::<REC>(key, &mut delete.try_delete, tid, guard, pool) {
            return ret;
        }

        loop {
            if let Ok(ret) = self.try_delete::<false>(key, &mut delete.try_delete, tid, guard, pool)
            {
                return ret;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pmem::RootObj,
        test_utils::tests::{run_test, TestRootObj},
    };

    use crossbeam_epoch::pin;

    use super::*;

    static mut SEND: Option<Vec<mpsc::Sender<()>>> = None;
    static mut RECV: Option<mpsc::Receiver<()>> = None;

    const SMOKE_CNT: usize = 100_000;

    struct Smoke {
        resize: Resize<usize, usize>,
        insert: [Insert<usize, usize>; SMOKE_CNT],
        delete: [Delete<usize, usize>; SMOKE_CNT],
    }

    impl Default for Smoke {
        fn default() -> Self {
            Self {
                resize: Default::default(),
                insert: array_init::array_init(|_| Insert::<usize, usize>::default()),
                delete: array_init::array_init(|_| Delete::<usize, usize>::default()),
            }
        }
    }

    impl Collectable for Smoke {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..SMOKE_CNT {
                // TODO(must): filter resize
                Insert::<usize, usize>::filter(&mut m.insert[i], tid, gc, pool);
                Delete::<usize, usize>::filter(&mut m.delete[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<Smoke> for TestRootObj<ClevelInner<usize, usize>> {
        fn run(&self, mmt: &mut Smoke, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let kv = &self.obj;

            match tid {
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let mut g = pin(); // TODO(must): Use guard param (use unsafe repin_after)
                    let _ = resize_loop::<_, _, true>(kv, recv, &mut mmt.resize, tid, &mut g, pool);
                }
                2 => {
                    let send = unsafe { SEND.as_mut().unwrap().pop().unwrap() };

                    for i in 0..SMOKE_CNT {
                        let _ =
                            kv.insert::<true>(i, i, &send, &mut mmt.insert[i], tid, guard, pool);
                        assert_eq!(kv.search(&i, guard, pool), Some(&i));

                        // TODO(opt): update 살리기
                        // let _ = kv.update(0, i, i + RANGE, &send, &guard, pool);
                        // assert_eq!(kv.search(&i, &guard, pool), Some(&(i + RANGE)));
                    }

                    for i in 0..SMOKE_CNT {
                        assert_eq!(kv.search(&i, guard, pool), Some(&i));
                        // TODO(opt): update 살리기
                        // assert_eq!(kv.search(&i, &guard, pool), Some(&(i + RANGE)));

                        let del_res = kv.delete::<true>(&i, &mut mmt.delete[i], tid, guard, pool);
                        assert!(del_res);
                        assert_eq!(kv.search(&i, guard, pool), None);
                    }
                }
                _ => {
                    panic!("The maximum number of thread is 2.")
                }
            }
        }
    }

    #[test]
    fn smoke() {
        const FILE_NAME: &str = "clevel_smoke.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        const NR_THREADS: usize = 2;

        let (send, recv) = mpsc::channel();
        let mut vec_s = Vec::new();
        for _ in 0..NR_THREADS - 1 {
            vec_s.push(send.clone());
        }
        drop(send);
        unsafe {
            SEND = Some(vec_s);
            RECV = Some(recv);
        }

        run_test::<TestRootObj<ClevelInner<usize, usize>>, Smoke, _>(
            FILE_NAME, FILE_SIZE, NR_THREADS,
        );
    }

    const INSERT_SEARCH_CNT: usize = 3_000;

    struct InsertSearch {
        insert: [Insert<usize, usize>; INSERT_SEARCH_CNT],
        resize: Resize<usize, usize>,
    }

    impl Default for InsertSearch {
        fn default() -> Self {
            Self {
                insert: array_init::array_init(|_| Insert::<usize, usize>::default()),
                resize: Default::default(),
            }
        }
    }

    impl Collectable for InsertSearch {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..INSERT_SEARCH_CNT {
                // TODO(must): filter resize
                Insert::<usize, usize>::filter(&mut m.insert[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsertSearch> for TestRootObj<ClevelInner<usize, usize>> {
        fn run(&self, mmt: &mut InsertSearch, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let kv = &self.obj;

            match tid {
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let mut g = pin(); // TODO(must): Use guard param (use unsafe repin_after)
                    let _ = resize_loop::<_, _, true>(kv, recv, &mut mmt.resize, tid, &mut g, pool);
                }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap().pop().unwrap() };

                    for i in 0..INSERT_SEARCH_CNT {
                        // println!("[test] tid = {tid}, i = {i}, insert");
                        let _ =
                            kv.insert::<true>(i, i, &send, &mut mmt.insert[i], tid, guard, pool);

                        // println!("[test] tid = {tid}, i = {i}, search");
                        if kv.search(&i, &guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail on {i}");
                            // assert_eq!(kv.search(&i, &guard), Some(&i));
                        }
                    }

                    for i in 0..INSERT_SEARCH_CNT {
                        // println!("[test] tid = {tid}, i = {i}, search");
                        if kv.search(&i, &guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail on {i}");
                            // assert_eq!(kv.search(&i, &guard), Some(&i));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn insert_search() {
        const FILE_NAME: &str = "clevel_insert_search.pool";
        const FILE_SIZE: usize = 16 * 1024 * 1024 * 1024;
        const NR_THREADS: usize = 1usize << 4;

        let (send, recv) = mpsc::channel();
        let mut vec_s = Vec::new();
        for _ in 0..NR_THREADS - 1 {
            vec_s.push(send.clone());
        }
        drop(send);
        unsafe {
            SEND = Some(vec_s);
            RECV = Some(recv);
        }

        run_test::<TestRootObj<ClevelInner<usize, usize>>, InsertSearch, _>(
            FILE_NAME, FILE_SIZE, NR_THREADS,
        )
    }

    // struct InsertUpdateSearch {}

    // impl Default for InsertUpdateSearch {
    //     fn default() -> Self {
    //         Self {}
    //     }
    // }

    // impl Collectable for InsertUpdateSearch {
    //     fn filter(_m: &mut Self, _tid: usize, _gc: &mut GarbageCollection, _pool: &PoolHandle) {
    //         todo!()
    //     }
    // }

    // impl RootObj<InsertUpdateSearch> for TestRootObj<ClevelInner<usize, usize>> {
    //     fn run(&self, _mmt: &mut InsertUpdateSearch, tid: usize, guard: &Guard, pool: &PoolHandle) {
    //         let kv = &self.obj;

    //         match tid {
    //             0 => {
    //                 let recv = unsafe { RECV.as_ref().unwrap() };
    //                 let mut g = pin();
    //                 let _ = resize_loop::<_, _, true>(kv, recv, &mut g, pool);
    //             }
    //             _ => {
    //                 let send = unsafe { SEND.as_mut().unwrap().pop().unwrap() };
    //                 const RANGE: usize = 1usize << 6;

    //                 for i in 0..RANGE {
    //                     // println!("[test] tid = {tid}, i = {i}, insert");
    //                     let _ = kv.insert::<true>(tid, i, i, &send, &guard, pool);

    //                     // println!("[test] tid = {tid}, i = {i}, update");
    //                     let _ = kv.update(tid, i, i + RANGE, &send, &guard, pool);

    //                     // println!("[test] tid = {tid}, i = {i}, search");
    //                     if kv.search(&i, &guard, pool) != Some(&i)
    //                         && kv.search(&i, &guard, pool) != Some(&(i + RANGE))
    //                     {
    //                         panic!("[test] tid = {tid} fail on {i}");
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    // #[test]
    // fn insert_update_search() {
    //     const FILE_NAME: &str = "clevel_insert_update_search.pool";
    //     const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
    //     const THREADS: usize = 1usize << 4;

    //     let (send, recv) = mpsc::channel();
    //     let mut vec_s = Vec::new();
    //     for _ in 0..THREADS - 1 {
    //         vec_s.push(send.clone());
    //     }
    //     drop(send);
    //     unsafe {
    //         SEND = Some(vec_s);
    //         RECV = Some(recv);
    //     }

    //     run_test::<TestRootObj<ClevelInner<usize, usize>>, InsertUpdateSearch, _>(
    //         FILE_NAME, FILE_SIZE, THREADS,
    //     )
    // }
}
