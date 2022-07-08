//! Concurrent Level Hash Table.
#![allow(missing_docs)]
#![allow(box_pointers)]
#![allow(unreachable_pub)]
#![allow(unused)]
use core::cmp;
use core::fmt::Debug;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use std::sync::mpsc;

use cfg_if::cfg_if;
use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::Backoff;
use etrace::*;
use fasthash::Murmur3HasherExt;
use itertools::*;
use lazy_static::__Deref;
use libc::c_void;
use parking_lot::{lock_api::RawMutex, RawMutex as RawMutexImpl};
use tinyvec::*;

use crate::pepoch::atomic::cut_as_high_tag_len;
use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, DetectableCASAtomic};
use crate::pmem::{global_pool, AsPPtr, Collectable, GarbageCollection, PoolHandle};
use crate::pmem::{persist_obj, PPtr};
use crate::PDefault;

use super::tlock::ThreadRecoverableSpinLock;

const TINY_VEC_CAPACITY: usize = 8;

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

/// Insert client
#[derive(Debug)]
pub struct Insert<K, V: Collectable> {
    found_slot: Checkpoint<(bool, PAtomic<Slot<K, V>>)>,
    insert_inner: InsertInner<K, V>,
    move_if_resized: MoveIfResized<K, V>,
}

impl<K, V: Collectable> Default for Insert<K, V> {
    fn default() -> Self {
        Self {
            found_slot: Default::default(),
            insert_inner: Default::default(),
            move_if_resized: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Insert<K, V> {
    fn filter(insert: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut insert.found_slot, tid, gc, pool);
        InsertInner::filter(&mut insert.insert_inner, tid, gc, pool);
        MoveIfResized::filter(&mut insert.move_if_resized, tid, gc, pool);
    }
}

impl<K, V: Collectable> Insert<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.found_slot.clear();
        self.insert_inner.clear();
        self.move_if_resized.clear();
    }
}

/// Move if resized client
#[derive(Debug)]
pub struct MoveIfResized<K, V: Collectable> {
    move_if_resized_inner: MoveIfResizedInner<K, V>,
}

impl<K, V: Collectable> Default for MoveIfResized<K, V> {
    fn default() -> Self {
        Self {
            move_if_resized_inner: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for MoveIfResized<K, V> {
    fn filter(
        move_if_resized: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        MoveIfResizedInner::filter(&mut move_if_resized.move_if_resized_inner, tid, gc, pool);
    }
}

impl<K, V: Collectable> MoveIfResized<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.move_if_resized_inner.clear();
    }
}

/// Move if resized client
#[derive(Debug)]
pub struct MoveIfResizedInner<K, V: Collectable> {
    prev_slot_chk: Checkpoint<PPtr<DetectableCASAtomic<Slot<K, V>>>>,
    context_new_chk: Checkpoint<PAtomic<Context<K, V>>>,
    slot_cas: Cas,
    insert_inner: InsertInner<K, V>,
}

impl<K, V: Collectable> Default for MoveIfResizedInner<K, V> {
    fn default() -> Self {
        Self {
            prev_slot_chk: Default::default(),
            context_new_chk: Default::default(),
            slot_cas: Default::default(),
            insert_inner: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for MoveIfResizedInner<K, V> {
    fn filter(
        move_if_resized_inner: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(&mut move_if_resized_inner.prev_slot_chk, tid, gc, pool);
        Checkpoint::filter(&mut move_if_resized_inner.context_new_chk, tid, gc, pool);
        Cas::filter(&mut move_if_resized_inner.slot_cas, tid, gc, pool);
        InsertInner::filter(&mut move_if_resized_inner.insert_inner, tid, gc, pool);
    }
}

impl<K, V: Collectable> MoveIfResizedInner<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.prev_slot_chk.clear();
        self.context_new_chk.clear();
        self.slot_cas.clear();
        self.insert_inner.clear();
    }
}

/// Insert inner client
#[derive(Debug)]
pub struct InsertInner<K, V: Collectable> {
    insert_inner_inner: InsertInnerInner<K, V>,
}

impl<K, V: Collectable> Default for InsertInner<K, V> {
    fn default() -> Self {
        Self {
            insert_inner_inner: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for InsertInner<K, V> {
    fn filter(
        insert_inner: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        InsertInnerInner::filter(&mut insert_inner.insert_inner_inner, tid, gc, pool);
    }
}

impl<K, V: Collectable> InsertInner<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.insert_inner_inner.clear();
    }
}

/// Insert inner inner client
#[derive(Debug)]
pub struct InsertInnerInner<K, V: Collectable> {
    try_slot_insert: TrySlotInsert<K, V>,
    add_lv: AddLevel<K, V>,
}

impl<K, V: Collectable> Default for InsertInnerInner<K, V> {
    fn default() -> Self {
        Self {
            try_slot_insert: Default::default(),
            add_lv: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for InsertInnerInner<K, V> {
    fn filter(
        insert_inner_inner: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        TrySlotInsert::filter(&mut insert_inner_inner.try_slot_insert, tid, gc, pool);
        AddLevel::filter(&mut insert_inner_inner.add_lv, tid, gc, pool);
    }
}

impl<K, V: Collectable> InsertInnerInner<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.try_slot_insert.clear();
        self.add_lv.clear();
    }
}

/// Try slot insert client
#[derive(Debug)]
pub struct TrySlotInsert<K, V: Collectable> {
    slot_chk: Checkpoint<(usize, PPtr<PAtomic<Slot<K, V>>>)>,
    slot_cas: Cas,
}

impl<K, V: Collectable> Default for TrySlotInsert<K, V> {
    fn default() -> Self {
        Self {
            slot_chk: Default::default(),
            slot_cas: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for TrySlotInsert<K, V> {
    fn filter(
        try_slot_insert: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(&mut try_slot_insert.slot_chk, tid, gc, pool);
        Cas::filter(&mut try_slot_insert.slot_cas, tid, gc, pool);
    }
}

impl<K, V: Collectable> TrySlotInsert<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.slot_chk.clear();
        self.slot_cas.clear();
    }
}

/// Add level client
#[derive(Debug)]
pub struct AddLevel<K, V: Collectable> {
    next_level: NextLevel<K, V>,
    context_chk: Checkpoint<PAtomic<Context<K, V>>>,
    context_cas: Cas,
}

impl<K, V: Collectable> Default for AddLevel<K, V> {
    fn default() -> Self {
        Self {
            next_level: Default::default(),
            context_chk: Default::default(),
            context_cas: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for AddLevel<K, V> {
    fn filter(add_lv: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        NextLevel::filter(&mut add_lv.next_level, tid, gc, pool);
        Checkpoint::filter(&mut add_lv.context_chk, tid, gc, pool);
        Cas::filter(&mut add_lv.context_cas, tid, gc, pool);
    }
}

impl<K, V: Collectable> AddLevel<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.next_level.clear();
        self.context_chk.clear();
        self.context_cas.clear();
    }
}

/// Next level client
#[derive(Debug)]
pub struct NextLevel<K, V: Collectable> {
    next_node_chk: Checkpoint<PAtomic<Node<Bucket<K, V>>>>,
    next_cas: Cas,
}

impl<K, V: Collectable> Default for NextLevel<K, V> {
    fn default() -> Self {
        Self {
            next_node_chk: Default::default(),
            next_cas: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for NextLevel<K, V> {
    fn filter(add_lv: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Checkpoint::filter(&mut add_lv.next_node_chk, tid, gc, pool);
        Cas::filter(&mut add_lv.next_cas, tid, gc, pool);
    }
}

impl<K, V: Collectable> NextLevel<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.next_node_chk.clear();
        self.next_cas.clear();
    }
}

/// Delete client
#[derive(Debug)]
pub struct TryDelete<K, V: Collectable> {
    slot_cas: Cas,
    find_result_chk: Checkpoint<(PPtr<DetectableCASAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
}

impl<K, V: Collectable> Default for TryDelete<K, V> {
    fn default() -> Self {
        Self {
            slot_cas: Default::default(),
            find_result_chk: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for TryDelete<K, V> {
    fn filter(
        try_delete: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Cas::filter(&mut try_delete.slot_cas, tid, gc, pool);
        Checkpoint::filter(&mut try_delete.find_result_chk, tid, gc, pool);
    }
}

impl<K, V: Collectable> TryDelete<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.slot_cas.clear();
        self.find_result_chk.clear();
    }
}

/// Delete client
#[derive(Debug)]
pub struct Delete<K, V: Collectable> {
    try_delete: TryDelete<K, V>,
}

impl<K, V: Collectable> Default for Delete<K, V> {
    fn default() -> Self {
        Self {
            try_delete: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Delete<K, V> {
    fn filter(delete: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        TryDelete::filter(&mut delete.try_delete, tid, gc, pool);
    }
}

impl<K, V: Collectable> Delete<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.try_delete.clear();
    }
}

/// ResizeLoop client
#[derive(Debug)]
pub struct ResizeLoop<K, V: Collectable> {
    recv_chk: Checkpoint<bool>,
    resize: Resize<K, V>,
}

impl<K, V: Collectable> Default for ResizeLoop<K, V> {
    fn default() -> Self {
        Self {
            recv_chk: Default::default(),
            resize: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeLoop<K, V> {
    fn filter(
        resize_loop: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(&mut resize_loop.recv_chk, tid, gc, pool);
        Resize::filter(&mut resize_loop.resize, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeLoop<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.recv_chk.clear();
        self.resize.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct Resize<K, V: Collectable> {
    resize_inner: ResizeInner<K, V>,
}

impl<K, V: Collectable> Default for Resize<K, V> {
    fn default() -> Self {
        Self {
            resize_inner: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for Resize<K, V> {
    fn filter(resize: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        ResizeInner::filter(&mut resize.resize_inner, tid, gc, pool);
    }
}

impl<K, V: Collectable> Resize<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.resize_inner.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct ResizeInner<K, V: Collectable> {
    context_chk: Checkpoint<PAtomic<Context<K, V>>>,
    context_cas: Cas,
    resize_clean: ResizeClean<K, V>,
}

impl<K, V: Collectable> Default for ResizeInner<K, V> {
    fn default() -> Self {
        Self {
            context_chk: Default::default(),
            context_cas: Default::default(),
            resize_clean: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeInner<K, V> {
    fn filter(
        resize_inner: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(&mut resize_inner.context_chk, tid, gc, pool);
        Cas::filter(&mut resize_inner.context_cas, tid, gc, pool);
        ResizeClean::filter(&mut resize_inner.resize_clean, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeInner<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.context_chk.clear();
        self.context_cas.clear();
        self.resize_clean.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct ResizeClean<K, V: Collectable> {
    slot_slot_ptr_chk: Checkpoint<(PPtr<PAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
    slot_cas: Cas,
    resize_move: ResizeMove<K, V>,
}

impl<K, V: Collectable> Default for ResizeClean<K, V> {
    fn default() -> Self {
        Self {
            slot_slot_ptr_chk: Default::default(),
            slot_cas: Default::default(),
            resize_move: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeClean<K, V> {
    fn filter(
        resize_clean: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(&mut resize_clean.slot_slot_ptr_chk, tid, gc, pool);
        Cas::filter(&mut resize_clean.slot_cas, tid, gc, pool);
        ResizeMove::filter(&mut resize_clean.resize_move, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeClean<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.slot_slot_ptr_chk.clear();
        self.slot_cas.clear();
        self.resize_move.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct ResizeMove<K, V: Collectable> {
    resize_move_inner: ResizeMoveInner<K, V>,
}

impl<K, V: Collectable> Default for ResizeMove<K, V> {
    fn default() -> Self {
        Self {
            resize_move_inner: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeMove<K, V> {
    fn filter(
        resize_move: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        ResizeMoveInner::filter(&mut resize_move.resize_move_inner, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeMove<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.resize_move_inner.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct ResizeMoveInner<K, V: Collectable> {
    resize_move_slot_insert: ResizeMoveSlotInsert<K, V>,
    add_lv: AddLevel<K, V>,
}

impl<K, V: Collectable> Default for ResizeMoveInner<K, V> {
    fn default() -> Self {
        Self {
            resize_move_slot_insert: Default::default(),
            add_lv: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeMoveInner<K, V> {
    fn filter(
        resize_move_inner: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        ResizeMoveSlotInsert::filter(
            &mut resize_move_inner.resize_move_slot_insert,
            tid,
            gc,
            pool,
        );
        AddLevel::filter(&mut resize_move_inner.add_lv, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeMoveInner<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.resize_move_slot_insert.clear();
        self.add_lv.clear();
    }
}

/// Resize client
#[derive(Debug)]
pub struct ResizeMoveSlotInsert<K, V: Collectable> {
    slot_slot_first_chk: Checkpoint<(PPtr<PAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
    slot_cas: Cas,
}

impl<K, V: Collectable> Default for ResizeMoveSlotInsert<K, V> {
    fn default() -> Self {
        Self {
            slot_slot_first_chk: Default::default(),
            slot_cas: Default::default(),
        }
    }
}

impl<K, V: Collectable> Collectable for ResizeMoveSlotInsert<K, V> {
    fn filter(
        resize_move_slot_insert: &mut Self,
        tid: usize,
        gc: &mut GarbageCollection,
        pool: &mut PoolHandle,
    ) {
        Checkpoint::filter(
            &mut resize_move_slot_insert.slot_slot_first_chk,
            tid,
            gc,
            pool,
        );
        Cas::filter(&mut resize_move_slot_insert.slot_cas, tid, gc, pool);
    }
}

impl<K, V: Collectable> ResizeMoveSlotInsert<K, V> {
    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.slot_slot_first_chk.clear();
        self.slot_cas.clear();
    }
}

#[derive(Debug, Default)]
struct Slot<K, V: Collectable> {
    key: K,
    value: V,
}

impl<K, V: Collectable> Collectable for Slot<K, V> {
    fn filter(slot: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        V::filter(&mut slot.value, tid, gc, pool);
    }
}

#[derive(Debug)]
#[repr(align(64))]
struct Bucket<K, V: Collectable> {
    slots: [DetectableCASAtomic<Slot<K, V>>; SLOTS_IN_BUCKET],
}

impl<K, V: Collectable> Collectable for Bucket<K, V> {
    fn filter(bucket: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        for slot in bucket.slots.iter_mut() {
            DetectableCASAtomic::filter(slot, tid, gc, pool);
        }
    }
}

#[derive(Debug)]
struct Node<T: Collectable> {
    data: PAtomic<[MaybeUninit<T>]>,
    next: DetectableCASAtomic<Self>,
}

impl<T: Collectable> From<PAtomic<[MaybeUninit<T>]>> for Node<T> {
    fn from(data: PAtomic<[MaybeUninit<T>]>) -> Self {
        Self {
            data,
            next: DetectableCASAtomic::default(),
        }
    }
}

impl<T: Collectable> Collectable for Node<T> {
    fn filter(node: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let guard = unsafe { epoch::unprotected() };

        let mut data = node.data.load(Ordering::SeqCst, guard);
        let data_ref = unsafe { data.deref_mut(pool) };
        for b in data_ref.iter_mut() {
            MaybeUninit::<T>::mark(b, tid, gc);
        }

        DetectableCASAtomic::filter(&mut node.next, tid, gc, pool);
    }
}

#[derive(Debug)]
struct NodeIter<'g, T: Collectable> {
    inner: PShared<'g, Node<T>>,
    last: PShared<'g, Node<T>>,
    guard: &'g Guard,
}

impl<'g, T: Debug + Collectable> Iterator for NodeIter<'g, T> {
    type Item = &'g [MaybeUninit<T>];

    fn next(&mut self) -> Option<Self::Item> {
        let pool = global_pool().unwrap();
        let inner_ref = unsafe { self.inner.as_ref(pool) }?;
        self.inner = if self.inner == self.last {
            PShared::null()
        } else {
            inner_ref.next.load(Ordering::Acquire, self.guard, pool)
        };
        Some(unsafe {
            inner_ref
                .data
                .load(Ordering::Relaxed, self.guard)
                .deref(pool)
        })
    }
}

#[derive(Debug)]
struct Context<K, V: Collectable> {
    first_level: PAtomic<Node<Bucket<K, V>>>,
    last_level: PAtomic<Node<Bucket<K, V>>>,

    /// Should resize until the last level's size > resize_size
    ///
    /// invariant: resize_size = first_level_size / 2 / 2
    resize_size: usize,
}

impl<K, V: Collectable> Collectable for Context<K, V> {
    fn filter(context: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut context.last_level, tid, gc, pool);
    }
}

#[derive(Debug)]
pub struct Clevel<K, V: Collectable> {
    context: DetectableCASAtomic<Context<K, V>>,
    add_level_lock: ThreadRecoverableSpinLock,
}

impl<K, V: Collectable> Collectable for Clevel<K, V> {
    fn filter(clevel: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut clevel.context, tid, gc, pool);
    }
}

impl<K, V: Collectable> PDefault for Clevel<K, V> {
    fn pdefault(pool: &PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() }; // SAFE when initialization

        let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
        let last_level = new_node(MIN_SIZE, pool);
        let last_level_ref = unsafe { last_level.deref(pool) };
        last_level_ref
            .next
            .inner
            .store(first_level, Ordering::Relaxed);
        persist_obj(&last_level_ref.next, true);

        let context = alloc_persist(
            Context {
                first_level: first_level.into(),
                last_level: last_level.into(),
                resize_size: 0,
            },
            pool,
        )
        .into_shared(guard);

        Clevel {
            context: DetectableCASAtomic::from(context),
            add_level_lock: ThreadRecoverableSpinLock::default(),
        }
    }
}

#[derive(Debug)]
struct FindResult<'g, K, V: Collectable> {
    /// level's size
    size: usize,
    slot: &'g DetectableCASAtomic<Slot<K, V>>,
    slot_ptr: PShared<'g, Slot<K, V>>,
}

impl<'g, K, V: Collectable> Default for FindResult<'g, K, V> {
    #[allow(deref_nullptr)]
    fn default() -> Self {
        Self {
            size: 0,
            slot: unsafe { &*ptr::null() },
            slot_ptr: PShared::null(),
        }
    }
}

impl<K: PartialEq + Hash, V: Collectable> Context<K, V> {
    pub fn level_iter<'g>(&'g self, guard: &'g Guard) -> NodeIter<'g, Bucket<K, V>> {
        NodeIter {
            inner: self.last_level.load(Ordering::Acquire, guard),
            last: self.first_level.load(Ordering::Acquire, guard),
            guard,
        }
    }
}

impl<K: Debug + PartialEq + Hash, V: Debug + Collectable> Context<K, V> {
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
                        .store(PShared::null().with_tag(1), Ordering::Release); // exploit invariant
                } else {
                    // If the moved item is not found again, retry.
                    return Err(());
                }
            } else {
                owned_found.push(find_result);
            }
        }

        // last is the find result to return.
        // remove everything else.
        for find_result in owned_found.into_iter() {
            // caution: we need **strong** CAS to guarantee uniqueness. maybe next time...
            // exploit invariant
            match find_result.slot.inner.compare_exchange(
                find_result.slot_ptr,
                PShared::null(),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => unsafe {
                    guard.defer_pdestroy(find_result.slot_ptr);
                },
                Err(e) => {
                    // exploit invariant
                    if e.current.with_tid(0) == find_result.slot_ptr.with_tag(1) {
                        // If the item is moved, retry.
                        return Err(());
                    }
                }
            }
        }

        Ok(Some(last))
    }
}

fn new_node<K, V: Collectable>(size: usize, pool: &PoolHandle) -> POwned<Node<Bucket<K, V>>> {
    let data = POwned::<[MaybeUninit<Bucket<K, V>>]>::init(size, &pool);
    let data_ref = unsafe { data.deref(pool) };
    unsafe {
        let _ = libc::memset(
            data_ref as *const _ as *mut c_void,
            0x0,
            size * std::mem::size_of::<Bucket<K, V>>(),
        );
    }
    persist_obj(&data_ref, true);

    alloc_persist(Node::from(PAtomic::from(data)), pool)
}

impl<K, V: Collectable> Drop for Clevel<K, V> {
    fn drop(&mut self) {
        let pool = global_pool().unwrap();
        let guard = unsafe { epoch::unprotected() };
        let context = self.context.load(Ordering::Relaxed, guard, pool);
        let context_ref = unsafe { context.deref(pool) };

        let mut node = context_ref.last_level.load(Ordering::Relaxed, guard);
        while let Some(node_ref) = unsafe { node.as_ref(pool) } {
            let next = node_ref.next.load(Ordering::Relaxed, guard, pool);
            let data = unsafe { node_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
            for bucket in data.iter() {
                for slot in unsafe { bucket.assume_init_ref().slots.iter() } {
                    let slot_ptr = slot.load(Ordering::Relaxed, guard, pool);
                    if !slot_ptr.is_null() {
                        unsafe { guard.defer_pdestroy(slot_ptr) };
                    }
                }
            }
            unsafe { guard.defer_pdestroy(node) };
            node = next;
        }
    }
}

impl<K: Debug + PartialEq + Hash, V: Debug + Collectable> Clevel<K, V> {
    fn next_level<'g, const REC: bool>(
        &self,
        first_level: &Node<Bucket<K, V>>,
        next_level_size: usize,
        mmt: &mut NextLevel<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<PShared<'g, Node<Bucket<K, V>>>, ()> {
        let next_level = first_level.next.load(Ordering::Acquire, guard, pool);
        // TODO: checkpoint next_level
        if !next_level.is_null() {
            return Ok(next_level);
        }

        if let Ok(_g) = self.add_level_lock.try_lock::<REC>(tid) {
            let next_node = new_node(next_level_size, pool).into_shared(guard);
            // TODO: checkpoint next_node
            let res = first_level.next.cas::<REC>(
                PShared::null(),
                next_node,
                &mut mmt.next_cas,
                tid,
                guard,
                pool,
            );
            if let Err(e) = res {
                unsafe { guard.defer_pdestroy(next_node) };
                return Ok(e);
            }

            return Ok(next_node);
        }

        Err(())
    }

    fn add_level<'g, const REC: bool>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>,
        first_level: &'g Node<Bucket<K, V>>, // must be stable
        add_lv: &mut AddLevel<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, bool) {
        let first_level_data =
            unsafe { first_level.data.load(Ordering::Relaxed, guard).deref(pool) };
        let next_level_size = level_size_next(first_level_data.len());

        // insert a new level to the next of the first level.
        let backoff = Backoff::default();
        let next_level = if let Ok(n) = self.next_level::<REC>(
            first_level,
            next_level_size,
            &mut add_lv.next_level,
            tid,
            guard,
            pool,
        ) {
            n
        } else {
            loop {
                backoff.snooze();
                if let Ok(n) = self.next_level::<false>(
                    first_level,
                    next_level_size,
                    &mut add_lv.next_level,
                    tid,
                    guard,
                    pool,
                ) {
                    break n;
                }
            }
        };

        // update context.
        let context_ref = unsafe { context.deref(pool) };
        // TODO: checkpoint context_new
        let mut context_new = alloc_persist(
            Context {
                first_level: PAtomic::from(next_level),
                last_level: context_ref.last_level.clone(),
                resize_size: level_size_prev(level_size_prev(next_level_size)),
            },
            pool,
        )
        .into_shared(guard);
        let context_new_ref = unsafe { context_new.deref(pool) };

        let mut res = self.context.cas::<REC>(
            context,
            context_new,
            &mut add_lv.context_cas,
            tid,
            guard,
            pool,
        );

        while let Err(e) = res {
            context = e;
            let context_ref = unsafe { e.deref(pool) };

            // TODO: checkpoint len
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

            context_new_ref.last_level.store(
                context_ref.last_level.load(Ordering::Acquire, guard),
                Ordering::Relaxed,
            ); // Exploit invariant

            res = self.context.cas::<false>(
                context,
                context_new,
                &mut add_lv.context_cas,
                tid,
                guard,
                pool,
            );
        }

        fence(Ordering::SeqCst);
        (context_new, true)
    }

    fn resize_move_slot_insert<const REC: bool>(
        &self,
        slot_ptr: PShared<'_, Slot<K, V>>, // must be stable
        key_tag: u16,                      // must be stable
        key_hashes: [u32; 2],              // must be stable
        first_level_ref: &Node<Bucket<K, V>>,
        resize_move_slot_insert: &mut ResizeMoveSlotInsert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), ()> {
        // TODO: if REC checkpoint Some(slot, slot_first_level) cas

        let mut first_level_data = unsafe {
            first_level_ref
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
        };
        let first_level_size = first_level_data.len();
        let key_hashes = key_hashes
            .into_iter()
            .map(|key_hash| key_hash as usize % first_level_size)
            .sorted()
            .dedup();
        for i in 0..SLOTS_IN_BUCKET {
            for key_hash in key_hashes.clone() {
                // TODO: checkpoint Some(slot, slot_first_level)
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

                    return Ok(());
                }

                if slot
                    .cas::<false>(
                        PShared::null(),
                        slot_ptr,
                        &mut resize_move_slot_insert.slot_cas,
                        tid,
                        guard,
                        pool,
                    )
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }

        // TODO: checkpoint None
        Err(())
    }

    fn resize_move_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot_ptr: PShared<'_, Slot<K, V>>, // must be stable
        key_tag: u16,                      // must be stable
        key_hashes: [u32; 2],              // must be stable
        first_level_ref: &'g Node<Bucket<K, V>>,
        resize_move_inner: &mut ResizeMoveInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<&'g Node<Bucket<K, V>>, (PShared<'g, Context<K, V>>, &'g Node<Bucket<K, V>>)> {
        if self
            .resize_move_slot_insert::<REC>(
                slot_ptr,
                key_tag,
                key_hashes,
                first_level_ref,
                &mut resize_move_inner.resize_move_slot_insert,
                tid,
                guard,
                pool,
            )
            .is_ok()
        {
            return Ok(first_level_ref);
        }

        // The first level is full. Resize and retry.
        let (context_new, _) = self.add_level::<REC>(
            context,
            first_level_ref,
            &mut resize_move_inner.add_lv,
            tid,
            guard,
            pool,
        );
        let ctx = context_new;
        let ctx_ref = unsafe { ctx.deref(pool) };
        let fst_lv = ctx_ref.first_level.load(Ordering::Acquire, guard);
        let fst_lv_ref = unsafe { fst_lv.deref(pool) };
        Err((ctx, fst_lv_ref))
    }

    fn resize_move<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot_ptr: PShared<'_, Slot<K, V>>,
        first_level_ref: &'g Node<Bucket<K, V>>,
        resize_move: &mut ResizeMove<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> &'g Node<Bucket<K, V>> {
        let (key_tag, key_hashes) = hashes(&unsafe { slot_ptr.deref(pool) }.key);

        let mut res = self.resize_move_inner::<REC>(
            context,
            slot_ptr,
            key_tag,
            key_hashes,
            first_level_ref,
            &mut resize_move.resize_move_inner,
            tid,
            guard,
            pool,
        );

        while let Err((ctx, fst_lv_ref)) = res {
            res = self.resize_move_inner::<false>(
                ctx,
                slot_ptr,
                key_tag,
                key_hashes,
                fst_lv_ref,
                &mut resize_move.resize_move_inner,
                tid,
                guard,
                pool,
            );
        }

        res.unwrap()
    }

    fn resize_clean<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        mut first_level_ref: &'g Node<Bucket<K, V>>,
        last_level_data: &'g [MaybeUninit<Bucket<K, V>>], // must be stable
        resize_clean: &mut ResizeClean<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) {
        // TODO: if REC checkpoint (slot, slot_ptr) cas resize_move

        for (_, bucket) in last_level_data.iter().enumerate() {
            for (_, slot) in unsafe { bucket.assume_init_ref().slots.iter().enumerate() } {
                let slot_ptr = some_or!(
                    {
                        let mut slot_ptr = slot.load(Ordering::Acquire, guard, pool);
                        loop {
                            if slot_ptr.is_null() {
                                break None;
                            }

                            // tagged with 1 by concurrent move_if_resized(). we should wait for the item to be moved before changing context.
                            // example: insert || lookup (1); lookup (2), maybe lookup (1) can see the insert while lookup (2) doesn't.
                            if slot_ptr.tag() == 1 {
                                slot_ptr = slot.load(Ordering::Acquire, guard, pool);
                                continue;
                            }

                            // TODO: checkpoint (slot, slot_ptr)
                            if let Err(e) = slot.cas::<false>(
                                slot_ptr,
                                slot_ptr.with_tag(1),
                                &mut resize_clean.slot_cas,
                                tid,
                                guard,
                                pool,
                            ) {
                                slot_ptr = e;
                                continue;
                            }

                            break Some(slot_ptr);
                        }
                    },
                    continue
                );

                first_level_ref = self.resize_move::<false>(
                    context,
                    slot_ptr,
                    first_level_ref,
                    &mut resize_clean.resize_move,
                    tid,
                    guard,
                    pool,
                );
            }
        }
    }

    fn resize_inner<'g, const REC: bool>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>, // must be stable
        resize_inner: &mut ResizeInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(), PShared<'g, Context<K, V>>> {
        let mut context_ref = unsafe { context.deref(pool) };

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
            return Ok(());
        }

        let mut first_level = context_ref.first_level.load(Ordering::Acquire, guard);
        let mut first_level_ref = unsafe { first_level.deref(pool) };

        self.resize_clean::<REC>(
            context,
            first_level_ref,
            last_level_data,
            &mut resize_inner.resize_clean,
            tid,
            guard,
            pool,
        );

        let next_level = last_level_ref.next.load(Ordering::Acquire, guard, pool);
        // TODO: checkpoint context_new
        let mut context_new = alloc_persist(
            Context {
                first_level: first_level.into(),
                last_level: next_level.into(),
                resize_size: context_ref.resize_size,
            },
            pool,
        )
        .into_shared(guard);
        let context_new_ref = unsafe { context_new.deref_mut(pool) };

        let mut res = self.context.cas::<REC>(
            context,
            context_new,
            &mut resize_inner.context_cas,
            tid,
            guard,
            pool,
        );

        while let Err(e) = res {
            context = e;
            let context_ref = unsafe { e.deref(pool) };
            context_new_ref.first_level.store(
                context_ref.first_level.load(Ordering::Acquire, guard),
                Ordering::Relaxed,
            );
            context_new_ref.resize_size =
                cmp::max(context_new_ref.resize_size, context_ref.resize_size);

            res = self.context.cas::<false>(
                context,
                context_new,
                &mut resize_inner.context_cas,
                tid,
                guard,
                pool,
            );
        }

        unsafe { guard.defer_pdestroy(last_level) };
        return Err(context_new);
    }

    fn resize<const REC: bool>(
        &self,
        resize: &mut Resize<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) {
        let context = self.context.load(Ordering::Acquire, guard, pool);
        // TODO: checkpoint context
        let mut res = self.resize_inner::<REC>(context, &mut resize.resize_inner, tid, guard, pool);
        while let Err(e) = res {
            res = self.resize_inner::<false>(e, &mut resize.resize_inner, tid, guard, pool);
        }
    }

    pub fn resize_loop<const REC: bool>(
        &self,
        resize_recv: &mpsc::Receiver<()>,
        resize_loop: &mut ResizeLoop<K, V>,
        tid: usize,
        guard: &mut Guard,
        pool: &PoolHandle,
    ) {
        // TODO: if checkpoint recv.is_ok()
        while let Ok(()) = resize_recv.recv() {
            // println!("[resize_loop] do resize!");
            self.resize::<false>(&mut resize_loop.resize, tid, guard, pool);
            guard.repin_after(|| {});
        }
    }

    fn find_fast<'g>(
        &self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let mut context = self.context.load(Ordering::Acquire, guard, pool);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find_fast(key, key_tag, key_hashes, guard, pool);
            let find_result = ok_or!(find_result, {
                context = self.context.load(Ordering::Acquire, guard, pool);
                continue;
            });
            let find_result = some_or!(find_result, {
                let context_new = self.context.load(Ordering::Acquire, guard, pool);

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
        let mut context = self.context.load(Ordering::Acquire, guard, pool);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find(key, key_tag, key_hashes, guard, pool);
            let find_result = ok_or!(find_result, {
                context = self.context.load(Ordering::Acquire, guard, pool);
                continue;
            });
            let find_result = some_or!(find_result, {
                let context_new = self.context.load(Ordering::Acquire, guard, pool);

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

    pub fn get_capacity(&self, guard: &Guard, pool: &PoolHandle) -> usize {
        let context = self.context.load(Ordering::Acquire, guard, pool);
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

    pub fn search<'g>(&'g self, key: &K, guard: &'g Guard, pool: &'g PoolHandle) -> Option<&'g V> {
        let (key_tag, key_hashes) = hashes(key);
        let (_, find_result) = self.find_fast(key, key_tag, key_hashes, guard, pool);
        Some(&unsafe { find_result?.slot_ptr.deref(pool) }.value)
    }

    fn try_slot_insert<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot_new: PShared<'g, Slot<K, V>>,   // must be stable
        key_hashes: [u32; 2],
        try_slot_insert: &mut TrySlotInsert<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<FindResult<'g, K, V>, ()> {
        // TODO: if REC peek slot and CAS

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
                    // TODO: checkpoint slot

                    if !slot.load(Ordering::Acquire, guard, pool).is_null() {
                        continue;
                    }

                    if let Ok(()) = slot.cas::<false>(
                        PShared::null(),
                        slot_new,
                        &mut try_slot_insert.slot_cas,
                        tid,
                        guard,
                        pool,
                    ) {
                        return Ok(FindResult {
                            size,
                            slot,
                            slot_ptr: slot_new,
                        });
                    }
                }
            }
        }

        // TODO: checkpoint result as slot is None
        Err(())
    }

    fn insert_inner_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot: PShared<'g, Slot<K, V>>,       // must be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        insert_inner_inner: &mut InsertInnerInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(PShared<'g, Context<K, V>>, FindResult<'g, K, V>), PShared<'g, Context<K, V>>>
    {
        if let Ok(result) = self.try_slot_insert::<REC>(
            context,
            slot,
            key_hashes,
            &mut insert_inner_inner.try_slot_insert,
            tid,
            guard,
            pool,
        ) {
            return Ok((context, result));
        }

        // No remaining slots. Resize.
        // TODO: checkpoint context? (depending on add_level)
        let context_ref = unsafe { context.deref(pool) };
        let first_level = context_ref.first_level.load(Ordering::Acquire, guard);
        let first_level_ref = unsafe { first_level.deref(pool) };
        let (context_new, added) = self.add_level::<REC>(
            context,
            first_level_ref,
            &mut insert_inner_inner.add_lv,
            tid,
            guard,
            pool,
        );
        if added {
            let _ = resize_send.send(());
        }
        Err(context_new)
    }

    fn insert_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot: PShared<'g, Slot<K, V>>,       // must be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        insert_inner: &mut InsertInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        let mut res = self.insert_inner_inner::<REC>(
            context,
            slot,
            key_hashes,
            resize_send,
            &mut insert_inner.insert_inner_inner,
            tid,
            guard,
            pool,
        );

        while let Err(context_new) = res {
            res = self.insert_inner_inner::<false>(
                context_new,
                slot,
                key_hashes,
                resize_send,
                &mut insert_inner.insert_inner_inner,
                tid,
                guard,
                pool,
            );
        }

        res.unwrap()
    }

    fn move_if_resized_inner<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // must be stable
        insert_result: FindResult<'g, K, V>, // no need to be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        move_if_resize_inner: &mut MoveIfResizedInner<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(), (PShared<'g, Context<K, V>>, FindResult<'g, K, V>)> {
        let prev_slot = unsafe {
            move_if_resize_inner
                .prev_slot_chk
                .checkpoint::<REC, _>(|| insert_result.slot.as_pptr(pool), tid, pool)
                .deref(pool)
        };

        // If the inserted slot is being resized, try again.
        fence(Ordering::SeqCst);

        // If the context remains the same, it's done.
        let context_new = move_if_resize_inner
            .context_new_chk
            .checkpoint::<REC, _>(
                || {
                    let context_new = self.context.load(Ordering::Acquire, guard, pool);
                    PAtomic::from(context_new)
                },
                tid,
                pool,
            )
            .load(Ordering::Relaxed, guard);
        if context == context_new {
            return Ok(());
        }

        // If the inserted array is not being resized, it's done.
        let context_new_ref = unsafe { context_new.deref(pool) };
        if context_new_ref.resize_size < insert_result.size {
            return Ok(());
        }

        // Move the slot if the slot is not already (being) moved.
        //
        // the resize thread may already have passed the slot. I need to move it.
        if insert_result
            .slot
            .cas::<REC>(
                insert_result.slot_ptr,
                insert_result.slot_ptr.with_tag(1),
                &mut move_if_resize_inner.slot_cas,
                tid,
                guard,
                pool,
            )
            .is_err()
        {
            return Ok(());
        }

        let (context_insert, insert_result_insert) = self.insert_inner::<REC>(
            context_new,
            insert_result.slot_ptr,
            key_hashes,
            resize_send,
            &mut move_if_resize_inner.insert_inner,
            tid,
            guard,
            pool,
        );
        prev_slot
            .inner
            .store(PShared::null().with_tag(1), Ordering::Release); // exploit invariant

        // stable error
        Err((context_insert, insert_result_insert))
    }

    fn move_if_resized<'g, const REC: bool>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // must be stable
        insert_result: FindResult<'g, K, V>, // ?
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        move_if_resized: &mut MoveIfResized<K, V>,
        tid: usize,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) {
        let mut res = self.move_if_resized_inner::<REC>(
            context,
            insert_result,
            key_hashes,
            resize_send,
            &mut move_if_resized.move_if_resized_inner,
            tid,
            guard,
            pool,
        );
        while let Err((context, insert_result)) = res {
            res = self.move_if_resized_inner::<false>(
                context, // stable by move_if_resized_inner
                insert_result,
                key_hashes,
                resize_send,
                &mut move_if_resized.move_if_resized_inner,
                tid,
                guard,
                pool,
            );
        }
    }

    pub fn insert<const REC: bool>(
        &self,
        key: K,
        value: V,
        resize_send: &mpsc::Sender<()>,
        insert: &mut Insert<K, V>,
        tid: usize,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        let (key_tag, key_hashes) = hashes(&key);
        let (context, find_result) = self.find(&key, key_tag, key_hashes, guard, pool);

        let chk = insert.found_slot.checkpoint::<REC, _>(
            || {
                let found = find_result.is_some();
                let slot = if found {
                    PAtomic::null()
                } else {
                    PAtomic::from(
                        alloc_persist(Slot { key, value }, pool).with_high_tag(key_tag as usize),
                    )
                };
                (found, slot)
            },
            tid,
            pool,
        );
        let (found, slot) = (chk.0, chk.1.load(Ordering::Relaxed, guard));
        if found {
            return Err(InsertError::Occupied);
        }

        let (context_new, insert_result) = self.insert_inner::<REC>(
            context,
            slot,
            key_hashes,
            resize_send,
            &mut insert.insert_inner,
            tid,
            guard,
            pool,
        );
        self.move_if_resized::<REC>(
            context_new,   // stable by insert_inner
            insert_result, // stable by insert_inner
            key_hashes,
            resize_send,
            &mut insert.move_if_resized,
            tid,
            guard,
            pool,
        );
        Ok(())
    }

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

        let (slot, slot_ptr) = match find_result {
            Some(res) => (
                unsafe { res.slot.as_pptr(pool) },
                PAtomic::from(res.slot_ptr),
            ),
            None => (PPtr::null(), PAtomic::null()),
        };

        let chk = try_delete
            .find_result_chk
            .checkpoint::<REC, _>(|| (slot, slot_ptr), tid, pool);

        if chk.0.is_null() {
            // slot is null if find result is none
            return Ok(false);
        }

        let slot = unsafe { chk.0.deref(pool) };
        let slot_ptr = chk.1.load(Ordering::Relaxed, guard);

        if slot
            .cas::<REC>(
                slot_ptr,
                PShared::null(),
                &mut try_delete.slot_cas,
                tid,
                guard,
                pool,
            )
            .is_err()
        {
            return Err(());
        }

        unsafe { guard.defer_pdestroy(slot_ptr) };
        return Ok(true);
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

#[derive(Debug, Clone)]
pub enum InsertError {
    Occupied,
}

#[cfg(test)]
mod tests {
    use crate::{
        pmem::{Pool, RootObj},
        test_utils::tests::{
            check_res, compose, decompose, get_test_abs_path, produce_res, run_test,
            DummyRootMemento, DummyRootObj, TestRootObj, JOB_FINISHED,
        },
    };

    use super::*;

    use crossbeam_epoch::pin;
    use crossbeam_utils::thread;

    const NR_THREAD: usize = 12;
    const SMOKE_CNT: usize = 100_000;
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    static mut SEND: Option<[Option<mpsc::Sender<()>>; 64]> = None;
    static mut RECV: Option<mpsc::Receiver<()>> = None;

    struct Smoke {
        resize: ResizeLoop<usize, usize>,
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
                ResizeLoop::<usize, usize>::filter(&mut m.resize, tid, gc, pool);
                Insert::<usize, usize>::filter(&mut m.insert[i], tid, gc, pool);
                Delete::<usize, usize>::filter(&mut m.delete[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<Smoke> for TestRootObj<Clevel<usize, usize>> {
        fn run(&self, mmt: &mut Smoke, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let kv = &self.obj;

            match tid {
                // T1: Resize loop
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let guard = unsafe { (guard as *const _ as *mut Guard).as_mut() }.unwrap();
                    kv.resize_loop::<true>(&recv, &mut mmt.resize, tid, guard, pool);
                }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..SMOKE_CNT {
                        assert!(kv
                            .insert::<true>(i, i, &send, &mut mmt.insert[i], tid, &guard, pool)
                            .is_ok());
                        assert_eq!(kv.search(&i, &guard, pool), Some(&i));
                    }

                    for i in 0..SMOKE_CNT {
                        assert!(kv.delete::<true>(&i, &mut mmt.delete[i], tid, &guard, pool));
                        assert_eq!(kv.search(&i, &guard, pool), None);
                    }
                }
            }
        }
    }

    #[test]
    fn smoke() {
        const FILE_NAME: &str = "smoke";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = mpsc::channel();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            SEND.as_mut().unwrap()[2] = Some(send);
            RECV = Some(recv);
        }

        run_test::<TestRootObj<Clevel<usize, usize>>, Smoke>(FILE_NAME, FILE_SIZE, 2);
    }

    const INS_SCH_CNT: usize = 3_000;

    struct InsSch {
        insert: [Insert<usize, usize>; INS_SCH_CNT],
        resize: ResizeLoop<usize, usize>,
    }

    impl Default for InsSch {
        fn default() -> Self {
            Self {
                insert: array_init::array_init(|_| Insert::<usize, usize>::default()),
                resize: Default::default(),
            }
        }
    }

    impl Collectable for InsSch {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..INS_SCH_CNT {
                Insert::<usize, usize>::filter(&mut m.insert[i], tid, gc, pool);
                ResizeLoop::<usize, usize>::filter(&mut m.resize, tid, gc, pool);
            }
        }
    }

    impl RootObj<InsSch> for TestRootObj<Clevel<usize, usize>> {
        fn run(&self, mmt: &mut InsSch, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let kv = &self.obj;
            match tid {
                // T1: Resize loop
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let guard = unsafe { (guard as *const _ as *mut Guard).as_mut() }.unwrap();
                    kv.resize_loop::<true>(&recv, &mut mmt.resize, tid, guard, pool);
                }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..INS_SCH_CNT {
                        let _ =
                            kv.insert::<true>(i, i, &send, &mut mmt.insert[i], tid, &guard, pool);

                        if kv.search(&i, &guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail n {i}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn insert_search() {
        const FILE_NAME: &str = "insert_search";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = mpsc::channel();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            RECV = Some(recv);
            for tid in 2..=NR_THREAD + 1 {
                let sends = SEND.as_mut().unwrap();
                sends[tid] = Some(send.clone());
            }
        }
        drop(send);

        run_test::<TestRootObj<Clevel<usize, usize>>, InsSch>(FILE_NAME, FILE_SIZE, NR_THREAD + 1);
    }

    const INS_DEL_LOOK_CNT: usize = 100_000;

    struct InsDelLook {
        resize_loop: ResizeLoop<usize, usize>,
        inserts: [Insert<usize, usize>; INS_DEL_LOOK_CNT],
        deletes: [Delete<usize, usize>; INS_DEL_LOOK_CNT],
    }

    impl Default for InsDelLook {
        fn default() -> Self {
            Self {
                resize_loop: Default::default(),
                inserts: array_init::array_init(|_| Default::default()),
                deletes: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for InsDelLook {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..INS_DEL_LOOK_CNT {
                ResizeLoop::filter(&mut m.resize_loop, tid, gc, pool);
                Insert::filter(&mut m.inserts[i], tid, gc, pool);
                Delete::filter(&mut m.deletes[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsDelLook> for TestRootObj<Clevel<usize, usize>> {
        fn run(&self, mmt: &mut InsDelLook, tid: usize, guard: &Guard, pool: &PoolHandle) {
            let kv = &self.obj;

            match tid {
                // T1: Check the execution results of other threads
                1 => {
                    check_res(tid, NR_THREAD, INS_DEL_LOOK_CNT);
                }
                // T2: Resize loop
                2 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let guard = unsafe { (guard as *const _ as *mut Guard).as_mut() }.unwrap();
                    kv.resize_loop::<true>(&recv, &mut mmt.resize_loop, tid, guard, pool);
                }
                // Threads other than T1 and T2 perform { insert; lookup; delete; lookup; }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..INS_DEL_LOOK_CNT {
                        let key = compose(tid, i, i % tid);

                        // insert and lookup
                        assert!(kv
                            .insert::<true>(key, key, &send, &mut mmt.inserts[i], tid, &guard, pool)
                            .is_ok());
                        let res = kv.search(&key, &guard, pool);
                        assert!(res.is_some());

                        // transfer the lookup result to the result array
                        let (tid, i, value) = decompose(*res.unwrap());
                        produce_res(tid, i, value);

                        // delete and lookup
                        assert!(kv.delete::<true>(&key, &mut mmt.deletes[i], tid, &guard, pool));
                        let res = kv.search(&key, &guard, pool);
                        assert!(res.is_none());
                    }
                    let _ = JOB_FINISHED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn clevel_ins_del_look() {
        const FILE_NAME: &str = "clevel_ins_del_look";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = mpsc::channel();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            RECV = Some(recv);
            for tid in 3..=NR_THREAD + 2 {
                let sends = SEND.as_mut().unwrap();
                sends[tid] = Some(send.clone());
            }
        }
        drop(send);

        run_test::<TestRootObj<Clevel<usize, usize>>, InsDelLook>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD + 2,
        );
    }
}

fn alloc_persist<T>(init: T, pool: &PoolHandle) -> POwned<T> {
    let ptr = POwned::new(init, pool);
    persist_obj(unsafe { ptr.deref(pool) }, true);
    ptr
}
