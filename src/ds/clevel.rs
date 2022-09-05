//! Concurrent Level Hash Table.
#![allow(missing_docs)]
use core::fmt::Debug;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use crossbeam_channel::{self as channel, Receiver, Sender};
use std::sync::atomic::AtomicUsize;

use cfg_if::cfg_if;
use crossbeam_epoch::{self as epoch, Guard};
use etrace::*;
use fasthash::Murmur3HasherExt;
use itertools::*;
use libc::c_void;
use mmt_derive::Collectable;
use tinyvec::*;

use crate::pepoch::atomic::cut_as_high_tag_len;
use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::ploc::{Cas, Checkpoint, DetectableCASAtomic};
use crate::pmem::*;
use crate::PDefault;
use crate::*;

const TINY_VEC_CAPACITY: usize = 8;

cfg_if! {
    if #[cfg(any(feature = "stress", feature = "tcrash"))] {
        // For stress test.

        const SLOTS_IN_BUCKET: usize = 1;
        const LEVEL_DIFF: usize = 2;
        const MIN_SIZE: usize = 1;

        #[inline]
        const fn level_size_next(size: usize) -> usize {
            size + LEVEL_DIFF
        }

        #[inline]
        const fn level_size_prev(size: usize) -> usize {
            size - LEVEL_DIFF
        }
    } else {
        // For real workload.

        // Size of hash: MIN_SIZE * SLOTS_IN_BUCKET * (1+LEVEL_RATIO)
        const SLOTS_IN_BUCKET: usize = 8; // fixed
        const LEVEL_RATIO: usize = 2; // fixed
        const MIN_SIZE: usize = 786432; // Change this size

        #[inline]
        const fn level_size_next(size: usize) -> usize {
            size * LEVEL_RATIO
        }

        #[inline]
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
#[derive(Debug, Memento, Collectable)]
pub struct Insert<K, V: Collectable> {
    found_slot: Checkpoint<(bool, PAtomic<Slot<K, V>>, PAtomic<Context<K, V>>)>,
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

/// Move if resized client
#[derive(Debug, Memento, Collectable)]
pub struct MoveIfResized<K, V: Collectable> {
    arg_chk: Checkpoint<(
        PAtomic<Context<K, V>>,
        PPtr<DetectableCASAtomic<Slot<K, V>>>,
        usize, // FindResult's size
    )>,
    move_if_resized_inner: MoveIfResizedInner<K, V>,
}

impl<K, V: Collectable> Default for MoveIfResized<K, V> {
    fn default() -> Self {
        Self {
            arg_chk: Default::default(),
            move_if_resized_inner: Default::default(),
        }
    }
}

/// Move if resized client
#[derive(Debug, Memento, Collectable)]
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

/// Insert inner client
#[derive(Debug, Memento, Collectable)]
pub struct InsertInner<K, V: Collectable> {
    ctx_chk: Checkpoint<PAtomic<Context<K, V>>>,
    try_slot_insert: TrySlotInsert<K, V>,
    add_lv: AddLevel<K, V>,
}

impl<K, V: Collectable> Default for InsertInner<K, V> {
    fn default() -> Self {
        Self {
            ctx_chk: Default::default(),
            try_slot_insert: Default::default(),
            add_lv: Default::default(),
        }
    }
}

/// Try slot insert client
#[derive(Debug, Memento, Collectable)]
pub struct TrySlotInsert<K, V: Collectable> {
    slot_chk: Checkpoint<Option<(usize, PPtr<DetectableCASAtomic<Slot<K, V>>>)>>,
    slot_cas: Cas,
    fail: Checkpoint<()>,
}

impl<K, V: Collectable> Default for TrySlotInsert<K, V> {
    fn default() -> Self {
        Self {
            slot_chk: Default::default(),
            slot_cas: Default::default(),
            fail: Default::default(),
        }
    }
}

/// Add level client
#[derive(Debug, Memento, Collectable)]
pub struct AddLevel<K, V: Collectable> {
    next_level: NextLevel<K, V>,
    context_new_chk: Checkpoint<PAtomic<Context<K, V>>>,
    context_cas: Cas,
    context_chk: Checkpoint<PAtomic<Context<K, V>>>,
}

impl<K, V: Collectable> Default for AddLevel<K, V> {
    fn default() -> Self {
        Self {
            next_level: Default::default(),
            context_new_chk: Default::default(),
            context_cas: Default::default(),
            context_chk: Default::default(),
        }
    }
}

/// Next level client
#[derive(Debug, Memento, Collectable)]
pub struct NextLevel<K, V: Collectable> {
    next_level_chk: Checkpoint<PAtomic<Node<Bucket<K, V>>>>,
    my_node_chk: Checkpoint<PAtomic<Node<Bucket<K, V>>>>,
    next_cas: Cas,
}

impl<K, V: Collectable> Default for NextLevel<K, V> {
    fn default() -> Self {
        Self {
            next_level_chk: Default::default(),
            my_node_chk: Default::default(),
            next_cas: Default::default(),
        }
    }
}

/// Delete client
#[derive(Debug, Memento, Collectable)]
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

/// Delete client
#[derive(Debug, Memento, Collectable)]
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

/// Resize client
#[derive(Debug, Memento, Collectable)]
pub struct Resize<K, V: Collectable> {
    recv_chk: Checkpoint<bool>,
    resize_inner: ResizeInner<K, V>,
}

impl<K, V: Collectable> Default for Resize<K, V> {
    fn default() -> Self {
        Self {
            recv_chk: Default::default(),
            resize_inner: Default::default(),
        }
    }
}

/// Resize inner client
#[derive(Debug, Memento, Collectable)]
pub struct ResizeInner<K, V: Collectable> {
    ctx_chk: Checkpoint<PAtomic<Context<K, V>>>,
    resize_clean: ResizeClean<K, V>,
    resize_chg_ctx: ResizeChangeContext<K, V>,
}

impl<K, V: Collectable> Default for ResizeInner<K, V> {
    fn default() -> Self {
        Self {
            ctx_chk: Default::default(),
            resize_clean: Default::default(),
            resize_chg_ctx: Default::default(),
        }
    }
}

/// Resize clean client
#[derive(Debug, Memento, Collectable)]
pub struct ResizeClean<K, V: Collectable> {
    slot_slot_ptr_chk: Checkpoint<(PPtr<DetectableCASAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>,
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

/// Resize chagne context client
#[derive(Debug, Memento, Collectable)]
pub struct ResizeChangeContext<K, V: Collectable> {
    ctx_chk: Checkpoint<PAtomic<Context<K, V>>>,
    ctx_new_chk: Checkpoint<PAtomic<Context<K, V>>>,
    ctx_cas: Cas,
}

impl<K, V: Collectable> Default for ResizeChangeContext<K, V> {
    fn default() -> Self {
        Self {
            ctx_chk: Default::default(),
            ctx_new_chk: Default::default(),
            ctx_cas: Default::default(),
        }
    }
}

/// Resize move client
#[derive(Debug, Memento, Collectable)]
pub struct ResizeMove<K, V: Collectable> {
    ctx_fst_chk: Checkpoint<(PAtomic<Context<K, V>>, PPtr<Node<Bucket<K, V>>>)>,
    resize_move_inner: ResizeMoveInner<K, V>,
}

impl<K, V: Collectable> Default for ResizeMove<K, V> {
    fn default() -> Self {
        Self {
            ctx_fst_chk: Default::default(),
            resize_move_inner: Default::default(),
        }
    }
}

/// Resize move inner client
#[derive(Debug, Memento, Collectable)]
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

/// Resize move slot insert client
#[derive(Debug, Memento, Collectable)]
pub struct ResizeMoveSlotInsert<K, V: Collectable> {
    slot_slot_first_chk:
        Checkpoint<Option<(PPtr<DetectableCASAtomic<Slot<K, V>>>, PAtomic<Slot<K, V>>)>>,
    slot_cas: Cas,
    fail: Checkpoint<()>,
}

impl<K, V: Collectable> Default for ResizeMoveSlotInsert<K, V> {
    fn default() -> Self {
        Self {
            slot_slot_first_chk: Default::default(),
            slot_cas: Default::default(),
            fail: Default::default(),
        }
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

impl<K, V: Collectable> Default for Bucket<K, V> {
    fn default() -> Self {
        Self {
            slots: array_init::array_init(|_| DetectableCASAtomic::<_>::default()),
        }
    }
}

impl<K, V: Collectable> Collectable for Bucket<K, V> {
    fn filter(bucket: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        for slot in bucket.slots.iter_mut() {
            DetectableCASAtomic::filter(slot, tid, gc, pool);
        }
    }
}

#[derive(Debug, Default)]
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
    resize_size: AtomicUsize,
}

impl<K, V: Collectable> Collectable for Context<K, V> {
    fn filter(context: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        PAtomic::filter(&mut context.last_level, tid, gc, pool);
    }
}

#[derive(Debug)]
pub struct Clevel<K, V: Collectable> {
    context: DetectableCASAtomic<Context<K, V>>,
}

impl<K, V: Collectable> Collectable for Clevel<K, V> {
    fn filter(clevel: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        DetectableCASAtomic::filter(&mut clevel.context, tid, gc, pool);
    }
}

impl<K, V: Collectable> PDefault for Clevel<K, V> {
    fn pdefault(handle: &Handle) -> Self {
        let pool = handle.pool;
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
                resize_size: AtomicUsize::new(0),
            },
            pool,
        )
        .into_shared(guard);

        Clevel {
            context: DetectableCASAtomic::from(context),
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

impl<K, V: Collectable> Default for FindResult<'_, K, V> {
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
    fn level_iter<'g>(&'g self, guard: &'g Guard) -> NodeIter<'g, Bucket<K, V>> {
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
                    // TODO: Non-detectable CAS
                    // exploit invariant
                    if e.current.with_aux_bit(0).with_tid(0) == find_result.slot_ptr.with_tag(1) {
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
    let data = POwned::<[MaybeUninit<Bucket<K, V>>]>::init(size, pool);
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
    /// Check if resizing
    pub fn is_resizing(&self, guard: &Guard, pool: &PoolHandle) -> bool {
        let context = self.context.load(Ordering::Acquire, guard, pool);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);

        (unsafe {
            last_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
                .len()
        }) <= context_ref.resize_size.load(Ordering::Relaxed)
    }

    fn next_level<'g>(
        &self,
        fst_lv: &'g Node<Bucket<K, V>>,
        next_lv_size: usize,
        mmt: &mut NextLevel<K, V>,
        handle: &'g Handle,
    ) -> PShared<'g, Node<Bucket<K, V>>> {
        let (guard, pool) = (&handle.guard, handle.pool);

        let next_lv = mmt
            .next_level_chk
            .checkpoint(
                || {
                    let next_lv = fst_lv.next.load(Ordering::Acquire, guard, pool);
                    PAtomic::from(next_lv)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);
        if !next_lv.is_null() {
            return next_lv;
        }

        // TODO: need lock?
        let my_node = mmt
            .my_node_chk
            .checkpoint(|| PAtomic::from(new_node(next_lv_size, pool)), handle)
            .load(Ordering::Relaxed, guard);

        if let Err(e) = fst_lv
            .next
            .cas(PShared::null(), my_node, &mut mmt.next_cas, handle)
        {
            unsafe { guard.defer_pdestroy(my_node) };
            return e;
        }

        my_node
    }

    fn add_level<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        mmt: &mut AddLevel<K, V>,
        handle: &'g Handle,
    ) -> (PShared<'g, Context<K, V>>, bool) {
        let (guard, pool) = (&handle.guard, handle.pool);

        let ctx_ref = unsafe { ctx.deref(pool) };
        let fst_lv = ctx_ref.first_level.load(Ordering::Acquire, guard);
        let fst_lv_ref = unsafe { fst_lv.deref(pool) };
        let fst_lv_data = unsafe { fst_lv_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
        let next_lv_size = level_size_next(fst_lv_data.len());

        // insert a new level to the next of the first level.
        let next_lv = self.next_level(fst_lv_ref, next_lv_size, &mut mmt.next_level, handle);

        // update context.
        let ctx_new = mmt
            .context_new_chk
            .checkpoint(
                || {
                    let c = Context {
                        first_level: PAtomic::from(next_lv),
                        last_level: ctx_ref.last_level.clone(),
                        resize_size: AtomicUsize::new(level_size_prev(level_size_prev(
                            next_lv_size,
                        ))),
                    };
                    let new = alloc_persist(c, pool);
                    PAtomic::from(new)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);
        let ctx_new_ref = unsafe { ctx_new.deref(pool) };

        while let Err(cur) = self.context.cas(ctx, ctx_new, &mut mmt.context_cas, handle) {
            ctx = cur;

            let ctx_ref = unsafe { ctx.deref(pool) };
            let len = unsafe {
                ctx_ref
                    .first_level
                    .load(Ordering::Acquire, guard)
                    .deref(pool)
                    .data
                    .load(Ordering::Relaxed, guard)
                    .deref(pool)
            }
            .len();

            // TODO: CAS Err return value must be stable
            if len >= next_lv_size {
                unsafe { guard.defer_pdestroy(ctx_new) };
                return (ctx, false);
            }

            // TODO: check if same & flush otherwise
            let last_lv = ctx_ref.last_level.load(Ordering::Acquire, guard);
            ctx_new_ref.last_level.store(last_lv, Ordering::Relaxed); // Exploit invariant
        }

        fence(Ordering::SeqCst);
        (ctx_new, true)
    }

    fn resize_move_slot_insert(
        &self,
        slot_ptr: PShared<'_, Slot<K, V>>,
        key_tag: u16,
        key_hashes: [u32; 2],
        fst_lv_ref: &Node<Bucket<K, V>>,
        mmt: &mut ResizeMoveSlotInsert<K, V>,
        handle: &Handle,
    ) -> Result<(), ()> {
        let (guard, pool) = (&handle.guard, handle.pool);

        if handle.rec.load(Ordering::Relaxed) {
            if mmt.fail.peek(handle).is_some() {
                return Err(());
            }

            if let Some(v) = mmt.slot_slot_first_chk.peek(handle) {
                let chk = some_or!(v, return Err(()));
                let (slot, slot_first_level) = (
                    unsafe { chk.0.deref(pool) },
                    chk.1.load(Ordering::Relaxed, guard),
                );

                let mut retry = false;
                if let Some(slot_ref) = unsafe { slot_first_level.as_ref(pool) } {
                    // 2-byte tag checking
                    if slot_first_level.high_tag() != key_tag as usize {
                        retry = true;
                    }

                    if slot_ref.key != unsafe { slot_ptr.deref(pool) }.key {
                        retry = true;
                    }

                    if !retry {
                        return Ok(());
                    }
                }

                if !retry
                    && slot
                        .cas(PShared::null(), slot_ptr, &mut mmt.slot_cas, handle)
                        .is_ok()
                {
                    return Ok(());
                }
            }
        }

        let fst_lv_data = unsafe { fst_lv_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
        let fst_lv_size = fst_lv_data.len();
        let key_hashes = key_hashes
            .into_iter()
            .map(|key_hash| key_hash as usize % fst_lv_size)
            .sorted()
            .dedup();
        for i in 0..SLOTS_IN_BUCKET {
            for key_hash in key_hashes.clone() {
                let chk = mmt.slot_slot_first_chk.checkpoint(
                    || {
                        let slot = unsafe {
                            fst_lv_data[key_hash]
                                .assume_init_ref()
                                .slots
                                .get_unchecked(i)
                        };
                        let slot_first_level = slot.load(Ordering::Acquire, guard, pool);
                        Some((
                            unsafe { slot.as_pptr(pool) },
                            PAtomic::from(slot_first_level),
                        ))
                    },
                    handle,
                );
                let (slot, slot_first_level) = {
                    let chk = chk.unwrap();
                    (
                        unsafe { chk.0.deref(pool) },
                        chk.1.load(Ordering::Relaxed, guard),
                    )
                };

                if let Some(slot_ref) = unsafe { slot_first_level.as_ref(pool) } {
                    // 2-byte tag checking
                    if slot_first_level.high_tag() != key_tag as usize {
                        continue;
                    }

                    if slot_ref.key != unsafe { slot_ptr.deref(pool) }.key {
                        continue;
                    }

                    return Ok(());
                }

                if slot
                    .cas(PShared::null(), slot_ptr, &mut mmt.slot_cas, handle)
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }

        Err(mmt.fail.checkpoint(|| (), handle))
    }

    fn resize_move_inner<'g>(
        &'g self,
        ctx: PShared<'g, Context<K, V>>,
        slot_ptr: PShared<'_, Slot<K, V>>, // must be stable
        key_tag: u16,                      // must be stable
        key_hashes: [u32; 2],              // must be stable
        fst_lv_ref: &'g Node<Bucket<K, V>>,
        mmt: &mut ResizeMoveInner<K, V>,
        handle: &'g Handle,
    ) -> Result<&'g Node<Bucket<K, V>>, (PShared<'g, Context<K, V>>, &'g Node<Bucket<K, V>>)> {
        let (guard, pool) = (&handle.guard, handle.pool);

        if self
            .resize_move_slot_insert(
                slot_ptr,
                key_tag,
                key_hashes,
                fst_lv_ref,
                &mut mmt.resize_move_slot_insert,
                handle,
            )
            .is_ok()
        {
            return Ok(fst_lv_ref);
        }

        // The first level is full. Resize and retry.
        let (ctx_new, _) = self.add_level(ctx, &mut mmt.add_lv, handle);
        let ctx_new_ref = unsafe { ctx_new.deref(pool) };
        let fst_lv_new = ctx_new_ref.first_level.load(Ordering::Acquire, guard);
        let fst_lv_new_ref = unsafe { fst_lv_new.deref(pool) };
        Err((ctx_new, fst_lv_new_ref))
    }

    fn resize_move<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        slot_ptr: PShared<'_, Slot<K, V>>,
        mut fst_lv_ref: &'g Node<Bucket<K, V>>,
        mmt: &mut ResizeMove<K, V>,
        handle: &'g Handle,
    ) -> &'g Node<Bucket<K, V>> {
        let (guard, pool) = (&handle.guard, handle.pool);

        let (key_tag, key_hashes) = hashes(&unsafe { slot_ptr.deref(handle.pool) }.key);

        loop {
            (ctx, fst_lv_ref) = {
                let chk = mmt.ctx_fst_chk.checkpoint(
                    || (PAtomic::from(ctx), unsafe { fst_lv_ref.as_pptr(pool) }),
                    handle,
                );
                (chk.0.load(Ordering::Relaxed, guard), unsafe {
                    chk.1.deref(pool)
                })
            };

            match self.resize_move_inner(
                ctx,
                slot_ptr,
                key_tag,
                key_hashes,
                fst_lv_ref,
                &mut mmt.resize_move_inner,
                handle,
            ) {
                Ok(f) => return f,
                Err((c, f)) => {
                    ctx = c;
                    fst_lv_ref = f;
                }
            }
        }
    }

    fn resize_clean<'g>(
        &'g self,
        ctx: PShared<'g, Context<K, V>>,
        mut fst_lv_ref: &'g Node<Bucket<K, V>>,
        last_lv_data: &'g [MaybeUninit<Bucket<K, V>>],
        mmt: &mut ResizeClean<K, V>,
        handle: &'g Handle,
    ) {
        let (guard, pool) = (&handle.guard, handle.pool);

        if handle.rec.load(Ordering::Relaxed) {
            if let Some(chk) = mmt.slot_slot_ptr_chk.peek(handle) {
                let (slot, slot_ptr) = (
                    unsafe { chk.0.deref(pool) },
                    chk.1.load(Ordering::Relaxed, guard),
                );

                if slot
                    .cas(slot_ptr, slot_ptr.with_tag(1), &mut mmt.slot_cas, handle)
                    .is_ok()
                {
                    fst_lv_ref =
                        self.resize_move(ctx, slot_ptr, fst_lv_ref, &mut mmt.resize_move, handle);
                }
            }
        }

        for (_, bucket) in last_lv_data.iter().enumerate() {
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

                            let (slot, slot_ptr_checked) = {
                                let chk = mmt.slot_slot_ptr_chk.checkpoint(
                                    || unsafe { (slot.as_pptr(pool), PAtomic::from(slot_ptr)) },
                                    handle,
                                );
                                (
                                    unsafe { chk.0.deref(pool) },
                                    chk.1.load(Ordering::Relaxed, guard),
                                )
                            };

                            if let Err(e) = slot.cas(
                                slot_ptr_checked,
                                slot_ptr_checked.with_tag(1),
                                &mut mmt.slot_cas,
                                handle,
                            ) {
                                slot_ptr = e;
                                continue;
                            }

                            break Some(slot_ptr_checked);
                        }
                    },
                    continue
                );

                fst_lv_ref =
                    self.resize_move(ctx, slot_ptr, fst_lv_ref, &mut mmt.resize_move, handle);
            }
        }
    }

    fn resize_change_context<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        mmt: &mut ResizeChangeContext<K, V>,
        handle: &'g Handle,
    ) -> PShared<'g, Context<K, V>> {
        let (guard, pool) = (&handle.guard, handle.pool);

        loop {
            ctx = mmt
                .ctx_chk
                .checkpoint(|| PAtomic::from(ctx), handle)
                .load(Ordering::Relaxed, guard);
            let ctx_ref = unsafe { ctx.deref(pool) };
            let old_last_lv = ctx_ref.last_level.load(Ordering::Acquire, guard);

            let ctx_new = mmt
                .ctx_new_chk
                .checkpoint(
                    || {
                        let c = Context {
                            first_level: ctx_ref.first_level.load(Ordering::Acquire, guard).into(),
                            last_level: unsafe { old_last_lv.deref(pool) }
                                .next
                                .load(Ordering::Acquire, guard, pool)
                                .into(),
                            resize_size: AtomicUsize::new(
                                ctx_ref.resize_size.load(Ordering::Relaxed),
                            ),
                        };
                        let n = alloc_persist(c, pool);
                        PAtomic::from(n)
                    },
                    handle,
                )
                .load(Ordering::Relaxed, guard);

            if let Err(e) = self.context.cas(ctx, ctx_new, &mut mmt.ctx_cas, handle) {
                unsafe { guard.defer_pdestroy(ctx_new) };
                ctx = e;
            } else {
                unsafe { guard.defer_pdestroy(old_last_lv) };
                return ctx_new;
            }
        }
    }

    fn resize_inner<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        mmt: &mut ResizeInner<K, V>,
        handle: &'g Handle,
    ) {
        let (guard, pool) = (&handle.guard, handle.pool);

        loop {
            ctx = mmt
                .ctx_chk
                .checkpoint(|| PAtomic::from(ctx), handle)
                .load(Ordering::Relaxed, &handle.guard);

            let ctx_ref = unsafe { ctx.deref(pool) };

            let last_lv = ctx_ref.last_level.load(Ordering::Acquire, guard);
            let last_lv_ref = unsafe { last_lv.deref(pool) };
            let last_lv_data =
                unsafe { last_lv_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
            let last_lv_size = last_lv_data.len();

            // if we don't need to resize, break out.
            if ctx_ref.resize_size.load(Ordering::Relaxed) < last_lv_size {
                return;
            }

            let fst_lv = ctx_ref.first_level.load(Ordering::Acquire, guard);
            let fst_lv_ref = unsafe { fst_lv.deref(pool) };

            self.resize_clean(ctx, fst_lv_ref, last_lv_data, &mut mmt.resize_clean, handle);
            ctx = self.resize_change_context(ctx, &mut mmt.resize_chg_ctx, handle);
        }
    }

    pub fn resize(&self, recv: &Receiver<()>, mmt: &mut Resize<K, V>, handle: &Handle) {
        let (guard, pool) = (&handle.guard, handle.pool);

        while mmt.recv_chk.checkpoint(|| recv.recv().is_ok(), handle) {
            println!("[resize] Do resize!");
            let ctx = self.context.load(Ordering::Acquire, guard, pool);
            self.resize_inner(ctx, &mut mmt.resize_inner, handle);
            // handle.guard.repin_after(|| {}); // TODO: uncomment
        }
    }

    fn find_fast<'g>(
        &self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        handle: &'g Handle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let (guard, pool) = (&handle.guard, handle.pool);

        let mut ctx = self.context.load(Ordering::Acquire, guard, pool);
        loop {
            let ctx_ref = unsafe { ctx.deref(pool) };
            let res = ctx_ref.find_fast(key, key_tag, key_hashes, guard, pool);
            let res = ok_or!(res, {
                ctx = self.context.load(Ordering::Acquire, guard, pool);
                continue;
            });
            let res = some_or!(res, {
                let ctx_new = self.context.load(Ordering::Acquire, guard, pool);

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
                if ctx != ctx_new {
                    ctx = ctx_new;
                    continue;
                }
                return (ctx, None);
            });
            return (ctx, Some(res));
        }
    }

    fn find<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        handle: &'g Handle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let (guard, pool) = (&handle.guard, handle.pool);

        let mut ctx = self.context.load(Ordering::Acquire, guard, pool);
        loop {
            let ctx_ref = unsafe { ctx.deref(pool) };
            let res = ctx_ref.find(key, key_tag, key_hashes, guard, pool);
            let res = ok_or!(res, {
                ctx = self.context.load(Ordering::Acquire, guard, pool);
                continue;
            });
            let res = some_or!(res, {
                let ctx_new = self.context.load(Ordering::Acquire, guard, pool);

                // the same possible corner case as `find_fast`
                if ctx != ctx_new {
                    ctx = ctx_new;
                    continue;
                }
                return (ctx, None);
            });
            return (ctx, Some(res));
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

    pub fn search<'g>(&'g self, key: &K, handle: &'g Handle) -> Option<&'g V> {
        let (key_tag, key_hashes) = hashes(key);
        let (_, find_result) = self.find_fast(key, key_tag, key_hashes, handle);
        Some(&unsafe { find_result?.slot_ptr.deref(handle.pool) }.value)
    }

    fn try_slot_insert<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        slot_new: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        mmt: &mut TrySlotInsert<K, V>,
        handle: &'g Handle,
    ) -> Result<FindResult<'g, K, V>, ()> {
        let (guard, pool) = (&handle.guard, handle.pool);

        if handle.rec.load(Ordering::Relaxed) {
            if mmt.fail.peek(handle).is_some() {
                return Err(());
            }

            if let Some(v) = mmt.slot_chk.peek(handle) {
                let (size, slot) = some_or!(v, return Err(()));
                let slot = unsafe { slot.deref(pool) };
                if let Ok(()) = slot.cas(PShared::null(), slot_new, &mut mmt.slot_cas, handle) {
                    return Ok(FindResult {
                        size,
                        slot,
                        slot_ptr: slot_new,
                    });
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
            if context_ref.resize_size.load(Ordering::Relaxed) >= size {
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

                    let _ = unsafe {
                        mmt.slot_chk
                            .checkpoint(|| Some((size, slot.as_pptr(pool))), handle)
                    };

                    if let Ok(()) = slot.cas(PShared::null(), slot_new, &mut mmt.slot_cas, handle) {
                        return Ok(FindResult {
                            size,
                            slot,
                            slot_ptr: slot_new,
                        });
                    }
                }
            }
        }

        Err(mmt.fail.checkpoint(|| (), handle))
    }

    fn insert_inner<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        snd: &Sender<()>,
        mmt: &mut InsertInner<K, V>,
        handle: &'g Handle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        loop {
            let ctx_chk = mmt
                .ctx_chk
                .checkpoint(|| PAtomic::from(ctx), handle)
                .load(Ordering::Relaxed, &handle.guard);

            if let Ok(res) =
                self.try_slot_insert(ctx_chk, slot, key_hashes, &mut mmt.try_slot_insert, handle)
            {
                return (ctx_chk, res);
            }

            // No remaining slots. Resize.
            let (ctx_new, added) = self.add_level(ctx_chk, &mut mmt.add_lv, handle);
            if added {
                let _ = snd.send(());
            }

            ctx = ctx_new;
        }
    }

    fn move_if_resized_inner<'g>(
        &'g self,
        ctx: PShared<'g, Context<K, V>>,
        ins_res: FindResult<'g, K, V>,
        key_hashes: [u32; 2],
        snd: &Sender<()>,
        mmt: &mut MoveIfResizedInner<K, V>,
        handle: &'g Handle,
    ) -> Result<(), (PShared<'g, Context<K, V>>, FindResult<'g, K, V>)> {
        let (guard, pool) = (&handle.guard, handle.pool);

        // If the inserted slot is being resized, try again.
        fence(Ordering::SeqCst);

        // If the context remains the same, it's done.
        let ctx_new = mmt
            .context_new_chk
            .checkpoint(
                || {
                    let ctx_new = self.context.load(Ordering::Acquire, guard, pool);
                    PAtomic::from(ctx_new)
                },
                handle,
            )
            .load(Ordering::Relaxed, guard);

        if ctx == ctx_new {
            return Ok(());
        }

        // If the inserted array is not being resized, it's done.
        let ctx_new_ref = unsafe { ctx_new.deref(pool) };
        if ctx_new_ref.resize_size.load(Ordering::Relaxed) < ins_res.size {
            return Ok(());
        }

        // Move the slot if the slot is not already (being) moved.
        //
        // the resize thread may already have passed the slot. I need to move it.
        if ins_res
            .slot
            .cas(
                ins_res.slot_ptr,
                ins_res.slot_ptr.with_tag(1),
                &mut mmt.slot_cas,
                handle,
            )
            .is_err()
        {
            return Ok(());
        }

        let (ctx2, ins_res2) = self.insert_inner(
            ctx_new,
            ins_res.slot_ptr,
            key_hashes,
            snd,
            &mut mmt.insert_inner,
            handle,
        );
        ins_res
            .slot
            .inner
            .store(PShared::null().with_tag(1), Ordering::Release); // exploit invariant

        // stable error
        Err((ctx2, ins_res2))
    }

    fn move_if_resized<'g>(
        &'g self,
        mut ctx: PShared<'g, Context<K, V>>,
        mut ins_res: FindResult<'g, K, V>,
        slot_ptr: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        snd: &Sender<()>,
        mmt: &mut MoveIfResized<K, V>,
        handle: &'g Handle,
    ) {
        loop {
            let chk = mmt.arg_chk.checkpoint(
                || {
                    (
                        PAtomic::from(ctx),
                        unsafe { ins_res.slot.as_pptr(handle.pool) },
                        ins_res.size,
                    )
                },
                handle,
            );

            let info = FindResult {
                size: chk.2,
                slot: unsafe { chk.1.deref(handle.pool) },
                slot_ptr,
            };

            if let Err((c, r)) = self.move_if_resized_inner(
                ctx, // stable by move_if_resized_inner
                info,
                key_hashes,
                snd,
                &mut mmt.move_if_resized_inner,
                handle,
            ) {
                ctx = c;
                ins_res = r;
            } else {
                return;
            }
        }
    }

    pub fn insert(
        &self,
        key: K,
        value: V,
        snd: &Sender<()>,
        mmt: &mut Insert<K, V>,
        handle: &Handle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        let (guard, pool) = (&handle.guard, handle.pool);

        let (key_tag, key_hashes) = hashes(&key);

        let chk = mmt.found_slot.checkpoint(
            || {
                let (ctx, find_res) = self.find(&key, key_tag, key_hashes, handle);
                let found = find_res.is_some();
                let slot = if found {
                    PAtomic::null()
                } else {
                    PAtomic::from(
                        alloc_persist(Slot { key, value }, pool).with_high_tag(key_tag as usize),
                    )
                };
                (found, slot, PAtomic::from(ctx))
            },
            handle,
        );
        let (found, slot, ctx) = (
            chk.0,
            chk.1.load(Ordering::Relaxed, guard),
            chk.2.load(Ordering::Relaxed, guard),
        );

        if found {
            return Err(InsertError::Occupied);
        }

        let (ctx_new, ins_res) =
            self.insert_inner(ctx, slot, key_hashes, snd, &mut mmt.insert_inner, handle);

        self.move_if_resized(
            ctx_new, // stable by insert_inner
            ins_res, // stable by insert_inner
            slot,
            key_hashes,
            snd,
            &mut mmt.move_if_resized,
            handle,
        );

        Ok(())
    }

    fn try_delete(
        &self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        mmt: &mut TryDelete<K, V>,
        handle: &Handle,
    ) -> Result<bool, ()> {
        let (guard, pool) = (&handle.guard, handle.pool);

        let chk = mmt.find_result_chk.checkpoint(
            || {
                let (_, find_result) = self.find(key, key_tag, key_hashes, handle);

                let (slot, slot_ptr) = match find_result {
                    Some(res) => (
                        unsafe { res.slot.as_pptr(pool) },
                        PAtomic::from(res.slot_ptr),
                    ),
                    None => (PPtr::null(), PAtomic::null()),
                };
                (slot, slot_ptr)
            },
            handle,
        );

        if chk.0.is_null() {
            // slot is null if find result is none
            return Ok(false);
        }

        let slot = unsafe { chk.0.deref(pool) };
        let slot_ptr = chk.1.load(Ordering::Relaxed, guard);

        if slot
            .cas(slot_ptr, PShared::null(), &mut mmt.slot_cas, handle)
            .is_err()
        {
            return Err(());
        }

        unsafe { guard.defer_pdestroy(slot_ptr) };
        Ok(true)
    }

    pub fn delete(&self, key: &K, mmt: &mut Delete<K, V>, handle: &Handle) -> bool {
        let (key_tag, key_hashes) = hashes(&key);

        loop {
            if let Ok(ret) = self.try_delete(key, key_tag, key_hashes, &mut mmt.try_delete, handle)
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
mod simple_test {
    use crate::{
        pmem::RootObj,
        test_utils::tests::{run_test, TestRootObj, TESTER},
    };

    use super::*;

    const SMOKE_CNT: usize = 100_000;

    static mut SEND: Option<[Option<Sender<()>>; 64]> = None;
    static mut RECV: Option<Receiver<()>> = None;

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

    impl Memento for Smoke {
        fn clear(&mut self) {
            self.resize.clear();
            for i in 0..SMOKE_CNT {
                self.insert[i].clear();
                self.delete[i].clear();
            }
        }
    }

    impl Collectable for Smoke {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut m.resize, tid, gc, pool);
            for i in 0..SMOKE_CNT {
                Collectable::filter(&mut m.insert[i], tid, gc, pool);
                Collectable::filter(&mut m.delete[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<Smoke> for TestRootObj<Clevel<usize, usize>> {
        fn run(&self, mmt: &mut Smoke, handle: &Handle) {
            let tid = handle.tid;

            let _testee = unsafe { TESTER.as_ref().unwrap().testee(tid, false) };
            let kv = &self.obj;

            match tid {
                // T1: Resize loop
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    kv.resize(&recv, &mut mmt.resize, handle);
                }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..SMOKE_CNT {
                        assert!(kv.insert(i, i, &send, &mut mmt.insert[i], handle).is_ok());
                        assert_eq!(kv.search(&i, handle), Some(&i));
                    }

                    for i in 0..SMOKE_CNT {
                        assert!(kv.delete(&i, &mut mmt.delete[i], handle));
                        assert_eq!(kv.search(&i, handle), None);
                    }
                }
            }
        }
    }

    #[test]
    fn smoke() {
        const FILE_NAME: &str = "clevel_smoke";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = channel::unbounded();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            SEND.as_mut().unwrap()[2] = Some(send);
            RECV = Some(recv);
        }

        run_test::<TestRootObj<Clevel<usize, usize>>, Smoke>(FILE_NAME, FILE_SIZE, 2, SMOKE_CNT);
    }

    const INS_SCH_CNT: usize = 3_000;

    struct InsSch {
        insert: [Insert<usize, usize>; INS_SCH_CNT],
        resize: Resize<usize, usize>,
    }

    impl Default for InsSch {
        fn default() -> Self {
            Self {
                insert: array_init::array_init(|_| Insert::<usize, usize>::default()),
                resize: Default::default(),
            }
        }
    }

    impl Memento for InsSch {
        fn clear(&mut self) {
            self.resize.clear();
            for i in 0..INS_SCH_CNT {
                self.insert[i].clear();
            }
        }
    }

    impl Collectable for InsSch {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut m.resize, tid, gc, pool);
            for i in 0..INS_SCH_CNT {
                Collectable::filter(&mut m.insert[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<InsSch> for TestRootObj<Clevel<usize, usize>> {
        fn run(&self, mmt: &mut InsSch, handle: &Handle) {
            let tid = handle.tid;
            let _testee = unsafe { TESTER.as_ref().unwrap().testee(tid, false) };

            let kv = &self.obj;
            match tid {
                // T1: Resize loop
                1 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    kv.resize(&recv, &mut mmt.resize, handle);
                }
                _ => {
                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..INS_SCH_CNT {
                        let _ = kv.insert(i, i, &send, &mut mmt.insert[i], handle);

                        if kv.search(&i, handle) != Some(&i) {
                            panic!("[test] tid = {tid} fail n {i}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn insert_search() {
        const NR_THREAD: usize = 12;

        const FILE_NAME: &str = "clevel_insert_search";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = channel::unbounded();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            RECV = Some(recv);
            for tid in 2..=NR_THREAD {
                let sends = SEND.as_mut().unwrap();
                sends[tid] = Some(send.clone());
            }
        }
        drop(send);

        run_test::<TestRootObj<Clevel<usize, usize>>, InsSch>(
            FILE_NAME,
            FILE_SIZE,
            NR_THREAD,
            INS_SCH_CNT,
        );
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::tests::*;

    use super::*;

    const NR_THREAD: usize = 1 /* Resizer */ + 5 /* Testee */;
    const NR_COUNT: usize = 10_000;

    static mut SEND: Option<[Option<Sender<()>>; NR_THREAD + 1]> = None;
    static mut RECV: Option<Receiver<()>> = None;

    struct InsDelLook {
        resize: Resize<TestValue, TestValue>,
        inserts: [Insert<TestValue, TestValue>; NR_COUNT],
        ins_lookups: [Checkpoint<Option<TestValue>>; NR_COUNT],
        deletes: [Delete<TestValue, TestValue>; NR_COUNT],
        del_lookups: [Checkpoint<Option<TestValue>>; NR_COUNT],
    }

    impl Memento for InsDelLook {
        fn clear(&mut self) {
            self.resize.clear();
            for i in 0..NR_COUNT {
                self.inserts[i].clear();
                self.ins_lookups[i].clear();
                self.deletes[i].clear();
                self.del_lookups[i].clear();
            }
        }
    }

    impl Collectable for InsDelLook {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            Collectable::filter(&mut m.resize, tid, gc, pool);
            for i in 0..NR_COUNT {
                Collectable::filter(&mut m.inserts[i], tid, gc, pool);
                Collectable::filter(&mut m.ins_lookups[i], tid, gc, pool);
                Collectable::filter(&mut m.deletes[i], tid, gc, pool);
                Collectable::filter(&mut m.del_lookups[i], tid, gc, pool);
            }
        }
    }

    impl Default for InsDelLook {
        fn default() -> Self {
            Self {
                resize: Default::default(),
                inserts: array_init::array_init(|_| Default::default()),
                ins_lookups: array_init::array_init(|_| Default::default()),
                deletes: array_init::array_init(|_| Default::default()),
                del_lookups: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl RootObj<InsDelLook> for TestRootObj<Clevel<TestValue, TestValue>> {
        fn run(&self, mmt: &mut InsDelLook, handle: &Handle) {
            let tid = handle.tid;

            match tid {
                // T1: Resize loop
                1 => {
                    let _testee = unsafe { TESTER.as_ref().unwrap().testee(tid, false) };

                    let recv = unsafe { RECV.as_ref().unwrap() };
                    self.obj.resize(&recv, &mut mmt.resize, handle);
                }
                // Threads other than T1 and T2 perform { insert; lookup; delete; lookup; }
                _ => {
                    let testee = unsafe { TESTER.as_ref().unwrap().testee(tid, true) };

                    let send = unsafe { SEND.as_mut().unwrap()[tid].as_ref().unwrap() };
                    for seq in 0..NR_COUNT {
                        let key = TestValue::new(tid, seq);

                        // insert and lookup
                        assert!(self
                            .obj
                            .insert(key, key, &send, &mut mmt.inserts[seq], handle)
                            .is_ok());
                        let res = mmt.ins_lookups[seq].checkpoint(
                            || self.obj.search(&key, handle).map_or(None, |v| Some(*v)),
                            handle,
                        );

                        assert!(res.is_some(), "tid:{tid}, seq:{seq}");
                        testee.report(seq, res.unwrap());

                        // delete and lookup
                        assert!(self.obj.delete(&key, &mut mmt.deletes[seq], handle));
                        let res = mmt.del_lookups[seq].checkpoint(
                            || self.obj.search(&key, handle).map_or(None, |v| Some(*v)),
                            handle,
                        );

                        assert!(res.is_none(), "tid:{tid}, seq:{seq}");
                    }
                }
            }
        }
    }

    #[test]
    fn clevel_ins_del_look() {
        const FILE_NAME: &str = "clevel";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        let (send, recv) = channel::unbounded();
        unsafe {
            SEND = Some(array_init::array_init(|_| None));
            RECV = Some(recv);
            for tid in 2..=NR_THREAD {
                let sends = SEND.as_mut().unwrap();
                sends[tid] = Some(send.clone());
            }
        }
        drop(send);

        let _ = std::thread::spawn(|| {
            let tester = unsafe {
                while !TESTER_FLAG.load(Ordering::Acquire) {}
                TESTER.as_ref().unwrap()
            };

            while !tester.is_finished() {}

            // drop sends
            for send in unsafe { SEND.as_mut().unwrap() } {
                let _ = send.take();
            }
        });

        run_test::<TestRootObj<Clevel<TestValue, TestValue>>, InsDelLook>(
            FILE_NAME, FILE_SIZE, NR_THREAD, NR_COUNT,
        );
    }
}

fn alloc_persist<T>(init: T, pool: &PoolHandle) -> POwned<T> {
    let ptr = POwned::new(init, pool);
    persist_obj(unsafe { ptr.deref(pool) }, true);
    ptr
}
