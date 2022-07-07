//! Concurrent Level Hash Table.
#![allow(missing_docs)]
#![allow(box_pointers)]
#![allow(unreachable_pub)]
#![allow(unused)]
use core::cmp;
use core::fmt::Debug;
use core::fmt::Display;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use std::sync::mpsc;

use cfg_if::cfg_if;
use crossbeam_epoch::{self as epoch, Guard};
use derivative::Derivative;
use etrace::*;
use fasthash::Murmur3HasherExt;
use itertools::*;
use libc::c_void;
use parking_lot::{lock_api::RawMutex, RawMutex as RawMutexImpl};
use tinyvec::*;

use crate::pepoch::atomic::cut_as_high_tag_len;
use crate::pepoch::{PAtomic, PDestroyable, POwned, PShared};
use crate::pmem::persist_obj;
use crate::pmem::{global_pool, Collectable, GarbageCollection, PoolHandle};
use crate::PDefault;

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

#[derive(Debug, Default)]
struct Slot<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
#[repr(align(64))]
struct Bucket<K, V> {
    slots: [PAtomic<Slot<K, V>>; SLOTS_IN_BUCKET],
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

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Clevel<K, V> {
    context: PAtomic<Context<K, V>>,

    #[derivative(Debug = "ignore")]
    add_level_lock: RawMutexImpl,
}

impl<K, V: Collectable> Collectable for Clevel<K, V> {
    fn filter(clevel: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        todo!()
    }
}

impl<K, V: Collectable> PDefault for Clevel<K, V> {
    fn pdefault(pool: &PoolHandle) -> Self {
        let guard = unsafe { epoch::unprotected() }; // SAFE when initialization

        let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
        let last_level = new_node(MIN_SIZE, pool);
        let last_level_ref = unsafe { last_level.deref(pool) };
        last_level_ref.next.store(first_level, Ordering::Relaxed);
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
            context: PAtomic::from(context),
            add_level_lock: RawMutexImpl::INIT,
        }
    }
}

#[derive(Debug)]
struct FindResult<'g, K, V> {
    /// level's size
    size: usize,
    slot: &'g PAtomic<Slot<K, V>>,
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
        let pool = global_pool().unwrap();
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
                    let slot_ptr = slot.load(Ordering::Acquire, guard);

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
                    let slot_ptr = slot.load(Ordering::Acquire, guard);

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
                        .store(PShared::null().with_tag(1), Ordering::Release);
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
            match find_result.slot.compare_exchange(
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
                    // TODO: CAS: with_tid(0)
                    if e.current == find_result.slot_ptr.with_tag(1) {
                        // If the item is moved, retry.
                        return Err(());
                    }
                }
            }
        }

        Ok(Some(last))
    }
}

fn new_node<K, V>(
    size: usize,
    pool: &PoolHandle,
) -> POwned<Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>> {
    // println!("[new_node] size: {size}");

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

impl<K, V> Drop for Clevel<K, V> {
    fn drop(&mut self) {
        let pool = global_pool().unwrap();
        let guard = unsafe { epoch::unprotected() };
        let context = self.context.load(Ordering::Relaxed, guard);
        let context_ref = unsafe { context.deref(pool) };

        let mut node = context_ref.last_level.load(Ordering::Relaxed, guard);
        while let Some(node_ref) = unsafe { node.as_ref(pool) } {
            let next = node_ref.next.load(Ordering::Relaxed, guard);
            let data = unsafe { node_ref.data.load(Ordering::Relaxed, guard).deref(pool) };
            for bucket in data.iter() {
                for slot in unsafe { bucket.assume_init_ref().slots.iter() } {
                    let slot_ptr = slot.load(Ordering::Relaxed, guard);
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

impl<K: PartialEq + Hash, V> Clevel<K, V> {
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
            self.add_level_lock.lock(); // TODO: persistent try lock
            let next_level = first_level.next.load(Ordering::Acquire, guard);
            let next_level = if !next_level.is_null() {
                next_level
            } else {
                let next_node = new_node(next_level_size, pool);
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
        let mut context_new = alloc_persist(
            Context {
                first_level: PAtomic::from(next_level),
                last_level: context_ref.last_level.clone(),
                resize_size: level_size_prev(level_size_prev(next_level_size)),
            },
            pool,
        );
        // TODO: checkpoint context_new
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

    pub fn resize_loop(
        &self,
        resize_recv: &mpsc::Receiver<()>,
        guard: &mut Guard,
        pool: &PoolHandle,
    ) {
        while let Ok(()) = resize_recv.recv() {
            // println!("[resize_loop] do resize!");
            self.resize(guard, pool);
            guard.repin_after(|| {});
        }
    }

    fn resize(&self, guard: &Guard, pool: &PoolHandle) {
        // // // println!("[resize]");
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
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
            // println!(
            //     "[reisze] resize_size: {}, last_level_size: {}",
            //     context_ref.resize_size, last_level_size
            // );
            if context_ref.resize_size < last_level_size {
                break;
            }

            let mut first_level = context_ref.first_level.load(Ordering::Acquire, guard);
            let mut first_level_ref = unsafe { first_level.deref(pool) };
            let mut first_level_data = unsafe {
                first_level_ref
                    .data
                    .load(Ordering::Relaxed, guard)
                    .deref(pool)
            };
            let mut first_level_size = first_level_data.len();
            // println!(
            //     "[resize] last_level_size: {last_level_size}, first_level_size: {first_level_size}"
            // );

            for (_bid, bucket) in last_level_data.iter().enumerate() {
                for (_sid, slot) in unsafe { bucket.assume_init_ref().slots.iter().enumerate() } {
                    let slot_ptr = some_or!(
                        {
                            let mut slot_ptr = slot.load(Ordering::Acquire, guard);
                            loop {
                                if slot_ptr.is_null() {
                                    break None;
                                }

                                // tagged with 1 by concurrent move_if_resized(). we should wait for the item to be moved before changing context.
                                // example: insert || lookup (1); lookup (2), maybe lookup (1) can see the insert while lookup (2) doesn't.
                                // TODO: should we do it...?
                                if slot_ptr.tag() == 1 {
                                    slot_ptr = slot.load(Ordering::Acquire, guard);
                                    continue;
                                }

                                if let Err(e) = slot.compare_exchange(
                                    slot_ptr,
                                    slot_ptr.with_tag(1),
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    guard,
                                ) {
                                    slot_ptr = e.current;
                                    continue;
                                }

                                break Some(slot_ptr);
                            }
                        },
                        continue
                    );

                    // // // println!("[resize] moving ({}, {}, {})...", last_level_size, bid, sid);

                    let mut moved = false;
                    loop {
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

                                let slot_first_level = slot.load(Ordering::Acquire, guard);
                                if let Some(slot) = unsafe { slot_first_level.as_ref(pool) } {
                                    // 2-byte tag checking
                                    if slot_first_level.high_tag() != key_tag as usize {
                                        continue;
                                    }

                                    if slot.key != unsafe { slot_ptr.deref(pool) }.key {
                                        continue;
                                    }

                                    moved = true;
                                    break;
                                }

                                if slot
                                    .compare_exchange(
                                        PShared::null(),
                                        slot_ptr,
                                        Ordering::AcqRel,
                                        Ordering::Relaxed,
                                        guard,
                                    )
                                    .is_ok()
                                {
                                    moved = true;
                                    break;
                                }
                            }

                            if moved {
                                break;
                            }
                        }

                        if moved {
                            break;
                        }

                        // println!(
                        //     "[resize] resizing again for ({last_level_size}, {bid}, {sid})..."
                        // );

                        // The first level is full. Resize and retry.
                        let (context_new, _) =
                            self.add_level(context, first_level_ref, guard, pool);
                        context = context_new;
                        context_ref = unsafe { context.deref(pool) };
                        first_level = context_ref.first_level.load(Ordering::Acquire, guard);
                        first_level_ref = unsafe { first_level.deref(pool) };
                        first_level_data = unsafe {
                            first_level_ref
                                .data
                                .load(Ordering::Relaxed, guard)
                                .deref(pool)
                        };
                        first_level_size = first_level_data.len();
                    }
                }
            }

            let next_level = last_level_ref.next.load(Ordering::Acquire, guard);
            let mut context_new = alloc_persist(
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

            // println!("[resize] done!");
        }
    }
}

impl<K: Debug + Display + PartialEq + Hash, V: Debug> Clevel<K, V> {
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

    pub fn search<'g>(&'g self, key: &K, guard: &'g Guard, pool: &'g PoolHandle) -> Option<&'g V> {
        let (key_tag, key_hashes) = hashes(key);
        let (_, find_result) = self.find_fast(key, key_tag, key_hashes, guard, pool);
        Some(&unsafe { find_result?.slot_ptr.deref(pool) }.value)
    }

    // TODO: memento
    fn try_slot_insert<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot_new: PShared<'g, Slot<K, V>>,   // must be stable
        key_hashes: [u32; 2],
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

                    if !slot.load(Ordering::Acquire, guard).is_null() {
                        continue;
                    }

                    // TODO: checkpoint slot

                    // TODO: CAS
                    if let Ok(slot_ptr) = slot.compare_exchange(
                        PShared::null(),
                        slot_new,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        return Ok(FindResult {
                            size,
                            slot,
                            slot_ptr,
                        });
                    }
                }
            }
        }

        Err(())
    }

    // TODO: memento
    fn insert_inner_inner<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot: PShared<'g, Slot<K, V>>,       // must be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(PShared<'g, Context<K, V>>, FindResult<'g, K, V>), PShared<'g, Context<K, V>>>
    {
        if let Ok(result) = self.try_slot_insert(context, slot, key_hashes, guard, pool) {
            return Ok((context, result));
        }

        // No remaining slots. Resize.
        // TODO: checkpoint context? (depending on add_level)
        let context_ref = unsafe { context.deref(pool) };
        let first_level = context_ref.first_level.load(Ordering::Acquire, guard);
        let first_level_ref = unsafe { first_level.deref(pool) };
        let (context_new, added) = self.add_level(context, first_level_ref, guard, pool);
        if added {
            let _ = resize_send.send(());
        }
        Err(context_new)
    }

    // TODO: memento
    fn insert_inner<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // no need to be stable
        slot: PShared<'g, Slot<K, V>>,       // must be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        let mut res = self.insert_inner_inner(context, slot, key_hashes, resize_send, guard, pool);

        while let Err(context_new) = res {
            res = self.insert_inner_inner(context_new, slot, key_hashes, resize_send, guard, pool);
        }

        res.unwrap()
    }

    // TODO: memento
    fn move_if_resized_inner<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>, // must be stable
        insert_result: FindResult<'g, K, V>, // no need to be stable
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) -> Result<(), (PShared<'g, Context<K, V>>, FindResult<'g, K, V>)> {
        // TODO: checkpoint insert_result (only prev_slot)

        // If the inserted slot is being resized, try again.
        fence(Ordering::SeqCst);

        // If the context remains the same, it's done.
        // TODO: checkpoint context_new
        let context_new = self.context.load(Ordering::Acquire, guard);
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
        // TODO: CAS
        if insert_result
            .slot
            .compare_exchange(
                insert_result.slot_ptr,
                insert_result.slot_ptr.with_tag(1),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            )
            .is_err()
        {
            return Ok(());
        }

        let (context_insert, insert_result_insert) = self.insert_inner(
            context_new,
            insert_result.slot_ptr,
            key_hashes,
            resize_send,
            guard,
            pool,
        );
        insert_result
            .slot
            .store(PShared::null().with_tag(1), Ordering::Release);

        // stable error
        Err((context_insert, insert_result_insert))
    }

    // TODO: memento
    fn move_if_resized<'g>(
        &'g self,
        context: PShared<'g, Context<K, V>>,
        insert_result: FindResult<'g, K, V>,
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        guard: &'g Guard,
        pool: &'g PoolHandle,
    ) {
        let mut res = self.move_if_resized_inner(
            context,
            insert_result,
            key_hashes,
            resize_send,
            guard,
            pool,
        );
        while let Err((context, insert_result)) = res {
            res = self.move_if_resized_inner(
                context, // stable by move_if_resized_inner
                insert_result,
                key_hashes,
                resize_send,
                guard,
                pool,
            );
        }
    }

    pub fn insert(
        &self,
        key: K,
        value: V,
        resize_send: &mpsc::Sender<()>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        let (key_tag, key_hashes) = hashes(&key);
        let (context, find_result) = self.find(&key, key_tag, key_hashes, guard, pool);
        // TODO: checkpoint: find_result
        if find_result.is_some() {
            return Err(InsertError::Occupied);
        }

        // TODO: checkpoint: slot (maybe with find_result)
        let slot = alloc_persist(Slot { key, value }, pool)
            .with_high_tag(key_tag as usize)
            .into_shared(guard);

        let (context_new, insert_result) =
            self.insert_inner(context, slot, key_hashes, resize_send, guard, pool);
        self.move_if_resized(
            context_new,   // stable by insert_inner
            insert_result, // stable by insert_inner
            key_hashes,
            resize_send,
            guard,
            pool,
        );
        Ok(())
    }

    pub fn delete(&self, key: &K, guard: &Guard, pool: &PoolHandle) -> bool {
        // // println!("[delete] key: {}", key);
        let (key_tag, key_hashes) = hashes(&key);
        loop {
            let (_, find_result) = self.find(key, key_tag, key_hashes, guard, pool);
            let find_result = some_or!(find_result, {
                // println!("[delete] suspicious...");
                return false;
            });

            if find_result
                .slot
                .compare_exchange(
                    find_result.slot_ptr,
                    PShared::null(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .is_err()
            {
                continue;
            }

            unsafe {
                guard.defer_pdestroy(find_result.slot_ptr);
            }
            // // println!("[delete] finish!");
            return true;
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
        pmem::Pool,
        test_utils::tests::{
            compose, decompose, get_test_abs_path, DummyRootMemento, DummyRootObj, TestRootObj,
        },
    };

    use super::*;

    use crossbeam_epoch::pin;
    use crossbeam_utils::thread;

    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    static mut SEND: Option<[Option<mpsc::Sender<()>>; 64]> = None;
    static mut RECV: Option<mpsc::Receiver<()>> = None;

    #[test]
    fn smoke() {
        const COUNT: usize = 100_000;
        let filepath = &get_test_abs_path("smoke");

        // open pool
        let pool_handle =
            unsafe { Pool::open::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE) }
                .unwrap_or_else(|_| {
                    Pool::create::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE, 1).unwrap()
                });

        let pool = global_pool().unwrap();
        let kv = Clevel::<usize, usize>::pdefault(pool);
        thread::scope(|s| {
            let (send, recv) = mpsc::channel();
            let kv = &kv;
            let _ = s.spawn(move |_| {
                let pool = global_pool().unwrap();
                let mut guard = pin();
                kv.resize_loop(&recv, &mut guard, pool);
            });

            let guard = pin();

            for i in 0..COUNT {
                assert!(kv.insert(i, i, &send, &guard, pool).is_ok());
                assert_eq!(kv.search(&i, &guard, pool), Some(&i));
            }

            for i in 0..COUNT {
                assert!(kv.delete(&i, &guard, pool));
                assert_eq!(kv.search(&i, &guard, pool), None);
            }
        })
        .unwrap();
    }

    #[test]
    fn insert_search() {
        const NR_THREAD: usize = 12;
        const COUNT: usize = 1_000;
        let filepath = &get_test_abs_path("insert_search");

        // open pool
        let pool_handle =
            unsafe { Pool::open::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE) }
                .unwrap_or_else(|_| {
                    Pool::create::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE, 1).unwrap()
                });

        let pool = global_pool().unwrap();
        let kv = Clevel::<usize, usize>::pdefault(pool);
        thread::scope(|s| {
            let (send, recv) = mpsc::channel();
            unsafe {
                SEND = Some(array_init::array_init(|_| None));
                RECV = Some(recv);
                for tid in 1..=NR_THREAD {
                    let sends = SEND.as_mut().unwrap();
                    sends[tid] = Some(send.clone());
                }
            }
            drop(send);

            let kv = &kv;
            let _ = s.spawn(move |_| {
                let mut guard = pin();
                let recv = unsafe { RECV.as_ref().unwrap() };
                kv.resize_loop(&recv, &mut guard, pool);
            });

            for tid in 1..=NR_THREAD {
                let _ = s.spawn(move |_| {
                    let pool = global_pool().unwrap();
                    let guard = pin();

                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..COUNT {
                        // // println!("[test] tid = {tid}, i = {i}, insert");
                        let _ = kv.insert(i, i, &send, &guard, pool);

                        // // println!("[test] tid = {tid}, i = {i}, search");
                        if kv.search(&i, &guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail n {i}");
                            // assert_eq!(kv.search(&i, &guard), Some(&i));
                        }
                    }
                });
            }
        })
        .unwrap();
    }

    #[test]
    fn clevel_ins_del_look() {
        const NR_THREAD: usize = 12;
        const COUNT: usize = 1_000_001;

        let filepath = &get_test_abs_path("insert_search");

        // open pool
        let pool_handle =
            unsafe { Pool::open::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE) }
                .unwrap_or_else(|_| {
                    Pool::create::<DummyRootObj, DummyRootMemento>(filepath, FILE_SIZE, 1).unwrap()
                });

        let pool = global_pool().unwrap();
        let kv = Clevel::<usize, usize>::pdefault(pool);
        thread::scope(|s| {
            let (send, recv) = mpsc::channel();
            unsafe {
                SEND = Some(array_init::array_init(|_| None));
                RECV = Some(recv);
                for tid in 1..=NR_THREAD {
                    let sends = SEND.as_mut().unwrap();
                    sends[tid] = Some(send.clone());
                }
            }
            drop(send);

            let kv = &kv;
            let _ = s.spawn(move |_| {
                let mut guard = pin();
                let recv = unsafe { RECV.as_ref().unwrap() };
                kv.resize_loop(&recv, &mut guard, pool);
            });

            for tid in 1..=NR_THREAD {
                let _ = s.spawn(move |_| {
                    let pool = global_pool().unwrap();
                    let guard = pin();

                    let send = unsafe { SEND.as_mut().unwrap()[tid].take().unwrap() };
                    for i in 0..COUNT {
                        let key = compose(tid, i, i % tid);

                        // insert and lookup
                        assert!(kv.insert(key, key, &send, &guard, pool).is_ok());
                        let res = kv.search(&key, &guard, pool);
                        assert!(res.is_some());

                        // transfer the lookup result to the result array
                        let (tid, i, value) = decompose(*res.unwrap());
                        // produce_res(tid, i, value);

                        // delete and lookup
                        assert!(kv.delete(&key, &guard, pool));
                        let res = kv.search(&key, &guard, pool);
                        assert!(res.is_none());
                    }
                });
            }
        })
        .unwrap();
    }
}

fn alloc_persist<T>(init: T, pool: &PoolHandle) -> POwned<T> {
    let ptr = POwned::new(init, pool);
    persist_obj(unsafe { ptr.deref(pool) }, true);
    ptr
}
