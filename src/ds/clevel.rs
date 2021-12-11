//! Concurrent Level Hash Table.
#![allow(missing_docs)]
#![allow(box_pointers)]
#![allow(unreachable_pub)]
#![recursion_limit = "512"]
use core::cmp;
use core::fmt::Debug;
use core::fmt::Display;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::{mpsc, Arc};

use cfg_if::cfg_if;
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};
use derivative::Derivative;
use etrace::*;
use hashers::fx_hash::FxHasher;
use itertools::*;
use parking_lot::{lock_api::RawMutex, RawMutex as RawMutexImpl};
use tinyvec::*;

use crate::pepoch::PAtomic;
use crate::pepoch::PDestroyable;
use crate::pepoch::POwned;
use crate::pepoch::PShared;
use crate::pmem::global_pool;
use crate::pmem::Collectable;
use crate::pmem::GarbageCollection;
use crate::pmem::PoolHandle;
use crate::Memento;
use crate::PDefault;

// Root obj
// TODO: persistent version으로 만들며 필드 재구성. 현재는 내부가 concurrent clevel을 사용하게끔 되있음
#[derive(Debug)]
pub struct PClevelInner<K, V> {
    kv: Clevel<K, V>,
    kv_resize: RefCell<ClevelResize<K, V>>,
}

impl<K, V> PDefault for PClevelInner<K, V>
where
    K: Debug,
    K: Display,
    K: PartialEq,
    K: Hash,
    V: Debug,
{
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let (kv, kv_resize) = Clevel::new(pool);
        Self {
            kv,
            kv_resize: RefCell::new(kv_resize),
        }
    }
}
impl<K, V> Collectable for PClevelInner<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K, V> PClevelInner<K, V>
where
    K: Debug,
    K: Display,
    K: PartialEq,
    K: Hash,
    V: Debug,
{
    pub fn search<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Option<&'g V> {
        self.kv.search(key, guard, pool)
    }

    pub fn get_capacity<'g>(&'g self, guard: &'g Guard, pool: &'static PoolHandle) -> usize {
        self.kv.get_capacity(guard, pool)
    }
}

#[derive(Debug, Clone)]
pub enum ModifyOp {
    Insert,
    Delete,
    Update,
}

#[derive(Debug)]
pub struct Modify<K, V> {
    insert: ClInsert<K, V>,
    delete: ClDelete<K, V>,
    update: ClUpdate<K, V>,
}

impl<K, V> Default for Modify<K, V> {
    fn default() -> Self {
        Self {
            insert: ClInsert::default(),
            delete: ClDelete::default(),
            update: ClUpdate::default(),
        }
    }
}

impl<K, V> Collectable for Modify<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K: 'static, V: 'static> Memento for Modify<K, V>
where
    K: 'static + Debug + Display + PartialEq + Hash + Clone,
    V: 'static + Debug + Clone,
{
    type Object<'o> = &'o PClevelInner<K, V>;
    type Input<'o> = (usize, ModifyOp, K, V);
    type Output<'o> = bool; // TODO: output도 enum으로 묶기?
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        (tid, op, k, v): Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let ret = match op {
            ModifyOp::Insert => self
                .insert
                .run(inner, (tid, k, v), rec, guard, pool)
                .is_ok(),
            ModifyOp::Delete => self.delete.run(inner, &k, rec, guard, pool).is_ok(),
            ModifyOp::Update => self
                .update
                .run(inner, (tid, k, v), rec, guard, pool)
                .is_ok(),
        };
        Ok(ret)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO
    }
}

#[derive(Debug)]
pub struct ResizeLoop<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ResizeLoop<K, V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<K, V> Collectable for ResizeLoop<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K: 'static + PartialEq + Hash, V: 'static> Memento for ResizeLoop<K, V> {
    type Object<'o> = &'o PClevelInner<K, V>;
    type Input<'o> = ();
    type Output<'o> = (); // TODO
    type Error<'o> = !; // TODO

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        input: Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        println!("[run resize loop]");
        let mut g = guard.clone(); // TODO: clone API 없어도 그냥 새로 pin하면 되지 않나?

        // TODO: persistent op
        inner.kv_resize.borrow_mut().resize_loop(&mut g, pool);
        Ok(())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

// TODO: memento
#[derive(Debug)]
struct ClInsert<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClInsert<K, V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<K, V> Collectable for ClInsert<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K, V> Memento for ClInsert<K, V>
where
    K: 'static + Debug + Display + PartialEq + Hash + Clone,
    V: 'static + Debug + Clone,
{
    type Object<'o> = &'o PClevelInner<K, V>;
    type Input<'o> = (usize, K, V); // tid, k, v
    type Output<'o> = (); // TODO
    type Error<'o> = InsertError; // TODO

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        (tid, k, v): Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        inner.kv.insert(tid, k, v, guard, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

// TODO: memento
#[derive(Debug)]
struct ClDelete<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClDelete<K, V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<K, V> Collectable for ClDelete<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K, V> Memento for ClDelete<K, V>
where
    K: 'static + Debug + Display + PartialEq + Hash + Clone,
    V: 'static + Debug + Clone,
{
    type Object<'o> = &'o PClevelInner<K, V>;
    type Input<'o> = &'o K;
    type Output<'o> = (); // TODO
    type Error<'o> = !; // TODO

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        k: Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        Ok(inner.kv.delete(&k, guard, pool))
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

// TODO: memento
#[derive(Debug)]
struct ClUpdate<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClUpdate<K, V> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<K, V> Collectable for ClUpdate<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl<K: 'static, V: 'static> Memento for ClUpdate<K, V>
where
    K: 'static + Debug + Display + PartialEq + Hash + Clone,
    V: 'static + Debug + Clone,
{
    type Object<'o> = &'o PClevelInner<K, V>;
    type Input<'o> = (usize, K, V); // tid, k, v
    type Output<'o> = (); // TODO
    type Error<'o> = (K, V); // TODO

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        (tid, k, v): Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        inner.kv.update(tid, k, v, guard, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

// -- 아래부터는 conccurent 버전. 이걸 persistent 버전으로 바꿔야함
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

fn hashes<T: Hash>(t: &T) -> [u32; 2] {
    let mut hasher = FxHasher::default();
    t.hash(&mut hasher);
    let hash = hasher.finish() as usize;

    // 32/32 비트로 쪼개서 반환
    let left = hash >> 32;
    let right = hash & ((1 << 32) - 1);
    debug_assert_eq!(hash, (left << 32) | right);

    [
        left as u32,
        if left != right {
            right as u32
        } else {
            right as u32 + 1
        },
    ]
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
struct Node<T: ?Sized> {
    data: Box<T>, // TODO: PAtomic
    next: PAtomic<Node<T>>,
}

#[derive(Debug)]
struct NodeIter<'g, T: ?Sized> {
    inner: PShared<'g, Node<T>>,
    last: PShared<'g, Node<T>>,
    guard: &'g Guard,
}

#[derive(Debug)]
struct Context<K, V> {
    first_level: PAtomic<Node<[MaybeUninit<Bucket<K, V>>]>>,
    last_level: PAtomic<Node<[MaybeUninit<Bucket<K, V>>]>>,

    /// Should resize until the last level's size > resize_size
    ///
    /// invariant: resize_size = first_level_size / 2 / 2
    resize_size: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClevelInner<K, V> {
    context: PAtomic<Context<K, V>>,

    #[derivative(Debug = "ignore")]
    add_level_lock: RawMutexImpl,
}

#[derive(Debug)]
pub struct Clevel<K, V> {
    inner: Arc<ClevelInner<K, V>>,
    resize_send: mpsc::Sender<()>,
}

#[derive(Debug)]
pub struct ClevelResize<K, V> {
    inner: Arc<ClevelInner<K, V>>,
    resize_recv: mpsc::Receiver<()>,
}

#[derive(Debug)]
struct FindResult<'g, K, V> {
    /// level's size
    size: usize,
    bucket_index: usize,
    slot: &'g PAtomic<Slot<K, V>>,
    slot_ptr: PShared<'g, Slot<K, V>>,
}

impl<'g, K, V> Default for FindResult<'g, K, V> {
    fn default() -> Self {
        Self {
            size: 0,
            bucket_index: 0,
            slot: unsafe { &*ptr::null() },
            slot_ptr: PShared::null(),
        }
    }
}

impl<'g, T: ?Sized + Debug> Iterator for NodeIter<'g, T> {
    type Item = &'g T;

    fn next(&mut self) -> Option<Self::Item> {
        let pool = global_pool().unwrap(); // TODO: global pool 안쓰기
        let inner_ref = unsafe { self.inner.as_ref(pool) }?;
        self.inner = if self.inner == self.last {
            PShared::null()
        } else {
            inner_ref.next.load(Ordering::Acquire, self.guard)
        };
        Some(&inner_ref.data)
    }
}

impl<K: PartialEq + Hash, V> Context<K, V> {
    pub fn level_iter<'g>(&'g self, guard: &'g Guard) -> NodeIter<'g, [MaybeUninit<Bucket<K, V>>]> {
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
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
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
                    // TODO: check 2-byte tag: slot_ptr's high 2 bytes should be the 2-byte LSB of key. otherwise, continue.

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
                        bucket_index: key_hash,
                        slot,
                        slot_ptr,
                    }));
                }
            }
        }

        if found_moved {
            // 1. the moved item may already have been removed by another thread.
            // 2. the being moved item may not yet been added again.
            //
            // so we cannot conclude neither we found an item nor we found none.
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
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
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
                    // TODO: check 2-byte tag

                    let slot_ref = some_or!(unsafe { slot_ptr.as_ref(pool) }, continue);
                    if *key != slot_ref.key {
                        continue;
                    }

                    found.push(FindResult {
                        size,
                        bucket_index: key_hash,
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

fn new_node<K, V>(size: usize, pool: &PoolHandle) -> POwned<Node<[MaybeUninit<Bucket<K, V>>]>> {
    println!("[new_node] size: {size}");

    // TODO: pallocation maybeuninit 잘 동작하나?
    POwned::new(
        Node {
            data: Box::new_zeroed_slice(size),
            next: PAtomic::null(),
        },
        pool,
    )
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
            for bucket in node_ref.data.iter() {
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

impl<K: PartialEq + Hash, V> ClevelInner<K, V> {
    fn add_level<'g>(
        &'g self,
        mut context: PShared<'g, Context<K, V>>,
        first_level: &'g Node<[MaybeUninit<Bucket<K, V>>]>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, bool) {
        println!("[add_level] hi");
        let next_level_size = level_size_next(first_level.data.len());

        // insert a new level to the next of the first level.
        let next_level = first_level.next.load(Ordering::Acquire, guard);
        let next_level = if !next_level.is_null() {
            next_level
        } else {
            self.add_level_lock.lock();
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
                    }
                    .data
                    .len()
                        >= next_level_size
                    {
                        return (context, false);
                    }

                    // TODO: maybe unreachable...
                    let context_new_ref = unsafe { context_new.deref(pool) };
                    context_new_ref.last_level.store(
                        context_ref.last_level.load(Ordering::Acquire, guard),
                        Ordering::Relaxed,
                    );
                    continue;
                }
            );

            println!("[add_level] next_level_size: {next_level_size}");
            break;
        }

        fence(Ordering::SeqCst);
        (context, true)
    }

    pub fn resize(&self, guard: &Guard, pool: &'static PoolHandle) {
        println!("[resize]");
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
            let mut context_ref = unsafe { context.deref(pool) };

            let last_level = context_ref.last_level.load(Ordering::Acquire, guard);
            let last_level_ref = unsafe { last_level.deref(pool) };
            let last_level_size = last_level_ref.data.len();

            // if we don't need to resize, break out.
            println!(
                "[reisze] reisze_size: {}, last_level_size: {}",
                context_ref.resize_size, last_level_size
            );
            if context_ref.resize_size < last_level_size {
                break;
            }

            let mut first_level = context_ref.first_level.load(Ordering::Acquire, guard);
            let mut first_level_ref = unsafe { first_level.deref(pool) };
            let mut first_level_size = first_level_ref.data.len();
            println!(
                "[resize] last_level_size: {last_level_size}, first_level_size: {first_level_size}"
            );

            for (bid, bucket) in last_level_ref.data.iter().enumerate() {
                for (sid, slot) in unsafe { bucket.assume_init_ref().slots.iter().enumerate() } {
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

                    // // println!("[resize] moving ({}, {}, {})...", last_level_size, bid, sid);

                    let mut moved = false;
                    loop {
                        let key_hashes = hashes(&unsafe { slot_ptr.deref(pool) }.key)
                            .into_iter()
                            .map(|key_hash| key_hash as usize % first_level_size)
                            .sorted()
                            .dedup();
                        for i in 0..SLOTS_IN_BUCKET {
                            for key_hash in key_hashes.clone() {
                                let slot = unsafe {
                                    first_level_ref.data[key_hash]
                                        .assume_init_ref()
                                        .slots
                                        .get_unchecked(i)
                                };

                                if let Some(slot) =
                                    unsafe { slot.load(Ordering::Acquire, guard).as_ref(pool) }
                                {
                                    // TODO: 2-byte tag checking

                                    if slot.key == unsafe { slot_ptr.deref(pool) }.key {
                                        moved = true;
                                        break;
                                    }
                                    continue;
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

                        println!(
                            "[resize] resizing again for ({}, {}, {})...",
                            last_level_size, bid, sid
                        );

                        // The first level is full. Resize and retry.
                        let (context_new, _) =
                            self.add_level(context, first_level_ref, guard, pool);
                        context = context_new;
                        context_ref = unsafe { context.deref(pool) };
                        first_level = context_ref.first_level.load(Ordering::Acquire, guard);
                        first_level_ref = unsafe { first_level.deref(pool) };
                        first_level_size = first_level_ref.data.len();
                    }
                }
            }

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

            println!("[resize] done!");
        }
    }
}

impl<K, V> Clone for Clevel<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            resize_send: self.resize_send.clone(),
        }
    }
}

impl<K: Debug + Display + PartialEq + Hash, V: Debug> ClevelInner<K, V> {
    fn find_fast<'g>(
        &'g self,
        key: &K,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find_fast(key, key_hashes, guard, pool);
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
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, Option<FindResult<'g, K, V>>) {
        let mut context = self.context.load(Ordering::Acquire, guard);
        loop {
            let context_ref = unsafe { context.deref(pool) };
            let find_result = context_ref.find(key, key_hashes, guard, pool);
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

    fn insert_inner<'g>(
        &'g self,
        tid: usize,
        context: PShared<'g, Context<K, V>>,
        slot_new: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Result<FindResult<'g, K, V>, ()> {
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

                    if let Ok(slot_ptr) = slot.compare_exchange(
                        PShared::null(),
                        slot_new,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        return Ok(FindResult {
                            size,
                            bucket_index: key_hash,
                            slot,
                            slot_ptr,
                        });
                    }
                }
            }
        }

        Err(())

        // println!("[insert_inner] tid = {tid}, key = {}, count = {}, level = {}, bucket index = {}, slot index = {}, slot = {:?}", unsafe { slot_new.deref() }.key, found.0, found.1, found.2, index, slot as *const _);
    }
}

#[derive(Debug, Clone)]
pub enum InsertError {
    Occupied,
}

impl<K: Debug + Display + PartialEq + Hash, V: Debug> Clevel<K, V> {
    pub fn new(pool: &PoolHandle) -> (Self, ClevelResize<K, V>) {
        let guard = unsafe { unprotected() };

        let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
        let last_level = new_node(MIN_SIZE, pool);
        let last_level_ref = unsafe { last_level.deref(pool) };
        last_level_ref.next.store(first_level, Ordering::Relaxed);
        let inner = Arc::new(ClevelInner {
            context: PAtomic::new(
                Context {
                    first_level: first_level.into(),
                    last_level: last_level.into(),
                    resize_size: 0,
                },
                pool,
            ),
            add_level_lock: RawMutexImpl::INIT,
        });
        let (resize_send, resize_recv) = mpsc::channel();
        (
            Self {
                inner: inner.clone(),
                resize_send,
            },
            ClevelResize { inner, resize_recv },
        )
    }

    pub fn get_capacity<'g>(&'g self, guard: &'g Guard, pool: &'static PoolHandle) -> usize {
        let context = self.inner.context.load(Ordering::Acquire, guard);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);
        let first_level = context_ref.first_level.load(Ordering::Relaxed, guard);

        (unsafe { first_level.deref(pool).data.len() * 2 - last_level.deref(pool).data.len() })
            * SLOTS_IN_BUCKET
    }

    pub fn search<'g>(
        &'g self,
        key: &K,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Option<&'g V> {
        let key_hashes = hashes(key);
        let (_, find_result) = self.inner.find_fast(key, key_hashes, guard, pool);
        Some(&unsafe { find_result?.slot_ptr.deref(pool) }.value)
    }

    fn insert_inner<'g>(
        &'g self,
        tid: usize,
        mut context: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Slot<K, V>>,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        loop {
            if let Ok(result) = self
                .inner
                .insert_inner(tid, context, slot, key_hashes, guard, pool)
            {
                return (context, result);
            }

            // No remaining slots. Resize.
            // println!("[insert] tid = {tid} triggering resize");
            let context_ref = unsafe { context.deref(pool) };
            let first_level = context_ref.first_level.load(Ordering::Acquire, guard);
            let first_level_ref = unsafe { first_level.deref(pool) };
            let (context_new, added) = self.inner.add_level(context, first_level_ref, guard, pool);
            if added {
                let _ = self.resize_send.send(());
            }
            context = context_new;
        }
    }

    fn move_if_resized<'g>(
        &'g self,
        tid: usize,
        mut context: PShared<'g, Context<K, V>>,
        mut insert_result: FindResult<'g, K, V>,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) {
        loop {
            // If the inserted slot is being resized, try again.
            fence(Ordering::SeqCst);

            // If the context remains the same, it's done.
            let mut context_new = self.inner.context.load(Ordering::Acquire, guard);
            if context == context_new {
                return;
            }

            // If the inserted array is not being resized, it's done.
            let context_new_ref = unsafe { context_new.deref(pool) };
            if context_new_ref.resize_size < insert_result.size {
                break;
            }

            // Move the slot if the slot is not already (being) moved.
            //
            // the resize thread may already have passed the slot. I need to move it.
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
                break;
            }

            // println!(
            //     "[insert] tid = {tid} inserted {} to resized array ({}, {}). move.",
            //     unsafe { insert_result.slot_ptr.deref() }.key,
            //     insert_result.size,
            //     insert_result.bucket_index
            // );
            let (context_insert, insert_result_insert) = self.insert_inner(
                tid,
                context_new,
                insert_result.slot_ptr,
                key_hashes,
                guard,
                pool,
            );
            insert_result
                .slot
                .store(PShared::null().with_tag(1), Ordering::Release);
            context = context_insert;
            insert_result = insert_result_insert;
        }
    }

    pub fn insert(
        &self,
        tid: usize,
        key: K,
        value: V,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        println!("[insert] tid: {tid} do insert");
        // println!("[insert] tid: {}, key: {}", tid, key);
        let key_hashes = hashes(&key);
        let (context, find_result) = self.inner.find(&key, key_hashes, guard, pool);
        if find_result.is_some() {
            return Err(InsertError::Occupied);
        }

        let slot = POwned::new(Slot { key, value }, pool).into_shared(guard);
        // question: why `context_new` is created?
        let (context_new, insert_result) =
            self.insert_inner(tid, context, slot, key_hashes, guard, pool);
        self.move_if_resized(tid, context_new, insert_result, key_hashes, guard, pool);
        Ok(())
    }

    pub fn update(
        &self,
        tid: usize,
        key: K,
        value: V,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), (K, V)>
    where
        K: Clone,
    {
        let key_hashes = hashes(&key);
        let mut slot_new = POwned::new(
            Slot {
                key: key.clone(),
                value,
            },
            pool,
        );

        loop {
            let (context, find_result) = self.inner.find(&key, key_hashes, guard, pool);
            let find_result = some_or!(find_result, {
                let slot_ref = unsafe { slot_new.deref(pool) };
                // TODO: 이렇게 k,v 리턴하면 안됨. 그냥 update 실패 리턴값 없애자
                let (k, v) = (slot_ref.key.clone(), unsafe { ptr::read(&slot_ref.value) });
                return Err((k, v));
            });

            if let Err(e) = find_result.slot.compare_exchange(
                find_result.slot_ptr,
                slot_new,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                slot_new = e.new;
                continue;
            }

            unsafe {
                guard.defer_pdestroy(find_result.slot_ptr);
            }
            self.move_if_resized(tid, context, find_result, key_hashes, guard, pool);
            return Ok(());
        }
    }

    pub fn delete(&self, key: &K, guard: &Guard, pool: &'static PoolHandle) {
        // println!("[delete] key: {}", key);
        let key_hashes = hashes(&key);
        loop {
            let (_, find_result) = self.inner.find(key, key_hashes, guard, pool);
            let find_result = some_or!(find_result, {
                println!("[delete] suspicious...");
                return;
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
            // println!("[delete] finish!");
            return;
        }
    }
}

impl<K: PartialEq + Hash, V> ClevelResize<K, V> {
    pub fn resize_loop(&mut self, guard: &mut Guard, pool: &'static PoolHandle) {
        println!("[resize loop] start loop");
        while let Ok(()) = self.resize_recv.recv() {
            println!("[resize_loop] do resize!");
            self.inner.resize(guard, pool);
            guard.repin_after(|| {});
        }
    }
}

// TODO: 테스트도 컴파일시키기
// #[cfg(test)]
// mod tests {
//     use super::*;

//     use crossbeam_epoch::pin;
//     use crossbeam_utils::thread;

//     #[test]
//     fn smoke() {
//         thread::scope(|s| {
//             let (kv, mut kv_resize) = Clevel::<usize, usize>::new();
//             let _ = s.spawn(move |_| {
//                 let mut guard = pin();
//                 kv_resize.resize_loop(&mut guard);
//             });

//             let guard = pin();

//             const RANGE: usize = 1usize << 8;

//             for i in 0..RANGE {
//                 let _ = kv.insert(0, i, i, &guard);
//                 assert_eq!(kv.search(&i, &guard), Some(&i));

//                 let _ = kv.update(0, i, i + RANGE, &guard);
//                 assert_eq!(kv.search(&i, &guard), Some(&(i + RANGE)));
//             }

//             for i in 0..RANGE {
//                 assert_eq!(kv.search(&i, &guard), Some(&(i + RANGE)));
//                 kv.delete(&i, &guard);
//                 assert_eq!(kv.search(&i, &guard), None);
//             }
//         })
//         .unwrap();
//     }

//     #[test]
//     fn insert_search() {
//         thread::scope(|s| {
//             let (kv, mut kv_resize) = Clevel::<usize, usize>::new();
//             let _ = s.spawn(move |_| {
//                 let mut guard = pin();
//                 kv_resize.resize_loop(&mut guard);
//             });

//             const THREADS: usize = 1usize << 4;
//             const RANGE: usize = 1usize << 6;
//             for tid in 0..THREADS {
//                 let kv = kv.clone();
//                 let _ = s.spawn(move |_| {
//                     let guard = pin();
//                     for i in 0..RANGE {
//                         // println!("[test] tid = {tid}, i = {i}, insert");
//                         let _ = kv.insert(tid, i, i, &guard);

//                         // println!("[test] tid = {tid}, i = {i}, search");
//                         if kv.search(&i, &guard) != Some(&i) {
//                             panic!("[test] tid = {tid} fail on {i}");
//                             // assert_eq!(kv.search(&i, &guard), Some(&i));
//                         }
//                     }
//                 });
//             }
//         })
//         .unwrap();
//     }

//     #[test]
//     fn insert_update_search() {
//         thread::scope(|s| {
//             let (kv, mut kv_resize) = Clevel::<usize, usize>::new();
//             let _ = s.spawn(move |_| {
//                 let mut guard = pin();
//                 kv_resize.resize_loop(&mut guard);
//             });

//             const THREADS: usize = 1usize << 4;
//             const RANGE: usize = 1usize << 6;
//             for tid in 0..THREADS {
//                 let kv = kv.clone();
//                 let _ = s.spawn(move |_| {
//                     let guard = pin();
//                     for i in 0..RANGE {
//                         // println!("[test] tid = {tid}, i = {i}, insert");
//                         let _ = kv.insert(tid, i, i, &guard);

//                         // println!("[test] tid = {tid}, i = {i}, update");
//                         let _ = kv.insert(tid, i, i + RANGE, &guard);

//                         // println!("[test] tid = {tid}, i = {i}, search");
//                         if kv.search(&i, &guard) != Some(&i)
//                             && kv.search(&i, &guard) != Some(&(i + RANGE))
//                         {
//                             panic!("[test] tid = {tid} fail on {i}");
//                         }
//                     }
//                 });
//             }
//         })
//         .unwrap();
//     }
// }
