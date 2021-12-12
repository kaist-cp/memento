//! Concurrent Level Hash Table.
#![allow(missing_docs)]
#![allow(box_pointers)]
#![allow(unreachable_pub)]
use core::cmp;
use core::fmt::Debug;
use core::fmt::Display;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{fence, Ordering};
use std::marker::PhantomData;
use std::mem;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc};

use cfg_if::cfg_if;
use crossbeam_epoch::{unprotected, Guard};
use derivative::Derivative;
use etrace::*;
use fasthash::Murmur3HasherExt;
use itertools::*;
use libc::c_void;
use parking_lot::{lock_api::RawMutex, RawMutex as RawMutexImpl};
use tinyvec::*;

use crate::node::Node;
use crate::pepoch::PShared;
use crate::pepoch::{PAtomic, PDestroyable, POwned};
use crate::ploc::Delete;
use crate::ploc::Insert;
use crate::ploc::NeedRetry;
use crate::ploc::SMOAtomic;
use crate::ploc::Traversable;
use crate::ploc::Update;
use crate::ploc::UpdateDeleteInfo;
use crate::pmem::global_pool;
use crate::pmem::persist_obj;
use crate::pmem::Collectable;
use crate::pmem::GarbageCollection;
use crate::pmem::PoolHandle;
use crate::Memento;
use crate::PDefault;

impl<K, V> PDefault for ClevelInner<K, V>
where
    K: Debug,
    K: Display,
    K: PartialEq,
    K: Hash,
    V: Debug,
{
    fn pdefault(pool: &'static PoolHandle) -> Self {
        let guard = unsafe { unprotected() }; // SAFE for initialization

        let first_level = new_node(level_size_next(MIN_SIZE), pool).into_shared(guard);
        let last_level = new_node(MIN_SIZE, pool);
        let last_level_ref = unsafe { last_level.deref(pool) };
        last_level_ref.next.store(first_level, Ordering::Relaxed);
        persist_obj(&last_level_ref.next, true); // TODO(opt): false

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

impl<K, V> Collectable for ClevelInner<K, V> {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

// TODO: 나중에 통합해서 벤치에 연결
// #[derive(Debug, Clone)]
// pub enum ModifyOp {
//     Insert,
//     Delete,
//     Update,
// }

// #[derive(Debug)]
// pub struct Modify<K, V> {
//     insert: ClInsert<K, V>,
//     delete: ClDelete<K, V>,
//     update: ClUpdate<K, V>,
// }

// impl<K, V> Default for Modify<K, V> {
//     fn default() -> Self {
//         Self {
//             insert: ClInsert::default(),
//             delete: ClDelete::default(),
//             update: ClUpdate::default(),
//         }
//     }
// }

// impl<K, V> Collectable for Modify<K, V> {
//     fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
//         todo!()
//     }
// }

// impl<K: 'static, V: 'static> Memento for Modify<K, V>
// where
//     K: 'static + Debug + Display + PartialEq + Hash + Clone,
//     V: 'static + Debug + Clone,
// {
//     type Object<'o> = &'o ClevelInner<K, V>;
//     type Input<'o> = (usize, ModifyOp, K, V);
//     type Output<'o> = bool; // TODO: output도 enum으로 묶기?
//     type Error<'o> = !;

//     fn run<'o>(
//         &mut self,
//         inner: Self::Object<'o>,
//         (tid, op, k, v): Self::Input<'o>,
//         rec: bool,
//         guard: &'o Guard,
//         pool: &'static PoolHandle,
//     ) -> Result<Self::Output<'o>, Self::Error<'o>> {
//         let ret = match op {
//             ModifyOp::Insert => self
//                 .insert
//                 .run(inner, (tid, k, v), rec, guard, pool)
//                 .is_ok(),
//             ModifyOp::Delete => self.delete.run(inner, &k, rec, guard, pool).is_ok(),
//             ModifyOp::Update => self
//                 .update
//                 .run(inner, (tid, k, v), rec, guard, pool)
//                 .is_ok(),
//         };
//         Ok(ret)
//     }

//     fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
//         // TODO
//     }
// }

// TODO: for inser, update, resize
trait InsertInner<K, V> {
    fn insert_inner(
        &mut self,
    ) -> &mut Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>>;
}

// TODO: 리커버리 런이면 무조건 한 번 돌리고, 아니면 기다리고 있음.
#[derive(Debug)]
pub struct ResizeLoop<K, V> {
    insert_inner: Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ResizeLoop<K, V> {
    fn default() -> Self {
        Self {
            insert_inner: Default::default(),
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
    type Object<'o> = &'o ClevelInner<K, V>;
    type Input<'o> = &'o mpsc::Receiver<()>; // TODO: receiver clone이 안 됨 global로 해야할 듯
    type Output<'o> = ();
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        resize_recv: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        let mut g = guard.clone();
        println!("[resize loop] start loop");
        while let Ok(()) = resize_recv.recv() {
            println!("[resize_loop] do resize!");
            inner.resize(self, &mut g, pool);
            g.repin_after(|| {}); // TODO: drop?
        }
        Ok(())

        // let mut g = guard.clone(); // TODO: clone API 없어도 그냥 새로 pin하면 되지 않나?

        // TODO: persistent op
        // inner.kv_resize.borrow_mut().resize_loop(&mut g, pool);
        // Ok(())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO
    }
}

impl<K, V> InsertInner<K, V> for ResizeLoop<K, V> {
    fn insert_inner(
        &mut self,
    ) -> &mut Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>> {
        &mut self.insert_inner
    }
}

impl<K, V> Traversable<Node<Slot<K, V>>> for SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>> {
    fn search(
        &self,
        target: PShared<'_, Node<Slot<K, V>>>,
        guard: &Guard,
        pool: &PoolHandle,
    ) -> bool {
        let cur = self.load(guard, pool);
        cur.as_ptr() == target.as_ptr()
    }
}

#[derive(Debug)]
pub struct ClInsert<K, V> {
    insert_inner: Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClInsert<K, V> {
    fn default() -> Self {
        Self {
            insert_inner: Default::default(),
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
    type Object<'o> = &'o ClevelInner<K, V>;
    type Input<'o> = (usize, K, V, &'o mpsc::Sender<()>); // tid, k, v
    type Output<'o> = ();
    type Error<'o> = InsertError;

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        (tid, k, v, resize_send): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        Clevel::insert(self, inner, tid, k, v, resize_send, rec, guard, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO
    }
}

impl<K, V> InsertInner<K, V> for ClInsert<K, V> {
    fn insert_inner(
        &mut self,
    ) -> &mut Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>> {
        &mut self.insert_inner
    }
}

#[derive(Debug)]
pub struct ClDelete<K, V> {
    delete: Delete<(), Node<Slot<K, V>>, Bucket<K, V>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClDelete<K, V> {
    fn default() -> Self {
        Self {
            delete: Default::default(),
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
    type Object<'o> = &'o ClevelInner<K, V>;
    type Input<'o> = &'o K;
    type Output<'o> = ();
    type Error<'o> = !;

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        k: Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        Clevel::delete(self, inner, &k, guard, pool);
        Ok(())
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO
    }
}

#[derive(Debug)]
pub struct ClUpdate<K, V> {
    insert_inner: Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for ClUpdate<K, V> {
    fn default() -> Self {
        Self {
            insert_inner: Default::default(),
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
    type Object<'o> = &'o ClevelInner<K, V>;
    type Input<'o> = (usize, K, V, &'o mpsc::Sender<()>); // tid, k, v
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        inner: Self::Object<'o>,
        (tid, k, v, resize_send): Self::Input<'o>,
        rec: bool,
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        // TODO: persistent op
        Clevel::update(self, inner, tid, k, v, guard, rec, resize_send, pool)
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        // TODO
    }
}

impl<K, V> InsertInner<K, V> for ClUpdate<K, V> {
    fn insert_inner(
        &mut self,
    ) -> &mut Insert<SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>, Node<Slot<K, V>>> {
        &mut self.insert_inner
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

fn hashes<T: Hash>(t: &T) -> (u16, [u32; 2]) {
    let mut hasher = Murmur3HasherExt::default();
    t.hash(&mut hasher);
    let hash = hasher.finish() as usize;

    let tag = hash.rotate_left(16) as u16;
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
    slots: [SMOAtomic<(), Node<Slot<K, V>>, Self>; SLOTS_IN_BUCKET],
}

impl<K, V> UpdateDeleteInfo<(), Node<Slot<K, V>>> for Bucket<K, V> {
    fn prepare_delete<'g>(
        cur: PShared<'_, Node<Slot<K, V>>>,
        expected: PShared<'_, Node<Slot<K, V>>>,
        obj: &(),
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> Result<Option<PShared<'g, Node<Slot<K, V>>>>, NeedRetry> {
        if cur == expected {
            Ok(Some(PShared::null()))
        } else {
            Err(NeedRetry)
        }
    }

    fn prepare_update<'g>(
        cur: PShared<'_, Node<Slot<K, V>>>,
        expected: PShared<'_, Node<Slot<K, V>>>,
        obj: &(),
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> bool {
        todo!()
    }

    fn node_when_deleted<'g>(
        deleted: PShared<'_, Node<Slot<K, V>>>,
        guard: &'g Guard,
        pool: &PoolHandle,
    ) -> PShared<'g, Node<Slot<K, V>>> {
        PShared::null()
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
    /// invariant: resize_size = level_size_prev(level_size_prev(first_level_size))
    resize_size: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ClevelInner<K, V> {
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
struct FindResult<'g, K, V> {
    /// level's size
    size: usize,
    bucket_index: usize,
    slot: &'g SMOAtomic<(), Node<Slot<K, V>>, Bucket<K, V>>,
    slot_ptr: PShared<'g, Node<Slot<K, V>>>,
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
    /// `Ok` means we found something (may not be unique); and `Err` means contention.
    fn find_fast<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
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
                    let slot_ptr = slot.load(guard, pool);

                    // check 2-byte tag
                    if slot_ptr.high_tag() != key_tag as usize {
                        continue;
                    }

                    let slot_ref = &some_or!(unsafe { slot_ptr.as_ref(pool) }, continue).data;
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
                        slot: slot,
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

    /// `Ok` means we found a unique tem (by deduplication); and `Err` means contention.
    fn find<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
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
                    let slot_ptr = slot.load(guard, pool);

                    // check 2-byte tag
                    if slot_ptr.high_tag() != key_tag as usize {
                        continue;
                    }

                    let slot_ref = &some_or!(unsafe { slot_ptr.as_ref(pool) }, continue).data;
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
        // TODO: store tag는 async persist 하고 여기서 fence

        // TODO: delete reset -> run
        // last is the find result to return.
        // remove everything else.
        for find_result in owned_found.into_iter() {
            // caution: we need **strong** CAS to guarantee uniqueness. maybe next time...
            // TODO(slot)
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

fn new_node<K, V>(
    size: usize,
    pool: &PoolHandle,
) -> POwned<Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>> {
    println!("[new_node] size: {size}");

    let data = POwned::<[MaybeUninit<Bucket<K, V>>]>::init(size, &pool);
    let data_ref = unsafe { data.deref(pool) };
    unsafe {
        let _ = libc::memset(
            data_ref as *const _ as *mut c_void,
            0x0,
            size * mem::size_of::<Bucket<K, V>>(),
        );
    }
    persist_obj(&data_ref, true);

    POwned::new(Node::from(PAtomic::from(data)), pool)
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
                    let slot_ptr = slot.load(guard, pool);
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

impl<K: 'static + PartialEq + Hash, V: 'static> ClevelInner<K, V> {
    fn add_level<'g, A: InsertInner<K, V>>(
        &'g self,
        client: &mut A,
        mut context: PShared<'g, Context<K, V>>,
        first_level: &'g Node<PAtomic<[MaybeUninit<Bucket<K, V>>]>>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, bool) {
        let first_level_data =
            unsafe { first_level.data.load(Ordering::Relaxed, guard).deref(pool) };
        let next_level_size = level_size_next(first_level_data.len());

        // insert a new level to the next of the first level.
        let next_level = first_level.next.load(Ordering::Acquire, guard);
        let next_level = if !next_level.is_null() {
            next_level
        } else {
            self.add_level_lock.lock(); // TODO: persistent spin lock
            let next_level = first_level.next.load(Ordering::Acquire, guard);
            let next_level = if !next_level.is_null() {
                next_level
            } else {
                let next_node = new_node(next_level_size, pool);
                // TODO: Should we use `Insert` for this?
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
            let res = self.context.compare_exchange(
                context,
                context_new,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            );

            if let Err(e) = res {
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

                let context_new_ref = unsafe { context_new.deref(pool) };
                context_new_ref.last_level.store(
                    context_ref.last_level.load(Ordering::Acquire, guard),
                    Ordering::Relaxed,
                );
                continue;
            }

            context = res.unwrap();
            println!("[add_level] next_level_size: {next_level_size}");
            break;
        }

        fence(Ordering::SeqCst);
        (context, true)
    }

    pub fn resize(&self, client: &mut ResizeLoop<K, V>, guard: &Guard, pool: &'static PoolHandle) {
        println!("[resize]");
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
            println!(
                "[resize] resize_size: {}, last_level_size: {}",
                context_ref.resize_size, last_level_size
            );
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
            println!(
                "[resize] last_level_size: {last_level_size}, first_level_size: {first_level_size}"
            );

            for (bid, bucket) in last_level_data.iter().enumerate() {
                for (sid, slot) in unsafe { bucket.assume_init_ref().slots.iter().enumerate() } {
                    let slot_ptr = some_or!(
                        {
                            let mut slot_ptr = slot.load(guard, pool);
                            loop {
                                if slot_ptr.is_null() {
                                    break None;
                                }

                                // tagged with 1 by concurrent move_if_resized(). we should wait for the item to be moved before changing context.
                                // example: insert || lookup (1); lookup (2), maybe lookup (1) can see the insert while lookup (2) doesn't.
                                // TODO: should we do it...?
                                if slot_ptr.tag() == 1 {
                                    slot_ptr = slot.load(guard, pool);
                                    continue;
                                }

                                // TODO(slot)
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
                        let (key_tag, key_hashes) =
                            hashes(&unsafe { slot_ptr.deref(pool) }.data.key);
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

                                let slot_first_level = slot.load(guard, pool);
                                if let Some(slot) = unsafe { slot_first_level.as_ref(pool) } {
                                    // 2-byte tag checking
                                    if slot_first_level.high_tag() != key_tag as usize {
                                        continue;
                                    }

                                    if slot.data.key != unsafe { slot_ptr.deref(pool) }.data.key {
                                        continue;
                                    }

                                    moved = true;
                                    break;
                                }

                                // TODO(check slot): before
                                // if slot
                                //     .compare_exchange(
                                //         PShared::null(),
                                //         slot_ptr,
                                //         Ordering::AcqRel,
                                //         Ordering::Relaxed,
                                //         guard,
                                //     )
                                //     .is_ok()
                                // {
                                //     moved = true;
                                //     break;
                                // }

                                // TODO(check slot): after
                                let move_insert = client.insert_inner();
                                if move_insert
                                    .run(slot, (slot_ptr, slot, |_| true), false, guard, pool) // TODO(must): normal run을 가정함
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
                            self.add_level(client, context, first_level_ref, guard, pool);
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

impl<K: 'static + Debug + Display + PartialEq + Hash, V: 'static + Debug> ClevelInner<K, V> {
    fn find_fast<'g>(
        &'g self,
        key: &K,
        key_tag: u16,
        key_hashes: [u32; 2],
        guard: &'g Guard,
        pool: &'static PoolHandle,
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
        pool: &'static PoolHandle,
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

    fn insert_inner<'g, C: InsertInner<K, V>>(
        &'g self,
        tid: usize,
        client: &mut C,
        context: PShared<'g, Context<K, V>>,
        slot_new: PShared<'g, Node<Slot<K, V>>>,
        key_hashes: [u32; 2],
        rec: bool,
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

                    if !slot.load(guard, pool).is_null() {
                        continue;
                    }

                    // TODO(check slot): before
                    // if let Ok(slot_ptr) = slot.compare_exchange(
                    //     PShared::null(),
                    //     slot_new,
                    //     Ordering::AcqRel,
                    //     Ordering::Relaxed,
                    //     guard,
                    // ) {
                    //     return Ok(FindResult {
                    //         size,
                    //         bucket_index: key_hash,
                    //         slot,
                    //         slot_ptr,
                    //     });
                    // }

                    // TODO(check slot): after
                    let insert = client.insert_inner();
                    if insert
                        .run(slot, (slot_new, slot, |_| true), false, guard, pool)
                        .is_ok()
                    {
                        // TODO(must): normal run을 가정함
                        return Ok(FindResult {
                            size,
                            bucket_index: key_hash,
                            slot,
                            slot_ptr: slot_new,
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

impl<K: 'static + Debug + Display + PartialEq + Hash, V: 'static + Debug> Clevel<K, V> {
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

    pub fn get_capacity<'g>(
        inner: &'g ClevelInner<K, V>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> usize {
        let context = inner.context.load(Ordering::Acquire, guard);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);
        let first_level = context_ref.first_level.load(Ordering::Relaxed, guard);

        (unsafe {
            first_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
                .len()
                * 2
                - last_level
                    .deref(pool)
                    .data
                    .load(Ordering::Relaxed, guard)
                    .deref(pool)
                    .len()
        }) * SLOTS_IN_BUCKET
    }

    pub fn is_resizing<'g>(
        inner: &'g ClevelInner<K, V>,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> bool {
        let context = inner.context.load(Ordering::Acquire, guard);
        let context_ref = unsafe { context.deref(pool) };
        let last_level = context_ref.last_level.load(Ordering::Relaxed, guard);
        let resize_size = context_ref.resize_size;

        (unsafe {
            last_level
                .deref(pool)
                .data
                .load(Ordering::Relaxed, guard)
                .deref(pool)
                .len()
        }) <= context_ref.resize_size
    }

    pub fn search<'g>(
        inner: &'g ClevelInner<K, V>,
        key: &K,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> Option<&'g V> {
        let (key_tag, key_hashes) = hashes(key);
        let (_, find_result) = inner.find_fast(key, key_tag, key_hashes, guard, pool);
        Some(&unsafe { find_result?.slot_ptr.deref(pool) }.data.value)
    }

    fn move_if_resized<'g, C: InsertInner<K, V>>(
        client: &mut C,
        inner: &'g ClevelInner<K, V>,
        tid: usize,
        mut context: PShared<'g, Context<K, V>>,
        mut insert_result: FindResult<'g, K, V>,
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        rec: bool,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) {
        loop {
            // If the inserted slot is being resized, try again.
            fence(Ordering::SeqCst);

            // If the context remains the same, it's done.
            let context_new = inner.context.load(Ordering::Acquire, guard);
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
            let (context_insert, insert_result_insert) = Self::insert_inner(
                client,
                inner,
                tid,
                context_new,
                insert_result.slot_ptr,
                key_hashes,
                resize_send,
                rec,
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
        client: &mut ClInsert<K, V>,
        inner: &ClevelInner<K, V>,
        tid: usize,
        key: K,
        value: V,
        resize_send: &mpsc::Sender<()>,
        rec: bool,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) -> Result<(), InsertError>
    where
        V: Clone,
    {
        // println!("[insert] tid: {}, key: {}", tid, key);
        let (key_tag, key_hashes) = hashes(&key);
        let (context, find_result) = inner.find(&key, key_tag, key_hashes, guard, pool);
        if find_result.is_some() {
            return Err(InsertError::Occupied);
        }

        let slot = POwned::new(Node::from(Slot { key, value }), pool)
            .with_high_tag(key_tag as usize)
            .into_shared(guard);
        // question: why `context_new` is created?
        let (context_new, insert_result) = Self::insert_inner(
            client,
            inner,
            tid,
            context,
            slot,
            key_hashes,
            resize_send,
            rec,
            guard,
            pool,
        );
        Self::move_if_resized(
            client,
            inner,
            tid,
            context_new,
            insert_result,
            key_hashes,
            resize_send,
            rec,
            guard,
            pool,
        );
        Ok(())
    }

    fn insert_inner<'g, C: InsertInner<K, V>>(
        client: &mut C,
        inner: &'g ClevelInner<K, V>,
        tid: usize,
        mut context: PShared<'g, Context<K, V>>,
        slot: PShared<'g, Node<Slot<K, V>>>,
        key_hashes: [u32; 2],
        resize_send: &mpsc::Sender<()>,
        rec: bool,
        guard: &'g Guard,
        pool: &'static PoolHandle,
    ) -> (PShared<'g, Context<K, V>>, FindResult<'g, K, V>) {
        loop {
            if let Ok(result) =
                inner.insert_inner(tid, client, context, slot, key_hashes, rec, guard, pool)
            {
                return (context, result);
            }

            // No remaining slots. Resize.
            // println!("[insert] tid = {tid} triggering resize");
            let context_ref = unsafe { context.deref(pool) };
            let first_level = context_ref.first_level.load(Ordering::Acquire, guard);
            let first_level_ref = unsafe { first_level.deref(pool) };
            let (context_new, added) =
                inner.add_level(client, context, first_level_ref, guard, pool);
            if added {
                let _ = resize_send.send(()); // TODO: channel
            }
            context = context_new;
        }
    }

    pub fn update<'g>(
        client: &mut ClUpdate<K, V>,
        inner: &'g ClevelInner<K, V>,
        tid: usize,
        key: K,
        value: V,
        guard: &Guard,
        rec: bool,
        resize_send: &mpsc::Sender<()>,
        pool: &'static PoolHandle,
    ) -> Result<(), ()>
    where
        K: Clone,
    {
        let (key_tag, key_hashes) = hashes(&key);
        let mut slot_new = POwned::new(
            Node::from(Slot {
                key: key.clone(),
                value,
            }),
            pool,
        )
        .with_high_tag(key_tag as usize);

        loop {
            let (context, find_result) = inner.find(&key, key_tag, key_hashes, guard, pool);
            let find_result = some_or!(find_result, {
                return Err(());
            });

            // TODO(slot)
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
            Self::move_if_resized(
                client,
                inner,
                tid,
                context,
                find_result,
                key_hashes,
                resize_send,
                rec,
                guard,
                pool,
            );
            return Ok(());
        }
    }

    pub fn delete<'g>(
        client: &mut ClDelete<K, V>,
        inner: &'g ClevelInner<K, V>,
        key: &K,
        guard: &Guard,
        pool: &'static PoolHandle,
    ) {
        // println!("[delete] key: {}", key);
        let (key_tag, key_hashes) = hashes(&key);
        loop {
            let (_, find_result) = inner.find(key, key_tag, key_hashes, guard, pool);
            let find_result = some_or!(find_result, {
                println!("[delete] suspicious...");
                return;
            });

            // TODO(check slot): before
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

            // TODO(check slot): after
            let res =
                client
                    .delete
                    .run(find_result.slot, (find_result.slot_ptr, &()), false, guard, pool); // TODO(must): normal run을 가정함

            if res.is_err() {
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

// TODO: 테스트도 컴파일시키기
#[cfg(test)]
mod tests {
    use std::sync::mpsc::{channel, Sender};

    use crate::test_utils::tests::{run_test, TestRootMemento, TestRootObj};

    use super::*;

    use crossbeam_epoch::pin;
    use crossbeam_utils::thread;

    impl TestRootObj for ClevelInner<usize, usize> {}

    static mut SEND: Option<Vec<Sender<()>>> = None;
    static mut RECV: Option<Receiver<()>> = None;

    struct Smoke {
        insert: ClInsert<usize, usize>,
        update: ClUpdate<usize, usize>,
        delete: ClDelete<usize, usize>,
        resize: ResizeLoop<usize, usize>,
    }

    impl Default for Smoke {
        fn default() -> Self {
            Self {
                insert: Default::default(),
                update: Default::default(),
                delete: Default::default(),
                resize: Default::default(),
            }
        }
    }

    impl Collectable for Smoke {
        fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for Smoke {
        type Object<'o> = &'o ClevelInner<usize, usize>;
        type Input<'o> = usize;
        type Output<'o> = ();
        type Error<'o> = ();

        fn run<'o>(
            &mut self,
            object: Self::Object<'o>,
            input: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            let (send, recv) = mpsc::channel();

            thread::scope(|s| {
                let (insert, update, delete, resize) = (
                    &mut self.insert,
                    &mut self.update,
                    &mut self.delete,
                    &mut self.resize,
                );

                let _ = s.spawn(move |_| {
                    let g = pin();
                    let recv = recv;
                    let _ = resize.run(object, &recv, rec, &g, pool);
                });

                const RANGE: usize = 1usize << 8;

                for i in 0..RANGE {
                    let _ = insert.run(object, (0, i, i, &send), rec, guard, pool);
                    assert_eq!(Clevel::search(object, &i, guard, pool), Some(&i));

                    let _ = update.run(object, (0, i, i + RANGE, &send), rec, guard, pool);
                    assert_eq!(Clevel::search(object, &i, guard, pool), Some(&(i + RANGE)));
                }

                for i in 0..RANGE {
                    assert_eq!(Clevel::search(object, &i, guard, pool), Some(&(i + RANGE)));
                    let _ = delete.run(object, &i, rec, guard, pool);
                    assert_eq!(Clevel::search(object, &i, guard, pool), None);
                }

                drop(send);
                println!("done");
            })
            .unwrap();
            Ok(())
        }

        fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {}
    }

    impl TestRootMemento<ClevelInner<usize, usize>> for Smoke {}

    #[test]
    fn smoke() {
        const FILE_NAME: &str = "clevel_smoke.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<ClevelInner<usize, usize>, Smoke, _>(FILE_NAME, FILE_SIZE, 1)
    }

    struct InsertSearch {
        insert: ClInsert<usize, usize>,
        resize: ResizeLoop<usize, usize>,
    }

    impl Default for InsertSearch {
        fn default() -> Self {
            Self {
                insert: Default::default(),
                resize: Default::default(),
            }
        }
    }

    impl Collectable for InsertSearch {
        fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for InsertSearch {
        type Object<'o> = &'o ClevelInner<usize, usize>;
        type Input<'o> = usize;
        type Output<'o> = ();
        type Error<'o> = ();

        fn run<'o>(
            &mut self,
            object: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            match tid {
                0 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };
                    let _ = self.resize.run(object, recv, rec, guard, pool);
                    println!("{tid}(resize) insert search done");
                }

                _ => {
                    let send = unsafe { SEND.as_mut().unwrap().pop().unwrap() };
                    const RANGE: usize = 1usize << 6;

                    for i in 0..RANGE {
                        // println!("[test] tid = {tid}, i = {i}, insert");
                        let _ = self
                            .insert
                            .run(object, (tid, i, i, &send), rec, guard, pool);

                        // println!("[test] tid = {tid}, i = {i}, search");
                        if Clevel::search(object, &i, guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail on {i}");
                            // assert_eq!(kv.search(&i, &guard), Some(&i));
                        }
                    }
                    println!("{tid} insert search done");
                }
            }

            Ok(())
        }

        fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {}
    }

    impl TestRootMemento<ClevelInner<usize, usize>> for InsertSearch {}

    #[test]
    fn insert_search() {
        const FILE_NAME: &str = "clevel_insert_search.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        const THREADS: usize = 1usize << 4;

        let (send, recv) = mpsc::channel();
        let mut vec_s = Vec::new();
        for _ in 0..THREADS - 1 {
            vec_s.push(send.clone());
        }
        drop(send);
        unsafe {
            SEND = Some(vec_s);
            RECV = Some(recv);
        }

        run_test::<ClevelInner<usize, usize>, InsertSearch, _>(FILE_NAME, FILE_SIZE, THREADS)
    }

    struct InsertUpdateSearch {
        insert: ClInsert<usize, usize>,
        resize: ResizeLoop<usize, usize>,
    }

    impl Default for InsertUpdateSearch {
        fn default() -> Self {
            Self {
                insert: Default::default(),
                resize: Default::default(),
            }
        }
    }

    impl Collectable for InsertUpdateSearch {
        fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
            todo!()
        }
    }

    impl Memento for InsertUpdateSearch {
        type Object<'o> = &'o ClevelInner<usize, usize>;
        type Input<'o> = usize;
        type Output<'o> = ();
        type Error<'o> = ();

        fn run<'o>(
            &mut self,
            object: Self::Object<'o>,
            tid: Self::Input<'o>,
            rec: bool,
            guard: &'o Guard,
            pool: &'static PoolHandle,
        ) -> Result<Self::Output<'o>, Self::Error<'o>> {
            match tid {
                0 => {
                    let recv = unsafe { RECV.as_ref().unwrap() };

                    let _ = self.resize.run(object, &recv, rec, guard, pool);
                    println!("{tid}(resize) insert search done");
                }

                _ => {
                    const RANGE: usize = 1usize << 6;
                    let send = unsafe { SEND.as_mut().unwrap().pop().unwrap() };

                    for i in 0..RANGE {
                        // println!("[test] tid = {tid}, i = {i}, insert");
                        let _ = self
                            .insert
                            .run(object, (tid, i, i, &send), rec, guard, pool);

                        // println!("[test] tid = {tid}, i = {i}, search");
                        if Clevel::search(object, &i, guard, pool) != Some(&i) {
                            panic!("[test] tid = {tid} fail on {i}");
                            // assert_eq!(kv.search(&i, &guard), Some(&i));
                        }
                    }

                    for i in 0..RANGE {
                        // println!("[test] tid = {tid}, i = {i}, insert");
                        let _ = self
                            .insert
                            .run(object, (tid, i, i, &send), rec, guard, pool);

                        // println!("[test] tid = {tid}, i = {i}, update");
                        let _ =
                            self.insert
                                .run(object, (tid, i, i + RANGE, &send), rec, guard, pool);

                        // println!("[test] tid = {tid}, i = {i}, search");
                        if Clevel::search(object, &i, guard, pool) != Some(&i)
                            && Clevel::search(object, &i, guard, pool) != Some(&(i + RANGE))
                        {
                            panic!("[test] tid = {tid} fail on {i}");
                        }
                    }

                    drop(send);
                    println!("{tid} insert search done");
                }
            }

            Ok(())
        }

        fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {}
    }

    impl TestRootMemento<ClevelInner<usize, usize>> for InsertUpdateSearch {}

    #[test]
    fn insert_update_search() {
        const FILE_NAME: &str = "clevel_insert_search.pool";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        const THREADS: usize = 1usize << 4;

        let (send, recv) = mpsc::channel();
        let mut vec_s = Vec::new();
        for _ in 0..THREADS - 1 {
            vec_s.push(send.clone());
        }
        drop(send);
        unsafe {
            SEND = Some(vec_s);
            RECV = Some(recv);
        }

        run_test::<ClevelInner<usize, usize>, InsertUpdateSearch, _>(FILE_NAME, FILE_SIZE, THREADS)
    }
}
