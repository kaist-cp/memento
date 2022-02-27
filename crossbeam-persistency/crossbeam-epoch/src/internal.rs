//! The global data and participant for garbage collection.
//!
//! # Registration
//!
//! In order to track all participants in one place, we need some form of participant
//! registration. When a participant is created, it is registered to a global lock-free
//! singly-linked list of registries; and when a participant is leaving, it is unregistered from the
//! list.
//!
//! # Pinning
//!
//! Every participant contains an integer that tells whether the participant is pinned and if so,
//! what was the global epoch at the time it was pinned. Participants also hold a pin counter that
//! aids in periodic global epoch advancement.
//!
//! When a participant is pinned, a `Guard` is returned as a witness that the participant is pinned.
//! Guards are necessary for performing atomic operations, and for freeing/dropping locations.
//!
//! # Thread-local bag
//!
//! Objects that get unlinked from concurrent data structures must be stashed away until the global
//! epoch sufficiently advances so that they become safe for destruction. Pointers to such objects
//! are pushed into a thread-local bag, and when it becomes full, the bag is marked with the current
//! global epoch and pushed into the global queue of bags. We store objects in thread-local storages
//! for amortizing the synchronization cost of pushing the garbages to a global queue.
//!
//! # Global queue
//!
//! Whenever a bag is pushed into a queue, the objects in some bags in the queue are collected and
//! destroyed along the way. This design reduces contention on data structures. The global queue
//! cannot be explicitly accessed: the only way to interact with it is by calling functions
//! `defer()` that adds an object to the thread-local bag, or `collect()` that manually triggers
//! garbage collection.
//!
//! Ideally each instance of concurrent data structure may have its own queue that gets fully
//! destroyed as soon as the data structure gets dropped.

use crate::primitive::cell::UnsafeCell;
use crate::primitive::sync::atomic;
use core::cell::Cell;
use core::mem::{self, ManuallyDrop};
use core::num::Wrapping;
use core::sync::atomic::Ordering;
use core::{fmt, ptr};

use crossbeam_utils::CachePadded;
use memoffset::offset_of;

use crate::atomic::{Owned, Shared};
use crate::collector::{Collector, LocalHandle};
use crate::deferred::Deferred;
use crate::epoch::{AtomicEpoch, Epoch};
use crate::guard::{unprotected, Guard};
use crate::ll::{persist, sfence};
use crate::sync::list::{Entry, IsElement, IterError, List};
use crate::sync::queue::Queue;

/// Maximum number of objects a bag can contain.
#[cfg(not(crossbeam_sanitize))]
const MAX_OBJECTS: usize = 40;
#[cfg(crossbeam_sanitize)]
const MAX_OBJECTS: usize = 4;

/// A bag of deferred functions.
pub(crate) struct Bag {
    /// Stashed objects.
    deferreds: [Deferred; MAX_OBJECTS],
    len: usize,
}

/// `Bag::try_push()` requires that it is safe for another thread to execute the given functions.
unsafe impl Send for Bag {}

impl Bag {
    /// Returns a new, empty bag.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the bag is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Attempts to insert a deferred function into the bag.
    ///
    /// Returns `Ok(())` if successful, and `Err(deferred)` for the given `deferred` if the bag is
    /// full.
    ///
    /// # Safety
    ///
    /// It should be safe for another thread to execute the given function.
    pub(crate) unsafe fn try_push(&mut self, deferred: Deferred) -> Result<(), Deferred> {
        if self.len < MAX_OBJECTS {
            self.deferreds[self.len] = deferred;
            self.len += 1;
            Ok(())
        } else {
            Err(deferred)
        }
    }

    /// Seals the bag with the given epoch.
    fn seal(self, epoch: Epoch) -> SealedBag {
        SealedBag { epoch, _bag: self }
    }

    /// 같은 key를 가진 deferred function이 있는지 여부 반환
    pub(crate) fn is_exist(&self, key: usize) -> bool {
        for d in &self.deferreds[0..self.len] {
            if let Some(k) = d.key() {
                if k == key {
                    return true;
                }
            }
        }
        false
    }
}

impl Default for Bag {
    #[rustfmt::skip]
    fn default() -> Self {
        // TODO: [no_op; MAX_OBJECTS] syntax blocked by https://github.com/rust-lang/rust/issues/49147
        #[cfg(not(crossbeam_sanitize))]
        return Bag {
            len: 0,
            deferreds: [
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
                Deferred::new(no_op_func, None),
            ],
        };
        #[cfg(crossbeam_sanitize)]
        return Bag {
            len: 0,
            deferreds: [
                Deferred::new(no_op_func),
                Deferred::new(no_op_func),
                Deferred::new(no_op_func),
                Deferred::new(no_op_func),
            ],
        };
    }
}

impl Drop for Bag {
    fn drop(&mut self) {
        // Call all deferred functions.
        for deferred in &mut self.deferreds[..self.len] {
            let no_op = Deferred::new(no_op_func, None);
            let owned_deferred = mem::replace(deferred, no_op);
            owned_deferred.call();
        }
    }
}

// can't #[derive(Debug)] because Debug is not implemented for arrays 64 items long
impl fmt::Debug for Bag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bag")
            .field("deferreds", &&self.deferreds[..self.len])
            .finish()
    }
}

fn no_op_func() {}

/// A pair of an epoch and a bag.
#[derive(Default, Debug)]
struct SealedBag {
    epoch: Epoch,
    _bag: Bag,
}

/// It is safe to share `SealedBag` because `is_expired` only inspects the epoch.
unsafe impl Sync for SealedBag {}

impl SealedBag {
    /// Checks if it is safe to drop the bag w.r.t. the given global epoch.
    fn is_expired(&self, global_epoch: Epoch) -> bool {
        // A pinned participant can witness at most one epoch advancement. Therefore, any bag that
        // is within one epoch of the current one cannot be destroyed yet.
        global_epoch.wrapping_sub(self.epoch) >= 2
    }
}

/// The global data for a garbage collector.
pub(crate) struct Global {
    /// The intrusive linked list of `Local`s.
    locals: List<Local>,

    /// The global queue of bags of deferred functions.
    queue: Queue<SealedBag>,

    /// The global epoch.
    pub(crate) epoch: CachePadded<AtomicEpoch>,
}

impl Global {
    /// Number of bags to destroy.
    const COLLECT_STEPS: usize = 8;

    /// Creates a new global data for garbage collection.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            locals: List::new(),
            queue: Queue::new(),
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
        }
    }

    /// Pushes the bag into the global queue and replaces the bag with a new empty bag.
    pub(crate) fn push_bag(&self, bag: &mut Bag, guard: &Guard) {
        let bag = mem::replace(bag, Bag::new());

        atomic::fence(Ordering::SeqCst);

        let epoch = self.epoch.load(Ordering::Relaxed);
        self.queue.push(bag.seal(epoch), guard);
    }

    /// Collects several bags from the global queue and executes deferred functions in them.
    ///
    /// Note: This may itself produce garbage and in turn allocate new bags.
    ///
    /// `pin()` rarely calls `collect()`, so we want the compiler to place that call on a cold
    /// path. In other words, we want the compiler to optimize branching for the case when
    /// `collect()` is not called.
    #[cold]
    pub(crate) fn collect(&self, guard: &Guard) {
        let global_epoch = self.try_advance(guard);

        let steps = if cfg!(crossbeam_sanitize) {
            usize::max_value()
        } else {
            Self::COLLECT_STEPS
        };

        for _ in 0..steps {
            // 만료된 bag을 drop시키며 안에 저장해두었던 deferred function 실행
            match self.queue.try_pop_if(
                &|sealed_bag: &SealedBag| sealed_bag.is_expired(global_epoch),
                guard,
            ) {
                None => break,
                Some(sealed_bag) => drop(sealed_bag),
            }
        }
    }

    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance only if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// `try_advance()` is annotated `#[cold]` because it is rarely called.
    #[cold]
    pub(crate) fn try_advance(&self, guard: &Guard) -> Epoch {
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        atomic::fence(Ordering::SeqCst);

        // TODO(stjepang): `Local`s are stored in a linked list because linked lists are fairly
        // easy to implement in a lock-free manner. However, traversal can be slow due to cache
        // misses and data dependencies. We should experiment with other data structures as well.
        for local in self.locals.iter(guard) {
            match local {
                Err(IterError::Stalled) => {
                    // A concurrent thread stalled this iteration. That thread might also try to
                    // advance the epoch, in which case we leave the job to it. Otherwise, the
                    // epoch will not be advanced.
                    return global_epoch;
                }
                Ok(local) => {
                    let local_epoch = local.epoch.load(Ordering::Relaxed);
                    // If the participant was pinned in a different epoch, we cannot advance the
                    // global epoch just yet.
                    if local_epoch.is_pinned() && local_epoch.unpinned() != global_epoch {
                        return global_epoch;
                    }
                }
            }
        }
        atomic::fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.
        // Now let's advance the global epoch...
        //
        // Note that if another thread already advanced it before us, this store will simply
        // overwrite the global epoch with the same value. This is true because `try_advance` was
        // called from a thread that was pinned in `global_epoch`, and the global epoch cannot be
        // advanced two steps ahead of it.
        let new_epoch = global_epoch.successor();
        self.epoch.store(new_epoch, Ordering::Release);
        new_epoch
    }
}

/// Participant for garbage collection.
pub(crate) struct Local {
    /// Owner of this `Local`
    tid: Option<usize>,

    /// A node in the intrusive linked list of `Local`s.
    entry: Entry,

    /// The local epoch.
    epoch: AtomicEpoch,

    /// A reference to the global data.
    ///
    /// When all guards and handles get dropped, this reference is destroyed.
    collector: UnsafeCell<ManuallyDrop<Collector>>,

    /// The local bag of deferred functions.
    pub(crate) bag: UnsafeCell<Bag>,

    /// The number of guards keeping this participant pinned.
    guard_count: Cell<usize>,

    /// The number of active handles.
    handle_count: Cell<usize>,

    /// Total number of pinnings performed.
    ///
    /// This is just an auxiliary counter that sometimes kicks off collection.
    pin_count: Cell<Wrapping<usize>>,

    /// Deferred to be persisted locations (ptr, len)
    persists: UnsafeCell<Vec<(usize, usize)>>,

    /// Set for deduplicate deferred function
    pfree: UnsafeCell<Vec<usize>>,

    /// repinning or not
    pub(crate) is_repinning: Cell<bool>,
}

// Make sure `Local` is less than or equal to 2048 bytes.
// https://github.com/crossbeam-rs/crossbeam/issues/551
#[cfg(not(crossbeam_sanitize))] // `crossbeam_sanitize` reduces the size of `Local`
#[test]
fn local_size() {
    assert!(
        core::mem::size_of::<Local>() <= 2048,
        "An allocation of `Local` should be <= 2048 bytes."
    );
}

impl Local {
    /// Number of pinnings after which a participant will execute some deferred functions from the
    /// global queue.
    const PINNINGS_BETWEEN_COLLECT: usize = 128;

    /// Registers a new `Local` in the provided `Global`.
    pub(crate) fn register(collector: &Collector, tid: Option<usize>) -> LocalHandle {
        unsafe {
            // Since we dereference no pointers in this block, it is safe to use `unprotected`.

            let local = Owned::new(Local {
                tid,
                entry: Entry::default(),
                epoch: AtomicEpoch::new(Epoch::starting()),
                collector: UnsafeCell::new(ManuallyDrop::new(collector.clone())),
                bag: UnsafeCell::new(Bag::new()),
                guard_count: Cell::new(0),
                handle_count: Cell::new(1),
                pin_count: Cell::new(Wrapping(0)),
                persists: UnsafeCell::new(vec![]),
                pfree: UnsafeCell::new(vec![]),
                is_repinning: Cell::new(false),
            })
            .into_shared(unprotected());
            collector.global.locals.insert(local, unprotected());
            LocalHandle {
                local: local.as_raw(),
            }
        }
    }

    /// global list에서 tid의 `Local` 찾아 handle 반환
    pub(crate) fn find<'a>(collector: &'a Collector, tid: usize) -> Option<LocalHandle> {
        // list 탐색용 임시 guard 얻기
        let tmp_handle = Local::register(collector, None);
        let tmp_guard = tmp_handle.pin();

        // list 탐색
        'find: loop {
            for local in collector.global.locals.iter(&tmp_guard) {
                match local {
                    Err(IterError::Stalled) => {
                        // 다른 스레드와 꼬인 경우 처음부터 다시 탐색
                        continue 'find;
                    }
                    Ok(local) => {
                        if let Some(owner) = local.owner() {
                            if owner == tid {
                                local.acquire_handle();
                                return Some(LocalHandle { local });
                            }
                        }
                    }
                }
            }
            return None;
        }
    }

    pub(crate) fn owner(&self) -> Option<usize> {
        self.tid
    }

    // Local이 세고 있는 obj의 수를, Local을 처음 만들 때처럼 초기화
    pub(crate) unsafe fn reset_count(&self) {
        self.handle_count.set(1);
        self.guard_count.set(0);
        self.pin_count.set(Wrapping(1)); // 얘는 초기화 안해도 되지만 API 이름과 맞추기 위해 수행. 별로 중요하지 않음
        self.is_repinning.set(false);
    }

    pub(crate) unsafe fn set_guard_count(&self, cnt: usize) {
        self.guard_count.set(cnt);
    }

    /// Returns a reference to the `Global` in which this `Local` resides.
    #[inline]
    pub(crate) fn global(&self) -> &Global {
        &self.collector().global
    }

    /// Returns a reference to the `Collector` in which this `Local` resides.
    #[inline]
    pub(crate) fn collector(&self) -> &Collector {
        self.collector.with(|c| unsafe { &**c })
    }

    /// Returns `true` if the current participant is pinned.
    #[inline]
    pub(crate) fn is_pinned(&self) -> bool {
        self.guard_count.get() > 0
    }

    /// Adds `deferred` to the thread-local bag.
    ///
    /// # Safety
    ///
    /// It should be safe for another thread to execute the given function.
    pub(crate) unsafe fn defer(&self, mut deferred: Deferred, guard: &Guard) {
        let bag = self.bag.with_mut(|b| &mut *b);

        while let Err(d) = bag.try_push(deferred) {
            self.push_pfrees(bag);
            self.global().push_bag(bag, guard);
            deferred = d;
        }
    }

    pub(crate) fn flush(&self, guard: &Guard) {
        let bag = self.bag.with_mut(|b| unsafe { &mut *b });

        if !bag.is_empty() {
            self.push_pfrees(bag);
            self.global().push_bag(bag, guard);
        }

        self.global().collect(guard);
    }

    /// Pins the `Local`.
    #[inline]
    pub(crate) fn pin(&self) -> Guard {
        let guard = Guard { local: self };

        let guard_count = self.guard_count.get();
        self.guard_count.set(guard_count.checked_add(1).unwrap());

        if guard_count == 0 {
            let global_epoch = self.global().epoch.load(Ordering::Relaxed);
            let new_epoch = global_epoch.pinned();

            // local의 epoch을 global epoch에 맞게 설정
            // Now we must store `new_epoch` into `self.epoch` and execute a `SeqCst` fence.
            // The fence makes sure that any future loads from `Atomic`s will not happen before
            // this store.
            if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                // HACK(stjepang): On x86 architectures there are two different ways of executing
                // a `SeqCst` fence.
                //
                // 1. `atomic::fence(SeqCst)`, which compiles into a `mfence` instruction.
                // 2. `_.compare_exchange(_, _, SeqCst, SeqCst)`, which compiles into a `lock cmpxchg`
                //    instruction.
                //
                // Both instructions have the effect of a full barrier, but benchmarks have shown
                // that the second one makes pinning faster in this particular case.  It is not
                // clear that this is permitted by the C++ memory model (SC fences work very
                // differently from SC accesses), but experimental evidence suggests that this
                // works fine.  Using inline assembly would be a viable (and correct) alternative,
                // but alas, that is not possible on stable Rust.
                let current = Epoch::starting();
                let res = self.epoch.compare_exchange(
                    current,
                    new_epoch,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                debug_assert!(res.is_ok(), "participant was expected to be unpinned");
                // We add a compiler fence to make it less likely for LLVM to do something wrong
                // here.  Formally, this is not enough to get rid of data races; practically,
                // it should go a long way.
                atomic::compiler_fence(Ordering::SeqCst);
            } else {
                self.epoch.store(new_epoch, Ordering::Relaxed);
                atomic::fence(Ordering::SeqCst);
            }

            // Increment the pin counter.
            let count = self.pin_count.get();
            self.pin_count.set(count + Wrapping(1));

            // After every `PINNINGS_BETWEEN_COLLECT` try advancing the epoch and collecting
            // some garbage.
            if count.0 % Self::PINNINGS_BETWEEN_COLLECT == 0 {
                // 첫 guard 생성을 128(PINNINGS_BETWEEN_COLLECT)번 할때마다 epoch을 advance시키며 만료된 deferred function을 호출
                self.global().collect(&guard);
            }
        }

        guard
    }

    /// Unpins the `Local`.
    #[inline]
    pub(crate) fn unpin(&self) {
        let guard_count = self.guard_count.get();
        self.guard_count.set(guard_count - 1);

        if guard_count == 1 {
            // Clear duplicate set
            self.pfree.with_mut(|v| unsafe { &mut *v }.clear());

            // Persist all deferred persisted locations
            let iter = self.persists.with(|v| unsafe { &*v }.iter());
            for (ptr, len) in iter {
                persist(*ptr, *len, false);
                sfence();
            }

            // local의 epoch을 다시 '시작' 상태로 초기화
            self.epoch.store(Epoch::starting(), Ordering::Release);

            if self.handle_count.get() == 0 {
                self.finalize();
            }
        }
    }

    /// Unpins and then pins the `Local`.
    #[inline]
    pub(crate) fn repin(&self) {
        self.is_repinning.set(true);
        let guard_count = self.guard_count.get();

        // Update the local epoch only if there's only one guard.
        if guard_count == 1 {
            let epoch = self.epoch.load(Ordering::Relaxed);
            let global_epoch = self.global().epoch.load(Ordering::Relaxed).pinned();

            // Clear duplicate set
            self.pfree.with_mut(|v| unsafe { &mut *v }.clear());

            // Update the local epoch only if the global epoch is greater than the local epoch.
            if epoch != global_epoch {
                // Persist all deferred persisted locations
                let iter = self.persists.with(|v| unsafe { &*v }.iter());
                for (ptr, len) in iter {
                    persist(*ptr, *len, false);
                    sfence();
                }

                // We store the new epoch with `Release` because we need to ensure any memory
                // accesses from the previous epoch do not leak into the new one.
                self.epoch.store(global_epoch, Ordering::Release);

                // However, we don't need a following `SeqCst` fence, because it is safe for memory
                // accesses from the new epoch to be executed before updating the local epoch. At
                // worse, other threads will see the new epoch late and delay GC slightly.
            }
        }
        self.is_repinning.set(false);
    }

    /// Increments the handle count.
    #[inline]
    pub(crate) fn acquire_handle(&self) {
        let handle_count = self.handle_count.get();

        // @anonymous: 이제 handle 개수는 0이 될 수 있음. thread-local crash 이후 old guard로 다시 handle 가져오기 전까진 0
        // debug_assert!(handle_count >= 1);
        self.handle_count.set(handle_count + 1);
    }

    /// Decrements the handle count.
    #[inline]
    pub(crate) fn release_handle(&self) {
        let guard_count = self.guard_count.get();
        let handle_count = self.handle_count.get();
        debug_assert!(handle_count >= 1);
        self.handle_count.set(handle_count - 1);

        if guard_count == 0 && handle_count == 1 {
            self.finalize();
        }
    }

    /// Removes the `Local` from the global linked list.
    #[cold]
    fn finalize(&self) {
        debug_assert_eq!(self.guard_count.get(), 0);
        debug_assert_eq!(self.handle_count.get(), 0);

        // Temporarily increment handle count. This is required so that the following call to `pin`
        // doesn't call `finalize` again.
        self.handle_count.set(1);
        unsafe {
            // Pin and move the local bag into the global queue. It's important that `push_bag`
            // doesn't defer destruction on any new garbage.
            let guard = &self.pin();
            self.global()
                .push_bag(self.bag.with_mut(|b| &mut *b), guard);
        }
        // Revert the handle count back to zero.
        self.handle_count.set(0);

        unsafe {
            // Take the reference to the `Global` out of this `Local`. Since we're not protected
            // by a guard at this time, it's crucial that the reference is read before marking the
            // `Local` as deleted.
            let collector: Collector = ptr::read(self.collector.with(|c| &*(*c)));

            // Mark this node in the linked list as deleted.
            // @anonymous: this is logically deleted i studied before.
            self.entry.delete(unprotected());

            // Finally, drop the reference to the global. Note that this might be the last reference
            // to the `Global`. If so, the global data will be destroyed and all deferred functions
            // in its queue will be executed.
            drop(collector);
        }
    }

    /// pfree set에 key가 있는지 확인
    pub(crate) fn is_exist_pfree(&self, k: usize) -> bool {
        self.pfree
            .with(|v| unsafe { &*v })
            .binary_search(&k)
            .is_ok()
    }

    /// bag에 있는 deferred function의 key들을 pfree set에도 기록
    pub(crate) fn push_pfrees(&self, bag: &Bag) {
        let v = self.pfree.with_mut(|v| unsafe { &mut *v });
        for d in &bag.deferreds[0..bag.len] {
            if let Some(k) = d.key() {
                if let Err(pos) = v.binary_search(&k) {
                    v.insert(pos, k);
                }
            }
        }
    }

    /// TODO: doc
    pub(crate) fn push_persist<T>(&self, obj: &T) {
        let ptr = obj as *const T as *const u8 as *mut u8 as usize;
        let len = std::mem::size_of_val(obj);

        let v = self.persists.with_mut(|v| unsafe { &mut *v });
        if let Err(pos) = v.binary_search_by_key(&ptr, |&(p, _)| p) {
            v.insert(pos, (ptr, len));
        }
    }
}

impl IsElement<Local> for Local {
    fn entry_of(local: &Local) -> &Entry {
        let entry_ptr = (local as *const Local as usize + offset_of!(Local, entry)) as *const Entry;
        unsafe { &*entry_ptr }
    }

    unsafe fn element_of(entry: &Entry) -> &Local {
        // offset_of! macro uses unsafe, but it's unnecessary in this context.
        #[allow(unused_unsafe)]
        let local_ptr = (entry as *const Entry as usize - offset_of!(Local, entry)) as *const Local;
        &*local_ptr
    }

    unsafe fn finalize(entry: &Entry, guard: &Guard) {
        guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _));
    }
}

#[cfg(all(test, not(crossbeam_loom)))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn check_defer() {
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        fn set() {
            FLAG.store(42, Ordering::Relaxed);
        }

        let d = Deferred::new(set, None);
        assert_eq!(FLAG.load(Ordering::Relaxed), 0);
        d.call();
        assert_eq!(FLAG.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn check_bag() {
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        fn incr() {
            FLAG.fetch_add(1, Ordering::Relaxed);
        }

        let mut bag = Bag::new();
        assert!(bag.is_empty());

        for _ in 0..MAX_OBJECTS {
            assert!(unsafe { bag.try_push(Deferred::new(incr, None)).is_ok() });
            assert!(!bag.is_empty());
            assert_eq!(FLAG.load(Ordering::Relaxed), 0);
        }

        let result = unsafe { bag.try_push(Deferred::new(incr, None)) };
        assert!(result.is_err());
        assert!(!bag.is_empty());
        assert_eq!(FLAG.load(Ordering::Relaxed), 0);

        drop(bag);
        assert_eq!(FLAG.load(Ordering::Relaxed), MAX_OBJECTS);
    }
}
