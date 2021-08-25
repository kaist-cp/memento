//! Persistent Atomic Pointer
//!
//! crossbeam Atomic/Owned/Shared의 persistent version
//! 원래 crossbeam과 크게 다른 점은:
//!   1. 포인터가 절대주소가 아닌 상대주소를 가지고 있고,
//!   2. load후 참조시 풀의 시작주소와 상대주소를 더한 절대주소 참조
//!
//! ## CHANGE LOG
//! - crossbeam에 원래 있던 TODO는 TODO(crossbeam)으로 명시
//! - 메모리 관련 operation(e.g. `init`, `deref`)은 `PoolHandle`을 받게 변경. 특히 `deref`는 다른 풀을 참조할 수 있으니 unsafe로 명시
//! - 변수명, 타입, 관련된 함수명을 아래와 같이 변경
//!     - 변경 전: `raw: *const T`(절대주소를 가리키는 포인터), `raw: usize`(절대주소), `from_raw(raw: *mut T) -> Owned<T>`
//!     - 변경 후: `ptr: PersistentPtr<T>`(상대주소를 가리키는 포인터), `offset: usize`(상대주소), `from_ptr(ptr: PersistentPtr<T>) -> Owned<T>`
//! - Box operation(e.g. into_box, from<Box>)은 주석처리 해놓고 TODO 남김(PersistentBox 구현할지 고민 필요)
//! - 모든 doc test를 persistent version으로 변경

use core::cmp;
use core::fmt;
use core::marker::PhantomData;
use core::mem::{self, MaybeUninit};
use core::slice;
use core::sync::atomic::Ordering;

use crate::pepoch::guard::Guard;
use crate::plocation::pool::PoolHandle;
use crate::plocation::ptr::PersistentPtr;
use crossbeam_utils::atomic::AtomicConsume;
use std::alloc;
use std::sync::atomic::AtomicUsize;

/// Given ordering for the success case in a compare-exchange operation, returns the strongest
/// appropriate ordering for the failure case.
#[inline]
fn strongest_failure_ordering(ord: Ordering) -> Ordering {
    use self::Ordering::*;
    match ord {
        Relaxed | Release => Relaxed,
        Acquire | AcqRel => Acquire,
        _ => SeqCst,
    }
}

/// The error returned on failed compare-and-set operation.
// TODO(crossbeam): remove in the next major version.
#[deprecated(note = "Use `CompareExchangeError` instead")]
pub type CompareAndSetError<'g, T, P> = CompareExchangeError<'g, T, P>;

/// The error returned on failed compare-and-swap operation.
pub struct CompareExchangeError<'g, T: ?Sized + Pointable, P: Pointer<T>> {
    /// The value in the atomic pointer at the time of the failed operation.
    pub current: Shared<'g, T>,

    /// The new value, which the operation failed to store.
    pub new: P,
}

impl<T, P: Pointer<T> + fmt::Debug> fmt::Debug for CompareExchangeError<'_, T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompareExchangeError")
            .field("current", &self.current)
            .field("new", &self.new)
            .finish()
    }
}

/// Memory orderings for compare-and-set operations.
///
/// A compare-and-set operation can have different memory orderings depending on whether it
/// succeeds or fails. This trait generalizes different ways of specifying memory orderings.
///
/// The two ways of specifying orderings for compare-and-set are:
///
/// 1. Just one `Ordering` for the success case. In case of failure, the strongest appropriate
///    ordering is chosen.
/// 2. A pair of `Ordering`s. The first one is for the success case, while the second one is
///    for the failure case.
// TODO(crossbeam): remove in the next major version.
#[deprecated(
    note = "`compare_and_set` and `compare_and_set_weak` that use this trait are deprecated, \
            use `compare_exchange` or `compare_exchange_weak instead`"
)]
pub trait CompareAndSetOrdering {
    /// The ordering of the operation when it succeeds.
    fn success(&self) -> Ordering;

    /// The ordering of the operation when it fails.
    ///
    /// The failure ordering can't be `Release` or `AcqRel` and must be equivalent or weaker than
    /// the success ordering.
    fn failure(&self) -> Ordering;
}

#[allow(deprecated)]
impl CompareAndSetOrdering for Ordering {
    #[inline]
    fn success(&self) -> Ordering {
        *self
    }

    #[inline]
    fn failure(&self) -> Ordering {
        strongest_failure_ordering(*self)
    }
}

#[allow(deprecated)]
impl CompareAndSetOrdering for (Ordering, Ordering) {
    #[inline]
    fn success(&self) -> Ordering {
        self.0
    }

    #[inline]
    fn failure(&self) -> Ordering {
        self.1
    }
}

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
fn low_bits<T: ?Sized + Pointable>() -> usize {
    (1 << T::ALIGN.trailing_zeros()) - 1
}

/// Panics if the pointer is not properly unaligned.
#[inline]
fn ensure_aligned<T: ?Sized + Pointable>(offset: usize) {
    assert_eq!(offset & low_bits::<T>(), 0, "unaligned pointer");
}

/// Given a tagged pointer `data`, returns the same pointer, but tagged with `tag`.
///
/// `tag` is truncated to fit into the unused bits of the pointer to `T`.
#[inline]
fn compose_tag<T: ?Sized + Pointable>(data: usize, tag: usize) -> usize {
    (data & !low_bits::<T>()) | (tag & low_bits::<T>())
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
fn decompose_tag<T: ?Sized + Pointable>(data: usize) -> (usize, usize) {
    (data & !low_bits::<T>(), data & low_bits::<T>())
}

/// Types that are pointed to by a single word.
///
/// In concurrent programming, it is necessary to represent an object within a word because atomic
/// operations (e.g., reads, writes, read-modify-writes) support only single words.  This trait
/// qualifies such types that are pointed to by a single word.
///
/// The trait generalizes `Box<T>` for a sized type `T`.  In a box, an object of type `T` is
/// allocated in heap and it is owned by a single-word pointer.  This trait is also implemented for
/// `[MaybeUninit<T>]` by storing its size along with its elements and pointing to the pair of array
/// size and elements.
///
/// Pointers to `Pointable` types can be stored in [`Atomic`], [`Owned`], and [`Shared`].  In
/// particular, Crossbeam supports dynamically sized slices as follows.
///
/// ```
/// # use compositional_persistent_object::util::*;
/// # let pool = get_test_handle("pointable");
/// use std::mem::MaybeUninit;
/// use compositional_persistent_object::pepoch::Owned;
///
/// let o = Owned::<[MaybeUninit<i32>]>::init(10, &pool); // allocating [i32; 10]
/// ```
pub trait Pointable {
    /// The alignment of pointer.
    const ALIGN: usize;

    /// The type for initializers.
    type Init;

    /// Initializes a with the given initializer in the pool.
    ///
    /// # Safety
    ///
    /// The result should be a multiple of `ALIGN`.
    unsafe fn init(init: Self::Init, pool: &PoolHandle) -> usize;

    /// Dereferences the given offset in the pool.
    ///
    /// # Safety
    ///
    /// - TODO: pool1의 obj에 pool2의 PoolHandle을 사용하는 일이 없도록 해야함
    /// - The given `offset` should have been initialized with [`Pointable::init`].
    /// - `offset` should not have yet been dropped by [`Pointable::drop`].
    /// - `offset` should not be mutably dereferenced by [`Pointable::deref_mut`] concurrently.
    // crossbeam에선 절대주소를 받아 deref하니 여기선 상대주소를 받도록 함
    unsafe fn deref<'a>(offset: usize, pool: &PoolHandle) -> &'a Self;

    /// Mutably dereferences the given offset in the pool.
    ///
    /// # Safety
    ///
    /// - TODO: pool1의 obj에 pool2의 PoolHandle을 사용하는 일이 없도록 해야함
    /// - The given `offset` should have been initialized with [`Pointable::init`].
    /// - `offset` should not have yet been dropped by [`Pointable::drop`].
    /// - `offset` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    // crossbeam에선 절대주소를 받아 deref하니 여기선 상대주소를 받도록 함
    unsafe fn deref_mut<'a>(offset: usize, pool: &PoolHandle) -> &'a mut Self;

    /// Drops the object pointed to by the given offset in the pool.
    ///
    /// # Safety
    ///
    /// - TODO: pool1의 obj에 pool2의 PoolHandle을 사용하는 일이 없도록 해야함
    /// - The given `offset` should have been initialized with [`Pointable::init`].
    /// - `offset` should not have yet been dropped by [`Pointable::drop`].
    /// - `offset` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    // crossbeam에선 절대주소를 받아 drop하니 여기선 상대주소를 받도록 함
    unsafe fn drop(offset: usize, pool: &PoolHandle);
}

impl<T> Pointable for T {
    const ALIGN: usize = mem::align_of::<T>();

    type Init = T;

    unsafe fn init(init: Self::Init, pool: &PoolHandle) -> usize {
        let mut ptr = pool.alloc::<T>();
        // TODO: 여기서 crash 나면 leak남. 해결 필요
        let t = ptr.deref_mut(pool);
        *t = init;
        ptr.into_offset()
    }

    unsafe fn deref<'a>(offset: usize, pool: &PoolHandle) -> &'a Self {
        &*((pool.start() + offset) as *const T)
    }

    unsafe fn deref_mut<'a>(offset: usize, pool: &PoolHandle) -> &'a mut Self {
        &mut *((pool.start() + offset) as *mut T)
    }

    unsafe fn drop(offset: usize, pool: &PoolHandle) {
        pool.free(PersistentPtr::<T>::from(offset));
    }
}

/// Array with size.
///
/// # Memory layout
///
/// An array consisting of size and elements:
///
/// ```text
///          elements
///          |
///          |
/// ------------------------------------
/// | size | 0 | 1 | 2 | 3 | 4 | 5 | 6 |
/// ------------------------------------
/// ```
///
/// Its memory layout is different from that of `Box<[T]>` in that size is in the allocation (not
/// along with pointer as in `Box<[T]>`).
///
/// Elements are not present in the type, but they will be in the allocation.
/// ```
///
// TODO(crossbeam)(@jeehoonkang): once we bump the minimum required Rust version to 1.44 or newer, use
// [`alloc::alloc::Layout::extend`] instead.
#[repr(C)]
struct Array<T> {
    /// The number of elements (not the number of bytes).
    len: usize,
    elements: [MaybeUninit<T>; 0],
}

impl<T> Pointable for [MaybeUninit<T>] {
    const ALIGN: usize = mem::align_of::<Array<T>>();

    type Init = usize;

    unsafe fn init(len: Self::Init, pool: &PoolHandle) -> usize {
        let size = mem::size_of::<Array<T>>() + mem::size_of::<MaybeUninit<T>>() * len;
        let align = mem::align_of::<Array<T>>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        let mut ptr = pool.alloc_layout::<Array<T>>(layout);
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        (ptr.deref_mut(pool)).len = len;
        ptr.into_offset()
    }

    unsafe fn deref<'a>(offset: usize, pool: &PoolHandle) -> &'a Self {
        let array = &*((pool.start() + offset) as *const Array<T>);
        slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len)
    }

    unsafe fn deref_mut<'a>(offset: usize, pool: &PoolHandle) -> &'a mut Self {
        let array = &*((pool.start() + offset) as *mut Array<T>);
        slice::from_raw_parts_mut(array.elements.as_ptr() as *mut _, array.len)
    }

    unsafe fn drop(offset: usize, pool: &PoolHandle) {
        let array = &*((pool.start() + offset) as *mut Array<T>);
        let size = mem::size_of::<Array<T>>() + mem::size_of::<MaybeUninit<T>>() * array.len;
        let align = mem::align_of::<Array<T>>();
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        pool.free_layout(offset, layout)
    }
}

/// An atomic pointer that can be safely shared between threads.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address. For example, the tag for a pointer to a sized type `T`
/// should be less than `(1 << mem::align_of::<T>().trailing_zeros())`.
///
/// Any method that loads the pointer must be passed a reference to a [`Guard`].
///
/// Crossbeam supports dynamically sized types.  See [`Pointable`] for details.
pub struct Atomic<T: ?Sized + Pointable> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: ?Sized + Pointable + Send + Sync> Send for Atomic<T> {}
unsafe impl<T: ?Sized + Pointable + Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Allocates `value` on the heap and returns a new atomic pointer pointing to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_new");
    /// use compositional_persistent_object::pepoch::Atomic;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// ```
    pub fn new(init: T, pool: &PoolHandle) -> Atomic<T> {
        Self::init(init, pool)
    }
}

impl<T: ?Sized + Pointable> Atomic<T> {
    /// Allocates `value` on the heap and returns a new atomic pointer pointing to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_init");
    /// use compositional_persistent_object::pepoch::Atomic;
    ///
    /// let a = Atomic::<i32>::init(1234, &pool);
    /// ```
    pub fn init(init: T::Init, pool: &PoolHandle) -> Atomic<T> {
        Self::from(Owned::init(init, pool))
    }

    /// Returns a new atomic pointer pointing to the tagged pointer `data`.
    fn from_usize(data: usize) -> Self {
        Self {
            data: AtomicUsize::new(data),
            _marker: PhantomData,
        }
    }

    /// Returns a new null atomic pointer.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::Atomic;
    ///
    /// let a = Atomic::<i32>::null();
    /// ```
    ///
    #[cfg_attr(all(feature = "nightly", not(crossbeam_loom)), const_fn::const_fn)]
    pub fn null() -> Atomic<T> {
        Self {
            data: AtomicUsize::new(usize::MAX),
            _marker: PhantomData,
        }
    }

    /// Loads a `Shared` from the atomic pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_load");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// ```
    pub fn load<'g>(&self, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.load(ord)) }
    }

    /// Loads a `Shared` from the atomic pointer using a "consume" memory ordering.
    ///
    /// This is similar to the "acquire" ordering, except that an ordering is
    /// only guaranteed with operations that "depend on" the result of the load.
    /// However consume loads are usually much faster than acquire loads on
    /// architectures with a weak memory model since they don't require memory
    /// fence instructions.
    ///
    /// The exact definition of "depend on" is a bit vague, but it works as you
    /// would expect in practice since a lot of software, especially the Linux
    /// kernel, rely on this behavior.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_load_consume");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = a.load_consume(guard);
    /// ```
    pub fn load_consume<'g>(&self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.load_consume()) }
    }

    /// Stores a `Shared` or `Owned` pointer into the atomic pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_store");
    /// use compositional_persistent_object::pepoch::{Atomic, Owned, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// a.store(Shared::null(), SeqCst);
    /// a.store(Owned::new(1234, &pool), SeqCst);
    /// ```
    pub fn store<P: Pointer<T>>(&self, new: P, ord: Ordering) {
        self.data.store(new.into_usize(), ord);
    }

    /// Stores a `Shared` or `Owned` pointer into the atomic pointer, returning the previous
    /// `Shared`.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_swap");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = a.swap(Shared::null(), SeqCst, guard);
    /// ```
    pub fn swap<'g, P: Pointer<T>>(&self, new: P, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.swap(new.into_usize(), ord)) }
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success the
    /// pointer that was written is returned. On failure the actual current value and `new` are
    /// returned.
    ///
    /// This method takes two `Ordering` arguments to describe the memory
    /// ordering of this operation. `success` describes the required ordering for the
    /// read-modify-write operation that takes place if the comparison with `current` succeeds.
    /// `failure` describes the required ordering for the load operation that takes place when
    /// the comparison fails. Using `Acquire` as success ordering makes the store part
    /// of this operation `Relaxed`, and using `Release` makes the successful load
    /// `Relaxed`. The failure ordering can only be `SeqCst`, `Acquire` or `Relaxed`
    /// and must be equivalent to or weaker than the success ordering.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_compare_exchange");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    ///
    /// let guard = &epoch::pin();
    /// let curr = a.load(SeqCst, guard);
    /// let res1 = a.compare_exchange(curr, Shared::null(), SeqCst, SeqCst, guard);
    /// let res2 = a.compare_exchange(curr, Owned::new(5678, &pool), SeqCst, SeqCst, guard);
    /// ```
    pub fn compare_exchange<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_usize();
        self.data
            .compare_exchange(current.into_usize(), new, success, failure)
            .map(|_| unsafe { Shared::from_usize(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_usize(current),
                    new: P::from_usize(new),
                }
            })
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// Unlike [`compare_exchange`], this method is allowed to spuriously fail even when comparison
    /// succeeds, which can result in more efficient code on some platforms.  The return value is a
    /// result indicating whether the new pointer was written. On success the pointer that was
    /// written is returned. On failure the actual current value and `new` are returned.
    ///
    /// This method takes two `Ordering` arguments to describe the memory
    /// ordering of this operation. `success` describes the required ordering for the
    /// read-modify-write operation that takes place if the comparison with `current` succeeds.
    /// `failure` describes the required ordering for the load operation that takes place when
    /// the comparison fails. Using `Acquire` as success ordering makes the store part
    /// of this operation `Relaxed`, and using `Release` makes the successful load
    /// `Relaxed`. The failure ordering can only be `SeqCst`, `Acquire` or `Relaxed`
    /// and must be equivalent to or weaker than the success ordering.
    ///
    /// [`compare_exchange`]: Atomic::compare_exchange
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_compare_exchange_weak");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    ///
    /// let mut new = Owned::new(5678, &pool);
    /// let mut ptr = a.load(SeqCst, guard);
    /// loop {
    ///     match a.compare_exchange_weak(ptr, new, SeqCst, SeqCst, guard) {
    ///         Ok(p) => {
    ///             ptr = p;
    ///             break;
    ///         }
    ///         Err(err) => {
    ///             ptr = err.current;
    ///             new = err.new;
    ///         }
    ///     }
    /// }
    ///
    /// let mut curr = a.load(SeqCst, guard);
    /// loop {
    ///     match a.compare_exchange_weak(curr, Shared::null(), SeqCst, SeqCst, guard) {
    ///         Ok(_) => break,
    ///         Err(err) => curr = err.current,
    ///     }
    /// }
    /// ```
    pub fn compare_exchange_weak<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_usize();
        self.data
            .compare_exchange_weak(current.into_usize(), new, success, failure)
            .map(|_| unsafe { Shared::from_usize(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_usize(current),
                    new: P::from_usize(new),
                }
            })
    }

    /// Fetches the pointer, and then applies a function to it that returns a new value.
    /// Returns a `Result` of `Ok(previous_value)` if the function returned `Some`, else `Err(_)`.
    ///
    /// Note that the given function may be called multiple times if the value has been changed by
    /// other threads in the meantime, as long as the function returns `Some(_)`, but the function
    /// will have been applied only once to the stored value.
    ///
    /// `fetch_update` takes two [`Ordering`] arguments to describe the memory
    /// ordering of this operation. The first describes the required ordering for
    /// when the operation finally succeeds while the second describes the
    /// required ordering for loads. These correspond to the success and failure
    /// orderings of [`Atomic::compare_exchange`] respectively.
    ///
    /// Using [`Acquire`] as success ordering makes the store part of this
    /// operation [`Relaxed`], and using [`Release`] makes the final successful
    /// load [`Relaxed`]. The (failed) load ordering can only be [`SeqCst`],
    /// [`Acquire`] or [`Relaxed`] and must be equivalent to or weaker than the
    /// success ordering.
    ///
    /// [`Relaxed`]: Ordering::Relaxed
    /// [`Acquire`]: Ordering::Acquire
    /// [`Release`]: Ordering::Release
    /// [`SeqCst`]: Ordering::SeqCst
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_fetch_update");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    ///
    /// let res1 = a.fetch_update(SeqCst, SeqCst, guard, |x| Some(x.with_tag(1)));
    /// assert!(res1.is_ok());
    ///
    /// let res2 = a.fetch_update(SeqCst, SeqCst, guard, |x| None);
    /// assert!(res2.is_err());
    /// ```
    pub fn fetch_update<'g, F>(
        &self,
        set_order: Ordering,
        fail_order: Ordering,
        guard: &'g Guard,
        mut func: F,
    ) -> Result<Shared<'g, T>, Shared<'g, T>>
    where
        F: FnMut(Shared<'g, T>) -> Option<Shared<'g, T>>,
    {
        let mut prev = self.load(fail_order, guard);
        while let Some(next) = func(prev) {
            match self.compare_exchange_weak(prev, next, set_order, fail_order, guard) {
                Ok(shared) => return Ok(shared),
                Err(next_prev) => prev = next_prev.current,
            }
        }
        Err(prev)
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// The return value is a result indicating whether the new pointer was written. On success the
    /// pointer that was written is returned. On failure the actual current value and `new` are
    /// returned.
    ///
    /// This method takes a [`CompareAndSetOrdering`] argument which describes the memory
    /// ordering of this operation.
    ///
    /// # Migrating to `compare_exchange`
    ///
    /// `compare_and_set` is equivalent to `compare_exchange` with the following mapping for
    /// memory orderings:
    ///
    /// Original | Success | Failure
    /// -------- | ------- | -------
    /// Relaxed  | Relaxed | Relaxed
    /// Acquire  | Acquire | Acquire
    /// Release  | Release | Relaxed
    /// AcqRel   | AcqRel  | Acquire
    /// SeqCst   | SeqCst  | SeqCst
    ///
    /// # Examples
    ///
    /// ```
    /// # #![allow(deprecated)]
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_compare_and_set");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    ///
    /// let guard = &epoch::pin();
    /// let curr = a.load(SeqCst, guard);
    /// let res1 = a.compare_and_set(curr, Shared::null(), SeqCst, guard);
    /// let res2 = a.compare_and_set(curr, Owned::new(5678, &pool), SeqCst, guard);
    /// ```
    // TODO(crossbeam): remove in the next major version.
    #[allow(deprecated)]
    #[deprecated(note = "Use `compare_exchange` instead")]
    pub fn compare_and_set<'g, O, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        ord: O,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareAndSetError<'g, T, P>>
    where
        O: CompareAndSetOrdering,
        P: Pointer<T>,
    {
        self.compare_exchange(current, new, ord.success(), ord.failure(), guard)
    }

    /// Stores the pointer `new` (either `Shared` or `Owned`) into the atomic pointer if the current
    /// value is the same as `current`. The tag is also taken into account, so two pointers to the
    /// same object, but with different tags, will not be considered equal.
    ///
    /// Unlike [`compare_and_set`], this method is allowed to spuriously fail even when comparison
    /// succeeds, which can result in more efficient code on some platforms.  The return value is a
    /// result indicating whether the new pointer was written. On success the pointer that was
    /// written is returned. On failure the actual current value and `new` are returned.
    ///
    /// This method takes a [`CompareAndSetOrdering`] argument which describes the memory
    /// ordering of this operation.
    ///
    /// [`compare_and_set`]: Atomic::compare_and_set
    ///
    /// # Migrating to `compare_exchange_weak`
    ///
    /// `compare_and_set_weak` is equivalent to `compare_exchange_weak` with the following mapping for
    /// memory orderings:
    ///
    /// Original | Success | Failure
    /// -------- | ------- | -------
    /// Relaxed  | Relaxed | Relaxed
    /// Acquire  | Acquire | Acquire
    /// Release  | Release | Relaxed
    /// AcqRel   | AcqRel  | Acquire
    /// SeqCst   | SeqCst  | SeqCst
    ///
    /// # Examples
    ///
    /// ```
    /// # #![allow(deprecated)]
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_compare_and_set_weak");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    ///
    /// let mut new = Owned::new(5678, &pool);
    /// let mut ptr = a.load(SeqCst, guard);
    /// loop {
    ///     match a.compare_and_set_weak(ptr, new, SeqCst, guard) {
    ///         Ok(p) => {
    ///             ptr = p;
    ///             break;
    ///         }
    ///         Err(err) => {
    ///             ptr = err.current;
    ///             new = err.new;
    ///         }
    ///     }
    /// }
    ///
    /// let mut curr = a.load(SeqCst, guard);
    /// loop {
    ///     match a.compare_and_set_weak(curr, Shared::null(), SeqCst, guard) {
    ///         Ok(_) => break,
    ///         Err(err) => curr = err.current,
    ///     }
    /// }
    /// ```
    // TODO(crossbeam): remove in the next major version.
    #[allow(deprecated)]
    #[deprecated(note = "Use `compare_exchange_weak` instead")]
    pub fn compare_and_set_weak<'g, O, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        ord: O,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareAndSetError<'g, T, P>>
    where
        O: CompareAndSetOrdering,
        P: Pointer<T>,
    {
        self.compare_exchange_weak(current, new, ord.success(), ord.failure(), guard)
    }

    /// Bitwise "and" with the current tag.
    ///
    /// Performs a bitwise "and" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::<i32>::from(Shared::null().with_tag(3));
    /// let guard = &epoch::pin();
    /// assert_eq!(a.fetch_and(2, SeqCst, guard).tag(), 3);
    /// assert_eq!(a.load(SeqCst, guard).tag(), 2);
    /// ```
    pub fn fetch_and<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_and(val | !low_bits::<T>(), ord)) }
    }

    /// Bitwise "or" with the current tag.
    ///
    /// Performs a bitwise "or" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::<i32>::from(Shared::null().with_tag(1));
    /// let guard = &epoch::pin();
    /// assert_eq!(a.fetch_or(2, SeqCst, guard).tag(), 1);
    /// assert_eq!(a.load(SeqCst, guard).tag(), 3);
    /// ```
    pub fn fetch_or<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_or(val & low_bits::<T>(), ord)) }
    }

    /// Bitwise "xor" with the current tag.
    ///
    /// Performs a bitwise "xor" operation on the current tag and the argument `val`, and sets the
    /// new tag to the result. Returns the previous pointer.
    ///
    /// This method takes an [`Ordering`] argument which describes the memory ordering of this
    /// operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Shared};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::<i32>::from(Shared::null().with_tag(1));
    /// let guard = &epoch::pin();
    /// assert_eq!(a.fetch_xor(3, SeqCst, guard).tag(), 1);
    /// assert_eq!(a.load(SeqCst, guard).tag(), 2);
    /// ```
    pub fn fetch_xor<'g>(&self, val: usize, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.fetch_xor(val & low_bits::<T>(), ord)) }
    }

    /// Takes ownership of the pointee.
    ///
    /// This consumes the atomic and converts it into [`Owned`]. As [`Atomic`] doesn't have a
    /// destructor and doesn't drop the pointee while [`Owned`] does, this is suitable for
    /// destructors of data structures.
    ///
    /// # Panics
    ///
    /// Panics if this pointer is null, but only in debug mode.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::mem;
    /// # use compositional_persistent_object::pepoch::Atomic;
    /// struct DataStructure {
    ///     ptr: Atomic<usize>,
    /// }
    ///
    /// impl Drop for DataStructure {
    ///     fn drop(&mut self) {
    ///         // By now the DataStructure lives only in our thread and we are sure we don't hold
    ///         // any Shared or & to it ourselves.
    ///         unsafe {
    ///             drop(mem::replace(&mut self.ptr, Atomic::null()).into_owned());
    ///         }
    ///     }
    /// }
    /// ```
    pub unsafe fn into_owned(self) -> Owned<T> {
        #[cfg(crossbeam_loom)]
        {
            // FIXME(crossbeam): loom does not yet support into_inner, so we use unsync_load for now,
            // which should have the same synchronization properties:
            // https://github.com/tokio-rs/loom/issues/117
            Owned::from_usize(self.data.unsync_load())
        }
        #[cfg(not(crossbeam_loom))]
        {
            Owned::from_usize(self.data.into_inner())
        }
    }

    /// PoolHandle을 받아야하므로 fmt::Pointer trait impl 하던 것을 직접 구현
    pub fn fmt(&self, f: &mut fmt::Formatter<'_>, pool: &PoolHandle) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (offset, _) = decompose_tag::<T>(data);
        fmt::Pointer::fmt(&(unsafe { T::deref(offset, pool) as *const _ }), f)
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (offset, tag) = decompose_tag::<T>(data);

        f.debug_struct("Atomic")
            .field("offset", &offset)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: ?Sized + Pointable> Clone for Atomic<T> {
    /// Returns a copy of the atomic value.
    ///
    /// Note that a `Relaxed` load is used here. If you need synchronization, use it with other
    /// atomics or fences.
    fn clone(&self) -> Self {
        let data = self.data.load(Ordering::Relaxed);
        Atomic::from_usize(data)
    }
}

impl<T: ?Sized + Pointable> Default for Atomic<T> {
    fn default() -> Self {
        Atomic::null()
    }
}

impl<T: ?Sized + Pointable> From<Owned<T>> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `owned`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("atomic_from_owned");
    /// use compositional_persistent_object::pepoch::{Atomic, Owned};
    ///
    /// let a = Atomic::<i32>::from(Owned::new(1234, &pool));
    /// ```
    fn from(owned: Owned<T>) -> Self {
        let data = owned.data;
        mem::forget(owned);
        Self::from_usize(data)
    }
}

// TODO: PersistentBox 구현?
//
// impl<T> From<Box<T>> for Atomic<T> {
//     fn from(b: Box<T>) -> Self {
//         Self::from(Owned::from(b))
//     }
// }

impl<'g, T: ?Sized + Pointable> From<Shared<'g, T>> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `ptr`.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::{Atomic, Shared};
    ///
    /// let a = Atomic::<i32>::from(Shared::<i32>::null());
    /// ```
    fn from(ptr: Shared<'g, T>) -> Self {
        Self::from_usize(ptr.data)
    }
}

impl<T> From<PersistentPtr<T>> for Atomic<T> {
    /// Returns a new atomic pointer pointing to `ptr`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ptr;
    /// use compositional_persistent_object::plocation::ptr::PersistentPtr;
    /// use compositional_persistent_object::pepoch::Atomic;
    ///
    /// let a = Atomic::<i32>::from(PersistentPtr::<i32>::null());
    /// ```
    fn from(ptr: PersistentPtr<T>) -> Self {
        Self::from_usize(ptr.into_offset())
    }
}

/// A trait for either `Owned` or `Shared` pointers.
pub trait Pointer<T: ?Sized + Pointable> {
    /// Returns the machine representation of the pointer.
    fn into_usize(self) -> usize;

    /// Returns a new pointer pointing to the tagged pointer `data`.
    ///
    /// # Safety
    ///
    /// The given `data` should have been created by `Pointer::into_usize()`, and one `data` should
    /// not be converted back by `Pointer::from_usize()` multiple times.
    unsafe fn from_usize(data: usize) -> Self;
}

/// An owned heap-allocated object.
///
/// This type is very similar to `Box<T>`.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address.
pub struct Owned<T: ?Sized + Pointable> {
    data: usize,
    // TODO: PhantomData<PersistentBox<T>>로 해야할지 고민 필요
    _marker: PhantomData<T>,
}

impl<T: ?Sized + Pointable> Pointer<T> for Owned<T> {
    #[inline]
    fn into_usize(self) -> usize {
        let data = self.data;
        mem::forget(self);
        data
    }

    /// Returns a new pointer pointing to the tagged pointer `data`.
    ///
    /// # Panics
    ///
    /// Panics if the data is zero in debug mode.
    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        debug_assert!(data != 0, "converting zero into `Owned`");
        Owned {
            data,
            _marker: PhantomData,
        }
    }
}

impl<T> Owned<T> {
    /// Returns a new owned pointer pointing to `raw`.
    ///
    /// This function is unsafe because improper use may lead to memory problems. Argument `raw`
    /// must be a valid pointer. Also, a double-free may occur if the function is called twice on
    /// the same raw pointer.
    ///
    /// # Panics
    ///
    /// Panics if `raw` is not properly aligned.
    ///
    /// # Safety
    ///
    /// The given `raw` should have been derived from `Owned`, and one `raw` should not be converted
    /// back by `Owned::from_raw()` multiple times.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owned_from_ptr");
    /// use compositional_persistent_object::plocation::ptr::PersistentPtr;
    /// use compositional_persistent_object::pepoch::Owned;
    ///
    /// let mut ptr = pool.alloc::<usize>();
    /// let o = unsafe { Owned::from_ptr(ptr) };
    /// ```
    pub unsafe fn from_ptr(ptr: PersistentPtr<T>) -> Owned<T> {
        let offset = ptr.into_offset();
        ensure_aligned::<T>(offset);
        Self::from_usize(offset)
    }

    // TODO: PersistentBox 구현할지 고민 필요
    // /// Converts the owned pointer into a `Box`.
    // ///
    // /// # Examples
    // ///
    // /// ```
    // /// use crossbeam_epoch::Owned;
    // ///
    // /// let o = Owned::new(1234);
    // /// let b: Box<i32> = o.into_box();
    // /// assert_eq!(*b, 1234);
    // /// ```
    // pub fn into_box(self) -> Box<T> {
    //     let (raw, _) = decompose_tag::<T>(self.data);
    //     mem::forget(self);
    //     unsafe { Box::from_raw(raw as *mut _) }
    // }

    /// Allocates `value` on the heap and returns a new owned pointer pointing to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owned_new");
    /// use compositional_persistent_object::pepoch::Owned;
    ///
    /// let o = Owned::new(1234, &pool);
    /// ```
    pub fn new(init: T, pool: &PoolHandle) -> Owned<T> {
        Self::init(init, pool)
    }

    // PoolHandle을 받아야하므로 From<T> trait impl 하던 것을 직접 구현
    // TODO: new와 시그니처 똑같은데 굳이 필요한지 고민
    /// 주어진 pool에 T 할당 후 이를 가리키는 Owned 포인터 반환
    pub fn from_t(t: T, pool: &PoolHandle) -> Self {
        Owned::new(t, pool)
    }
}

impl<T: ?Sized + Pointable> Owned<T> {
    /// Allocates `value` on the heap and returns a new owned pointer pointing to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owend_init");
    /// use compositional_persistent_object::pepoch::Owned;
    ///
    /// let o = Owned::<i32>::init(1234, &pool);
    /// ```
    pub fn init(init: T::Init, pool: &PoolHandle) -> Owned<T> {
        unsafe { Self::from_usize(T::init(init, pool)) }
    }

    /// Converts the owned pointer into a [`Shared`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owned_into_shared");
    /// use compositional_persistent_object::pepoch::{self as epoch, Owned};
    ///
    /// let o = Owned::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = o.into_shared(guard);
    /// ```
    #[allow(clippy::needless_lifetimes)]
    pub fn into_shared<'g>(self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.into_usize()) }
    }

    /// Returns the tag stored within the pointer.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owend_tag");
    /// use compositional_persistent_object::pepoch::Owned;
    ///
    /// assert_eq!(Owned::new(1234, &pool).tag(), 0);
    /// ```
    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("owend_with_tag");
    /// use compositional_persistent_object::pepoch::Owned;
    ///
    /// let o = Owned::new(0u64, &pool);
    /// assert_eq!(o.tag(), 0);
    /// let o = o.with_tag(2);
    /// assert_eq!(o.tag(), 2);
    /// ```
    pub fn with_tag(self, tag: usize) -> Owned<T> {
        let data = self.into_usize();
        unsafe { Self::from_usize(compose_tag::<T>(data, tag)) }
    }

    // PoolHandle을 받아야하므로 Deref trait impl 하던 것을 직접 구현
    /// 절대주소 참조
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn deref(&self, pool: &PoolHandle) -> &T {
        let (offset, _) = decompose_tag::<T>(self.data);
        T::deref(offset, pool)
    }

    // PoolHandle을 받아야하므로 DerefMut trait impl 하던 것을 직접 구현
    /// 절대주소 mutable 참조
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn deref_mut(&mut self, pool: &PoolHandle) -> &mut T {
        let (offset, _) = decompose_tag::<T>(self.data);
        T::deref_mut(offset, pool)
    }

    // PoolHandle을 받아야하므로 Borrow trait impl 하던 것을 직접 구현
    /// borrow
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn borrow(&self, pool: &PoolHandle) -> &T {
        self.deref(pool)
    }

    // PoolHandle을 받아야하므로 BorrowMut trait impl 하던 것을 직접 구현
    /// borrow_mut
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn borrow_mut(&mut self, pool: &PoolHandle) -> &mut T {
        self.deref_mut(pool)
    }

    // PoolHandle을 받아야하므로 AsRef trait impl 하던 것을 직접 구현
    /// as_ref
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn as_ref(&self, pool: &PoolHandle) -> &T {
        self.deref(pool)
    }

    // PoolHandle을 받아야하므로 AsMut trait impl 하던 것을 직접 구현
    /// as_mut
    ///
    /// # Safety
    ///
    /// TODO: pool1의 ptr이 pool2의 시작주소를 사용하는 일이 없도록 해야함
    pub unsafe fn as_mut(&mut self, pool: &PoolHandle) -> &mut T {
        self.deref_mut(pool)
    }
}

impl<T: ?Sized + Pointable> Drop for Owned<T> {
    fn drop(&mut self) {
        // TODO: 어느 풀에서 free할지 알아야함 (i.e. PoolHandle을 받아야함)
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (offset, tag) = decompose_tag::<T>(self.data);

        f.debug_struct("Owned")
            .field("offset", &offset)
            .field("tag", &tag)
            .finish()
    }
}

// PoolHandle을 받아야하므로 Clone trait impl 하던 것을 직접 구현
impl<T: Clone> Owned<T> {
    /// 주어진 pool에 clone
    pub fn clone(&self, pool: &PoolHandle) -> Self {
        Owned::new(unsafe { self.deref(pool) }.clone(), pool).with_tag(self.tag())
    }
}

// TODO: PersistentBox 구현할지 고민 필요
// impl<T> From<Box<T>> for Owned<T> {
//     /// Returns a new owned pointer pointing to `b`.
//     ///
//     /// # Panics
//     ///
//     /// Panics if the pointer (the `Box`) is not properly aligned.
//     ///
//     /// # Examples
//     ///
//     /// ```
//     /// use crossbeam_epoch::Owned;
//     ///
//     /// let o = unsafe { Owned::from_raw(Box::into_raw(Box::new(1234))) };
//     /// ```
//     fn from(b: Box<T>) -> Self {
//         unsafe { Self::from_raw(Box::into_raw(b)) }
//     }
// }
/// A pointer to an object protected by the epoch GC.
///
/// The pointer is valid for use only during the lifetime `'g`.
///
/// The pointer must be properly aligned. Since it is aligned, a tag can be stored into the unused
/// least significant bits of the address.
pub struct Shared<'g, T: 'g + ?Sized + Pointable> {
    data: usize,
    _marker: PhantomData<(&'g (), *const T)>,
}

impl<T: ?Sized + Pointable> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Pointable> Copy for Shared<'_, T> {}

impl<T: ?Sized + Pointable> Pointer<T> for Shared<'_, T> {
    #[inline]
    fn into_usize(self) -> usize {
        self.data
    }

    #[inline]
    unsafe fn from_usize(data: usize) -> Self {
        Shared {
            data,
            _marker: PhantomData,
        }
    }
}

// lint "deny single_use_lifetimes" 로 인해 lifetime을 'g에서 '_로 바꿈
impl<T> Shared<'_, T> {
    /// Converts the pointer to a raw pointer (without the tag).
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_as_ptr");
    /// use compositional_persistent_object::plocation::ptr::PersistentPtr;
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let o = Owned::new(1234, &pool);
    /// let ptr = PersistentPtr::from(unsafe { o.deref(&pool) as *const _ as usize } - pool.start());
    /// let a = Atomic::from(o);
    ///
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// assert_eq!(p.as_ptr(), ptr);
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn as_ptr(&self) -> PersistentPtr<T> {
        let (offset, _) = decompose_tag::<T>(self.data);
        PersistentPtr::from(offset)
    }
}

impl<'g, T: ?Sized + Pointable> Shared<'g, T> {
    /// Returns a new null pointer.
    ///
    /// # Examples
    ///
    /// ```
    /// use compositional_persistent_object::pepoch::Shared;
    ///
    /// let p = Shared::<i32>::null();
    /// assert!(p.is_null());
    /// ```
    pub fn null() -> Shared<'g, T> {
        Shared {
            data: usize::MAX,
            _marker: PhantomData,
        }
    }

    /// Returns `true` if the pointer is null.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_is_null");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::null();
    /// let guard = &epoch::pin();
    /// assert!(a.load(SeqCst, guard).is_null());
    /// a.store(Owned::new(1234, &pool), SeqCst);
    /// assert!(!a.load(SeqCst, guard).is_null());
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn is_null(&self) -> bool {
        // NOTE: decompose_tag하면 안됨. null 식별자인 usize::MAX랑 달라짐
        self.data == usize::MAX
    }

    /// Dereferences the pointer.
    ///
    /// Returns a reference to the pointee that is valid during the lifetime `'g`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possibility of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_deref");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert_eq!(p.deref(&pool), &1234);
    /// }
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[allow(clippy::should_implement_trait)]
    pub unsafe fn deref(&self, pool: &PoolHandle) -> &'g T {
        let (offset, _) = decompose_tag::<T>(self.data);
        T::deref(offset, pool)
    }

    /// Dereferences the pointer.
    ///
    /// Returns a mutable reference to the pointee that is valid during the lifetime `'g`.
    ///
    /// # Safety
    ///
    /// * There is no guarantee that there are no more threads attempting to read/write from/to the
    ///   actual object at the same time.
    ///
    ///   The user must know that there are no concurrent accesses towards the object itself.
    ///
    /// * Other than the above, all safety concerns of `deref()` applies here.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_deref_mut");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(vec![1, 2, 3, 4], &pool);
    /// let guard = &epoch::pin();
    ///
    /// let mut p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert!(!p.is_null());
    ///     let b = p.deref_mut(&pool);
    ///     assert_eq!(b, &vec![1, 2, 3, 4]);
    ///     b.push(5);
    ///     assert_eq!(b, &vec![1, 2, 3, 4, 5]);
    /// }
    ///
    /// let p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert_eq!(p.deref(&pool), &vec![1, 2, 3, 4, 5]);
    /// }
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub unsafe fn deref_mut(&mut self, pool: &PoolHandle) -> &'g mut T {
        let (offset, _) = decompose_tag::<T>(self.data);
        T::deref_mut(offset, pool)
    }

    /// Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possibility of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_as_ref");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert_eq!(p.as_ref(&pool), Some(&1234));
    /// }
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub unsafe fn as_ref(&self, pool: &PoolHandle) -> Option<&'g T> {
        let (offset, _) = decompose_tag::<T>(self.data);
        if offset == usize::MAX {
            None
        } else {
            Some(T::deref(offset, pool))
        }
    }

    /// Takes ownership of the pointee.
    ///
    /// # Panics
    ///
    /// Panics if this pointer is null, but only in debug mode.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid and nobody else is holding a
    /// reference to the same object.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_into_owned");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234, &pool);
    /// unsafe {
    ///     let guard = &epoch::unprotected();
    ///     let p = a.load(SeqCst, guard);
    ///     drop(p.into_owned());
    /// }
    /// ```
    pub unsafe fn into_owned(self) -> Owned<T> {
        debug_assert!(!self.is_null(), "converting a null `Shared` into `Owned`");
        Owned::from_usize(self.data)
    }

    /// Returns the tag stored within the pointer.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_tag");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic, Owned};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::<u64>::from(Owned::new(0u64, &pool).with_tag(2));
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// assert_eq!(p.tag(), 2);
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_tag::<T>(self.data);
        tag
    }

    /// Returns the same pointer, but tagged with `tag`. `tag` is truncated to be fit into the
    /// unused bits of the pointer to `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_with_tag");
    /// use compositional_persistent_object::pepoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(0u64, &pool);
    /// let guard = &epoch::pin();
    /// let p1 = a.load(SeqCst, guard);
    /// let p2 = p1.with_tag(2);
    ///
    /// assert_eq!(p1.tag(), 0);
    /// assert_eq!(p2.tag(), 2);
    /// assert_eq!(p1.as_ptr(), p2.as_ptr());
    /// ```
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn with_tag(&self, tag: usize) -> Shared<'g, T> {
        unsafe { Self::from_usize(compose_tag::<T>(self.data, tag)) }
    }

    // PoolHandle을 받아야하므로 fmt trait impl 하던 것을 직접 구현
    /// formatting Pointer
    pub fn fmt(&self, f: &mut fmt::Formatter<'_>, pool: &PoolHandle) -> fmt::Result {
        fmt::Pointer::fmt(&(unsafe { self.deref(pool) as *const _ }), f)
    }
}

impl<T> From<PersistentPtr<T>> for Shared<'_, T> {
    /// Returns a new pointer pointing to `raw`.
    ///
    /// # Panics
    ///
    /// Panics if `raw` is not properly aligned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use compositional_persistent_object::util::*;
    /// # let pool = get_test_handle("shared_from_ptr");
    /// use compositional_persistent_object::pepoch::Shared;
    ///
    /// let ptr = pool.alloc::<usize>();
    /// let p = Shared::from(ptr);
    /// assert!(!p.is_null());
    /// ```
    fn from(ptr: PersistentPtr<T>) -> Self {
        let offset = ptr.into_offset();
        ensure_aligned::<T>(offset);
        unsafe { Self::from_usize(offset) }
    }
}

impl<'g, T: ?Sized + Pointable> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: ?Sized + Pointable> Eq for Shared<'_, T> {}

impl<'g, T: ?Sized + Pointable> PartialOrd<Shared<'g, T>> for Shared<'g, T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.data.partial_cmp(&other.data)
    }
}

impl<T: ?Sized + Pointable> Ord for Shared<'_, T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl<T: ?Sized + Pointable> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (offset, tag) = decompose_tag::<T>(self.data);

        f.debug_struct("Shared")
            .field("offset", &offset)
            .field("tag", &tag)
            .finish()
    }
}

impl<T: ?Sized + Pointable> Default for Shared<'_, T> {
    fn default() -> Self {
        Shared::null()
    }
}

#[cfg(all(test, not(crossbeam_loom)))]
mod tests {
    use super::{Owned, Shared};
    use std::mem::MaybeUninit;

    #[test]
    fn valid_tag_i8() {
        let _ = Shared::<i8>::null().with_tag(0);
    }

    #[test]
    fn valid_tag_i64() {
        let _ = Shared::<i64>::null().with_tag(7);
    }

    #[cfg(feature = "nightly")]
    #[test]
    fn const_atomic_null() {
        use super::Atomic;
        static _U: Atomic<u8> = Atomic::<u8>::null();
    }

    #[test]
    fn array_init() {
        let pool = crate::util::get_test_handle("atomic_array_init.pool");
        let owned = Owned::<[MaybeUninit<usize>]>::init(10, &pool);
        let arr: &[MaybeUninit<usize>] = unsafe { owned.deref(&pool) };
        assert_eq!(arr.len(), 10);
    }
}
