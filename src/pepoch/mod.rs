//! Persistent epoch-based garbage collector
// TODO: 모듈 이름 바꾸든가 하기. persistent EBR이 아닌 volatile EBR을 사용중

pub mod atomic;

pub use atomic::{PAtomic, POwned, PShared};
pub use crossbeam_epoch::{pin, unprotected, Guard};

/// crossbeam의 Guard가 PAtomic 포인터도 다룰 수 있도록 하기 위한 trait
pub trait PDestroyable {
    /// Stores a destructor for an persistent object so that it can be deallocated and dropped at some point
    /// after all currently pinned threads get unpinned.
    ///
    /// This method first stores the destructor into the thread-local (or handle-local) cache. If
    /// this cache becomes full, some destructors are moved into the global cache. At the same
    /// time, some destructors from both local and global caches may get executed in order to
    /// incrementally clean up the caches as they fill up.
    ///
    /// There is no guarantee when exactly the destructor will be executed. The only guarantee is
    /// that it won't be executed until all currently pinned threads get unpinned. In theory, the
    /// destructor might never run, but the epoch-based garbage collection will make an effort to
    /// execute it reasonably soon.
    ///
    /// If this method is called from an [`unprotected`] guard, the destructor will simply be
    /// executed immediately.
    ///
    /// # Safety
    ///
    /// The object must not be reachable by other threads anymore, otherwise it might be still in
    /// use when the destructor runs.
    ///
    /// Apart from that, keep in mind that another thread may execute the destructor, so the object
    /// must be sendable to other threads.
    ///
    /// We intentionally didn't require `T: Send`, because Rust's type systems usually cannot prove
    /// `T: Send` for typical use cases. For example, consider the following code snippet, which
    /// exemplifies the typical use case of deferring the deallocation of a shared reference:
    ///
    /// ```ignore
    /// // Assume there is PoolHandle, `pool`
    /// let shared = POwned::new(7i32, &pool).into_shared(guard);
    /// guard.defer_pdestroy(shared); // `Shared` is not `Send`!
    /// ```
    ///
    /// While `Shared` is not `Send`, it's safe for another thread to call the destructor, because
    /// it's called only after the grace period and `shared` is no longer shared with other
    /// threads. But we don't expect type systems to prove this.
    ///
    /// # Examples
    ///
    /// When a persistent heap-allocated object in a data structure becomes unreachable, it has to be
    /// deallocated. However, the current thread and other threads may be still holding references
    /// on the stack to that same object. Therefore it cannot be deallocated before those references
    /// get dropped. This method can defer deallocation until all those threads get unpinned and
    /// consequently drop all their references on the stack.
    ///
    /// ```
    /// # // 테스트용 pool 얻기
    /// # use memento::plocation::pool::*;
    /// # use memento::persistent::*;
    /// # use memento::utils::tests::get_dummy_handle;
    /// # let pool = get_dummy_handle(8 * 1024 * 1024 * 1024).unwrap();
    /// use memento::pepoch::{self as epoch, PAtomic, POwned, PShared, PDestroyable};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// // Assume there is PoolHandle, `pool`
    /// let a = PAtomic::new("foo", &pool);
    ///
    /// // Now suppose that `a` is shared among multiple threads and concurrently
    /// // accessed and modified...
    ///
    /// // Pin the current thread.
    /// let guard = &epoch::pin();
    ///
    /// // Steal the object currently stored in `a` and swap it with another one.
    /// let p = a.swap(POwned::new("bar", &pool).into_shared(guard), SeqCst, guard);
    ///
    /// if !p.is_null() {
    ///     // The persistent object `p` is pointing to is now unreachable.
    ///     // Defer its deallocation until all currently pinned threads get unpinned.
    ///     unsafe {
    ///         guard.defer_pdestroy(p);
    ///     }
    /// }
    /// ```
    unsafe fn defer_pdestroy<T>(&self, ptr: PShared<'_, T>);
}

impl PDestroyable for Guard {
    unsafe fn defer_pdestroy<T>(&self, ptr: PShared<'_, T>) {
        self.defer_unchecked(move || ptr.into_owned());
    }
}
