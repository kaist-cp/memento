//! Persistent epoch-based garbage collector

pub mod atomic;

pub use atomic::{PAtomic, POwned, PShared};
pub use crossbeam_epoch::{pin, unprotected, Guard};

/// crossbeam의 Guard가 PAtomic 포인터도 다룰 수 있도록 하기 위한 trait
pub trait PDestroy {
    /// Stores a destructor for an object so that it can be deallocated and dropped at some point after all currently pinned threads get unpinned.
    /// 
    /// # Safety
    /// 
    /// TODO
    unsafe fn p_defer_destroy<T>(&self, ptr: PShared<'_, T>);
}

impl PDestroy for Guard {
    unsafe fn p_defer_destroy<T>(&self, ptr: PShared<'_, T>) {
        self.defer_unchecked(move || ptr.into_owned());
    }
}
