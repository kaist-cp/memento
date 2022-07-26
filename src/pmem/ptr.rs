//! Persistent Pointer
use super::{pool::PoolHandle, Collectable, GarbageCollection};
use std::marker::PhantomData;

/// NULL identifier of relative address
const NULL_OFFSET: usize = 0;

/// Pointer to an object belonging to the pool
/// - It has an offset from the starting address of the pool.
/// - When referencing, refer to the absolute address of the pool start address plus the offset
#[derive(Debug, Default)]
pub struct PPtr<T: ?Sized> {
    offset: usize,
    _marker: PhantomData<T>,
}

impl<T: ?Sized> Clone for PPtr<T> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            _marker: PhantomData,
        }
    }
}

impl<T: Collectable> Collectable for PPtr<T> {
    fn filter(ptr: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        if !ptr.is_null() {
            let t_ref = unsafe { ptr.deref_mut(pool) };
            T::mark(t_ref, tid, gc);
        }
    }
}

impl<T: ?Sized> Copy for PPtr<T> {}

impl<T: ?Sized> PPtr<T> {
    /// Return a null pointer
    pub const fn null() -> Self {
        Self {
            offset: NULL_OFFSET,
            _marker: PhantomData,
        }
    }

    /// Convert to offset
    ///
    /// # Example
    ///
    /// Required to convert the `PPtr` that comes out when allocated to the pool into an Atomic Pointer.
    /// - `POwned::from_usize(ptr.into_offset())`
    pub fn into_offset(self) -> usize {
        self.offset
    }

    /// Check for null pointer
    pub fn is_null(self) -> bool {
        self.offset == NULL_OFFSET
    }
}

impl<T> PPtr<T> {
    /// Refer by absolute address
    ///
    /// # Safety
    ///
    /// When multiple pools are opened at the same time, the ptr of pool1 should not use the starting address of pool2.
    pub unsafe fn deref(self, pool: &PoolHandle) -> &'_ T {
        let addr = pool.start() + self.offset;
        debug_assert!(
            self != PPtr::null() && pool.valid(addr),
            "offset: {}",
            self.offset
        );

        &*(addr as *const T)
    }

    /// Refer mutably by absolute address
    ///
    /// # Safety
    ///
    /// When multiple pools are opened at the same time, the ptr of pool1 should not use the starting address of pool2.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn deref_mut(self, pool: &PoolHandle) -> &'_ mut T {
        let addr = pool.start() + self.offset;
        debug_assert!(
            self != PPtr::null() && pool.valid(addr),
            "offset: {}",
            self.offset
        );
        &mut *(addr as *mut T)
    }
}

/// Convert reference to persistent ptr
pub trait AsPPtr {
    /// Convert reference to persistent ptr
    ///
    /// # Safety
    ///
    /// object must be a reference in `pool`
    unsafe fn as_pptr(&self, pool: &PoolHandle) -> PPtr<Self>;
}

impl<T> AsPPtr for T {
    unsafe fn as_pptr(&self, pool: &PoolHandle) -> PPtr<Self> {
        PPtr {
            offset: self as *const T as usize - pool.start(),
            _marker: PhantomData,
        }
    }
}

impl<T> From<usize> for PPtr<T> {
    /// Regard the given offset as the starting address of T obj and returns a pointer referencing it.
    fn from(off: usize) -> Self {
        Self {
            offset: off,
            _marker: PhantomData,
        }
    }
}

impl<T> PartialEq<PPtr<T>> for PPtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl<T> Eq for PPtr<T> {}
