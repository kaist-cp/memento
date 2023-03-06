//! API for Persistent Memory Allocator
#![allow(warnings)] // TODO: Remove

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::RootIdx,
};
use crossbeam_utils::CachePadded;
use etrace::some_or;
use libc::*;
use std::{
    ffi::CString,
    io::Error,
    mem::{self, transmute, MaybeUninit},
    path::Path,
    sync::atomic::AtomicUsize,
};

use super::{global_pool, Pool, PoolHandle};

#[cfg(not(feature = "pmcheck"))]
mod ralloc;
#[cfg(not(feature = "pmcheck"))]
pub(crate) type PMEMAllocator = ralloc::RallocAllocator;

#[cfg(feature = "pmcheck")]
mod pmdk;
/// Persistent Allocator
#[cfg(feature = "pmcheck")]
pub(crate) type PMEMAllocator = pmdk::PMDKAllocator;
#[cfg(feature = "pmcheck")]
pub(crate) use pmdk::POPS;

/// Trait for persistent allocator
#[allow(missing_docs)]
pub trait PAllocator {
    ///  Persistent pool file management
    unsafe fn open(filepath: *const c_char, filesize: u64) -> c_int;
    unsafe fn create(filepath: *const c_char, filesize: u64) -> c_int;
    unsafe fn mmapped_addr() -> usize;
    unsafe fn close(start: usize, len: usize);
    unsafe fn recover() -> c_int;

    /// Root management
    unsafe fn set_root(ptr: *mut c_void, i: u64) -> *mut c_void;
    unsafe fn get_root(i: u64) -> *mut c_void;

    // Dyanmic allocation
    unsafe fn malloc(sz: c_ulong) -> *mut c_void;
    unsafe fn free(ptr: *mut c_void, _len: usize);

    /// Functions for recovery
    unsafe fn mark<T: Collectable>(s: &mut T, tid: usize, gc: &mut GarbageCollection);
    unsafe extern "C" fn filter_inner<T: Collectable>(
        ptr: *mut T,
        tid: usize,
        gc: &mut GarbageCollection,
    );
    unsafe fn set_root_filter<T: Collectable>(i: u64);
}

/// GarbageCollection
#[cfg(not(feature = "pmcheck"))]
pub type GarbageCollection = ralloc::GarbageCollection;
/// GarbageCollection
#[cfg(feature = "pmcheck")]
pub type GarbageCollection = ();

/// Trait for Garbage Collection
///
/// For a persistent obj to be marked by the Ralloc GC, it must impl this trait.
///
/// ```text
///             ----------- Black box ------------------      Ralloc
///            |                                        |
/// fn mark   --->          unsafe RP_mark             --->    ...
///     ^      |                                        |       |
///     |      |                                        |       |
///     |      |                                        |       v
/// fn filter <---       unsafe filter_inner           <---    ...
///            |                                        |
///             ----------------------------------------
/// ```
pub trait Collectable: Sized {
    /// Mark itself and reserve the next marking with its filter func
    fn mark(s: &mut Self, tid: usize, gc: &mut GarbageCollection) {
        unsafe { PMEMAllocator::mark(s, tid, gc) }
    }

    /// - This function is intended to be called by Ralloc and should not be used by the user.
    /// - When Ralloc calls this function, Rust finds obj's filter func and calls it
    ///
    /// # Guaranteed by Ralloc
    ///
    /// - The ptr passed by Ralloc is a raw pointer pointing to itself.
    ///
    /// # Safety
    ///
    /// Do not use this function
    unsafe extern "C" fn filter_inner(ptr: *mut c_char, tid: usize, gc: &mut GarbageCollection) {
        PMEMAllocator::filter_inner::<Self>(ptr as *mut _ as *mut Self, tid, gc);
    }

    /// Find something to mark in its field and mark it
    ///
    /// # Example
    ///
    /// ```
    /// # use memento::pmem::pool::PoolHandle;
    /// # use memento::pmem::ralloc::GarbageCollection;
    /// # use memento::pmem::ralloc::Collectable;
    /// # use memento::pmem::ptr::PPtr;
    /// # struct Inner {}
    /// # impl Collectable for Inner {
    /// #    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
    /// #    }
    /// # }
    /// struct Node {
    ///     inner: Inner, // Assume `Inner` impl Collectable
    ///     next: PPtr<Node>,
    /// }
    ///
    /// impl Collectable for Node {
    ///     fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
    ///         // Call filter of inner to mark the ptr in the inner struct
    ///         Inner::filter(&mut node.inner, gc, pool);
    ///
    ///         // Mark the next node if the pointer is valid
    ///         if !node.next.is_null() {
    ///             let next = unsafe { node.next.deref_mut(pool) };
    ///             Node::mark(next, gc);
    ///         }
    ///     }
    /// }
    /// ```
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle);
}

impl<T: Collectable, U: Collectable> Collectable for (T, U) {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut s.0, tid, gc, pool);
        U::filter(&mut s.1, tid, gc, pool);
    }
}

impl<T: Collectable, U: Collectable, V: Collectable> Collectable for (T, U, V) {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut s.0, tid, gc, pool);
        U::filter(&mut s.1, tid, gc, pool);
        V::filter(&mut s.2, tid, gc, pool);
    }
}

impl<T: Collectable, U: Collectable, V: Collectable, W: Collectable> Collectable for (T, U, V, W) {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut s.0, tid, gc, pool);
        U::filter(&mut s.1, tid, gc, pool);
        V::filter(&mut s.2, tid, gc, pool);
        W::filter(&mut s.3, tid, gc, pool);
    }
}

impl Collectable for () {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for usize {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for u64 {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for u32 {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for u8 {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for bool {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for AtomicUsize {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl Collectable for c_void {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {}
}

impl<T: Collectable> Collectable for Option<T> {
    fn filter(opt: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let v = some_or!(opt, return);
        T::filter(v, tid, gc, pool);
    }
}

impl<T: Collectable> Collectable for MaybeUninit<T> {
    fn filter(mu: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(unsafe { mu.assume_init_mut() }, tid, gc, pool);
    }
}

impl<T: Collectable> Collectable for CachePadded<T> {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        T::filter(&mut *s, tid, gc, pool);
    }
}
