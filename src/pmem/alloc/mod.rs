//! API for Persistent Memory Allocator
#![allow(warnings)] // TODO: Remove

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
mod ralloc;
use ralloc::*;

use crate::{
    pepoch::{atomic::Pointer, PAtomic, PShared},
    pmem::RootIdx,
};

use super::{global_pool, Pool, PoolHandle};

const NUM_ROOT: usize = 128;

struct Root {
    objs: [pmemobj_sys::pmemoid; NUM_ROOT],
    filters: [Option<filter_func>; NUM_ROOT],
}

impl Root {
    fn new() -> Self {
        Root {
            objs: array_init::array_init(|_| pmemobj_sys::pmemoid {
                off: 0,
                pool_uuid_lo: 0,
            }),
            filters: array_init::array_init(|_| None),
        }
    }
}

type filter_func =
    unsafe fn(s: *mut c_void, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle);
unsafe impl Sync for Root {}

static mut ROOT: *mut Root = std::ptr::null_mut();
pub(crate) static mut POPS: *mut pmemobj_sys::PMEMobjpool = std::ptr::null_mut();

pub(crate) unsafe fn pmem_open(filepath: *const c_char, filesize: u64) -> c_int {
    println!("[pmem_open] start!]");
    if cfg!(feature = "pmcheck") {
        let res = chmod(filepath, 0o777);
        POPS = pmemobj_sys::pmemobj_open(filepath, std::ptr::null_mut());
        if POPS.is_null() {
            let msg = pmemobj_sys::pmemobj_errormsg();
            let msgg = msg.as_ref().unwrap().to_string();
            panic!("err: {:?}", msgg);
        }

        let root = pmemobj_sys::pmemobj_root(POPS, mem::size_of::<Root>());
        ROOT = pmemobj_sys::pmemobj_direct(root) as *mut Root;
        println!("[pmem_open] finish!]");
        return 1;
    } else {
        // Ralloc
        RP_init(filepath, filesize)
    }
}

pub(crate) unsafe fn pmem_create(filepath: *const c_char, filesize: u64) -> c_int {
    if cfg!(feature = "pmcheck") {
        unsafe {
            POPS = pmemobj_sys::pmemobj_create(
                filepath,
                std::ptr::null_mut(),
                filesize as usize,
                0o777,
            );
            if POPS.is_null() {
                let msg = pmemobj_sys::pmemobj_errormsg();
                let msgg = msg.as_ref().unwrap().to_string();
                panic!("err: {:?}", msgg)
            }

            let root = pmemobj_sys::pmemobj_root(POPS, mem::size_of::<Root>());
            ROOT = pmemobj_sys::pmemobj_direct(root) as *mut Root;
            return 0;
        }
    } else {
        RP_init(filepath, filesize)
    }
}

pub(crate) unsafe fn pmem_mmapped_addr() -> usize {
    if cfg!(feature = "pmcheck") {
        let root_oid = pmemobj_sys::pmemobj_oid(ROOT as *mut c_void);
        ROOT as usize - root_oid.off as usize
    } else {
        // Ralloc
        RP_mmapped_addr()
    }
}

pub(crate) unsafe fn pmem_close(start: usize, len: usize) {
    if cfg!(feature = "pmcheck") {
        if !POPS.is_null() {
            pmemobj_sys::pmemobj_close(POPS);
            POPS = std::ptr::null_mut();
        }
    } else {
        // Ralloc
        RP_close();
    }
}

pub(crate) unsafe fn pmem_recover() -> c_int {
    if cfg!(feature = "pmcheck") {
        // Call root filters
        let root = ROOT.as_mut().unwrap();
        for (i, filter) in root.filters.iter().enumerate() {
            let oid = root.objs[i];
            if let Some(filter) = filter {
                let obj = pmemobj_sys::pmemobj_direct(oid);
                assert!(!obj.is_null());
                filter(
                    obj,
                    i,
                    &mut *(&mut () as *mut _ as *mut GarbageCollection),
                    global_pool().unwrap(),
                );
            }
        }
        1
    } else {
        // Ralloc
        RP_recover()
    }
}

pub(crate) unsafe fn pmem_set_root(ptr: *mut c_void, i: u64) -> *mut c_void {
    if cfg!(feature = "pmcheck") {
        let root = ROOT.as_mut().unwrap();
        let old = root.objs[i as usize];
        let oid = pmemobj_sys::pmemobj_oid(ptr);
        root.objs[i as usize] = oid;
        pmemobj_sys::pmemobj_direct(old)
    } else {
        // Ralloc
        RP_set_root(ptr, i)
    }
}

pub(crate) unsafe fn pmem_get_root(i: u64) -> *mut c_void {
    if cfg!(feature = "pmcheck") {
        let oid = ROOT.as_mut().unwrap().objs[i as usize];
        if oid.pool_uuid_lo == 0 {
            panic!("err: !!!");
        }
        pmemobj_sys::pmemobj_direct(oid)
    } else {
        // Ralloc
        RP_get_root_c(i)
    }
}

pub(crate) unsafe fn pmem_malloc(sz: c_ulong) -> *mut c_void {
    let addr = if cfg!(feature = "pmcheck") {
        let mut oid = pmemobj_sys::PMEMoid {
            off: 0,
            pool_uuid_lo: 0,
        };
        let oidp = &mut oid;
        let status = unsafe {
            pmemobj_sys::pmemobj_zalloc(
                POPS,
                oidp as *mut pmemobj_sys::PMEMoid,
                if sz == 0 { 64 } else { sz.try_into().unwrap() },
                0,
                // None,
                // std::ptr::null_mut(),
            )
        };
        if status == 0 {
            pmemobj_sys::pmemobj_direct(oid)
        } else {
            panic!("err");
        }
    } else {
        // Ralloc
        RP_malloc(sz)
    };
    addr
}

pub(crate) unsafe fn pmem_free(ptr: *mut c_void, _len: usize) {
    if cfg!(feature = "pmcheck") {
        let mut oid = pmemobj_sys::pmemobj_oid(ptr);
        pmemobj_sys::pmemobj_free(&mut oid as *mut _);
    } else {
        // Ralloc
        RP_free(ptr)
    }
}

pub(crate) unsafe fn pmem_set_root_filter<T: Collectable>(i: u64) {
    if cfg!(feature = "pmcheck") {
        unsafe fn root_filter<T: Collectable>(
            s: *mut c_void,
            tid: usize,
            gc: &mut GarbageCollection,
            pool: &mut PoolHandle,
        ) {
            T::filter(&mut *(s as *mut T), tid, gc, pool)
        };

        ROOT.as_mut().unwrap().filters[i as usize] = Some(root_filter::<T>);
    } else {
        unsafe extern "C" fn root_filter<T: Collectable>(
            ptr: *mut c_char,
            tid: usize,
            gc: &mut GarbageCollection,
        ) {
            RP_mark(
                gc,
                ptr,
                tid.wrapping_sub(RootIdx::MementoStart as usize),
                Some(T::filter_inner),
            );
        }

        RP_set_root_filter(Some(root_filter::<T>), i)
    }
}

/// GarbageCollection
#[cfg(not(features = "pmcheck"))]
pub type GarbageCollection = ralloc::GarbageCollection;
#[cfg(features = "pmcheck")]
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
        if cfg!(feature = "pmcheck") {
            Self::filter(s, tid, gc, global_pool().unwrap());
        } else {
            // Ralloc
            let ptr = s as *mut _ as *mut c_char;
            unsafe { RP_mark(gc, ptr, tid, Some(Self::filter_inner)) };
        }
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
        if cfg!(feature = "pmcheck") {
            unreachable!("This function is only for Ralloc GC.")
        } else {
            let pool = global_pool().unwrap();
            let s = (ptr as *mut _ as *mut Self).as_mut().unwrap();
            Self::filter(s, tid, gc, pool);
        }
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
