//! Linking Ralloc (https://github.com/urcs-sync/ralloc)

use crossbeam_utils::CachePadded;
use etrace::some_or;

use crate::pmem::{Collectable, RootIdx};

use super::{
    super::{global_pool, PoolHandle},
    PAllocator,
};
use std::{
    mem::MaybeUninit,
    os::raw::{c_char, c_int, c_ulong, c_void},
    sync::atomic::AtomicUsize,
};

/* automatically generated by rust-bindgen 0.59.1 */
//
// command: bindgen --allowlist-function "RP.*" ext/ralloc/src/ralloc.hpp -o ralloc.rs
#[link(name = "ralloc", kind = "static")]
extern "C" {
    /// If return is 1, the original file is opened, otherwise the file is newly created.
    pub(crate) fn RP_init(_id: *const c_char, size: u64) -> c_int;

    /// If the return is 1, it means that it is dirty, so it is garbage-collected, otherwise, it is not dirty, not garbage-collected.
    pub(crate) fn RP_recover() -> c_int;

    pub(crate) fn RP_close();

    pub(crate) fn RP_malloc(sz: c_ulong) -> *mut c_void;

    pub(crate) fn RP_free(ptr: *mut c_void);

    pub(crate) fn RP_set_root(ptr: *mut c_void, i: u64) -> *mut c_void;

    pub(crate) fn RP_get_root_c(i: u64) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_malloc_size(ptr: *mut c_void) -> c_ulong;

    #[allow(dead_code)]
    pub(crate) fn RP_calloc(num: c_ulong, size: c_ulong) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_realloc(ptr: *mut c_void, new_size: c_ulong) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_in_prange(ptr: *mut c_void) -> c_int;

    #[allow(dead_code)]
    pub(crate) fn RP_region_range(
        idx: c_int,
        start_addr: *mut *mut c_void,
        end_addr: *mut *mut c_void,
    ) -> c_int;
}

/// Return the mapped address when RP_init
#[allow(non_snake_case)]
pub(crate) unsafe fn RP_mmapped_addr() -> usize {
    let mut start: *mut i32 = std::ptr::null_mut();
    let mut end: *mut i32 = std::ptr::null_mut();
    let res = RP_region_range(
        1, // superblock region's index.
        &mut start as *mut *mut _ as *mut *mut c_void,
        &mut end as *mut *mut _ as *mut *mut c_void,
    );
    assert!(res == 0);
    start as usize
}

// Functions added for GC that are not in the original ralloc
#[link(name = "ralloc", kind = "static")]
extern "C" {
    /// Recognize Ralloc's type `GarbageCollection`
    pub type GarbageCollection;

    /// Register the root filter function, which is the starting point of GC.
    pub(crate) fn RP_set_root_filter(
        filter_func: ::std::option::Option<
            unsafe extern "C" fn(*mut c_char, usize, &mut GarbageCollection),
        >,
        i: u64,
    );

    #[link_name = "\u{1}_ZN17GarbageCollection11mark_func_cEPcmPFvS0_mRS_E"]
    pub(crate) fn RP_mark(
        this: *mut GarbageCollection,
        ptr: *mut c_char,
        tid: usize,
        filter_func: ::std::option::Option<
            unsafe extern "C" fn(*mut c_char, usize, &mut GarbageCollection),
        >,
    );
}

pub(crate) struct RallocAllocator {}

impl PAllocator for RallocAllocator {
    unsafe fn open(filepath: *const libc::c_char, filesize: u64) -> libc::c_int {
        RP_init(filepath, filesize)
    }

    unsafe fn create(filepath: *const libc::c_char, filesize: u64) -> libc::c_int {
        RP_init(filepath, filesize)
    }

    unsafe fn mmapped_addr() -> usize {
        RP_mmapped_addr()
    }

    unsafe fn close(start: usize, len: usize) {
        RP_close();
    }

    unsafe fn recover() -> libc::c_int {
        RP_recover()
    }

    unsafe fn set_root(ptr: *mut libc::c_void, i: u64) -> *mut libc::c_void {
        RP_set_root(ptr, i)
    }

    unsafe fn get_root(i: u64) -> *mut libc::c_void {
        RP_get_root_c(i)
    }

    unsafe fn malloc(sz: libc::c_ulong) -> *mut libc::c_void {
        RP_malloc(sz)
    }

    unsafe fn free(ptr: *mut libc::c_void, _len: usize) {
        RP_free(ptr)
    }

    unsafe fn set_root_filter<T: Collectable>(i: u64) {
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

    unsafe fn mark<T: Collectable>(s: &mut T, tid: usize, gc: &mut super::GarbageCollection) {
        let ptr = s as *mut _ as *mut c_char;
        unsafe { RP_mark(gc, ptr, tid, Some(T::filter_inner)) };
    }

    unsafe extern "C" fn filter_inner<T: Collectable>(
        ptr: *mut T,
        tid: usize,
        gc: &mut GarbageCollection,
    ) {
        let pool = global_pool().unwrap();
        // let s = (ptr as *mut _ as *mut T).as_mut().unwrap();
        T::filter(ptr.as_mut().unwrap(), tid, gc, pool);
    }
}