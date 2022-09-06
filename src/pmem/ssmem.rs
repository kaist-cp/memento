//! ssmem allocator
#![allow(warnings)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use libc::c_void;
use std::{
    alloc::Layout,
    cell::RefCell,
    intrinsics,
    mem::size_of,
    ptr::{null, null_mut},
};

use crate::pmem::*;

/* ****************************************************************************************
 */
/* parameters */
/* ****************************************************************************************
 */

const SSMEM_TRANSPARENT_HUGE_PAGES: usize = 0; /* Use or not Linux transparent huge pages */
const SSMEM_ZERO_MEMORY: usize = 1; /* Initialize allocated memory to 0 or not */
const SSMEM_GC_FREE_SET_SIZE: usize = 507; /* mem objects to free before doing a GC pass */
const SSMEM_GC_RLSE_SET_SIZE: usize = 3; /* num of released object before doing a GC pass */

/// memory-chunk size that each threads gives to the allocators
pub const SSMEM_DEFAULT_MEM_SIZE: usize = 32 * 1024 * 1024;

const SSMEM_MEM_SIZE_DOUBLE: usize = 0; /* if the allocator is out of memory, should it allocate \
                                         a 2x larger chunk than before? (in order to stop asking \
                                        for memory again and again */
const SSMEM_MEM_SIZE_MAX: usize = 4 * 1024 * 1024 * 1024; /* absolute max chunk size \
                                                          (e.g., if doubling is 1) */

/* increase the thread-local timestamp of activity on each ssmem_alloc() and/or
ssmem_free() call. If enabled (>0), after some memory is alloced and/or
freed, the thread should not access ANY ssmem-protected memory that was read
(the reference were taken) before the current alloc or free invocation. If
disabled (0), the program should employ manual SSMEM_SAFE_TO_RECLAIM() calls
to indicate when the thread does not hold any ssmem-allocated memory
references. */

// strategy to increase timestamp
const SSMEM_TS_INCR_ON_NONE: usize = 0;
const SSMEM_TS_INCR_ON_BOTH: usize = 1;
const SSMEM_TS_INCR_ON_ALLOC: usize = 2;
const SSMEM_TS_INCR_ON_FREE: usize = 3;

const SSMEM_TS_INCR_ON: usize = SSMEM_TS_INCR_ON_FREE;

/* ****************************************************************************************
 */
/* help definitions */
/* ****************************************************************************************
 */
const CACHE_LINE_SIZE: usize = 64;

/* ****************************************************************************************
 */
/* data structures used by ssmem */
/* ****************************************************************************************
 */

/// an ssmem allocator
#[derive(Debug)]
#[repr(align(64))]
pub struct SsmemAllocator {
    mem: *mut c_void,

    mem_curr: usize,

    mem_size: usize,

    tot_size: usize,

    fs_size: usize,

    /// list of memory chunks
    pub mem_chunks: *const SsmemList,

    // timestamp
    ts: *mut SsmemTS,

    free_set_list: *mut SsmemFreeSet,

    free_set_num: usize,

    collected_set_list: *mut SsmemFreeSet,

    collected_set_num: usize,

    available_set_list: *mut SsmemFreeSet,

    released_num: usize,

    released_mem_list: *const SsmemReleased,
}

#[repr(align(64))]
struct SsmemTS {
    version: usize,
    id: usize,
    next: *mut SsmemTS,
}

/// ssmem allocator manage free blocks using `SsmemFreeSet`
///
/// # Possible transition of `SsmemFreeSet` on `SsmemAllocator`
///
/// free_set -> collected_set -> available_set -> free_set ...
#[repr(align(64))]
struct SsmemFreeSet {
    ts_set: *mut usize,
    size: usize,
    curr: isize,
    set_next: *mut SsmemFreeSet,

    /// start address of free obj(s)
    set: *mut usize,
}

struct SsmemReleased {
    ts_set: *const usize,
    mem: *const c_void,
    next: *const SsmemReleased,
}

/// SsmemList
#[derive(Debug)]
pub struct SsmemList {
    /// pointing memory chunk
    pub obj: *const c_void,

    /// pointing next memory chunk
    pub next: *const SsmemList,
}

/* ****************************************************************************************
 */
/* ssmem interface */
/* ****************************************************************************************
 */

/// initialize an allocator with the default number of objects
pub fn ssmem_alloc_init(a: *mut SsmemAllocator, size: usize, id: isize, pool: Option<&PoolHandle>) {
    ssmem_alloc_init_fs_size(a, size, SSMEM_GC_FREE_SET_SIZE, id, pool)
}

/// initialize an allocator and give the number of objects in free_sets
pub fn ssmem_alloc_init_fs_size(
    a: *mut SsmemAllocator,
    size: usize,
    free_set_size: usize,
    id: isize,
    pool: Option<&PoolHandle>,
) {
    SSMEM_NUM_ALLOCATORS.with(|x| *x.borrow_mut() += 1);
    SSMEM_ALLOCATOR_LIST.with(|x| {
        let next = *x.borrow();
        *x.borrow_mut() = ssmem_list_node_new(a as *mut _, next, pool)
    });

    // allocate first memory chunk
    let a = unsafe { a.as_mut() }.unwrap();
    a.mem = alloc(
        Layout::from_size_align(size, CACHE_LINE_SIZE).unwrap(),
        pool,
    );
    assert!(a.mem != null_mut());

    a.mem_curr = 0;
    a.mem_size = size;
    a.tot_size = size;
    a.fs_size = free_set_size;

    // zero-initialize memory chunk
    ssmem_zero_memory(a);

    let new_mem_chunks: *const SsmemList = ssmem_list_node_new(a.mem, null(), pool);
    persist_obj(unsafe { new_mem_chunks.as_ref() }.unwrap(), true);

    a.mem_chunks = new_mem_chunks;
    persist_obj(unsafe { a.mem_chunks.as_ref() }.unwrap(), true);
    ssmem_gc_thread_init(a, id, pool);

    a.free_set_list = ssmem_free_set_new(a.fs_size, null_mut(), pool);
    a.free_set_num = 1;

    a.collected_set_list = null_mut();
    a.collected_set_num = 0;

    a.available_set_list = null_mut();

    a.released_mem_list = null();
    a.released_num = 0;
}

/// explicitely subscribe to the list of threads in order to used timestamps for GC
pub fn ssmem_gc_thread_init(a: *mut SsmemAllocator, id: isize, pool: Option<&PoolHandle>) {
    let a_ref = unsafe { a.as_mut() }.unwrap();
    a_ref.ts = SSMEM_TS_LOCAL.with(|ts| *ts.borrow());
    if a_ref.ts.is_null() {
        a_ref.ts = alloc(
            Layout::from_size_align(size_of::<SsmemTS>(), CACHE_LINE_SIZE).unwrap(),
            pool,
        );
        assert!(!a_ref.ts.is_null());
        SSMEM_TS_LOCAL.with(|ts| {
            let prev = ts.replace(a_ref.ts);
            assert!(prev.is_null())
        });

        let ts_ref = unsafe { a_ref.ts.as_mut() }.unwrap();
        ts_ref.id = id as usize;
        ts_ref.version = 0;

        loop {
            ts_ref.next = unsafe { SSMEM_TS_LIST };

            let (_, ok) = unsafe {
                intrinsics::atomic_cxchg_seqcst_seqcst(
                    &mut SSMEM_TS_LIST as *mut _,
                    ts_ref.next,
                    ts_ref as *mut _,
                )
            };
            if ok {
                break;
            }
        }
        let _ = unsafe { intrinsics::atomic_xadd_seqcst(&mut SSMEM_TS_LIST_LEN as *mut usize, 1) };
    }
}

/// terminate the system (all allocators) and free all memory
pub fn ssmem_term(_: Option<&PoolHandle>) {
    unimplemented!("no need for SOFT hash")
}

/// terminate the allocator a and free all its memory.
///
/// # Safety
///
/// This function should NOT be used if the memory allocated by this allocator
/// might have been freed (and is still in use) by other allocators
pub unsafe fn ssmem_alloc_term(_: &SsmemAllocator, _: Option<&PoolHandle>) {
    unimplemented!("no need for SOFT hash")
}

/// allocate some memory using allocator a
pub fn ssmem_alloc(a: *mut SsmemAllocator, size: usize, pool: Option<&PoolHandle>) -> *mut c_void {
    let mut m: *mut c_void = null_mut();
    let a_ref = unsafe { a.as_mut() }.unwrap();

    /* 1st try to use from the collected memory */
    let cs = a_ref.collected_set_list;
    if !cs.is_null() {
        let cs_ref = unsafe { cs.as_mut() }.unwrap();

        cs_ref.curr -= 1;
        m = unsafe { *(cs_ref.set.offset(cs_ref.curr)) as *mut _ };
        prefetchw(m);

        // zero-initialize
        unsafe {
            let _ = libc::memset(m, 0x0, size);
        }

        if cs_ref.curr <= 0 {
            a_ref.collected_set_list = cs_ref.set_next;
            a_ref.collected_set_num -= 1;

            ssmem_free_set_make_avail(a, cs);
        }
    } else {
        if (a_ref.mem_curr + size) >= a_ref.mem_size {
            if SSMEM_MEM_SIZE_DOUBLE == 1 {
                a_ref.mem_size <<= 1;
                if a_ref.mem_size > SSMEM_MEM_SIZE_MAX {
                    a_ref.mem_size = SSMEM_MEM_SIZE_MAX;
                }
            }

            // check if size is larger than memory chunk
            if size > a_ref.mem_size {
                while a_ref.mem_size < size {
                    if a_ref.mem_size > SSMEM_MEM_SIZE_MAX {
                        eprintln!(
                            "[ALLOC] asking for memory chunk larger than max ({} MB)",
                            SSMEM_MEM_SIZE_MAX / (1024 * 1024)
                        );
                        assert!(a_ref.mem_size <= SSMEM_MEM_SIZE_MAX);
                    }

                    a_ref.mem_size <<= 1;
                }
            }

            // allocate new memory chunk
            a_ref.mem = alloc(
                Layout::from_size_align(a_ref.mem_size, CACHE_LINE_SIZE).unwrap(),
                pool,
            );
            assert!(a_ref.mem != null_mut());

            a_ref.mem_curr = 0;
            a_ref.tot_size += a_ref.mem_size;

            // zero-initialize
            ssmem_zero_memory(a);

            // add new memory chunk to list
            let new_mem_chunks = ssmem_list_node_new(a_ref.mem, a_ref.mem_chunks, pool);
            persist_obj(unsafe { new_mem_chunks.as_ref() }.unwrap(), true);

            a_ref.mem_chunks = new_mem_chunks;
            persist_obj(unsafe { a_ref.mem_chunks.as_ref() }.unwrap(), true);
        }

        m = (a_ref.mem as usize + a_ref.mem_curr) as *mut c_void; // addr of available block
        a_ref.mem_curr += size;
    }

    if SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_ALLOC || SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_BOTH {
        ssmem_ts_next();
    }
    m
}

/// free some memory using allocator a
///
/// # Safety
///
/// use `free` carefully
pub unsafe fn ssmem_free(a: *mut SsmemAllocator, obj: *mut c_void, pool: Option<&PoolHandle>) {
    let a = unsafe { a.as_mut() }.unwrap();
    let mut fs = unsafe { a.free_set_list.as_mut() }.unwrap();

    if fs.curr as usize == fs.size {
        fs.ts_set = ssmem_ts_set_collect(fs.ts_set, pool);
        let _ = ssmem_mem_reclaim(a as *mut _, pool);

        let fs_new = ssmem_free_set_get_avail(a as *mut _, a.fs_size, a.free_set_list, pool);
        a.free_set_list = fs_new;
        a.free_set_num += 1;
        fs = unsafe { fs_new.as_mut() }.unwrap();
    }

    unsafe { *(fs.set.offset(fs.curr)) = obj as usize };
    fs.curr += 1;

    if SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_FREE || SSMEM_TS_INCR_ON == SSMEM_TS_INCR_ON_BOTH {
        ssmem_ts_next();
    }
}

/// release some memory to the OS using allocator a
pub fn ssmem_release(_: &SsmemAllocator, _: *mut c_void, _: Option<&PoolHandle>) {
    unimplemented!("no need for SOFT hash")
}

/// increment the thread-local activity counter. Invoking this function suggests
/// that no memory references to ssmem-allocated memory are held by the current
/// thread beyond this point.
pub fn ssmem_ts_next() {
    SSMEM_TS_LOCAL.with(|ts| {
        let ts_ref = unsafe { ts.borrow_mut().as_mut() }.unwrap();
        ts_ref.version += 1;
    });
}

/* ****************************************************************************************
 */
/* global variables or private functions from ssmem.cpp */
/* ****************************************************************************************
 */

static mut SSMEM_TS_LIST: *mut SsmemTS = null_mut();
static mut SSMEM_TS_LIST_LEN: usize = 0;
thread_local! {
    static SSMEM_TS_LOCAL: RefCell<*mut SsmemTS> = RefCell::new(null_mut());
    static SSMEM_NUM_ALLOCATORS: RefCell<usize>  = RefCell::new(0);
    static SSMEM_ALLOCATOR_LIST: RefCell<*const SsmemList> = RefCell::new(null());
}

fn ssmem_list_node_new(
    mem: *mut c_void,
    next: *const SsmemList,
    pool: Option<&PoolHandle>,
) -> *const SsmemList {
    let mc: *mut SsmemList = alloc(Layout::new::<SsmemList>(), pool);
    let mc_ref = unsafe { mc.as_mut() }.unwrap();
    assert!(!mc.is_null());
    mc_ref.obj = mem;
    mc_ref.next = next;
    mc
}

fn ssmem_zero_memory(a: *mut SsmemAllocator) {
    if SSMEM_ZERO_MEMORY == 1 {
        let a_ref = unsafe { a.as_mut() }.unwrap();
        unsafe {
            let _ = libc::memset(a_ref.mem, 0x0, a_ref.mem_size);
        }
        let mut i = 0;
        while i < a_ref.mem_size / CACHE_LINE_SIZE {
            let curr = (a_ref.mem as usize + i) as *mut c_void;
            persist_obj(unsafe { curr.as_ref() }.unwrap(), true);
            i += CACHE_LINE_SIZE;
        }
    }
}

fn ssmem_free_set_new(
    size: usize,
    next: *mut SsmemFreeSet,
    pool: Option<&PoolHandle>,
) -> *mut SsmemFreeSet {
    /* allocate both the ssmem_free_set_t and the free_set with one call */
    let mut fs: *mut SsmemFreeSet = null_mut();
    fs = alloc(
        Layout::from_size_align(
            size_of::<SsmemFreeSet>() + size * size_of::<usize>(),
            CACHE_LINE_SIZE,
        )
        .unwrap(),
        pool,
    );
    assert!(!fs.is_null());

    let fs_ref = unsafe { fs.as_mut() }.unwrap();
    fs_ref.size = size;
    fs_ref.curr = 0;

    fs_ref.set = (fs as usize + size_of::<SsmemFreeSet>()) as *mut _; // start addr of free obj(s)
    fs_ref.ts_set = null_mut();
    fs_ref.set_next = next;

    fs
}

fn ssmem_ts_set_collect(ts_set: *mut usize, pool: Option<&PoolHandle>) -> *mut usize {
    let ts_set = if ts_set.is_null() {
        alloc(
            Layout::array::<usize>(unsafe { SSMEM_TS_LIST_LEN }).unwrap(),
            pool,
        )
    } else {
        ts_set
    };
    assert!(ts_set != null_mut());

    unsafe {
        let mut cur = SSMEM_TS_LIST;
        while !cur.is_null() && cur.as_ref().unwrap().id < SSMEM_TS_LIST_LEN {
            let cur_ref = cur.as_ref().unwrap();
            *ts_set.offset(cur_ref.id as isize) = cur_ref.version;
            cur = cur_ref.next;
        }
    }

    ts_set
}

fn ssmem_mem_reclaim(a: *mut SsmemAllocator, _: Option<&PoolHandle>) -> isize {
    let a_ref = unsafe { a.as_mut() }.unwrap();

    if a_ref.released_num > 0 {
        unimplemented!("no need for SOFT hash")
    }

    let fs_cur = a_ref.free_set_list;
    let fs_cur_ref = unsafe { fs_cur.as_mut() }.unwrap();
    if fs_cur_ref.ts_set.is_null() {
        return 0;
    }
    let fs_nxt = fs_cur_ref.set_next;
    let mut gced_num = 0;
    if fs_nxt.is_null() || unsafe { fs_nxt.as_ref() }.unwrap().ts_set.is_null() {
        // need at least 2 sets to compare
        return 0;
    }
    let fs_nxt_ref = unsafe { fs_nxt.as_mut() }.unwrap();

    if ssmem_ts_compare(fs_cur_ref.ts_set, fs_nxt_ref.ts_set) == 1 {
        gced_num = a_ref.free_set_num - 1;

        /* take the the suffix of the list (all collected free_sets) away from the
        free_set list of a and set the correct num of free_sets*/
        fs_cur_ref.set_next = null_mut();
        a_ref.free_set_num = 1;

        /* find the tail for the collected_set list in order to append the new
        free_sets that were just collected */
        let mut collected_set_cur = a_ref.collected_set_list;
        if !collected_set_cur.is_null() {
            let mut collected_set_cur_ref = unsafe { collected_set_cur.as_mut() }.unwrap();
            while !collected_set_cur_ref.set_next.is_null() {
                collected_set_cur = collected_set_cur_ref.set_next;
                collected_set_cur_ref = unsafe { collected_set_cur.as_mut() }.unwrap();
            }

            collected_set_cur_ref.set_next = fs_nxt;
        } else {
            a_ref.collected_set_list = fs_nxt;
        }
        a_ref.collected_set_num += gced_num;
    }

    gced_num as isize
}

fn ssmem_free_set_get_avail(
    a: *mut SsmemAllocator,
    size: usize,
    next: *mut SsmemFreeSet,
    pool: Option<&PoolHandle>,
) -> *mut SsmemFreeSet {
    let a_ref = unsafe { a.as_mut() }.unwrap();
    let mut fs = null_mut();

    // reuse available free set
    if !a_ref.available_set_list.is_null() {
        fs = a_ref.available_set_list;
        let fs_ref = unsafe { fs.as_mut() }.unwrap();
        a_ref.available_set_list = fs_ref.set_next;

        fs_ref.curr = 0;
        fs_ref.set_next = next;
    }
    // make new free set
    else {
        fs = ssmem_free_set_new(size, next, pool);
    };
    fs
}

fn ssmem_free_set_make_avail(a: *mut SsmemAllocator, set: *mut SsmemFreeSet) {
    let a = unsafe { a.as_mut() }.unwrap();
    let set = unsafe { set.as_mut() }.unwrap();
    set.curr = 0;
    set.set_next = a.available_set_list;
    a.available_set_list = set;
}

fn ssmem_ts_compare(s_new: *const usize, s_old: *const usize) -> usize {
    let len = unsafe { SSMEM_TS_LIST_LEN };
    let s_new_arr = unsafe { std::slice::from_raw_parts(s_new, len) };
    let s_old_arr = unsafe { std::slice::from_raw_parts(s_old, len) };

    let mut is_newer = 1;
    for i in 0..len {
        if s_new_arr[i] <= s_old_arr[i] {
            is_newer = 0;
            break;
        }
    }
    return is_newer;
}

fn alloc<T>(layout: Layout, pool: Option<&PoolHandle>) -> *mut T {
    unsafe {
        return match pool {
            // persistent alloc
            Some(pool) => pool.alloc_layout(layout).deref_mut(pool),
            // volatile alloc
            None => std::alloc::alloc(layout),
        } as *mut T;
    }
}
