//! PMDK

use std::mem;

use libc::*;

use crate::pmem::{global_pool, PoolHandle};

use super::{Collectable, GarbageCollection, PAllocator};

const NUM_ROOT: usize = 128;

static mut ROOT: *mut Root = std::ptr::null_mut();
pub(crate) static mut POPS: *mut pmemobj_sys::PMEMobjpool = std::ptr::null_mut();

struct Root {
    objs: [pmemobj_sys::pmemoid; NUM_ROOT],
    filters: [Option<filter_func>; NUM_ROOT],
}
type filter_func =
    unsafe fn(s: *mut c_void, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle);
unsafe impl Sync for Root {}

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

pub(crate) struct PMDKAllocator {}

impl PAllocator for PMDKAllocator {
    unsafe fn open(filepath: *const c_char, filesize: u64) -> c_int {
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
    }

    unsafe fn create(filepath: *const c_char, filesize: u64) -> c_int {
        POPS =
            pmemobj_sys::pmemobj_create(filepath, std::ptr::null_mut(), filesize as usize, 0o777);
        if POPS.is_null() {
            let msg = pmemobj_sys::pmemobj_errormsg();
            let msgg = msg.as_ref().unwrap().to_string();
            panic!("err: {:?}", msgg)
        }

        let root = pmemobj_sys::pmemobj_root(POPS, mem::size_of::<Root>());
        ROOT = pmemobj_sys::pmemobj_direct(root) as *mut Root;
        return 0;
    }

    unsafe fn mmapped_addr() -> usize {
        let root_oid = pmemobj_sys::pmemobj_oid(ROOT as *mut c_void);
        let start = ROOT as usize - root_oid.off as usize;
        assert!(root_oid.off % 64 == 16);
        start + 16
    }

    unsafe fn close(start: usize, len: usize) {
        if !POPS.is_null() {
            pmemobj_sys::pmemobj_close(POPS);
            POPS = std::ptr::null_mut();
        }
    }

    unsafe fn recover() -> c_int {
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
    }

    unsafe fn set_root(ptr: *mut c_void, i: u64) -> *mut c_void {
        let root = ROOT.as_mut().unwrap();
        let old = root.objs[i as usize];
        let oid = pmemobj_sys::pmemobj_oid(ptr);
        root.objs[i as usize] = oid;
        pmemobj_sys::pmemobj_direct(old)
    }

    unsafe fn get_root(i: u64) -> *mut c_void {
        let oid = ROOT.as_mut().unwrap().objs[i as usize];
        if oid.pool_uuid_lo == 0 {
            panic!("err: !!!");
        }
        pmemobj_sys::pmemobj_direct(oid)
    }

    unsafe fn malloc(sz: c_ulong) -> *mut c_void {
        let mut oid = pmemobj_sys::PMEMoid {
            off: 0,
            pool_uuid_lo: 0,
        };
        let oidp = &mut oid;
        let sz = if sz < 64 { 64 } else { sz.try_into().unwrap() };
        let status = unsafe {
            pmemobj_sys::pmemobj_zalloc(
                POPS,
                oidp as *mut pmemobj_sys::PMEMoid,
                sz,
                0,
                // None,
                // std::ptr::null_mut(),
            )
        };
        assert!(oid.off % 64 == 16, "oid: {:?}, sz: {}", oid, sz);
        if status == 0 {
            pmemobj_sys::pmemobj_direct(oid)
        } else {
            panic!("err");
        }
    }

    unsafe fn free(ptr: *mut c_void, _len: usize) {
        let mut oid = pmemobj_sys::pmemobj_oid(ptr);
        pmemobj_sys::pmemobj_free(&mut oid as *mut _);
    }

    unsafe fn set_root_filter<T: Collectable>(i: u64) {
        unsafe fn root_filter<T: Collectable>(
            s: *mut c_void,
            tid: usize,
            gc: &mut GarbageCollection,
            pool: &mut PoolHandle,
        ) {
            T::filter(&mut *(s as *mut T), tid, gc, pool)
        };

        ROOT.as_mut().unwrap().filters[i as usize] = Some(root_filter::<T>);
    }

    unsafe fn mark<T: Collectable>(s: &mut T, tid: usize, gc: &mut GarbageCollection) {
        T::filter(s, tid, gc, global_pool().unwrap());
    }

    unsafe extern "C" fn filter_inner<T: Collectable>(
        ptr: *mut T,
        tid: usize,
        gc: &mut GarbageCollection,
    ) {
        unreachable!("This function is only for Ralloc GC.")
    }
}
