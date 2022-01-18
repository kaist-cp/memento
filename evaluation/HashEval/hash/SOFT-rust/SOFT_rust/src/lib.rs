#![feature(generic_associated_types)]

use crossbeam_epoch::{self as epoch, Guard};
use memento::ds::soft_hash::*;
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle, RootObj};
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

static mut POOL: Option<&'static PoolHandle> = None;

type Key = usize;
type Value = u64;

#[derive(Debug, Default)]
pub struct SOFTMemento {}

impl Collectable for SOFTMemento {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl RootObj<SOFTMemento> for SOFTHashTable<Value> {
    fn run(&self, mmt: &mut SOFTMemento, tid: usize, guard: &Guard, pool: &PoolHandle) {
        todo!()
    }
}

const MAX_THREAD: usize = 256;
static mut GUARD: Option<[Option<Guard>; MAX_THREAD]> = None;
static mut CNT: [usize; MAX_THREAD] = [0; MAX_THREAD];

fn get_guard(tid: usize) -> &'static mut Guard {
    let guard = unsafe { GUARD.as_mut().unwrap()[tid].as_mut().unwrap() };
    unsafe {
        CNT[tid] += 1;
        if CNT[tid] % 1024 == 0 {
            guard.repin_after(|| {});
        }
    }
    guard
}

#[no_mangle]
pub extern "C" fn thread_init(tid: usize) {
    hash_thread_ini(tid, unsafe { POOL.as_ref().unwrap() });
    let guards = unsafe { GUARD.get_or_insert(array_init::array_init(|_| None)) };
    guards[tid] = Some(epoch::pin());
}

#[no_mangle]
pub extern "C" fn pool_create(
    path: *const c_char,
    size: usize,
    nr_thread: usize,
) -> &'static PoolHandle {
    let c_str: &CStr = unsafe { CStr::from_ptr(path) };
    let pool =
        Pool::create::<SOFTHashTable<Value>, SOFTMemento>(c_str.to_str().unwrap(), size, nr_thread)
            .unwrap();
    unsafe { POOL = Some(pool) };
    pool
}

#[no_mangle]
pub unsafe extern "C" fn get_root(ix: u64, pool: &PoolHandle) -> *mut c_void {
    pool.get_root(ix)
}

#[no_mangle]
pub extern "C" fn run_insert(
    m: &mut SOFTMemento,
    obj: &SOFTHashTable<Value>,
    tid: usize,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.insert(k, v, guard, pool)
}

#[no_mangle]
pub extern "C" fn run_delete(
    m: &mut SOFTMemento,
    obj: &SOFTHashTable<Value>,
    tid: usize,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.remove(k, &guard, pool)
}

#[no_mangle]
pub extern "C" fn search(
    obj: &SOFTHashTable<Value>,
    tid: usize,
    k: Key,
    pool: &PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.contains(k, &guard)
}
