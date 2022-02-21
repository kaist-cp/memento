#![feature(generic_associated_types)]
#![deny(warnings)]
#![allow(non_snake_case)]

use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::soft_hash::*;
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle, RootObj};
use memento::PDefault;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

type Key = usize;
type Value = u64;

/// Persistent root for SOFT hash
#[derive(Debug)]
pub struct SOFTHash<T: Default> {
    inner: Box<SOFTHashTable<T>>,
}

impl<T: Default> PDefault for SOFTHash<T> {
    #![allow(box_pointers)]
    fn pdefault(_: &PoolHandle) -> Self {
        Self {
            inner: Box::new(SOFTHashTable::default()),
        }
    }
}

impl<T: Default> Collectable for SOFTHash<T> {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct SOFTMemento {
    insert: CachePadded<HashInsert<Value>>,
    delete: CachePadded<HashRemove<Value>>,
}

impl Collectable for SOFTMemento {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
        todo!()
    }
}

impl RootObj<SOFTMemento> for SOFTHash<Value> {
    fn run(&self, _: &mut SOFTMemento, _: usize, _: &Guard, _: &PoolHandle) {
        todo!()
    }
}

const MAX_THREAD: usize = 256;
static mut GUARD: Option<[Option<Guard>; MAX_THREAD]> = None;
static mut CNT: [usize; MAX_THREAD] = [0; MAX_THREAD];

#[inline]
fn get_guard(tid: usize) -> &'static mut Guard {
    let guard = unsafe { GUARD.as_mut().unwrap()[tid].as_mut().unwrap() };
    unsafe {
        CNT[tid] += 1;
        if CNT[tid] % 1024 == 0 {
            // TODO: repin_after하기 전에 memento들을 clear 해줘야함
            guard.repin_after(|| {});
        }
    }
    guard
}

#[no_mangle]
pub extern "C" fn thread_init(tid: usize, pool: &PoolHandle) {
    let guards = unsafe { GUARD.get_or_insert(array_init::array_init(|_| None)) };
    guards[tid] = Some(epoch::pin());
    hash_thread_ini(tid, pool);
}

#[no_mangle]
pub extern "C" fn pool_create(
    path: *const c_char,
    size: usize,
    nr_thread: usize,
) -> &'static PoolHandle {
    let c_str: &CStr = unsafe { CStr::from_ptr(path) };
    Pool::create::<SOFTHash<Value>, SOFTMemento>(c_str.to_str().unwrap(), size, nr_thread).unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn get_root(ix: u64, pool: &PoolHandle) -> *mut c_void {
    pool.get_root(ix)
}

#[no_mangle]
pub extern "C" fn run_insert(
    m: &mut SOFTMemento,
    obj: &SOFTHash<Value>,
    tid: usize,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.inner
        .insert::<false>(k, v, &mut m.insert, tid, guard, pool)
}

#[no_mangle]
pub extern "C" fn run_delete(
    m: &mut SOFTMemento,
    obj: &SOFTHash<Value>,
    tid: usize,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.inner
        .remove::<false>(k, &mut m.delete, tid, guard, pool)
}

#[no_mangle]
pub extern "C" fn search(obj: &SOFTHash<Value>, _tid: usize, k: Key, _: &PoolHandle) -> bool {
    obj.inner.contains(k)
}
