#![feature(generic_associated_types)]
#![deny(warnings)]

use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::CachePadded;
use memento::ds::clevel::*;
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle, RootObj};
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

type Key = u64;
type Value = u64;

#[derive(Debug, Default)]
pub struct ClevelMemento {
    insert: CachePadded<Insert<Key, Value>>, // insert client
    delete: CachePadded<Delete<Key, Value>>, // delete client
    resize: CachePadded<Resize<Key, Value>>, // resize client
}

impl Collectable for ClevelMemento {
    fn filter(root_mmt: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        Collectable::filter(&mut *root_mmt.insert, tid, gc, pool);
        Collectable::filter(&mut *root_mmt.delete, tid, gc, pool);
        Collectable::filter(&mut *root_mmt.resize, tid, gc, pool);
    }
}

impl RootObj<ClevelMemento> for ClevelInner<Key, Value> {
    fn run(&self, _: &mut ClevelMemento, _: usize, _: &Guard, _: &PoolHandle) {
        todo!()
    }
}

const MAX_THREAD: usize = 256;
static mut SEND: Option<[Sender<()>; MAX_THREAD]> = None;
static mut RECV: Option<Receiver<()>> = None;
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

fn get_send(tid: usize) -> &'static Sender<()> {
    unsafe { &SEND.as_ref().unwrap()[tid] }
}

#[no_mangle]
pub extern "C" fn thread_init(tid: usize) {
    // println!("[thread_init] thread {tid} init");
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
    let (send, recv) = mpsc::channel();
    unsafe {
        SEND = Some(array_init::array_init(|_| send.clone()));
        RECV = Some(recv);
    }

    Pool::create::<ClevelInner<Key, Value>, ClevelMemento>(
        c_str.to_str().unwrap(),
        size,
        nr_thread + 1, // +1은 resize loop 역할. 나머지는 pibench가 넘겨주는 insert/delete/search op 실행
    )
    .unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn get_root(ix: u64, pool: &PoolHandle) -> *mut c_void {
    pool.get_root(ix)
}

#[no_mangle]
pub extern "C" fn run_insert(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    tid: usize,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    let res = obj
        .insert::<false>(k, v, get_send(tid), &mut m.insert, tid, &guard, pool)
        .is_ok();
    res
}

#[no_mangle]
pub extern "C" fn run_update(
    _m: &mut ClevelMemento,
    _obj: &ClevelInner<Key, Value>,
    _tid: usize,
    _k: Key,
    _v: Value,
    _pool: &'static PoolHandle,
) -> bool {
    // let guard = get_guard(tid);
    // obj.update(tid, k, v, get_send(tid), &guard, pool).is_ok()
    todo!()
}

#[no_mangle]
pub extern "C" fn run_delete(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    tid: usize,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    let res = obj.delete::<false>(&k, &mut m.delete, tid, &guard, pool);
    res
}
#[no_mangle]
pub extern "C" fn run_resize_loop(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    tid: usize,
    pool: &'static PoolHandle,
) {
    let mut guard = epoch::pin();
    let recv = unsafe { RECV.as_ref().unwrap() };
    resize_loop::<_, _, false>(obj, recv, &mut m.resize, tid, &mut guard, pool);
}

#[no_mangle]
pub extern "C" fn search(
    obj: &ClevelInner<Key, Value>,
    tid: usize,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = get_guard(tid);
    obj.search(&k, &guard, pool).is_some()
}

#[no_mangle]
pub extern "C" fn get_capacity(obj: &ClevelInner<Key, Value>, pool: &PoolHandle) -> usize {
    let guard = crossbeam_epoch::pin();
    obj.get_capacity(&guard, pool)
}

#[no_mangle]
pub extern "C" fn is_resizing(_obj: &ClevelInner<Key, Value>, _pool: &PoolHandle) -> bool {
    // let guard = crossbeam_epoch::pin();
    // obj.is_resizing(&guard, pool)
    false
}
