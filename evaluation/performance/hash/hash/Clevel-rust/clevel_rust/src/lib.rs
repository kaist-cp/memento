#![feature(generic_associated_types)]
#![deny(warnings)]

use crossbeam_channel::{Receiver, Sender};
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use memento::ds::clevel::*;
use memento::ploc::Handle;
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle, RootObj};
use memento::{Collectable, Memento};
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

type Key = u64;
type Value = u64;

#[derive(Debug, Default, Memento, Collectable)]
pub struct ClevelMemento {
    insert: CachePadded<Insert<Key, Value>>, // insert client
    delete: CachePadded<Delete<Key, Value>>, // delete client
    resize: CachePadded<Resize<Key, Value>>, // resize client
}

impl RootObj<ClevelMemento> for Clevel<Key, Value> {
    fn run(&self, _: &mut ClevelMemento, _: &Handle) {}
}

const MAX_THREAD: usize = 256;
static mut SEND: Option<[Sender<()>; MAX_THREAD]> = None;
static mut RECV: Option<Receiver<()>> = None;
static mut HANDLES: Option<[Option<Handle>; MAX_THREAD]> = None;
static mut CNT: [usize; MAX_THREAD] = [0; MAX_THREAD];

#[inline]
fn get_handle(tid: usize) -> &'static Handle {
    let handle = unsafe { HANDLES.as_mut().unwrap()[tid].as_mut().unwrap() };

    unsafe {
        CNT[tid] += 1;
        if CNT[tid] % 1024 == 0 {
            handle.repin_guard();
        }
    }
    handle
}

fn get_send(tid: usize) -> &'static Sender<()> {
    unsafe { &SEND.as_ref().unwrap()[tid] }
}

#[no_mangle]
pub extern "C" fn thread_init(tid: usize, pool: &'static PoolHandle) {
    let handles = unsafe { HANDLES.as_mut().unwrap() };
    handles[tid] = Some(Handle::new(tid, epoch::pin(), pool));
}

#[no_mangle]
pub extern "C" fn pool_create(
    path: *const c_char,
    size: usize,
    nr_thread: usize,
) -> &'static PoolHandle {
    let c_str: &CStr = unsafe { CStr::from_ptr(path) };
    let filepath = c_str.to_str().unwrap();
    let (send, recv) = crossbeam_channel::bounded(1024);
    unsafe {
        SEND = Some(array_init::array_init(|_| send.clone()));
        RECV = Some(recv);
        HANDLES = Some(array_init::array_init(|_| None));
    }

    let _ = Pool::remove(&filepath);
    Pool::create::<Clevel<Key, Value>, ClevelMemento>(
        &filepath,
        size,
        nr_thread + 1, // +1 for resize loop thread.
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
    obj: &Clevel<Key, Value>,
    tid: usize,
    k: Key,
    v: Value,
) -> bool {
    obj.insert(k, v, get_send(tid), &mut m.insert, get_handle(tid))
        .is_ok()
}

#[no_mangle]
pub extern "C" fn run_delete(
    m: &mut ClevelMemento,
    obj: &Clevel<Key, Value>,
    tid: usize,
    k: Key,
) -> bool {
    obj.delete(&k, &mut m.delete, get_handle(tid))
}
#[no_mangle]
pub extern "C" fn run_resize(m: &mut ClevelMemento, obj: &Clevel<Key, Value>, tid: usize) {
    let recv = unsafe { RECV.as_ref().unwrap() };
    obj.resize(recv, &mut m.resize, get_handle(tid));
}

#[no_mangle]
pub extern "C" fn search(obj: &Clevel<Key, Value>, tid: usize, k: Key) -> bool {
    obj.search(&k, get_handle(tid)).is_some()
}

#[no_mangle]
pub extern "C" fn get_capacity(obj: &Clevel<Key, Value>, tid: usize) -> usize {
    obj.get_capacity(get_handle(tid))
}

#[no_mangle]
pub extern "C" fn is_resizing(obj: &Clevel<Key, Value>, tid: usize) -> bool {
    obj.is_resizing(get_handle(tid))
}
