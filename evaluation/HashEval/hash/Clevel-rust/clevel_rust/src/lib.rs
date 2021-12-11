#![feature(generic_associated_types)]

use crossbeam_epoch::{self as epoch, Guard};
use memento::ds::clevel::{Modify, ModifyOp, PClevelInner, ResizeLoop};
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle};
use memento::Memento;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

type Key = u64;
type Value = u64;

#[derive(Default)]
pub struct ClevelClient {
    modify: Modify<Key, Value>,
    resize_loop: ResizeLoop<Key, Value>,
}

impl Collectable for ClevelClient {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl Memento for ClevelClient {
    type Object<'o> = &'o PClevelInner<Key, Value>;
    type Input<'o> = usize; // tid
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &mut self,
        object: Self::Object<'o>,
        input: Self::Input<'o>,
        rec: bool, // TODO(opt): template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        todo!()
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

// TODO: Queue-memento API로 동작시켜 놓은 걸 Clevel-memento API로 동작시키게 하기
#[no_mangle]
pub extern "C" fn pool_create(
    path: *const c_char,
    size: usize,
    nr_thread: usize,
) -> &'static PoolHandle {
    let c_str: &CStr = unsafe { CStr::from_ptr(path) };
    Pool::create::<PClevelInner<Key, Value>, ClevelClient>(
        c_str.to_str().unwrap(),
        size,
        nr_thread + 1, // +1은 resize loop 역할
    )
    .unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn get_root(ix: u64, pool: &PoolHandle) -> *mut c_void {
    pool.get_root(ix)
}

#[no_mangle]
pub extern "C" fn search(clevel: &PClevelInner<Key, Value>, k: Key, pool: &PoolHandle) -> bool {
    let guard = epoch::pin();
    clevel.search(&k, &guard, pool).is_some()
}

#[no_mangle]
pub extern "C" fn run_insert(
    client: &mut ClevelClient, // TODO: ClevelClient
    clevel: &PClevelInner<Key, Value>,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = client
        .modify
        .run(clevel, ModifyOp::Insert, false, &guard, pool)
        .is_ok();
    client.modify.reset(&guard, pool);
    ret
}

#[no_mangle]
pub extern "C" fn run_update(
    client: &mut ClevelClient, // TODO: ClevelClient
    clevel: &PClevelInner<Key, Value>,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = client
        .modify
        .run(clevel, ModifyOp::Update, false, &guard, pool)
        .is_ok();
    client.modify.reset(&guard, pool);
    ret
}

#[no_mangle]
pub extern "C" fn run_delete(
    client: &mut ClevelClient, // TODO: ClevelClient
    clevel: &PClevelInner<Key, Value>,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = client
        .modify
        .run(clevel, ModifyOp::Delete, false, &guard, pool)
        .is_ok();
    client.modify.reset(&guard, pool);
    ret
}
#[no_mangle]
pub extern "C" fn run_resize_loop(
    client: &mut ClevelClient, // TODO: ClevelClient
    clevel: &PClevelInner<Key, Value>,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = client
        .resize_loop
        .run(clevel, (), false, &guard, pool)
        .is_ok();
    client.resize_loop.reset(&guard, pool);
    ret
}
