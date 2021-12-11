#![feature(generic_associated_types)]

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

use crossbeam_epoch::{self as epoch, Guard};
use memento::ds::queue::*;
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle};
use memento::Memento;

type Key = u64;
type Value = u64;

#[derive(Debug, Default)]
pub struct QueueClient {
    enq: Enqueue<Value>,
    deq: Dequeue<Value>,
}

impl Memento for QueueClient {
    type Object<'o> = &'o Queue<Value>;
    type Input<'o> = usize;
    type Output<'o> = ();
    type Error<'o> = ();

    fn run<'o>(
        &'o mut self,
        object: Self::Object<'o>,
        input: Self::Input<'o>,
        rec: bool, // TODO: template parameter
        guard: &'o Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        todo!()
    }

    fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
        todo!()
    }
}

impl Collectable for QueueClient {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
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
    // TODO: Queue<Value> -> Clevel<Key, Value>
    // TODO: QueueClient -> ClevelClient
    Pool::create::<Queue<Value>, QueueClient>(c_str.to_str().unwrap(), size, nr_thread).unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn get_root(ix: u64, pool: &PoolHandle) -> *mut c_void {
    pool.get_root(ix)
}

#[no_mangle]
pub extern "C" fn run_search() -> bool {
    todo!()
}

#[no_mangle]
pub extern "C" fn run_insert(
    c: &mut QueueClient, // TODO: ClevelClient
    q: &Queue<Value>,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = c.enq.run(q, v, false, &guard, pool).is_ok();
    println!("[enq] {}", v);
    c.enq.reset(&guard, pool);
    ret
}

#[no_mangle]
pub extern "C" fn run_update() -> bool {
    todo!()
}
#[no_mangle]
pub extern "C" fn run_delete(
    c: &mut QueueClient, // TODO: ClevelClient
    q: &Queue<Value>,
    _: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = c.deq.run(q, (), false, &guard, pool).unwrap();
    println!("[deq] {}", ret.unwrap());
    c.deq.reset(&guard, pool);
    ret.is_some()
}
