#![feature(generic_associated_types)]

use crossbeam_epoch::{self as epoch, Guard};
use memento::ds::clevel::{ClDelete, ClInsert, ClUpdate, Clevel, ClevelInner, ResizeLoop};
use memento::pmem::{Collectable, GarbageCollection, Pool, PoolHandle};
use memento::Memento;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::ptr;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};

const MAX_THREAD: usize = 256;
static mut SEND: Option<[Sender<()>; MAX_THREAD]> = None;
static mut RECV: Option<Receiver<()>> = None;

type Key = u64;
type Value = u64;

#[derive(Default)]
pub struct ClevelMemento {
    insert: ClInsert<Key, Value>,
    update: ClUpdate<Key, Value>,
    delete: ClDelete<Key, Value>,
    resize: ResizeLoop<Key, Value>,
}

impl Collectable for ClevelMemento {
    fn filter(s: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
        todo!()
    }
}

impl Memento for ClevelMemento {
    type Object<'o> = &'o ClevelInner<Key, Value>;
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
    // TODO: maybe pinning for each operation is too pessimistic. Let's optimize it for Memento...
    let guard = epoch::pin();
    let input = (tid, k, v, unsafe { &SEND.as_ref().unwrap()[tid] });
    let ret = m.insert.run(obj, input, false, &guard, pool).is_ok();
    m.insert.reset(&guard, pool);
    ret
}

#[no_mangle]
pub extern "C" fn run_update(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    tid: usize,
    k: Key,
    v: Value,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let input = (tid, k, v, unsafe { &SEND.as_ref().unwrap()[tid] });
    let ret = m.update.run(obj, input, false, &guard, pool).is_ok();
    m.update.reset(&guard, pool);
    ret
}

#[no_mangle]
pub extern "C" fn run_delete(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    k: Key,
    pool: &'static PoolHandle,
) -> bool {
    let guard = epoch::pin();
    let ret = m.delete.run(obj, &k, false, &guard, pool).is_ok();
    m.delete.reset(&guard, pool);
    ret
}
#[no_mangle]
pub extern "C" fn run_resize_loop(
    m: &mut ClevelMemento,
    obj: &ClevelInner<Key, Value>,
    pool: &'static PoolHandle,
) {
    let guard = epoch::pin();
    let _ = m
        .resize
        .run(obj, unsafe { RECV.as_ref().unwrap() }, false, &guard, pool);
}

#[no_mangle]
pub extern "C" fn search(obj: &ClevelInner<Key, Value>, k: Key, pool: &'static PoolHandle) -> bool {
    let guard = epoch::pin();
    Clevel::search(obj, &k, &guard, pool).is_some()
}

#[no_mangle]
pub extern "C" fn get_capacity(obj: &ClevelInner<Key, Value>, pool: &'static PoolHandle) -> usize {
    let guard = crossbeam_epoch::pin();
    Clevel::get_capacity(obj, &guard, pool)
}
