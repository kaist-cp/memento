#![feature(generic_associated_types)] // to define fields of `Memento`

pub mod persistent; // Persistent version using Memento

use memento::ds::clevel::Clevel;

type Key = u64;
type Value = u64;

#[no_mangle]
pub extern "C" fn clevel_new() -> Box<Clevel<Key, Value>> {
    let (kv, mut kv_resize) = Clevel::<Key, Value>::new();
    let _ = std::thread::spawn(move || {
        let mut guard = crossbeam_epoch::pin();
        kv_resize.resize_loop(&mut guard);
    });
    Box::new(kv)
}

#[no_mangle]
pub extern "C" fn clevel_free(c: Box<Clevel<Key, Value>>) {
    drop(c);
}

#[no_mangle]
pub extern "C" fn clevel_search(c: &Clevel<Key, Value>, k: Key) -> bool {
    // TODO: maybe pinning for each operation is too pessimistic. Let's optimize it for Memento...
    let guard = crossbeam_epoch::pin();
    c.search(&k, &guard).is_some()
}

#[no_mangle]
pub extern "C" fn clevel_insert(c: &Clevel<Key, Value>, k: Key, v: Value, tid: usize) -> bool {
    let guard = crossbeam_epoch::pin();
    c.insert(tid, k, v, &guard).is_ok()
}

#[no_mangle]
pub extern "C" fn clevel_update(c: &Clevel<Key, Value>, k: Key, v: Value, tid: usize) -> bool {
    let guard = crossbeam_epoch::pin();
    c.update(tid, k, v, &guard).is_ok()
}

#[no_mangle]
pub extern "C" fn clevel_delete(c: &Clevel<Key, Value>, k: Key, tid: usize) -> bool {
    let guard = crossbeam_epoch::pin();
    c.delete(&k, &guard);
    true
}

#[no_mangle]
pub extern "C" fn clevel_get_capacity(c: &Clevel<Key, Value>) -> usize {
    let guard = crossbeam_epoch::pin();
    c.get_capacity(&guard)
}
