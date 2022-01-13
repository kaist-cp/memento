//! TODO: doc

use crossbeam_epoch::Atomic;

use crate::pmem::{
    ssmem_alloc, ssmem_alloc_init, ssmem_allocator, PoolHandle, SSMEM_DEFAULT_MEM_SIZE,
};
use std::{
    cell::RefCell,
    mem::size_of,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicUsize},
};

thread_local! {
    // per-thread persistent ssmem allocator
    static ALLOC: RefCell<*mut ssmem_allocator> = RefCell::new(null_mut());

    // per-thread volatile ssmem allocator
    static VOLATILE_ALLOC: *mut ssmem_allocator = null_mut();
}

/// initialize thread-local persistent allocator
pub fn init_alloc(id: isize, pool: &'static PoolHandle) {
    let r = pool.alloc::<ssmem_allocator>();
    ALLOC.with(|a| {
        let mut alloc = a.borrow_mut();
        *alloc = unsafe { r.deref_mut(pool) };
        ssmem_alloc_init(*alloc, SSMEM_DEFAULT_MEM_SIZE, id, Some(pool));
    });
}

/// initialize thread-local volatile allocator
pub fn init_volatileAlloc(id: usize) {
    todo!()
}

/// TODO: doc
#[derive(Debug)]
pub struct SOFTList<T> {
    head: VNode<T>,
}

impl<T> SOFTList<T> {
    fn allocNewPNode(&self, pool: &'static PoolHandle) -> *mut PNode<T> {
        ALLOC
            .try_with(|a| {
                let r = ssmem_alloc(*a.borrow_mut(), size_of::<PNode<T>>(), Some(pool));
                r as *mut PNode<T>
            })
            .unwrap()
    }

    fn allocNewVNode(&self) -> *mut VNode<T> {
        todo!("VNode는 굳이 ssmem을 써야할 필요있나?")
    }

    fn trim(&self, prev: *mut VNode<T>, curr: *mut VNode<T>) -> bool {
        todo!()
    }

    fn find(&self) -> *mut VNode<T> {
        todo!()
    }

    /// TODO: doc
    pub fn insert(&self, key: usize, value: T) -> bool {
        todo!()
    }

    /// TODO: doc
    pub fn remove(&self, key: usize) -> bool {
        todo!()
    }

    /// TODO: doc
    pub fn contains(&self, key: usize) -> *mut T {
        todo!()
    }

    /// TODO: 아마 recovery용
    pub fn quickInsert(&self, newPNode: *mut PNode<T>, pValid: bool) {
        todo!()
    }

    /// TODO: doc
    pub fn recovery(&self) {
        todo!()
    }
}

/// persistent node
#[repr(align(32))]
#[derive(Debug)]
pub struct PNode<T> {
    validStart: AtomicBool,
    validEnd: AtomicBool,
    deleted: AtomicBool,
    key: AtomicUsize,
    value: Atomic<T>,
}

impl<T> PNode<T> {
    fn alloc() -> bool {
        todo!()
    }

    fn create() {
        todo!()
    }

    fn destroy() {
        todo!()
    }

    fn isValid() -> bool {
        todo!()
    }

    fn isDeleted() -> bool {
        todo!()
    }

    fn recoveryValidity() -> bool {
        todo!()
    }
}

/// volatile node
#[derive(Debug)]
struct VNode<T> {
    key: usize,
    value: T,
    pptr: *mut PNode<T>,
    pValidity: bool,
    next: Atomic<*mut VNode<T>>,
}
