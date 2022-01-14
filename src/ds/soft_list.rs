//! TODO: doc

use crossbeam_epoch::Atomic;
use libc::c_void;

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
    /// per-thread persistent ssmem allocator
    // TODO:
    //  - 이 reference는 persistent 해야할듯: 왜냐하면 복구시 crash 이전에 쓰던 durable area과 같은 곳을 가리킬 수 있도록 해야함
    //  - 이게 가능하면 volatile하게 둬도 됨: 복구시 reference를 다시 세팅할 때 crash 이전과 같은 durable area를 가리키게 하기
    // TODO: Ralloc GC시 ssmem_allocator가 가진 memory chunk들은 mark 되게 해야할 듯. 안그러면 Ralloc GC가 ssmem이 사용하던 memory chunk들을 free해감
    static ALLOC: RefCell<*mut ssmem_allocator> = RefCell::new(null_mut());

    /// per-thread volatile ssmem allocator
    // TODO: volatile ssmem allocator는 굳이 필요한가? volatile node는 그냥 Rust standard allocator 써도 되는 거 아닌가?
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

enum State {
    INSERTED,
    INTEND_TO_DELETE,
    INTEND_TO_INSERT,
    DELETED,
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

    fn find(&self, key: usize, predPtr: *mut *mut VNode<T>) -> *mut VNode<T> {
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

// helper function

#[inline]
fn getRef<Node>(ptr: *mut Node) -> *mut Node {
    todo!()
}

#[inline]
fn createRef<Node>(p: *mut Node, s: State) -> *mut Node {
    todo!()
}

#[inline]
fn stateCAS<Node>(atomicP: &Atomic<*mut Node>, expected: State, newVal: State) -> bool {
    todo!()
}

#[inline]
fn getState(p: *mut c_void) {
    todo!()
}

#[inline]
fn isOut() -> bool {
    todo!()
}
