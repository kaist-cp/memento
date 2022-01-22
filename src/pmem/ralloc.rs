//! Linking Ralloc (https://github.com/urcs-sync/ralloc)

use super::{global_pool, PoolHandle};
use std::os::raw::{c_char, c_int, c_ulong, c_void};

/* automatically generated by rust-bindgen 0.59.1 */
//
// command: bindgen --allowlist-function "RP.*" ext/ralloc/src/ralloc.hpp -o ralloc.rs
//
// 원래 ralloc에 있던 함수
#[link(name = "ralloc", kind = "static")]
extern "C" {
    /// return이 1이면 원래 존재하는 파일을 open한 것, 0이면 파일 새로 만든 것
    pub(crate) fn RP_init(_id: *const c_char, size: u64) -> c_int;

    /// return이 1이면 dirty라서 gc 돌린 것, 0이면 dirty 아니라서 gc 안돌린 것
    pub(crate) fn RP_recover() -> c_int;

    pub(crate) fn RP_close();

    pub(crate) fn RP_malloc(sz: c_ulong) -> *mut c_void;

    pub(crate) fn RP_free(ptr: *mut c_void);

    pub(crate) fn RP_set_root(ptr: *mut c_void, i: u64) -> *mut c_void;

    pub(crate) fn RP_get_root_c(i: u64) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_malloc_size(ptr: *mut c_void) -> c_ulong;

    #[allow(dead_code)]
    pub(crate) fn RP_calloc(num: c_ulong, size: c_ulong) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_realloc(ptr: *mut c_void, new_size: c_ulong) -> *mut c_void;

    #[allow(dead_code)]
    pub(crate) fn RP_in_prange(ptr: *mut c_void) -> c_int;

    #[allow(dead_code)]
    pub(crate) fn RP_region_range(
        idx: c_int,
        start_addr: *mut *mut c_void,
        end_addr: *mut *mut c_void,
    ) -> c_int;
}

/// RP_init시 매핑된 주소를 반환
#[allow(non_snake_case)]
pub(crate) unsafe fn RP_mmapped_addr() -> usize {
    let mut start: *mut i32 = std::ptr::null_mut();
    let mut end: *mut i32 = std::ptr::null_mut();
    let _ret = RP_region_range(
        1, // superblock region의 index.
        &mut start as *mut *mut _ as *mut *mut c_void,
        &mut end as *mut *mut _ as *mut *mut c_void,
    );
    start as usize
}

// 원래 ralloc에는 없고 GC를 위해 추가한 함수
#[link(name = "ralloc", kind = "static")]
extern "C" {
    /// Ralloc의 type `GarbageCollection`을 인식
    pub type GarbageCollection;

    /// GC의 시작점인 root filter function 등록
    pub(crate) fn RP_set_root_filter(
        filter_func: ::std::option::Option<
            unsafe extern "C" fn(*mut c_char, usize, &mut GarbageCollection),
        >,
        i: u64,
    );

    #[link_name = "\u{1}_ZN17GarbageCollection11mark_func_cEPcmPFvS0_mRS_E"]
    pub(crate) fn RP_mark(
        this: *mut GarbageCollection,
        ptr: *mut c_char,
        tid: usize,
        filter_func: ::std::option::Option<
            unsafe extern "C" fn(*mut c_char, usize, &mut GarbageCollection),
        >,
    );
}

/// Trait for Garbage Collection
///
/// Persistent obj가 Ralloc GC에 의해 mark되기 위해선 이 trait을 impl해야함
///
/// 유저는 safe fn만 impl하면 내부에서 unsafe로 Ralloc과 상호작용
///
/// ```text
///             ----------- Black box ------------------      Ralloc
///            |                                        |
/// fn mark  ---> unsafe RP_mark (Rust에서 C 함수를 호출) --->    ...
///     ^      |                                        |       |
///     |      |                                        |       |
///     |      |                                        |       v
/// fn filter <--- unsafe filter_inner (C에서 Rust함수를 호출)  <---
///            |                                        |
///             ----------------------------------------
/// ```
pub trait Collectable: Sized {
    /// 자신을 marking하고, 자신의 filter func으로 다음 marking을 예약
    fn mark(s: &mut Self, tid: usize, gc: &mut GarbageCollection) {
        let ptr = s as *mut _ as *mut c_char;
        unsafe { RP_mark(gc, ptr, tid, Some(Self::filter_inner)) };
    }

    /// # NOTE: do not use this function
    ///
    /// * 이 함수는 Ralloc에서 호출되게 할 용도로, 유저는 사용하면 안됨
    /// * Ralloc에서 이 함수를 호출하면 우리는 obj의 filter func를 찾아서 호출
    ///
    /// # Guaranteed by Ralloc
    ///
    /// * Ralloc에서 넘겨주는 ptr은 자신을 가리키는 raw 포인터
    ///
    /// # Safety
    ///
    /// TODO
    // TODO: Collectable trait에서 이 함수 안보이게 하기
    // C에서 이 함수의 이름이 아닌 주소로 호출하므로 #[no_mangle] 필요 없을듯
    unsafe extern "C" fn filter_inner(ptr: *mut c_char, tid: usize, gc: &mut GarbageCollection) {
        let pool = global_pool().unwrap();
        let s = (ptr as *mut _ as *mut Self).as_mut().unwrap();
        Self::filter(s, tid, gc, pool);
    }

    /// 자신의 필드 중 marking 할 것을 찾아서 marking
    ///
    /// # Example
    ///
    /// ```
    /// # use memento::pmem::pool::PoolHandle;
    /// # use memento::pmem::ralloc::GarbageCollection;
    /// # use memento::pmem::ralloc::Collectable;
    /// # use memento::pmem::ptr::PPtr;
    /// # struct Inner {}
    /// # impl Collectable for Inner {
    /// #    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {
    /// #    }
    /// # }
    /// struct Node {
    ///     inner: Inner, // Assume `Inner` impl Collectable
    ///     next: PPtr<Node>,
    /// }
    ///
    /// impl Collectable for Node {
    ///     fn filter(node: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
    ///         // Call filter of inner to mark the ptr in the inner struct
    ///         Inner::filter(&mut node.inner, gc, pool);
    ///
    ///         // Mark the next node if the pointer is valid
    ///         if !node.next.is_null() {
    ///             let next = unsafe { node.next.deref_mut(pool) };
    ///             Node::mark(next, gc);
    ///         }
    ///     }
    /// }
    /// ```
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle);
}

impl<T: Collectable, U: Collectable> Collectable for (T, U) {
    fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &PoolHandle) {
        T::filter(&mut s.0, tid, gc, pool);
        U::filter(&mut s.1, tid, gc, pool);
    }
}

impl Collectable for usize {
    fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &PoolHandle) {}
}
