/* automatically generated by rust-bindgen 0.59.1 */
//
// command: bindgen --allowlist-function "RP.*" ralloc/src/ralloc.hpp -o ralloc.rs

#![allow(non_camel_case_types)]
#![allow(missing_docs)]

pub type size_t = ::std::os::raw::c_ulong;
pub type __uint64_t = ::std::os::raw::c_ulong;

#[link(name = "ralloc", kind = "static")]
extern "C" {
    /// return이 1이면 원래 존재하는 파일을 open한 것, 0이면 파일 새로 만든 것
    pub fn RP_init(_id: *const ::std::os::raw::c_char, size: u64) -> ::std::os::raw::c_int;

    /// return이 1이면 dirty라서 gc 돌린 것, 0이면 dirty 아니라서 gc 안돌린 것
    pub fn RP_recover() -> ::std::os::raw::c_int;

    pub fn RP_close();

    pub fn RP_malloc(sz: size_t) -> *mut ::std::os::raw::c_void;

    pub fn RP_free(ptr: *mut ::std::os::raw::c_void);

    pub fn RP_set_root(ptr: *mut ::std::os::raw::c_void, i: u64) -> *mut ::std::os::raw::c_void;

    pub fn RP_get_root_c(i: u64) -> *mut ::std::os::raw::c_void;

    pub fn RP_malloc_size(ptr: *mut ::std::os::raw::c_void) -> size_t;

    pub fn RP_calloc(num: size_t, size: size_t) -> *mut ::std::os::raw::c_void;

    pub fn RP_realloc(
        ptr: *mut ::std::os::raw::c_void,
        new_size: size_t,
    ) -> *mut ::std::os::raw::c_void;

    pub fn RP_in_prange(ptr: *mut ::std::os::raw::c_void) -> ::std::os::raw::c_int;

    pub fn RP_region_range(
        idx: ::std::os::raw::c_int,
        start_addr: *mut *mut ::std::os::raw::c_void,
        end_addr: *mut *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int;
}
