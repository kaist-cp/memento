//! PMDK functions from pmcheck
#![allow(warnings)]
use libc::*;

// type void = c_void;
// type size_t = usize;
// type int =

// #[cfg(feature = "pmcheck")]

use libc::{c_char, c_int, c_void, mode_t};

use std::fmt;

// #[cfg(feature = "pmcheck")]
// #[link(name = "pmcheck")]

// // bindgen --allowlist-function "pm.*" ../pmcheck/Memory/libpmem.h -o pmdk.rs
// extern "C" {
//     pub(crate) fn pmem_map_file(
//         path: *const ::std::os::raw::c_char,
//         len: usize,
//         flags: ::std::os::raw::c_int,
//         mode: mode_t,
//         mapped_lenp: *mut usize,
//         is_pmemp: *mut ::std::os::raw::c_int,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_file_exists(path: *const ::std::os::raw::c_char) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_unmap(
//         addr: *mut ::std::os::raw::c_void,
//         len: usize,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_is_pmem(
//         addr: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_persist(addr: *const ::std::os::raw::c_void, len: usize);
//     pub(crate) fn pmem_msync(
//         addr: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_has_auto_flush() -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_flush(addr: *const ::std::os::raw::c_void, len: usize);
//     pub(crate) fn pmem_deep_flush(addr: *const ::std::os::raw::c_void, len: usize);
//     pub(crate) fn pmem_deep_drain(
//         addr: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_deep_persist(
//         addr: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_drain();
//     pub(crate) fn pmem_has_hw_drain() -> ::std::os::raw::c_int;
//     pub(crate) fn pmem_memmove_persist(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memcpy_persist(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memset_persist(
//         pmemdest: *mut ::std::os::raw::c_void,
//         c: ::std::os::raw::c_int,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memmove_nodrain(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memcpy_nodrain(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memset_nodrain(
//         pmemdest: *mut ::std::os::raw::c_void,
//         c: ::std::os::raw::c_int,
//         len: usize,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memmove(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//         flags: ::std::os::raw::c_uint,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memcpy(
//         pmemdest: *mut ::std::os::raw::c_void,
//         src: *const ::std::os::raw::c_void,
//         len: usize,
//         flags: ::std::os::raw::c_uint,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_memset(
//         pmemdest: *mut ::std::os::raw::c_void,
//         c: ::std::os::raw::c_int,
//         len: usize,
//         flags: ::std::os::raw::c_uint,
//     ) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmem_register_file(
//         path: *const ::std::os::raw::c_char,
//         addr: *mut ::std::os::raw::c_void,
//     ) -> ::std::os::raw::c_int;
//     pub(crate) fn pmdk_malloc(size: usize) -> *mut ::std::os::raw::c_void;
//     pub(crate) fn pmdk_pagealigned_calloc(size: usize) -> *mut ::std::os::raw::c_void;
//     // void *pmem_map_file(const char *path, size_t len, int flags, mode_t mode, size_t *mapped_lenp, int *is_pmemp);
//     // fn pmem_map_file(path: *mut char, len: usize , isize flags, usize mode, *mut usize mapped_lenp, *mut isize is_pmemp) -> *mut c_void;

//     // void * pmdk_malloc(size_t size)
//     // void pmem_flush(const void *addr, size_t len);
//     // int pmem_msync(const void *addr, size_t len);

// }
