//! Low-level utils
//!
//! src: https://github.com/NVSL/Corundum/blob/main/src/ll.rs
#![allow(unused)]

#[cfg(target_arch = "x86")]
use std::arch::x86::{_mm_mfence, _mm_sfence, clflush};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_mm_clflush, _mm_mfence, _mm_sfence};

/// Synchronize caches and memories and acts like a write barrier
#[inline(always)]
pub(crate) fn persist(ptr: usize, len: usize, fence: bool) {
    #[cfg(not(feature = "no_persist"))]
    {
        #[cfg(not(feature = "use_msync"))]
        clflush(ptr, len, fence);

        #[cfg(feature = "use_msync")]
        unsafe {
            let off = ptr as *const T as *const u8 as usize;
            let end = off + len;
            let off = (off >> 12) << 12;
            let len = end - off;
            let ptr = off as *const u8;
            if libc::persist(
                ptr as *mut libc::c_void,
                len,
                libc::MS_SYNC | libc::MS_INVALIDATE,
            ) != 0
            {
                panic!("persist failed");
            }
        }
    }
}

/// Synchronize caches and memories and acts like a write barrier
#[inline(always)]
pub(crate) fn persist_obj<T: ?Sized>(obj: &T, fence: bool) {
    #[cfg(not(feature = "no_persist"))]
    {
        let ptr = obj as *const T as *const u8 as *mut u8 as usize;
        persist(ptr, std::mem::size_of_val(obj), fence);
    }
}

/// Flushes cache line back to memory
#[inline(always)]
pub(crate) fn clflush(ptr: usize, len: usize, fence: bool) {
    #[cfg(not(feature = "no_persist"))]
    {
        let mut start = ptr;
        start = (start >> 9) << 9;
        let end = start + len;

        #[cfg(feature = "stat_print_flushes")]
        println!("flush {:x} ({})", start, len);

        while start < end {
            unsafe {
                #[cfg(not(any(feature = "use_clflushopt", feature = "use_clwb")))]
                {
                    asm!("clflush [{}]", in(reg) (start as *const u8), options(nostack));
                }
                #[cfg(all(feature = "use_clflushopt", not(feature = "use_clwb")))]
                {
                    asm!("clflushopt [{}]", in(reg) (start as *const u8), options(nostack));
                    // llvm_asm!("clflushopt ($0)" :: "r"(start as *const u8));
                }
                #[cfg(all(feature = "use_clwb", not(feature = "use_clflushopt")))]
                {
                    asm!("clwb [{}]", in(reg) (start as *const u8), options(nostack));
                    // llvm_asm!("clwb ($0)" :: "r"(start as *const u8));
                }
                #[cfg(all(feature = "use_clwb", feature = "use_clflushopt"))]
                {
                    compile_error!("Please Select only one from clflushopt and clwb")
                }
            }
            start += 64;
        }
    }
    if (fence) {
        sfence();
    }
}

/// Store fence
#[inline(always)]
pub(crate) fn sfence() {
    #[cfg(any(feature = "use_clwb", feature = "use_clflushopt"))]
    unsafe {
        _mm_sfence();
    }
}

/// Memory fence
#[inline]
pub(crate) fn mfence() {
    unsafe {
        _mm_mfence();
    }
}
