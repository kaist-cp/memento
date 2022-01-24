//! Low-level utils
//!
//! src: https://github.com/NVSL/Corundum/blob/main/src/ll.rs
#![allow(unused)]

const CACHE_LINE_SHIFT: usize = 6;

#[cfg(target_arch = "x86")]
use std::arch::x86::{_mm_mfence, _mm_sfence, clflush};

use std::arch::x86_64::_MM_HINT_ET1;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __rdtscp, _mm_clflush, _mm_lfence, _mm_mfence, _mm_prefetch, _mm_sfence, _rdtsc,
};

/// Synchronize caches and memories and acts like a write barrier
#[inline(always)]
pub fn persist<T: ?Sized>(ptr: *const T, len: usize, fence: bool) {
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
pub fn persist_obj<T: ?Sized>(obj: &T, fence: bool) {
    #[cfg(not(feature = "no_persist"))]
    {
        persist(obj, std::mem::size_of_val(obj), fence);
    }
}

/// Flushes cache line back to memory
#[inline(always)]
pub fn clflush<T: ?Sized>(ptr: *const T, len: usize, fence: bool) {
    #[cfg(not(feature = "no_persist"))]
    {
        let ptr = ptr as *const u8 as *mut u8;
        let start = ptr as usize;
        let end = start + len;

        let mut cur = (start >> CACHE_LINE_SHIFT) << CACHE_LINE_SHIFT;

        #[cfg(feature = "stat_print_flushes")]
        println!("flush {:x} ({})", cur, len);

        while cur < end {
            unsafe {
                #[cfg(not(any(feature = "use_clflushopt", feature = "use_clwb")))]
                {
                    asm!("clflush [{}]", in(reg) (cur as *const u8), options(nostack));
                }
                #[cfg(all(feature = "use_clflushopt", not(feature = "use_clwb")))]
                {
                    asm!("clflushopt [{}]", in(reg) (cur as *const u8), options(nostack));
                    // llvm_asm!("clflushopt ($0)" :: "r"(start as *const u8));
                }
                #[cfg(all(feature = "use_clwb", not(feature = "use_clflushopt")))]
                {
                    asm!("clwb [{}]", in(reg) (cur as *const u8), options(nostack));
                    // llvm_asm!("clwb ($0)" :: "r"(cur as *const u8));
                }
                #[cfg(all(feature = "use_clwb", feature = "use_clflushopt"))]
                {
                    compile_error!("Please Select only one from clflushopt and clwb")
                }
            }
            cur += 1 << CACHE_LINE_SHIFT;
        }
    }
    if (fence) {
        sfence();
    }
}

/// Store fence
#[inline(always)]
pub fn sfence() {
    #[cfg(any(feature = "use_clwb", feature = "use_clflushopt"))]
    unsafe {
        _mm_sfence();
    }
}

/// Memory fence
#[inline]
pub fn mfence() {
    unsafe {
        _mm_mfence();
    }
}

/// Load fence
#[inline]
pub fn lfence() {
    unsafe {
        _mm_lfence();
    }
}

/// Rdtsc
#[inline]
pub fn rdtsc() -> u64 {
    unsafe { _rdtsc() }
}

/// Rdtscp
#[inline]
pub fn rdtscp() -> u64 {
    unsafe {
        let mut rdtscp_result = 0;
        __rdtscp(&mut rdtscp_result)
    }
}

/// Fetches the cache line of data from memory that contains the byte specified with the source operand to a location in the 1st or 2nd level cache and invalidates other cached instances of the line.
// meaning of "w": indicate an anticipation to write to the address.
#[inline]
pub fn prefetchw<T>(p: *const T) {
    unsafe { _mm_prefetch::<_MM_HINT_ET1>(p as *const i8) }
}
