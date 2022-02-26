//! Compositional Construction of Failure-Safe Persistent Objects

// # Tries to deny all lints (`rustc -W help`).
#![deny(absolute_paths_not_starting_with_crate)]
#![deny(anonymous_parameters)]
#![deny(box_pointers)]
#![deny(deprecated_in_future)]
#![deny(explicit_outlives_requirements)]
#![deny(keyword_idents)]
#![deny(macro_use_extern_crate)]
#![deny(missing_debug_implementations)]
#![deny(non_ascii_idents)]
#![deny(pointer_structural_match)]
#![deny(rust_2018_idioms)]
#![deny(trivial_numeric_casts)]
#![deny(unaligned_references)]
// #![deny(unused_crate_dependencies)]
#![deny(unused_extern_crates)]
#![deny(unused_import_braces)]
#![deny(unused_qualifications)]
#![deny(unused_results)]
#![deny(variant_size_differences)]
// #![deny(warnings)]
#![deny(rustdoc::invalid_html_tags)]
#![deny(rustdoc::missing_doc_code_examples)]
#![deny(missing_docs)]
#![deny(rustdoc::all)]
#![deny(unreachable_pub)]
// #![deny(single_use_lifetimes)] // Allowed due to GAT
// #![deny(unused_lifetimes)] // Allowed due to GAT
// #![deny(unstable_features)] // Allowed due to GAT
#![allow(clippy::type_complexity)] // to allow SMO prepare functions
#![feature(associated_type_defaults)] // to use composition of Stack::TryPush for Stack::Push as default
#![feature(generic_associated_types)] // to define fields of `Memento`
#![feature(asm)]
#![feature(never_type)] // to use `!`
#![feature(extern_types)] // to use extern types (e.g. `GarbageCollection` of Ralloc)
#![feature(new_uninit)] // for clevel
#![feature(core_intrinsics)]
#![recursion_limit = "512"]

// Persistent objects collection
pub mod ds;
pub mod ploc;

// Persistent memory underline
pub mod pmem;

// Persistent version of crossbeam_epoch
pub mod pepoch;

// Utility
pub mod test_utils;

use crate::pmem::{pool::PoolHandle, ralloc::Collectable};
use crossbeam_epoch::Guard;
use std::{mem::ManuallyDrop, ptr};

/// Ownership을 얼리기 위한 wrapper.
///
/// - `from()`을 통해 target object의 ownership을 얼림
/// - `own()`을 통해 object의 ownership을 다시 획득
/// - `ManuallyDrop`과 유사. 차이점은 `ManuallyDrop`은 value가 `Clone`일 때에만 `clone()`하지만
///   `Frozen`은 어떤 value든 `clone()` 가능하다는 것임
#[derive(Debug)]
pub struct Frozen<T> {
    value: ManuallyDrop<T>,
}

impl<T> Clone for Frozen<T> {
    fn clone(&self) -> Self {
        Self {
            value: unsafe { ptr::read(&self.value) },
        }
    }
}

impl<T> From<T> for Frozen<T> {
    fn from(item: T) -> Self {
        Self {
            value: ManuallyDrop::new(item),
        }
    }
}

impl<T> Frozen<T> {
    /// object의 ownership을 획득
    ///
    /// # Safety
    ///
    /// 다음 두 조건을 모두 만족할 때에만 safe:
    /// - `own()` 후 object로의 마지막 접근(*t1*)과
    ///   object가 다른 스레드에 넘겨지는 시점 혹은 own한 스레드에서 drop 되는 시점(*t2*) 사이에
    ///   checkpoint(*c*)가 있어야 함.
    ///   + checkpoint(*c*): object가 더 이상 필요하지 않음을 나타낼 수 있는 어떠한 증거든 상관 없음 (e.g. flag, states)
    /// - *c*를 아직 거치지 않았다는 것을 알아야 함.
    ///
    /// # Examples
    ///
    /// ```rust
    ///    use memento::Frozen;
    ///
    ///    // 이 변수들은 언제나 pmem에서 접근 가능함을 가정
    ///    let src = Frozen::<Box<i32>>::from(Box::new(42)); // TODO(opt): use `PBox`
    ///    let mut data = 0;
    ///    let mut flag = false;
    ///
    ///    {
    ///        // `src`로부터 메시지를 받아와 data에 저장하는 로직
    ///        // 이 로직은 crash 이전이나 이후 모두 안전함
    ///        if !flag { // Checking if the checkpoint c has not yet passed
    ///            let msg = src.clone(); // Cloning a `Frozen` object from somewhere.
    ///            let x = unsafe { msg.own() }; // This is always safe because `flag` shows that the inner value of `msg` is still valid.
    ///            data = *x; // The last access to `x` (t1)
    ///            flag = true; // Checkpointing that `msg` is no longer needed. (c)
    ///            // x is dropped (t2), no more valid.
    ///        }
    ///        assert_eq!(data, 42);
    ///    }
    /// ```
    pub unsafe fn own(self) -> T {
        ManuallyDrop::into_inner(self.value)
    }
}

/// TODO(doc)
pub trait PDefault: Collectable {
    /// TODO(doc)
    fn pdefault(pool: &PoolHandle) -> Self;
}
