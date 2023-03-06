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
#![deny(unused_crate_dependencies)]
#![deny(unused_extern_crates)]
#![deny(unused_import_braces)]
#![deny(unused_qualifications)]
#![deny(unused_results)]
#![deny(variant_size_differences)]
#![deny(warnings)]
#![deny(rustdoc::invalid_html_tags)]
#![deny(rustdoc::missing_doc_code_examples)]
#![deny(missing_docs)]
#![deny(rustdoc::all)]
#![deny(unreachable_pub)]
#![deny(single_use_lifetimes)]
#![deny(unused_lifetimes)]
// #![deny(unstable_features)] // Allowed due to below
#![feature(extern_types)] // to use extern types (e.g. `GarbageCollection` of Ralloc)
#![feature(update_panic_count)] // to simulate thread crash
#![feature(rt)] // to simulate thread crash
#![feature(backtrace)] // to debug test
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
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

use crate::pmem::ralloc::Collectable;
use crossbeam_utils::CachePadded;
use ploc::Handle;
use std::{mem::ManuallyDrop, ptr};

/// A wrapper to freeze Ownership
///
/// - Freeze ownership of the target object via `from()`
/// - Using `own()` to regain ownership of the object
/// - Similar to `ManuallyDrop`. The difference is that `ManuallyDrop` only `clone()` when value is `Clone`
///   whereas `Frozen` can `clone()` any value
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
    /// Get ownership of an object
    ///
    /// # Safety
    ///
    /// Safe only when both of the following conditions are satisfied:
    /// - After `own()`, there must be a checkpoint (*c*) between the last access to the object (*t1*)
    ///   and the point when the object is (1) handed over to another thread (2) or dropped from the own thread (*t2*).
    ///   + checkpoint(*c*): Any evidence that could indicate that the object is no longer needed (e.g. flags, states)
    /// - Note that we haven't gone through *c* yet.
    ///
    /// # Examples
    ///
    /// ```rust
    ///    use memento::Frozen;
    ///
    ///    // Assume that these variables are always accessible from pmem
    ///    let src = Frozen::<Box<i32>>::from(Box::new(42));
    ///    let mut data = 0;
    ///    let mut flag = false;
    ///
    ///    {
    ///        // Receive message from `src` and store it in data
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

/// Default trait for Persistent object
pub trait PDefault: Collectable {
    /// Persistent default using pool to allocate persistent object
    fn pdefault(handle: &Handle) -> Self;
}

impl PDefault for usize {
    fn pdefault(_: &Handle) -> Self {
        Default::default()
    }
}

pub use mmt_derive::*;

/// Trait for Memento
pub trait Memento: Default + Collectable {
    /// clear
    fn clear(&mut self);
}

impl Memento for usize {
    fn clear(&mut self) {
        *self = 0
    }
}

impl Memento for u32 {
    fn clear(&mut self) {
        *self = 0
    }
}

impl<T: Memento> Memento for Option<T> {
    fn clear(&mut self) {
        *self = None
    }
}

impl<T: Memento> Memento for CachePadded<T> {
    fn clear(&mut self) {
        (**self).clear();
    }
}
