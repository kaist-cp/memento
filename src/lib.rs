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
#![deny(warnings)]
#![deny(rustdoc::invalid_html_tags)]
#![deny(rustdoc::missing_doc_code_examples)]
#![deny(missing_docs)]
#![deny(rustdoc::all)]
#![deny(unreachable_pub)]
// #![deny(single_use_lifetimes)] // Allowed due to GAT
// #![deny(unused_lifetimes)] // Allowed due to GAT
// #![deny(unstable_features)] // Allowed due to GAT
#![feature(associated_type_defaults)] // to use composition of Stack::TryPush for Stack::Push as default
#![feature(generic_associated_types)] // to define fields of `Memento`
#![feature(asm)]
#![feature(never_type)] // to use `!`
#![feature(extern_types)] // to use extern types (e.g. `GarbageCollection` of Ralloc)

// Persistent objects collection
pub mod atomic_update_common;
pub mod atomic_update_unopt;
// pub mod elim_stack;
// pub mod exchanger;
// pub mod list;
// pub mod lock;
pub mod persistent;
// pub mod pipe;
// pub mod queue;
// pub mod queue_lp;
pub mod queue_unopt;
// pub mod queue_unopt_lp;
pub mod stack;
// pub mod ticket_lock;
pub mod treiber_stack;
pub mod unopt_node;

// Persistent location
pub mod plocation;

// Persistent version of crossbeam_epoch
pub mod pepoch;

// Utility
pub mod utils;
