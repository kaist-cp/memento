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
// #![deny(unused_crate_dependencies)] // TODO: obj 주석 해제하면서 이 주석도 같이 해제
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
// #![deny(single_use_lifetimes)] // Allowd due to GAT
// #![deny(unused_lifetimes)] // Allowd due to GAT
// #![deny(unstable_features)] // Allowd due to GAT
#![feature(associated_type_defaults)] // to use composition of Stack::TryPush for Stack::Push as default
#![feature(generic_associated_types)] // to define fields of `Memento`
#![feature(asm)]
#![feature(never_type)] // to use `!`
#![feature(extern_types)] // to use extern types (e.g. `GarbageCollection` of Ralloc)

// Persistent objects collection
pub mod elim_stack;
pub mod exchanger;
pub mod list;
// pub mod lock; // TODO: free, persist 추가하며 주석 해제
pub mod persistent;
pub mod pipe;
pub mod queue;

pub mod stack;

// pub mod ticket_lock; // TODO: free, persist 추가하며 주석 해제
pub mod treiber_stack;

// Persistent location
pub mod plocation;

// Persistent version of crossbeam_epoch
pub mod pepoch;

// Utility
pub mod utils;
