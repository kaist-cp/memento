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
// #![deny(unused_crate_dependencies)] // TODO: newop_pipe 리뷰 후 다른 obj들 고칠 때 주석 해제
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
#![feature(generic_associated_types)]

// Persistent objects collection
// pub mod exchanger; // TODO: plocation으로 동작시키며 주석해제
// pub mod list; // TODO: plocation으로 동작시키며 주석해제
// pub mod lock; // TODO: plocation으로 동작시키며 주석해제
pub mod persistent;
// pub mod pipe; // TODO: plocation으로 동작시키며 주석해제
pub mod queue;
// pub mod stack; // TODO: plocation으로 동작시키며 주석해제
// pub mod treiber_stack; // TODO: plocation으로 동작시키며 주석해제

// Persistent location
pub mod plocation; // TODO: 바뀐 POp 인터페이스 적용

// Persistent version of crossbeam_epoch
pub mod pepoch; // TODO: 바뀐 POp 인터페이스 적용

// Utility
pub mod utils; // TODO: 바뀐 POp 인터페이스 적용
