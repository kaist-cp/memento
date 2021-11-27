//! benchmarking을 위한 구현들

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
// #![deny(unused_crate_dependencies)] // main에서만 쓰는 crate 때문에 allow 필요 (e.g. regex, csv)
#![deny(unused_extern_crates)]
#![deny(unused_import_braces)]
#![deny(unused_qualifications)]
#![deny(unused_results)]
#![deny(variant_size_differences)]
// #![deny(warnings)]
#![deny(rustdoc::invalid_html_tags)]
#![deny(rustdoc::missing_doc_code_examples)]
// #![deny(missing_docs)] // TODO: evaluation도 doc 필요하면 주석해제
#![deny(rustdoc::all)]
#![deny(unreachable_pub)]
// #![deny(single_use_lifetimes)] // Allowd due to GAT
// #![deny(unused_lifetimes)] // Allowd due to GAT
// #![deny(unstable_features)] // Allowd due to GAT
#![feature(generic_associated_types)]
#![feature(asm)]

pub mod compositional_pobj;
// pub mod crndm;
pub mod dss;
pub mod friedman;

pub mod common;
