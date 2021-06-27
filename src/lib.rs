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
#![deny(unused_crate_dependencies)]
#![deny(unused_extern_crates)]
#![deny(unused_import_braces)]
#![deny(unused_qualifications)]
#![deny(unused_results)]
#![deny(variant_size_differences)]
#![deny(warnings)]
#![deny(invalid_html_tags)]
#![deny(missing_doc_code_examples)]
#![deny(missing_docs)]
#![deny(rustdoc::all)]
#![deny(single_use_lifetimes)]
#![deny(unreachable_pub)]
#![deny(unstable_features)]
#![deny(unused_lifetimes)]

//! Persistent objects collection

/// Common constants or utils
// mod common;

/// Single-threaded counter
pub mod counter;

/// Concurrent exchanger
pub mod exchanger;

/// Trait collection for persistent objects
pub mod persistent;

use crossbeam_epoch as _;
use crossbeam_utils as _;
