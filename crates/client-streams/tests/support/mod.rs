//! Shared helpers for the client-streams integration suites.
//!
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as a
//! non-binary submodule, so it does not compile the file as its own test crate.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// A path under `tests/testdata`, resolved wherever the suite is run from.
///
/// Cargo runs a test with the crate directory as the working directory, so a
/// bare `tests/testdata/...` resolves; a build system that runs it from a
/// workspace root instead needs the crate prefix. Reading the variable at run
/// time rather than through `env!` also keeps the absolute build directory out
/// of the compiled test binary, which a sandboxed build refuses to produce.
#[must_use]
pub fn testdata(relative: &str) -> PathBuf {
    let base =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| "crates/client-streams".to_owned());
    Path::new(&base).join("tests/testdata").join(relative)
}
