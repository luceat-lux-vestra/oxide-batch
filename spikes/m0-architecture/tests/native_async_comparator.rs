//! Compiler comparator for native async trait dyn compatibility.

#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn native_async_trait_is_not_dyn_compatible_on_the_supported_toolchain() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ui/native_async_trait_dyn.rs");
    let output = Command::new("rustc")
        .args(["--edition=2024", "--crate-type=lib"])
        .arg(&fixture)
        .arg("--out-dir")
        .arg(std::env::temp_dir())
        .output()
        .expect("rustc comparator must run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not dyn compatible"),
        "unexpected compiler diagnostic: {stderr}"
    );
}
