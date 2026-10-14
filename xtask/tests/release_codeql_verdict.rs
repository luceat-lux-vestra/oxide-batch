//! Stage A1 offline CodeQL release SARIF verdict regression.
//! No release authorization, PR CodeQL mutation, or workflow execution.

#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn release_codeql_sarif_verdict_rejects_hostile_reports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives under workspace root")
        .to_path_buf();
    let script = root.join(".github/scripts/release-codeql-verdict.py");
    assert!(script.is_file(), "missing offline verdict script");
    let output = Command::new("python3")
        .arg(script)
        .arg("--self-test")
        .current_dir(&root)
        .output()
        .expect("python3 offline SARIF self-tests must execute");
    assert!(
        output.status.success(),
        "offline SARIF self-tests rejected:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
