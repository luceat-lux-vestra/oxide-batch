//! End-to-end negative fixture for the real repository-wide retained-evidence policy verifier.

#![allow(clippy::expect_used)]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

struct Fixture {
    source_root: PathBuf,
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_root = manifest_dir
            .parent()
            .expect("xtask must live directly under the workspace root")
            .to_path_buf();
        let root = std::env::temp_dir().join(format!(
            "oxide-batch-retained-evidence-policy-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let output = Command::new("git")
            .arg("-C")
            .arg(&source_root)
            .args(["worktree", "add", "--detach", "--quiet"])
            .arg(&root)
            .arg("HEAD")
            .output()
            .expect("git worktree add must run");
        assert!(
            output.status.success(),
            "could not create retained-evidence fixture worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        Self { source_root, root }
    }

    fn replace_once(&self, relative: &str, from: &str, to: &str) {
        let path = self.root.join(relative);
        let text = fs::read_to_string(&path).expect("fixture file must be readable");
        assert_eq!(
            text.matches(from).count(),
            1,
            "fixture mutation anchor must occur exactly once in {}",
            path.display()
        );
        fs::write(&path, text.replacen(from, to, 1)).expect("fixture file must be writable");
    }

    fn run_policy_check(&self) -> Output {
        Command::new(env!("CARGO_BIN_EXE_retained_evidence_policy"))
            .current_dir(&self.root)
            .output()
            .expect("retained-evidence policy verifier must run")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Command::new("git")
            .arg("-C")
            .arg(&self.source_root)
            .args(["worktree", "remove", "--force"])
            .arg(&self.root)
            .status();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn rejected(output: &Output, expected: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "broken retained-evidence policy unexpectedly passed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains(expected),
        "rejection did not prove the intended retained-evidence drift {expected:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn rejects_canonical_verdict_authority_drift() {
    let fixture = Fixture::new();
    fixture.replace_once(
        "docs/engineering/retained-evidence-policy.json",
        "\"canonical_verdict\": \"violations\"",
        "\"canonical_verdict\": \"producer-passed\"",
    );

    rejected(
        &fixture.run_policy_check(),
        "canonical verdict must remain the violations collection",
    );
}
