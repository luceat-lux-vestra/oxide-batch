#![allow(clippy::expect_used)]

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Mutex;

static WORKTREE_LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    source_root: PathBuf,
    root: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_root = manifest_dir
            .parent()
            .expect("xtask must live directly under the workspace root")
            .to_path_buf();
        let root = std::env::temp_dir().join(format!(
            "oxide-batch-release-contract-{}-{name}",
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
            "could not create fixture worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        Self { source_root, root }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    fn replace_once(&self, relative: &str, from: &str, to: &str) {
        let path = self.path(relative);
        let text = fs::read_to_string(&path).expect("fixture file must be readable");
        assert_eq!(
            text.matches(from).count(),
            1,
            "fixture mutation anchor must occur exactly once in {}",
            path.display()
        );
        fs::write(&path, text.replacen(from, to, 1)).expect("fixture file must be writable");
    }

    fn append(&self, relative: &str, text: &str) {
        let path = self.path(relative);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("fixture file must open for append");
        file.write_all(text.as_bytes())
            .expect("fixture text must append");
    }

    fn run_release_check(&self) -> Output {
        Command::new(env!("CARGO_BIN_EXE_oxide-batch-xtask"))
            .arg("release-crates")
            .current_dir(&self.root)
            .env("CARGO_TERM_COLOR", "never")
            .output()
            .expect("release verifier must run")
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

fn rejected(output: &Output, expected: &[&str]) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "broken release contract unexpectedly passed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    for needle in expected {
        assert!(
            stderr.contains(needle),
            "rejection did not prove the intended contract gap {needle:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
}

fn serial_fixture(name: &str, test: impl FnOnce(&Fixture)) {
    let _guard = WORKTREE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let fixture = Fixture::new(name);
    test(&fixture);
}

#[test]
fn rejects_a_publishable_crate_missing_from_the_expected_set() {
    serial_fixture("missing-crate", |fixture| {
        fixture.replace_once(
            "crates/oxide-batch-test/Cargo.toml",
            "publish = [\"crates-io\"]",
            "publish = false",
        );
        rejected(
            &fixture.run_release_check(),
            &["missing released crate", "oxide-batch-test"],
        );
    });
}

#[test]
fn rejects_a_duplicate_crate_attestation_entry() {
    serial_fixture("duplicate-crate", |fixture| {
        fixture.append(
            ".github/workflows/release-draft.yml",
            "\n      - name: Attest duplicate oxide-batch-core SBOM\n        uses: actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6 # v4.2.2\n        with:\n          subject-path: target/package/oxide-batch-core-${{ steps.package.outputs.version }}.crate\n          sbom-path: target/package/oxide-batch-core-${{ steps.package.outputs.version }}.cdx.json\n",
        );
        rejected(
            &fixture.run_release_check(),
            &["SBOM attestations", "expected exactly"],
        );
    });
}

#[test]
fn rejects_a_cargo_version_that_does_not_match_the_release_version() {
    serial_fixture("version-mismatch", |fixture| {
        fixture.replace_once(
            "crates/oxide-batch-test/Cargo.toml",
            "version.workspace = true",
            "version = \"0.6.1\"",
        );
        rejected(
            &fixture.run_release_check(),
            &["oxide-batch-test", "expected lockstep 0.6.0"],
        );
    });
}

#[test]
fn rejects_a_stale_sbom_digest_in_the_reviewed_evidence_manifest() {
    serial_fixture("stale-sbom", |fixture| {
        fixture.replace_once(
            "docs/release/evidence/v0.6.0.json",
            "7dc67d5f9f5e91ede4a3b55ae18f97dc98d29eac76a99c597c195f13c94eb9b5",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        rejected(
            &fixture.run_release_check(),
            &["crates.oxide-batch-core.sbomSha256", "expected"],
        );
    });
}

#[test]
fn rejects_an_sbom_attestation_with_the_wrong_subject() {
    serial_fixture("wrong-subject", |fixture| {
        fixture.replace_once(
            ".github/workflows/release-draft.yml",
            "subject-path: target/package/oxide-batch-core-${{ steps.package.outputs.version }}.crate",
            "subject-path: target/package/oxide-batch-repository-${{ steps.package.outputs.version }}.crate",
        );
        rejected(
            &fixture.run_release_check(),
            &["oxide-batch-repository", "with sbom-path", "expected"],
        );
    });
}

#[test]
fn rejects_a_publish_registered_flow_that_accepts_a_published_release() {
    serial_fixture("draft-state-mismatch", |fixture| {
        fixture.replace_once(
            ".github/workflows/release.yml",
            "test \"$(jq -r '.isDraft' <<<\"${view}\")\" = \"true\"",
            "test \"$(jq -r '.isDraft' <<<\"${view}\")\" = \"false\"",
        );
        rejected(
            &fixture.run_release_check(),
            &["must verify isDraft == true in publish-registered mode"],
        );
    });
}
