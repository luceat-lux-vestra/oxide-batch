//! The Gate B campaign runner: typed-vs-`Boxed*` transaction/restart
//! equivalence (#153 §3).
//!
//! Gate B is a fixed, frozen set of eight scenarios
//! (`docs/project/m6-design-gate-evidence.md#gate-b--transactionrestart-equivalence-protocol`),
//! not an evolving multi-report campaign the way the M5 crash-and-restore
//! campaign is. This runner is deliberately simpler than
//! [`crate::crash_restore`]'s scope-document-driven design for that reason:
//! there is one fixed list of test targets and test names to require, not a
//! reconciled set of reports/phases/reused scenarios that changes as M5
//! campaigns are added. If Gate B ever needs to grow that kind of structure,
//! follow `crash_restore.rs`'s shape rather than growing this file ad hoc.
//!
//! ## Forged-pass prevention
//!
//! Every Gate B test checks `OXIDEBATCH_POSTGRES_TEST_URL`/
//! `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and skips with `Ok(())` -- exit
//! code 0 -- when either is absent. Under a bare `cargo test` that is
//! indistinguishable from real evidence, so this runner requires both
//! variables to be set *before* running a single target
//! (`resolve_fixture`), the same fail-closed shape
//! `crate::crash_restore`'s own private `resolve_fixtures` uses (not linked:
//! it is a different module's private item). It also requires every
//! named test in [`EXPECTED_TESTS`] to report `ok` by name, not just that
//! each target process exited successfully -- a target could exit 0 having
//! skipped every test inside it.
//!
//! ## Execution manifest
//!
//! Unlike the M5 crash-restore/performance campaigns, this runner computes
//! the execution manifest itself (`execution_manifest`, below) rather than
//! hoisting it from an in-test self-recorded observation. Those campaigns
//! hoist because they reuse M2-M4 test binaries that were not necessarily
//! all built in the same `cargo test` invocation the runner drives, so the
//! manifest has to be recorded *from inside* the process that actually ran
//! to prove which tree that specific binary saw. Every Gate B target here
//! is compiled and run back-to-back within this one runner's own
//! [`suite::run_target`] calls, so the runner's own git state at the point
//! all targets have passed is the tree that produced them -- there is no
//! separately-built-earlier binary whose tree could have silently drifted
//! underneath it.

use std::path::Path;
use std::process::Command;

use serde_json::{Map, Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "gate-b-campaign.json";

/// Environment variables that must be set before any target runs, or every
/// scenario silently skips and still exits `0`.
const REQUIRED_FIXTURE_VARS: &[&str] = &[
    "OXIDEBATCH_POSTGRES_TEST_URL",
    "OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL",
];

/// Every Gate B test target, in the order the frozen protocol lists its
/// scenarios (foundation smoke first, since B-01..B-08 all build on it).
const TARGETS: &[&str] = &[
    "gate_b_foundation_smoke",
    "gate_b_01_normal_commit",
    "gate_b_02_writer_failure_rollback",
    "gate_b_03_atomic_boundary",
    "gate_b_04_unknown_outcome",
    "gate_b_05_kill_before_commit",
    "gate_b_06_kill_around_acknowledgement",
    "gate_b_07_multi_chunk_restart",
    "gate_b_08_representation_transparent_identity",
];

/// Every test name Gate B requires to report `ok`. A target reporting
/// success while a named test inside it is absent, failed, or ignored is a
/// campaign failure, not a pass -- this is what makes a target that
/// silently skipped every scenario (fixture absent, filtered by a typo)
/// distinguishable from real evidence.
const EXPECTED_TESTS: &[&str] = &[
    "park_smoke_worker_process",
    "parking_writer_reaches_its_announced_point_and_is_killed",
    "typed_and_boxed_representations_produce_identical_durable_observations",
    "normal_enlisted_commit_is_representation_identical",
    "writer_failure_before_commit_rolls_back_identically",
    "state_checkpoint_counter_share_one_atomic_boundary",
    "unknown_commit_outcome_forces_recovery_not_inference",
    "unknown_outcome_worker_process",
    "kill_before_commit_worker_process",
    "process_kill_before_commit_restart_is_identical",
    "kill_around_acknowledgement_worker_process",
    "process_kill_around_commit_acknowledgement_is_identical",
    "multi_chunk_restart_first_worker_process",
    "multi_chunk_restart_second_worker_process",
    "multi_chunk_restart_selects_identically",
    "cross_representation_worker_process",
    "definition_fingerprint_is_representation_independent",
    "representation_does_not_change_definition_or_restart_identity",
];

/// The declared semantic closure this campaign's evidence is bound to.
const SEMANTICS: &str = "tests/fixtures/gate-b/campaign-semantics.json";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: std::path::PathBuf,
}

/// Runs the Gate B campaign and writes its report.
///
/// # Errors
///
/// Returns the failure that prevents the campaign from producing a result at
/// all, such as an unwritable report directory or an unreadable semantics
/// document.
pub fn run() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;

    let mut violations = Vec::new();
    resolve_fixture(&mut violations);

    let mut outcomes: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let mut target_reports = Vec::new();
    if violations.is_empty() {
        for target in TARGETS {
            eprintln!("==> gate-b {target}");
            let run = suite::run_target(
                &root,
                &TargetCommand {
                    package: "oxide-batch",
                    selector: &["--test".to_owned(), (*target).to_owned()],
                    filters: &[],
                    environment: &[],
                    nocapture: false,
                    release: false,
                },
            )?;
            if !run.succeeded {
                violations.push(format!("{target} exited unsuccessfully"));
            }
            target_reports.push(json!({
                "target": target,
                "succeeded": run.succeeded,
                "results": run.results,
            }));
            outcomes.extend(run.results);
        }

        for name in EXPECTED_TESTS {
            match outcomes.get(*name).map(String::as_str) {
                Some("ok") => {}
                Some(other) => violations.push(format!("{name} reported {other}, not ok")),
                None => violations.push(format!("{name} did not run")),
            }
        }
    }

    let (manifest, manifest_violations) = execution_manifest(&root);
    violations.extend(manifest_violations);

    let report = write_report(&root, &target_reports, &violations, &manifest)?;
    Ok(Campaign { violations, report })
}

/// Requires every fixture variable Gate B's tests read to select whether
/// they run for real, before a single target starts.
fn resolve_fixture(violations: &mut Vec<String>) {
    for variable in REQUIRED_FIXTURE_VARS {
        if std::env::var(variable).is_ok_and(|value| !value.is_empty()) {
            continue;
        }
        violations.push(format!(
            "{variable} is required for the Gate B campaign and is absent -- every scenario \
             would silently skip and still exit 0"
        ));
    }
}

/// Computes the execution manifest over Gate B's declared semantic closure.
///
/// See this module's own doc comment for why this runner computes the
/// manifest directly rather than hoisting it from an in-test observation.
fn execution_manifest(root: &Path) -> (Value, Vec<String>) {
    let mut violations = Vec::new();
    let Some(commit) = git(root, &["rev-parse", "HEAD"]) else {
        return (
            Value::Null,
            vec!["the campaign is not running inside a git tree".to_owned()],
        );
    };
    let Ok(paths) = semantics_paths(root) else {
        return (Value::Null, vec![format!("could not read {SEMANTICS}")]);
    };

    let mut objects = Map::new();
    for path in paths {
        match git(root, &["rev-parse", &format!("HEAD:{path}")]) {
            Some(object) => {
                objects.insert(path, Value::String(object));
            }
            None => violations.push(format!(
                "{path} is declared as Gate B campaign semantics and is not present"
            )),
        }
    }

    let manifest = json!({
        "execution_commit": commit,
        "execution_commit_note": "The tree this run actually executed against, read from the \
                                  checkout the campaign is running in. In CI this is the \
                                  pull-request merge commit rather than the branch head.",
        "tree_clean": git(root, &["status", "--porcelain"]).map(|status| status.is_empty()),
        "objects": Value::Object(objects),
    });
    (manifest, violations)
}

/// Reads the declared semantic closure from its canonical document.
fn semantics_paths(root: &Path) -> Result<Vec<String>, String> {
    let path = root.join(SEMANTICS);
    let source = std::fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;
    let paths = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| "the semantics document declares no categories".to_owned())?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Err("the semantics document declares no paths".to_owned());
    }
    Ok(paths)
}

/// Runs one git command against the workspace, tolerating failure.
fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Reads the `PostgreSQL` major the campaign was configured to run at.
fn expected_matrix_major() -> Option<String> {
    std::env::var(suite::MATRIX)
        .ok()
        .and_then(|matrix| matrix.strip_prefix("postgres-").map(str::to_owned))
}

/// Writes the retained campaign report and returns its path.
fn write_report(
    root: &Path,
    target_reports: &[Value],
    violations: &[String],
    manifest: &Value,
) -> Result<std::path::PathBuf, String> {
    let directory = suite::directory(root);
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let document = json!({
        "report": "gate-b",
        "campaign": "M6 Gate B transaction/restart equivalence",
        "postgresql_major_version": expected_matrix_major(),
        "targets": target_reports,
        "environment": suite::environment(),
        "observation": { "execution_manifest": manifest },
        "violations": violations,
        "passed": violations.is_empty(),
    });

    std::fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&document)
                .map_err(|error| format!("could not render the report: {error}"))?
        ),
    )
    .map_err(|error| format!("could not write {}: {error}", path.display()))?;

    Ok(path)
}
