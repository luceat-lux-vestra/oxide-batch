//! The M6 full component conformance and failure campaign (#153).
//!
//! This is a bounded campaign over the shipped M6 test targets. It reuses the
//! M5 target runner and report shape, but keeps its denominator explicit: the
//! workspace library target and the integration targets that exercise the
//! M6 component catalog, failure contracts, state, and restart behavior. The
//! `PostgreSQL` matrix is deliberately only 15 and 18, as required by the M6
//! exit protocol.

use std::path::Path;
use std::process::Command;

use serde_json::{Map, Value, json};

use crate::suite::{self, TargetCommand};

const REPORT: &str = "m6-conformance-campaign.json";
const SEMANTICS: &str = "tests/fixtures/m6-conformance/campaign-semantics.json";
const REQUIRED_FIXTURE_VARS: &[&str] = &[
    "OXIDEBATCH_POSTGRES_ADMIN_TEST_URL",
    "OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL",
    "OXIDEBATCH_POSTGRES_TEST_URL",
];

/// The fixed M6 shipped-component denominator. `__lib` selects the package's
/// library target; every other name selects one integration-test target.
const TARGETS: &[(&str, &str)] = &[
    ("oxide-batch", "__lib"),
    ("oxide-batch", "chunk"),
    ("oxide-batch", "chunk_builder"),
    ("oxide-batch", "chunk_fault_runtime"),
    ("oxide-batch", "chunk_runtime"),
    ("oxide-batch", "flow"),
    ("oxide-batch", "item_components_allocation"),
    ("oxide-batch", "item_components_equivalence"),
    ("oxide-batch", "item_components_flat_file_allocation"),
    ("oxide-batch", "item_components_json_allocation"),
    ("oxide-batch", "item_listeners"),
    ("oxide-batch", "item_stream"),
    ("oxide-batch", "item_stream_state"),
    ("oxide-batch", "postgres_completion_policy_restart"),
    ("oxide-batch", "postgres_fault_crash_recovery"),
    ("oxide-batch", "postgres_flow"),
    ("oxide-batch", "postgres_flow_crash_recovery"),
    ("oxide-batch", "postgres_item_components_batch_writer"),
    ("oxide-batch", "postgres_item_components_crash_recovery"),
    ("oxide-batch", "postgres_item_components_cursor"),
    ("oxide-batch", "postgres_item_components_cursor_fault"),
    ("oxide-batch", "postgres_item_components_paging"),
    ("oxide-batch", "postgres_item_stream_crash_recovery"),
    ("oxide-batch", "postgres_restart_after_many_chunks"),
    ("oxide-batch", "postgres_retention_component_state"),
    ("oxide-batch-test", "gate_g_scenarios"),
    ("oxide-batch-test", "item_components_basic"),
    ("oxide-batch-test", "item_components_classify"),
    ("oxide-batch-test", "item_components_composite"),
    ("oxide-batch-test", "item_components_decorators"),
    ("oxide-batch-test", "item_components_delimited"),
    ("oxide-batch-test", "item_components_fixed_width"),
    ("oxide-batch-test", "item_components_flat_file_fault"),
    ("oxide-batch-test", "item_components_json_array"),
    ("oxide-batch-test", "item_components_json_fault"),
    ("oxide-batch-test", "item_components_jsonl"),
    ("oxide-batch-test", "item_components_stream_composition"),
    ("oxide-batch-test", "postgres_fixture"),
    ("oxide-batch-test", "postgres_flat_file_restart"),
    ("oxide-batch-test", "postgres_item_components_db_restart"),
    ("oxide-batch-test", "postgres_item_components_restart"),
    ("oxide-batch-test", "postgres_json_restart"),
    ("oxide-batch-test", "postgres_multi_resource_restart"),
    ("oxide-batch-test", "process_fixture"),
    ("oxide-batch-test", "restart_harness"),
];

pub struct Campaign {
    pub violations: Vec<String>,
    pub report: std::path::PathBuf,
}

pub fn run() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;
    let mut violations = Vec::new();
    resolve_environment(&mut violations);

    let mut target_reports = Vec::new();
    if violations.is_empty() {
        let environment = REQUIRED_FIXTURE_VARS
            .iter()
            .filter_map(|name| std::env::var(name).ok().map(|value| (*name, value)))
            .collect::<Vec<_>>();
        for (package, name) in TARGETS {
            eprintln!("==> m6 conformance {package}/{name}");
            let selector = if *name == "__lib" {
                vec!["--lib".to_owned()]
            } else {
                vec!["--test".to_owned(), (*name).to_owned()]
            };
            let run = suite::run_target(
                &root,
                &TargetCommand {
                    package,
                    selector: &selector,
                    filters: &[],
                    environment: &environment,
                    nocapture: false,
                    release: false,
                },
            )?;
            if !run.succeeded {
                violations.push(format!("{package}/{name} exited unsuccessfully"));
            }
            let ignored = run
                .results
                .values()
                .filter(|outcome| outcome.as_str() == "ignored")
                .count();
            if ignored != 0 {
                violations.push(format!(
                    "{package}/{name} reported {ignored} ignored test(s); M6 campaign targets must run real evidence"
                ));
            }
            let failed = run
                .results
                .values()
                .filter(|outcome| outcome.as_str() != "ok")
                .count();
            if failed != 0 {
                violations.push(format!(
                    "{package}/{name} reported {failed} non-ok test outcome(s)"
                ));
            }
            if run.results.is_empty() {
                violations.push(format!("{package}/{name} reported no test outcomes"));
            }
            target_reports.push(json!({
                "package": package,
                "target": name,
                "selector": selector,
                "succeeded": run.succeeded,
                "tests": run.results.len(),
                "ignored": ignored,
                "results": run.results,
            }));
        }
    }

    let (manifest, manifest_violations) = execution_manifest(&root);
    violations.extend(manifest_violations);
    let report = write_report(&root, &target_reports, &violations, &manifest)?;
    Ok(Campaign { violations, report })
}

fn resolve_environment(violations: &mut Vec<String>) {
    match std::env::var(suite::MATRIX).as_deref() {
        Ok("postgres-15" | "postgres-18") => {}
        Ok(value) => violations.push(format!(
            "{} must be postgres-15 or postgres-18, got {value}",
            suite::MATRIX
        )),
        Err(_) => violations.push(format!(
            "{} is required so the PostgreSQL matrix point is recorded",
            suite::MATRIX
        )),
    }
    for variable in REQUIRED_FIXTURE_VARS {
        if std::env::var(variable).is_ok_and(|value| !value.is_empty()) {
            continue;
        }
        violations.push(format!(
            "{variable} is required for the M6 campaign and is absent"
        ));
    }
}

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
                "{path} is declared as M6 campaign semantics and is not present"
            )),
        }
    }
    (
        json!({
            "execution_commit": commit,
            "execution_commit_note": "The tree this run actually executed against; in CI this is the pull-request merge commit.",
            "tree_clean": git(root, &["status", "--porcelain"]).map(|status| status.is_empty()),
            "objects": Value::Object(objects),
        }),
        violations,
    )
}

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

fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

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
        "report": "m6-conformance",
        "campaign": "M6 full component conformance, malformed/failure, lifecycle, and restart campaign",
        "postgresql_major_version": std::env::var(suite::MATRIX).ok().and_then(|value| value.strip_prefix("postgres-").map(str::to_owned)),
        "environment": suite::environment_with_profile("debug"),
        "target_denominator": TARGETS.len(),
        "targets": target_reports,
        "observation": { "execution_manifest": manifest },
        "scope_note": "Every selected target ran in full; ignored or empty targets fail closed.",
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
