//! The Gate H campaign runner: the P-002 real-component performance
//! campaign (#153 §6).
//!
//! See [`crate::gate_b`]'s module doc for why this runner (like Gate B's) is
//! deliberately simpler than [`crate::crash_restore`]/[`crate::performance`]'s
//! scope-document-driven design, and computes its execution manifest
//! directly rather than hoisting it from an in-test observation: Gate H is a
//! fixed, frozen set of four test targets, all built and run back-to-back
//! within this runner's own invocation.
//!
//! Unlike Gate B, no `PostgreSQL` fixture is required -- the M6 P-002
//! reference workload is the real, shipped `DelimitedReader`/
//! `DelimitedWriter` CSV components, which are file-based. This campaign
//! runs in **release** profile, per the frozen protocol's own requirement
//! that a debug-build figure is not comparable to anything release planning
//! could use (the same reason [`crate::performance`] is the one M5 campaign
//! that does).
//!
//! The two hard pass/fail criteria (typed per-item future allocation == 0;
//! typed path requires no framework-controlled dynamic dispatch per item)
//! are proved structurally by `gate_h_dispatch.rs`'s own assertions, which
//! this runner requires to pass like every other named test. Throughput,
//! latency, and the allocation-delta disclosure numbers are retained as
//! evidence, never compared against an invented threshold, per the frozen
//! protocol's own "no invented performance threshold" rule -- this runner
//! does not add one either.

use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::{Map, Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "gate-h-campaign.json";

/// Every Gate H test target.
const TARGETS: &[&str] = &[
    "gate_h_allocation",
    "gate_h_dispatch",
    "gate_h_throughput",
    "item_listener_allocation",
];

/// Targets that emit raw machine-readable measurements through the campaign
/// runner's per-target observation path.
const OBSERVATION_TARGETS: &[&str] = &[
    "gate_h_allocation",
    "gate_h_throughput",
    "item_listener_allocation",
];

/// Every test name Gate H requires to report `ok`.
const EXPECTED_TESTS: &[&str] = &[
    "typed_csv_pipeline_allocates_no_more_per_item_than_erased",
    "boxed_components_are_exactly_fat_pointer_sized",
    "typed_path_framework_controlled_per_item_allocation_is_zero",
    "typed_path_requires_no_framework_controlled_dynamic_dispatch_per_item",
    "throughput_and_latency_recorded_without_an_invented_threshold",
    "listener_enabled_allocation_is_reported_separately_from_typed_path",
];

/// The declared semantic closure this campaign's evidence is bound to.
const SEMANTICS: &str = "tests/fixtures/gate-h/campaign-semantics.json";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: std::path::PathBuf,
}

/// Runs the Gate H campaign and writes its report.
///
/// # Errors
///
/// Returns the failure that prevents the campaign from producing a result at
/// all, such as an unwritable report directory or an unreadable semantics
/// document.
pub fn run() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;

    let mut violations = Vec::new();
    let mut outcomes: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let mut target_reports = Vec::new();
    let observation_directory = suite::directory(&root).join("gate-h-observations");
    std::fs::create_dir_all(&observation_directory).map_err(|error| {
        format!(
            "could not create {}: {error}",
            observation_directory.display()
        )
    })?;
    for target in TARGETS {
        eprintln!("==> gate-h {target} (release)");
        let observation_path = observation_directory.join(format!("{target}.json"));
        let _ = std::fs::remove_file(&observation_path);
        let observation_path_string = observation_path.to_string_lossy().into_owned();
        let environment = if OBSERVATION_TARGETS.contains(target) {
            vec![(
                "OXIDEBATCH_GATE_H_OBSERVATION",
                observation_path_string.clone(),
            )]
        } else {
            Vec::new()
        };
        let run = suite::run_target(
            &root,
            &TargetCommand {
                package: "oxide-batch",
                selector: &["--test".to_owned(), (*target).to_owned()],
                filters: &[],
                environment: &environment,
                nocapture: true,
                release: true,
            },
        )?;
        if !run.succeeded {
            violations.push(format!("{target} exited unsuccessfully"));
        }
        let mut target_report = json!({
            "target": target,
            "succeeded": run.succeeded,
            "results": run.results,
        });
        if OBSERVATION_TARGETS.contains(target) {
            match std::fs::read_to_string(&observation_path) {
                Ok(source) => match serde_json::from_str::<Value>(&source) {
                    Ok(observation) => {
                        violations.extend(validate_observation(target, &observation));
                        target_report["observation"] = observation;
                    }
                    Err(error) => {
                        violations
                            .push(format!("{target} wrote invalid JSON observation: {error}"));
                    }
                },
                Err(error) => violations.push(format!(
                    "{target} did not write {}: {error}",
                    observation_path.display()
                )),
            }
        }
        target_reports.push(target_report);
        outcomes.extend(run.results);
    }

    for name in EXPECTED_TESTS {
        match outcomes.get(*name).map(String::as_str) {
            Some("ok") => {}
            Some(other) => violations.push(format!("{name} reported {other}, not ok")),
            None => violations.push(format!("{name} did not run")),
        }
    }

    let (manifest, manifest_violations) = execution_manifest(&root);
    violations.extend(manifest_violations);

    let (code_size, code_size_violations) = binary_size_and_compile_time(&root);
    violations.extend(code_size_violations);

    let report = write_report(&root, &target_reports, &violations, &manifest, &code_size)?;
    Ok(Campaign { violations, report })
}

fn validate_observation(target: &str, observation: &Value) -> Vec<String> {
    let required = match target {
        "gate_h_allocation" => [
            "/workload",
            "/typed/delta/allocator_calls",
            "/typed/delta/bytes_allocated",
            "/boxed/delta/allocator_calls",
            "/boxed/delta/bytes_allocated",
            "/copied_bytes",
            "/buffer_reuse",
            "/framework_controlled",
        ]
        .as_slice(),
        "gate_h_throughput" => [
            "/workload",
            "/typed/raw_latency_nanoseconds",
            "/boxed/raw_latency_nanoseconds",
            "/typed/throughput_items_per_second",
            "/boxed/throughput_items_per_second",
            "/buffer_reuse",
            "/framework_controlled",
        ]
        .as_slice(),
        "item_listener_allocation" => [
            "/workload",
            "/listener_enabled/delta_allocator_calls",
            "/listener_enabled/allocator_calls_per_item",
            "/listener_representation",
        ]
        .as_slice(),
        _ => &[],
    };
    required
        .iter()
        .filter(|pointer| observation.pointer(pointer).is_none())
        .map(|pointer| format!("{target} observation is missing {pointer}"))
        .collect()
}

/// The two binary-size/compile-time reference examples, isolating
/// representation as their only variable (see their own doc comments).
const REFERENCE_EXAMPLES: [(&str, &str); 2] = [
    ("typed", "gate_h_typed_reference"),
    ("boxed", "gate_h_boxed_reference"),
];

/// Builds each reference example from its own clean target directory in
/// release profile, timing the build and measuring the resulting binary's size --
/// disclosure evidence, per the frozen protocol's required-metrics list, not
/// a pass/fail threshold. A build failure is still recorded as a violation:
/// not because a size or time number missed some invented target, but
/// because the disclosure evidence this section owes could not be produced
/// at all.
fn binary_size_and_compile_time(root: &Path) -> (Value, Vec<String>) {
    let mut violations = Vec::new();
    let mut measurements = Map::new();
    for (label, example) in REFERENCE_EXAMPLES {
        match build_and_measure(root, example) {
            Ok(measurement) => {
                measurements.insert(label.to_owned(), measurement);
            }
            Err(error) => violations.push(format!(
                "could not measure the {label} reference binary: {error}"
            )),
        }
    }
    (Value::Object(measurements), violations)
}

/// Builds one reference example in release profile and measures its
/// compiled size and build wall-clock time.
fn build_and_measure(root: &Path, example: &str) -> Result<Value, String> {
    let target_directory = std::env::temp_dir().join(format!(
        "oxide-batch-gate-h-target-{}-{example}",
        std::process::id()
    ));
    // A prior interrupted campaign may leave this exact per-process path
    // behind. It is not a workspace or user-data path, and each reference is
    // measured in this isolated directory rather than reusing the other
    // representation's dependency artifacts.
    let _ = fs::remove_dir_all(&target_directory);
    let started = std::time::Instant::now();
    let status = Command::new("cargo")
        .current_dir(root)
        .args([
            "build",
            "--package",
            "oxide-batch",
            "--example",
            example,
            "--release",
        ])
        .env("CARGO_TARGET_DIR", &target_directory)
        .status()
        .map_err(|error| format!("could not run cargo build: {error}"))?;
    let elapsed = started.elapsed();
    if !status.success() {
        let _ = fs::remove_dir_all(&target_directory);
        return Err("cargo build did not succeed".to_owned());
    }
    let binary = target_directory.join("release/examples").join(example);
    let size_bytes = fs::metadata(&binary)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("could not read {}: {error}", binary.display()));
    let _ = fs::remove_dir_all(&target_directory);
    let size_bytes = size_bytes?;
    Ok(json!({
        "compile_time_seconds": elapsed.as_secs_f64(),
        "binary_size_bytes": size_bytes,
        "target_directory_isolation": "clean-per-reference",
    }))
}

/// Computes the execution manifest over Gate H's declared semantic closure.
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
                "{path} is declared as Gate H campaign semantics and is not present"
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

/// Writes the retained campaign report and returns its path.
fn write_report(
    root: &Path,
    target_reports: &[Value],
    violations: &[String],
    manifest: &Value,
    code_size: &Value,
) -> Result<std::path::PathBuf, String> {
    let directory = suite::directory(root);
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let document = json!({
        "report": "gate-h",
        "campaign": "M6 Gate H P-002 real-component performance",
        "postgresql_major_version": "not-applicable",
        "environment": suite::environment_with_profile("release"),
        "targets": target_reports,
        "observation": { "execution_manifest": manifest },
        "hard_invariants": {
            "typed_per_item_future_allocation_is_zero": {
                "value": 0,
                "proof": "proved structurally by gate_h_dispatch.rs"
            },
            "typed_dynamic_dispatch_per_item_is_zero": {
                "value": 0,
                "proof": "proved structurally by gate_h_dispatch.rs"
            },
        },
        "binary_size_and_compile_time": code_size,
        "no_invented_threshold_note": "Throughput, latency, allocation-delta, binary-size, and \
                                       compile-time numbers are retained as disclosure evidence \
                                       per the frozen protocol; none is compared against an \
                                       invented pass/fail threshold by this runner.",
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
