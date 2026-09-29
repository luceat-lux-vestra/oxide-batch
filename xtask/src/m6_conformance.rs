//! The M6 full component conformance and failure campaign (#153).
//!
//! This is a bounded campaign over the shipped M6 test targets. It reuses the
//! M5 target runner and report shape, but keeps its denominator explicit: the
//! workspace library target and the integration targets that exercise the
//! M6 component catalog, failure contracts, state, and restart behavior. The
//! `PostgreSQL` matrix is deliberately only 15 and 18, as required by the M6
//! exit protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Map, Value, json};

use crate::suite::{self, TargetCommand};

const REPORT: &str = "m6-conformance-campaign.json";
const SHARD_REPORT_PREFIX: &str = "m6-conformance-shard-";
const SHARD_INDEX_ENV: &str = "OXIDEBATCH_M6_CONFORMANCE_SHARD_INDEX";
const SHARD_COUNT_ENV: &str = "OXIDEBATCH_M6_CONFORMANCE_SHARD_COUNT";
const SHARD_MERGE_DIR_ENV: &str = "OXIDEBATCH_M6_CONFORMANCE_MERGE_DIR";
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
    pub report: PathBuf,
}

pub fn run() -> Result<Campaign, String> {
    let shard_index = env::var(SHARD_INDEX_ENV).ok();
    let shard_count = env::var(SHARD_COUNT_ENV).ok();
    let merge_dir = env::var(SHARD_MERGE_DIR_ENV).ok();

    match (shard_index, shard_count, merge_dir) {
        (None, None, None) => run_full(),
        (Some(index), Some(count), None) => run_shard(&index, &count),
        (None, Some(count), Some(directory)) => merge_shards(&count, Path::new(&directory)),
        _ => Err(
            "M6 conformance CI mode is ambiguous: shard execution requires shard index+count only, \
             canonical merge requires shard count+merge directory only"
                .to_owned(),
        ),
    }
}

fn run_full() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;
    let mut violations = Vec::new();
    resolve_environment(&mut violations);

    let environment = suite::environment_with_profile("debug");
    let target_reports = if violations.is_empty() {
        run_targets(&root, TARGETS, &mut violations)?
    } else {
        Vec::new()
    };

    let (manifest, manifest_violations) = execution_manifest(&root);
    violations.extend(manifest_violations);
    let report = write_report(&root, &target_reports, &violations, &manifest, &environment)?;
    Ok(Campaign { violations, report })
}

fn run_shard(index: &str, count: &str) -> Result<Campaign, String> {
    let index = parse_shard_number("index", index)?;
    let count = parse_shard_number("count", count)?;
    if count < 2 {
        return Err("M6 conformance shard count must be at least 2".to_owned());
    }
    if index >= count {
        return Err(format!(
            "M6 conformance shard index {index} is outside shard count {count}"
        ));
    }

    let root = suite::workspace_root()?;
    let mut violations = Vec::new();
    resolve_environment(&mut violations);
    let environment = suite::environment_with_profile("debug");
    let (manifest, manifest_violations) = execution_manifest(&root);
    violations.extend(manifest_violations);

    let partitions = partition_targets(TARGETS, count)?;
    let selected = &partitions[index];
    let target_reports = if violations.is_empty() {
        run_targets(&root, selected, &mut violations)?
    } else {
        Vec::new()
    };

    let report = write_shard_report(
        &root,
        ShardReportInput {
            index,
            count,
            selected,
            target_reports: &target_reports,
            violations: &violations,
            manifest: &manifest,
            environment: &environment,
        },
    )?;
    Ok(Campaign { violations, report })
}

fn merge_shards(count: &str, directory: &Path) -> Result<Campaign, String> {
    let count = parse_shard_number("count", count)?;
    if count < 2 {
        return Err("M6 conformance shard count must be at least 2".to_owned());
    }

    let root = suite::workspace_root()?;
    let mut violations = Vec::new();
    resolve_environment(&mut violations);
    if !violations.is_empty() {
        return Err(format!(
            "M6 conformance canonical merge environment is invalid: {}",
            violations.join("; ")
        ));
    }

    let environment = suite::environment_with_profile("debug");
    let (manifest, manifest_violations) = execution_manifest(&root);
    if !manifest_violations.is_empty() {
        return Err(format!(
            "M6 conformance canonical merge manifest is invalid: {}",
            manifest_violations.join("; ")
        ));
    }
    let expected_major = expected_matrix_major().ok_or_else(|| {
        "M6 conformance canonical merge requires OXIDEBATCH_CAMPAIGN_MATRIX".to_owned()
    })?;
    let partitions = partition_targets(TARGETS, count)?;
    ensure_exact_shard_report_set(directory, count)?;

    let mut merged = BTreeMap::<(String, String), Value>::new();
    for (index, expected_targets) in partitions.iter().enumerate() {
        let path = directory.join(shard_report_name(index));
        let reports = read_validated_shard(
            &path,
            index,
            count,
            &expected_major,
            expected_targets,
            &manifest,
            &environment,
        )?;
        for report in reports {
            let package = required_string(&report, "package", &path)?;
            let target = required_string(&report, "target", &path)?;
            let key = (package, target);
            if merged.insert(key.clone(), report).is_some() {
                return Err(format!(
                    "{} duplicates target {}/{} across M6 conformance shards",
                    path.display(),
                    key.0,
                    key.1
                ));
            }
        }
    }

    let expected = target_pairs(TARGETS);
    if merged.len() != expected.len() {
        return Err(format!(
            "M6 conformance shard merge covered {} targets, expected {}",
            merged.len(),
            expected.len()
        ));
    }
    let observed = merged.keys().cloned().collect::<BTreeSet<_>>();
    let expected_set = expected.iter().cloned().collect::<BTreeSet<_>>();
    if observed != expected_set {
        return Err("M6 conformance shard merge is not an exact target cover".to_owned());
    }

    let target_reports = expected
        .into_iter()
        .map(|key| {
            merged
                .remove(&key)
                .ok_or_else(|| format!("M6 conformance merge omitted {}/{}", key.0, key.1))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let report = write_report(&root, &target_reports, &[], &manifest, &environment)?;
    Ok(Campaign {
        violations: Vec::new(),
        report,
    })
}

fn run_targets(
    root: &Path,
    targets: &[(&str, &str)],
    violations: &mut Vec<String>,
) -> Result<Vec<Value>, String> {
    let environment = REQUIRED_FIXTURE_VARS
        .iter()
        .filter_map(|name| std::env::var(name).ok().map(|value| (*name, value)))
        .collect::<Vec<_>>();
    let mut target_reports = Vec::new();

    for (package, name) in targets {
        eprintln!("==> m6 conformance {package}/{name}");
        let selector = target_selector(name);
        let run = suite::run_target(
            root,
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
    Ok(target_reports)
}

fn target_selector(name: &str) -> Vec<String> {
    if name == "__lib" {
        vec!["--lib".to_owned()]
    } else {
        vec!["--test".to_owned(), name.to_owned()]
    }
}

fn parse_shard_number(label: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid M6 conformance shard {label} {value:?}: {error}"))
}

fn partition_targets<'a>(
    targets: &'a [(&'a str, &'a str)],
    count: usize,
) -> Result<Vec<Vec<(&'a str, &'a str)>>, String> {
    if count == 0 {
        return Err("M6 conformance shard count cannot be zero".to_owned());
    }
    if targets.len() < count {
        return Err(format!(
            "M6 conformance shard count {count} exceeds {} selected targets",
            targets.len()
        ));
    }

    let mut shards = vec![Vec::new(); count];
    for (position, target) in targets.iter().copied().enumerate() {
        shards[position % count].push(target);
    }
    if shards.iter().any(Vec::is_empty) {
        return Err("M6 conformance shard partition produced an empty shard".to_owned());
    }

    let flattened = shards.iter().flatten().copied().collect::<Vec<_>>();
    let expected = targets.iter().copied().collect::<BTreeSet<_>>();
    let observed = flattened.iter().copied().collect::<BTreeSet<_>>();
    if flattened.len() != targets.len() || observed.len() != flattened.len() || observed != expected
    {
        return Err("M6 conformance shard partition is not an exact one-to-one cover".to_owned());
    }
    Ok(shards)
}

fn target_pairs(targets: &[(&str, &str)]) -> Vec<(String, String)> {
    targets
        .iter()
        .map(|(package, target)| ((*package).to_owned(), (*target).to_owned()))
        .collect()
}

fn shard_report_name(index: usize) -> String {
    format!("{SHARD_REPORT_PREFIX}{index}.json")
}

struct ShardReportInput<'a> {
    index: usize,
    count: usize,
    selected: &'a [(&'a str, &'a str)],
    target_reports: &'a [Value],
    violations: &'a [String],
    manifest: &'a Value,
    environment: &'a Value,
}

fn write_shard_report(root: &Path, input: ShardReportInput<'_>) -> Result<PathBuf, String> {
    let ShardReportInput {
        index,
        count,
        selected,
        target_reports,
        violations,
        manifest,
        environment,
    } = input;
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(shard_report_name(index));
    let targets = selected
        .iter()
        .map(|(package, target)| json!({"package": package, "target": target}))
        .collect::<Vec<_>>();

    let document = json!({
        "report": "m6-conformance-shard",
        "schema_version": 1,
        "postgresql_major_version": expected_matrix_major(),
        "environment": environment,
        "target_denominator": TARGETS.len(),
        "observation": { "execution_manifest": manifest },
        "shard": {
            "index": index,
            "count": count,
            "targets": targets,
        },
        "targets": target_reports,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&document)
                .map_err(|error| format!("could not render M6 shard report: {error}"))?
        ),
    )
    .map_err(|error| format!("could not write {}: {error}", path.display()))?;
    Ok(path)
}

fn ensure_exact_shard_report_set(directory: &Path, count: usize) -> Result<(), String> {
    let mut observed = fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?
        .map(|entry| {
            entry
                .map_err(|error| format!("could not read shard directory entry: {error}"))
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| "M6 conformance shard filename is not UTF-8".to_owned())
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    observed.sort();

    let expected = (0..count).map(shard_report_name).collect::<Vec<_>>();
    if observed != expected {
        return Err(format!(
            "M6 conformance shard directory mismatch: expected={expected:?} observed={observed:?}"
        ));
    }
    Ok(())
}

fn read_validated_shard(
    path: &Path,
    index: usize,
    count: usize,
    expected_major: &str,
    expected_targets: &[(&str, &str)],
    manifest: &Value,
    environment: &Value,
) -> Result<Vec<Value>, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

    if document.get("report").and_then(Value::as_str) != Some("m6-conformance-shard")
        || document.get("schema_version").and_then(Value::as_u64) != Some(1)
    {
        return Err(format!(
            "{} is not an M6 conformance shard v1 report",
            path.display()
        ));
    }
    if document
        .get("postgresql_major_version")
        .and_then(Value::as_str)
        != Some(expected_major)
    {
        return Err(format!(
            "{} was produced for another PostgreSQL major",
            path.display()
        ));
    }
    if document.get("environment") != Some(environment) {
        return Err(format!(
            "{} environment disagrees with the canonical M6 merge job",
            path.display()
        ));
    }
    if document.pointer("/observation/execution_manifest") != Some(manifest) {
        return Err(format!(
            "{} execution manifest disagrees with the canonical M6 merge checkout",
            path.display()
        ));
    }
    if document.get("target_denominator").and_then(Value::as_u64)
        != u64::try_from(TARGETS.len()).ok()
    {
        return Err(format!(
            "{} target denominator is not {}",
            path.display(),
            TARGETS.len()
        ));
    }
    if document.get("passed").and_then(Value::as_bool) != Some(true) {
        return Err(format!("{} did not pass", path.display()));
    }
    let shard = document
        .get("shard")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{} has no shard object", path.display()))?;
    if shard.get("index").and_then(Value::as_u64) != u64::try_from(index).ok()
        || shard.get("count").and_then(Value::as_u64) != u64::try_from(count).ok()
    {
        return Err(format!(
            "{} declares the wrong M6 shard identity",
            path.display()
        ));
    }

    let expected_pairs = target_pairs(expected_targets);
    let declared_pairs = shard
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no shard target list", path.display()))?
        .iter()
        .map(|value| {
            Ok((
                required_string(value, "package", path)?,
                required_string(value, "target", path)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if declared_pairs != expected_pairs {
        return Err(format!(
            "{} target partition drift: expected={expected_pairs:?} observed={declared_pairs:?}",
            path.display()
        ));
    }

    let shard_violations = document
        .get("violations")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no violations array", path.display()))?;
    if !shard_violations.is_empty() {
        return Err(format!("{} reports shard violations", path.display()));
    }

    let reports = document
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no target report array", path.display()))?
        .clone();
    if reports.len() != expected_targets.len() {
        return Err(format!(
            "{} reports {} targets but its exact shard owns {}",
            path.display(),
            reports.len(),
            expected_targets.len()
        ));
    }

    for (report, (package, target)) in reports.iter().zip(expected_targets.iter()) {
        validate_target_report(report, package, target, path)?;
    }
    Ok(reports)
}

fn validate_target_report(
    report: &Value,
    package: &str,
    target: &str,
    path: &Path,
) -> Result<(), String> {
    if required_string(report, "package", path)? != package
        || required_string(report, "target", path)? != target
    {
        return Err(format!(
            "{} target report ownership drift for {package}/{target}",
            path.display()
        ));
    }
    let selector = report
        .get("selector")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} {package}/{target} has no selector", path.display()))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                format!(
                    "{} {package}/{target} selector is malformed",
                    path.display()
                )
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if selector != target_selector(target) {
        return Err(format!(
            "{} {package}/{target} selector drift",
            path.display()
        ));
    }
    if report.get("succeeded").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "{} {package}/{target} did not succeed",
            path.display()
        ));
    }
    if report.get("ignored").and_then(Value::as_u64) != Some(0) {
        return Err(format!(
            "{} {package}/{target} reported ignored tests",
            path.display()
        ));
    }
    let tests = report
        .get("tests")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{} {package}/{target} has no test count", path.display()))?;
    let results = report
        .get("results")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            format!(
                "{} {package}/{target} has no results object",
                path.display()
            )
        })?;
    if tests == 0 || usize::try_from(tests).ok() != Some(results.len()) {
        return Err(format!(
            "{} {package}/{target} test count does not match non-empty results",
            path.display()
        ));
    }
    if results
        .values()
        .any(|outcome| outcome.as_str() != Some("ok"))
    {
        return Err(format!(
            "{} {package}/{target} contains a non-ok result",
            path.display()
        ));
    }
    Ok(())
}

fn required_string(value: &Value, key: &str, path: &Path) -> Result<String, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{} has no string {key}", path.display()))
}

fn expected_matrix_major() -> Option<String> {
    env::var(suite::MATRIX)
        .ok()?
        .strip_prefix("postgres-")
        .map(str::to_owned)
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
    environment: &Value,
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);
    let document = json!({
        "report": "m6-conformance",
        "campaign": "M6 full component conformance, malformed/failure, lifecycle, and restart campaign",
        "postgresql_major_version": std::env::var(suite::MATRIX).ok().and_then(|value| value.strip_prefix("postgres-").map(str::to_owned)),
        "environment": environment,
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::collections::BTreeSet;

    use super::{TARGETS, partition_targets, target_pairs};

    #[test]
    fn two_way_partition_is_an_exact_non_empty_cover() {
        let shards = partition_targets(TARGETS, 2).expect("partition");
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].len(), 23);
        assert_eq!(shards[1].len(), 22);
        assert!(shards.iter().all(|shard| !shard.is_empty()));

        let observed = shards
            .iter()
            .flatten()
            .map(|(package, target)| ((*package).to_owned(), (*target).to_owned()))
            .collect::<BTreeSet<_>>();
        let expected = target_pairs(TARGETS).into_iter().collect::<BTreeSet<_>>();
        assert_eq!(observed, expected);
        assert_eq!(shards.iter().map(Vec::len).sum::<usize>(), TARGETS.len());
    }

    #[test]
    fn target_denominator_remains_forty_five() {
        assert_eq!(TARGETS.len(), 45);
    }
}
