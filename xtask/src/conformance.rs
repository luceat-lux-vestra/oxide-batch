//! The M5 conformance campaign runner.
//!
//! The M5 design gate names
//! `full_embedded_conformance_suite_passes_on_the_accepted_scope` as the
//! evidence the conformance campaign owes. This command is that scenario, and
//! it distinguishes two things that earlier revisions of this file
//! conflated:
//!
//! - **the row-proof denominator**: the `42` accepted M0-M4 rows and the
//!   `133` scenarios the accepted-scope document assigns to them. Every
//!   assigned scenario must report `ok`, and every row must be proved by at
//!   least one.
//! - **the execution envelope**: the `30` unique `(package, target)` test
//!   binaries [`required_targets`] derives from those `133` assignments. Each
//!   selected target is run in full — not filtered down to only its assigned
//!   scenarios — so a test inside a selected target that the accepted scope
//!   never named still runs, and its failure still fails the campaign,
//!   exactly as an assigned scenario's failure would. Only a test outside
//!   every selected target is unable to affect the campaign's result.
//!   Workspace documentation tests are a third, separate obligation, run
//!   regardless of the envelope.
//!
//! The target set is derived from the scope document rather than enumerated
//! from `cargo metadata` directly, and that used to be the other way around:
//! every workspace test target that carried the `test` kind was selected —
//! not merely the `30` the accepted scope's assignments touch — and any of
//! them exiting unsuccessfully failed the campaign. That made the campaign's
//! pass/fail gate depend on targets the accepted scope never named at all —
//! including the other M5 campaigns' own reconciliation tests, several of
//! which read fixtures and the shared evidence record no accepted scenario's
//! semantic closure could name without creating a retention-time
//! self-reference (the record is rewritten with a report's own provenance
//! after the report is produced). `required_targets` is the fix: the
//! execution envelope is the set of targets the row-proof denominator
//! actually touches, so an entire workspace test target outside that
//! envelope can change freely and the campaign's result is unaffected by it,
//! and general Rust CI is still what runs and fails on it. A test *inside* a
//! selected target that is not itself an assigned scenario is not covered by
//! this narrowing, and never was: the campaign has always run selected
//! targets in full.
//!
//! It is a command rather than a test for two reasons, and both are about not
//! forging a pass:
//!
//! - a test process observes only its own target, while several scenario names
//!   exist in more than one target, so attribution needs the runner;
//! - a database-backed scenario returns success without a database, because
//!   it prints a skip line and returns. Under `cargo test` that is
//!   indistinguishable from evidence. Here the fixture is checked first, and a
//!   campaign run without it fails before the suite starts.
//!
//! The reconciliation of the scope document against the ledger is not repeated
//! here. It runs in `crates/oxide-batch/tests/m5_conformance_campaign.rs`, so
//! ordinary review catches ledger drift, and this runner consumes the document
//! that test validates.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "conformance-campaign.json";

/// Prefix for one CI shard's intermediate evidence.
const SHARD_REPORT_PREFIX: &str = "conformance-shard-";

/// CI shard index, present only for partial execution.
const SHARD_INDEX_ENV: &str = "OXIDEBATCH_CONFORMANCE_SHARD_INDEX";

/// CI shard count, present for both partial execution and canonical merge.
const SHARD_COUNT_ENV: &str = "OXIDEBATCH_CONFORMANCE_SHARD_COUNT";

/// Directory containing all shard reports for canonical merge.
const SHARD_MERGE_DIR_ENV: &str = "OXIDEBATCH_CONFORMANCE_MERGE_DIR";

/// The declared semantic closure of the conformance campaign.
const SEMANTICS: &str = "tests/fixtures/conformance/campaign-semantics.json";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means every accepted row's scenarios ran, on their
/// required fixtures, and passed.
///
/// # Errors
///
/// Returns the first failure that prevents the campaign from producing a
/// result at all, such as an unreadable scope document or a suite that could
/// not be built.
pub fn run() -> Result<Campaign, String> {
    let shard_index = env::var(SHARD_INDEX_ENV).ok();
    let shard_count = env::var(SHARD_COUNT_ENV).ok();
    let merge_dir = env::var(SHARD_MERGE_DIR_ENV).ok();

    match (shard_index, shard_count, merge_dir) {
        (None, None, None) => run_full(),
        (Some(index), Some(count), None) => run_shard(&index, &count),
        (None, Some(count), Some(directory)) => merge_shards(&count, Path::new(&directory)),
        _ => Err(
            "conformance CI mode is ambiguous: shard execution requires shard index+count only, \
             canonical merge requires shard count+merge directory only"
                .to_owned(),
        ),
    }
}

/// Runs the original single-process campaign.
///
/// Kept as the default local/manual mode so CI sharding does not silently
/// change developer invocation semantics.
fn run_full() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;
    let manifest = execution_manifest(&root)?;

    let mut violations = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut violations);
    if !violations.is_empty() {
        let empty = Suite::default();
        let environment = suite::environment();
        let report = write_report(
            &root,
            &scope,
            &fixtures,
            &empty,
            &violations,
            &manifest,
            &environment,
        )?;
        return Ok(Campaign { violations, report });
    }

    let targets = suite_targets(&scope)?;
    let suite = run_suite(&root, &targets, true)?;
    violations.extend(reconcile(&scope, &suite));
    let environment = suite::environment();

    let report = write_report(
        &root,
        &scope,
        &fixtures,
        &suite,
        &violations,
        &manifest,
        &environment,
    )?;
    Ok(Campaign { violations, report })
}

/// Runs exactly one deterministic target shard and retains intermediate evidence.
///
/// Scenario reconciliation is deliberately deferred until merge, because any
/// one shard owns only part of the accepted execution envelope. Target-process
/// failures, fixture failures, and the documentation-test obligation owned by
/// shard zero still fail the shard immediately.
fn run_shard(index: &str, count: &str) -> Result<Campaign, String> {
    let index = parse_shard_number("index", index)?;
    let count = parse_shard_number("count", count)?;
    if count < 2 {
        return Err("conformance shard count must be at least 2".to_owned());
    }
    if index >= count {
        return Err(format!(
            "conformance shard index {index} is outside shard count {count}"
        ));
    }

    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;
    let manifest = execution_manifest(&root)?;
    let environment = suite::environment();
    let targets = suite_targets(&scope)?;
    let partitions = partition_targets(&targets, count)?;
    let selected = &partitions[index];

    let mut preflight = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut preflight);
    let suite = if preflight.is_empty() {
        run_suite(&root, selected, index == 0)?
    } else {
        Suite::default()
    };

    let report = write_shard_report(
        &root,
        ShardReportInput {
            index,
            count,
            selected,
            fixtures: &fixtures,
            suite: &suite,
            preflight_violations: &preflight,
            manifest: &manifest,
            environment: &environment,
        },
    )?;

    let mut violations = preflight;
    violations.extend(suite.failed_targets.iter().cloned());
    if suite.documentation == Some(false)
        && !violations
            .iter()
            .any(|violation| violation == "the workspace documentation tests failed")
    {
        violations.push("the workspace documentation tests failed".to_owned());
    }
    violations.sort();
    violations.dedup();

    Ok(Campaign { violations, report })
}

/// Merges every shard into the one canonical report retained by the campaign.
///
/// The merge recomputes the target partition from the accepted scope and
/// rejects partial evidence that is missing, duplicated, from another
/// `PostgreSQL` major/tree/environment, or claims a target outside its exact
/// shard. Only after that exact-cover proof does ordinary 133-scenario
/// reconciliation run.
struct ShardMergeState {
    suite: Suite,
    fixtures: Option<BTreeMap<String, bool>>,
    environment: Option<Value>,
    violations: Vec<String>,
}

fn merge_shards(count: &str, directory: &Path) -> Result<Campaign, String> {
    let count = parse_shard_number("count", count)?;
    if count < 2 {
        return Err("conformance shard count must be at least 2".to_owned());
    }

    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;
    let manifest = execution_manifest(&root)?;
    let targets = suite_targets(&scope)?;
    let partitions = partition_targets(&targets, count)?;
    let expected_major = expected_matrix_major().ok_or_else(|| {
        "canonical conformance merge requires OXIDEBATCH_CAMPAIGN_MATRIX".to_owned()
    })?;

    ensure_exact_shard_report_set(directory, count)?;

    let mut state = ShardMergeState {
        suite: Suite::default(),
        fixtures: None,
        environment: None,
        violations: Vec::new(),
    };

    for (index, expected_targets) in partitions.iter().enumerate() {
        let path = directory.join(shard_report_name(index));
        let (shard, expected_pairs) = read_validated_shard(
            &path,
            index,
            count,
            &expected_major,
            expected_targets,
            &manifest,
        )?;
        merge_shard_payload(&path, index, &expected_pairs, shard, &mut state)?;
    }

    if state.suite.targets != targets.len() {
        return Err(format!(
            "conformance shard merge covered {} targets, expected {}",
            state.suite.targets,
            targets.len()
        ));
    }
    if state.suite.documentation.is_none() {
        return Err("conformance shard merge has no documentation-test proof".to_owned());
    }

    state.violations.extend(reconcile(&scope, &state.suite));
    state.violations.sort();
    state.violations.dedup();

    let fixtures = state
        .fixtures
        .ok_or_else(|| "conformance shard merge has no fixture evidence".to_owned())?;
    let environment = state
        .environment
        .ok_or_else(|| "conformance shard merge has no environment evidence".to_owned())?;
    let report = write_report(
        &root,
        &scope,
        &fixtures,
        &state.suite,
        &state.violations,
        &manifest,
        &environment,
    )?;

    Ok(Campaign {
        violations: state.violations,
        report,
    })
}

fn ensure_exact_shard_report_set(directory: &Path, count: usize) -> Result<(), String> {
    let observed_files = fs::read_dir(directory)
        .map_err(|error| {
            format!(
                "could not read shard directory {}: {error}",
                directory.display()
            )
        })?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(SHARD_REPORT_PREFIX))
        .collect::<BTreeSet<_>>();
    let expected_files = (0..count).map(shard_report_name).collect::<BTreeSet<_>>();
    if observed_files != expected_files {
        return Err(format!(
            "conformance shard report set is not exact: expected={expected_files:?} observed={observed_files:?}"
        ));
    }
    Ok(())
}

fn read_validated_shard(
    path: &Path,
    index: usize,
    count: usize,
    expected_major: &str,
    expected_targets: &[Target],
    manifest: &Value,
) -> Result<(ShardReport, Vec<(String, String)>), String> {
    let shard = read_shard_report(path)?;

    if shard.index != index || shard.count != count {
        return Err(format!(
            "{} declares shard {}/{} but merge expected {index}/{count}",
            path.display(),
            shard.index,
            shard.count
        ));
    }
    if shard.major != expected_major {
        return Err(format!(
            "{} was produced for PostgreSQL {}, expected {}",
            path.display(),
            shard.major,
            expected_major
        ));
    }

    let expected_pairs = target_pairs(expected_targets);
    if shard.targets != expected_pairs {
        return Err(format!(
            "{} target partition drift: expected={expected_pairs:?} observed={:?}",
            path.display(),
            shard.targets
        ));
    }
    if shard.target_count != expected_targets.len() {
        return Err(format!(
            "{} ran {} targets but its exact shard owns {}",
            path.display(),
            shard.target_count,
            expected_targets.len()
        ));
    }
    if shard.manifest != *manifest {
        return Err(format!(
            "{} execution manifest does not match the canonical merge checkout",
            path.display()
        ));
    }

    Ok((shard, expected_pairs))
}

fn merge_shard_payload(
    path: &Path,
    index: usize,
    expected_pairs: &[(String, String)],
    shard: ShardReport,
    state: &mut ShardMergeState,
) -> Result<(), String> {
    let ShardReport {
        target_count,
        results,
        failed_targets,
        documentation,
        fixtures,
        environment,
        preflight_violations,
        ..
    } = shard;

    match &state.environment {
        Some(common) if common != &environment => {
            return Err(format!(
                "{} environment disagrees with the other conformance shards",
                path.display()
            ));
        }
        None => state.environment = Some(environment),
        _ => {}
    }
    match &state.fixtures {
        Some(common) if common != &fixtures => {
            return Err(format!(
                "{} fixture resolution disagrees with the other conformance shards",
                path.display()
            ));
        }
        None => state.fixtures = Some(fixtures),
        _ => {}
    }

    if index == 0 {
        let documentation = documentation.ok_or_else(|| {
            format!(
                "{} shard zero omitted the workspace documentation-test proof",
                path.display()
            )
        })?;
        state.suite.documentation = Some(documentation);
    } else if documentation.is_some() {
        return Err(format!(
            "{} non-zero shard duplicated the workspace documentation-test proof",
            path.display()
        ));
    }

    for ((package, target, name), outcome) in results {
        if !expected_pairs.contains(&(package.clone(), target.clone())) {
            return Err(format!(
                "{} reported a result for target {package}/{target} outside its exact shard",
                path.display()
            ));
        }
        let key = (package, target, name);
        if state.suite.results.insert(key.clone(), outcome).is_some() {
            return Err(format!(
                "{} duplicated test result {}::{}::{}",
                path.display(),
                key.0,
                key.1,
                key.2
            ));
        }
    }

    state.suite.failed_targets.extend(failed_targets);
    state.violations.extend(preflight_violations);
    state.suite.targets += target_count;
    Ok(())
}

fn parse_shard_number(label: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid conformance shard {label} {value:?}: {error}"))
}

/// Deterministically partitions the already-sorted execution envelope.
///
/// Round-robin over the canonical package/target ordering makes the
/// partition independent of runtime timing and guarantees stable ownership.
fn partition_targets(targets: &[Target], count: usize) -> Result<Vec<Vec<Target>>, String> {
    if count == 0 {
        return Err("conformance shard count cannot be zero".to_owned());
    }
    if targets.len() < count {
        return Err(format!(
            "conformance shard count {count} exceeds {} selected targets",
            targets.len()
        ));
    }

    let mut shards = vec![Vec::new(); count];
    for (position, target) in targets.iter().cloned().enumerate() {
        shards[position % count].push(target);
    }

    if shards.iter().any(Vec::is_empty) {
        return Err("conformance shard partition produced an empty shard".to_owned());
    }
    let flattened = shards
        .iter()
        .flatten()
        .map(|target| (target.package.clone(), target.name.clone()))
        .collect::<Vec<_>>();
    let expected = target_pairs(targets);
    let observed = flattened.iter().cloned().collect::<BTreeSet<_>>();
    let expected_set = expected.iter().cloned().collect::<BTreeSet<_>>();
    if flattened.len() != expected.len()
        || observed.len() != flattened.len()
        || observed != expected_set
    {
        return Err("conformance shard partition is not an exact one-to-one cover".to_owned());
    }

    Ok(shards)
}

fn target_pairs(targets: &[Target]) -> Vec<(String, String)> {
    targets
        .iter()
        .map(|target| (target.package.clone(), target.name.clone()))
        .collect()
}

fn shard_report_name(index: usize) -> String {
    format!("{SHARD_REPORT_PREFIX}{index}.json")
}

struct ShardReportInput<'a> {
    index: usize,
    count: usize,
    selected: &'a [Target],
    fixtures: &'a BTreeMap<String, bool>,
    suite: &'a Suite,
    preflight_violations: &'a [String],
    manifest: &'a Value,
    environment: &'a Value,
}

fn write_shard_report(root: &Path, input: ShardReportInput<'_>) -> Result<PathBuf, String> {
    let ShardReportInput {
        index,
        count,
        selected,
        fixtures,
        suite,
        preflight_violations,
        manifest,
        environment,
    } = input;
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(shard_report_name(index));

    let results = suite
        .results
        .iter()
        .map(|((package, target, name), outcome)| {
            json!({
                "package": package,
                "target": target,
                "name": name,
                "result": outcome,
            })
        })
        .collect::<Vec<_>>();
    let targets = selected
        .iter()
        .map(|target| json!({"package": target.package, "target": target.name}))
        .collect::<Vec<_>>();
    let passed = preflight_violations.is_empty()
        && suite.failed_targets.is_empty()
        && suite.documentation != Some(false);

    let document = json!({
        "report": "conformance-shard",
        "schema_version": 1,
        "postgresql_major_version": expected_matrix_major(),
        "environment": environment,
        "observation": {
            "execution_manifest": manifest,
        },
        "fixtures": fixtures,
        "shard": {
            "index": index,
            "count": count,
            "targets": targets,
        },
        "suite": {
            "targets": suite.targets,
            "results": results,
            "failed_targets": suite.failed_targets,
            "documentation_tests_passed": suite.documentation,
        },
        "violations": preflight_violations,
        "passed": passed,
    });

    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&document)
                .map_err(|error| format!("could not render shard report: {error}"))?
        ),
    )
    .map_err(|error| format!("could not write {}: {error}", path.display()))?;

    Ok(path)
}

struct ShardReport {
    index: usize,
    count: usize,
    major: String,
    targets: Vec<(String, String)>,
    target_count: usize,
    results: BTreeMap<(String, String, String), String>,
    failed_targets: Vec<String>,
    documentation: Option<bool>,
    fixtures: BTreeMap<String, bool>,
    environment: Value,
    manifest: Value,
    preflight_violations: Vec<String>,
}

fn read_shard_report(path: &Path) -> Result<ShardReport, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

    if document.get("report").and_then(Value::as_str) != Some("conformance-shard")
        || document.get("schema_version").and_then(Value::as_u64) != Some(1)
    {
        return Err(format!(
            "{} is not a conformance shard v1 report",
            path.display()
        ));
    }

    let shard = document
        .get("shard")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{} has no shard object", path.display()))?;
    let index = shard
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("{} has no valid shard index", path.display()))?;
    let count = shard
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("{} has no valid shard count", path.display()))?;
    let targets = shard
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no shard target list", path.display()))?
        .iter()
        .map(|target| {
            Ok((
                required_string(target, "package", path)?,
                required_string(target, "target", path)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let suite = document
        .get("suite")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{} has no suite object", path.display()))?;
    let target_count = suite
        .get("targets")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("{} has no valid suite target count", path.display()))?;
    let mut results = BTreeMap::new();
    for result in suite
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no suite result list", path.display()))?
    {
        let key = (
            required_string(result, "package", path)?,
            required_string(result, "target", path)?,
            required_string(result, "name", path)?,
        );
        let outcome = required_string(result, "result", path)?;
        if results.insert(key.clone(), outcome).is_some() {
            return Err(format!(
                "{} duplicates test result {}::{}::{}",
                path.display(),
                key.0,
                key.1,
                key.2
            ));
        }
    }
    let failed_targets = string_array(suite.get("failed_targets"), "failed_targets", path)?;
    let documentation = match suite.get("documentation_tests_passed") {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::Null) | None => None,
        _ => {
            return Err(format!(
                "{} has invalid documentation_tests_passed",
                path.display()
            ));
        }
    };

    let fixtures = document
        .get("fixtures")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{} has no fixture object", path.display()))?
        .iter()
        .map(|(name, value)| {
            value
                .as_bool()
                .map(|present| (name.clone(), present))
                .ok_or_else(|| format!("{} fixture {name} is not boolean", path.display()))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let environment = document
        .get("environment")
        .cloned()
        .ok_or_else(|| format!("{} has no environment", path.display()))?;
    let manifest = document
        .pointer("/observation/execution_manifest")
        .cloned()
        .ok_or_else(|| format!("{} has no execution manifest", path.display()))?;
    let major = document
        .get("postgresql_major_version")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{} has no PostgreSQL major", path.display()))?
        .to_owned();
    let preflight_violations = string_array(document.get("violations"), "violations", path)?;

    Ok(ShardReport {
        index,
        count,
        major,
        targets,
        target_count,
        results,
        failed_targets,
        documentation,
        fixtures,
        environment,
        manifest,
        preflight_violations,
    })
}

fn required_string(value: &Value, key: &str, path: &Path) -> Result<String, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{} has no string {key}", path.display()))
}

fn string_array(value: Option<&Value>, label: &str, path: &Path) -> Result<Vec<String>, String> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} has no {label} array", path.display()))?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{} {label} contains a non-string", path.display()))
        })
        .collect()
}

/// Records the object identity of the campaign's closure, as executed.
///
/// Taken here, by the producer itself running inside its own checkout, rather
/// than reconstructed later: this process is the campaign, so the tree it can
/// see is by definition the tree that ran. In CI that is the pull-request
/// merge commit the workflow checked out — an ephemeral object no later clone
/// can resolve — so a verifier that tried to re-derive these identities from
/// a commit name would depend on something GitHub throws away. Matches the
/// pattern the performance, soak, and cancellation producers already use.
fn execution_manifest(root: &Path) -> Result<Value, String> {
    let commit = git(root, &["rev-parse", "HEAD"])
        .ok_or_else(|| "the campaign is not running inside a git tree".to_owned())?;
    let mut objects = serde_json::Map::new();
    for path in semantics_paths(root)? {
        let object = git(root, &["rev-parse", &format!("HEAD:{path}")]).ok_or_else(|| {
            format!("{path} is declared as campaign semantics and is not present")
        })?;
        objects.insert(path, Value::String(object));
    }
    Ok(json!({
        "execution_commit": commit,
        "execution_commit_note": "The tree this run actually executed against, read from the \
                                  checkout the campaign is running in. In CI this is the \
                                  pull-request merge commit rather than the branch head, and it \
                                  is the authority: the objects below are its objects.",
        "tree_clean": git(root, &["status", "--porcelain"]).map(|status| status.is_empty()),
        "objects": Value::Object(objects),
    }))
}

/// Reads the canonical closure of what the campaign executes.
///
/// Read from `tests/fixtures/conformance/campaign-semantics.json` rather than
/// listed here, because the verifier reads the same document: a closure kept
/// in two places is one that will disagree.
fn semantics_paths(root: &Path) -> Result<Vec<String>, String> {
    let path = root.join(SEMANTICS);
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;
    let categories = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| "the semantics document declares no categories".to_owned())?;
    let mut paths = categories
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return Err("the semantics document declares no paths".to_owned());
    }
    Ok(paths)
}

/// Reads the `PostgreSQL` major the campaign was configured to run at.
///
/// A runner cannot see the database version through a fixture, which is a
/// connection string it never opens, so the campaign matrix variable is the
/// recorded major — the same source of truth `suite::environment`'s own
/// `matrix` field already reads.
fn expected_matrix_major() -> Option<String> {
    let matrix = env::var(suite::MATRIX).ok()?;
    matrix.strip_prefix("postgres-").map(str::to_owned)
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

/// Reports which declared fixtures the environment supplies.
///
/// A fixture no scenario needs is not required to be present, so an absent
/// optional fixture is recorded rather than reported.
fn resolve_fixtures(scope: &Scope, violations: &mut Vec<String>) -> BTreeMap<String, bool> {
    let needed = scope
        .rows
        .values()
        .flatten()
        .map(|scenario| scenario.fixture.clone())
        .collect::<BTreeSet<_>>();

    let mut resolved = BTreeMap::new();
    for (fixture, variables) in &scope.fixtures {
        let present = variables
            .iter()
            .all(|variable| env::var(variable).is_ok_and(|value| !value.is_empty()));
        resolved.insert(fixture.clone(), present);

        if present || !needed.contains(fixture) {
            continue;
        }
        violations.push(format!(
            "the {fixture} fixture is required by the accepted scope and is \
             absent: set {}",
            variables.join(", ")
        ));
    }

    resolved
}

/// Returns the package/target pairs the accepted scope assigns at least one
/// scenario to.
///
/// This is the campaign's execution envelope: each pair names one test
/// binary that is run in full, not filtered to the scenarios that put it
/// here. It is derived from the same document the ledger reconciliation test
/// validates rather than stated again here, for the reason every other
/// derived value in this file is derived rather than restated: a second list
/// is a list that will drift.
fn required_targets(scope: &Scope) -> BTreeSet<(String, String)> {
    scope
        .rows
        .values()
        .flatten()
        .map(|scenario| (scenario.package.clone(), scenario.target.clone()))
        .collect()
}

/// Resolves the accepted scope's required targets against the workspace
/// metadata, so each carries the cargo selector its kind requires.
///
/// The list comes from the workspace metadata rather than from a build,
/// because a build is not needed to know what exists and building the whole
/// workspace only to rebuild it per package wastes the larger part of the run.
/// Metadata is still consulted, rather than trusting the scope document's
/// target names outright, because a scope entry naming a target that no
/// longer exists (or now has a different kind) must be caught: `reconcile`
/// reports it as a scenario that never ran, exactly as it would for a
/// deleted test function.
fn suite_targets(scope: &Scope) -> Result<Vec<Target>, String> {
    let required = required_targets(scope);
    let metadata = suite::metadata()?;
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| "cargo metadata returned no packages".to_owned())?;

    let mut targets = Vec::new();
    for package in packages {
        let (Some(package_name), Some(declared)) = (
            package.get("name").and_then(Value::as_str),
            package.get("targets").and_then(Value::as_array),
        ) else {
            continue;
        };

        for target in declared {
            if target.get("test").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let (Some(name), Some(kinds)) = (
                target.get("name").and_then(Value::as_str),
                target.get("kind").and_then(Value::as_array),
            ) else {
                continue;
            };
            if !required.contains(&(package_name.to_owned(), name.to_owned())) {
                continue;
            }
            let Some(selector) = selector(name, kinds) else {
                continue;
            };

            targets.push(Target {
                package: package_name.to_owned(),
                name: name.to_owned(),
                selector,
            });
        }
    }

    targets.sort_by(|left, right| (&left.package, &left.name).cmp(&(&right.package, &right.name)));
    Ok(targets)
}

/// Returns the cargo arguments that select one test target, if it has tests.
fn selector(name: &str, kinds: &[Value]) -> Option<Vec<String>> {
    let kinds = kinds
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();

    if kinds.contains("test") {
        return Some(vec!["--test".to_owned(), name.to_owned()]);
    }
    if kinds.contains("lib") || kinds.contains("rlib") {
        return Some(vec!["--lib".to_owned()]);
    }
    if kinds.contains("bin") {
        return Some(vec!["--bin".to_owned(), name.to_owned()]);
    }
    None
}

/// Runs every test target and records each result by the target that produced
/// it.
///
/// Targets run through cargo, one at a time, with a single test thread. Cargo
/// rather than the compiled executable, because a test can depend on the
/// environment cargo supplies — the compile-fail suite needs its manifest
/// directory, and running its binary directly fails for a reason that has
/// nothing to do with the facade. One at a time, because that is what
/// attributes a result: several scenario names exist in more than one target.
fn run_suite(root: &Path, targets: &[Target], run_documentation: bool) -> Result<Suite, String> {
    let mut suite = Suite::default();

    for target in targets {
        eprintln!("==> {} {}", target.package, target.name);

        let run = suite::run_target(
            root,
            &TargetCommand {
                package: &target.package,
                selector: &target.selector,
                filters: &[],
                environment: &[],
                nocapture: false,
                release: false,
            },
        )?;

        for (name, outcome) in run.results {
            suite
                .results
                .insert((target.package.clone(), target.name.clone(), name), outcome);
        }
        if !run.succeeded {
            suite.failed_targets.push(format!(
                "{} {} exited unsuccessfully",
                target.package, target.name
            ));
        }
        suite.targets += 1;
    }

    if run_documentation {
        let documentation = run_documentation_tests(root)?;
        suite.documentation = Some(documentation);
        if !documentation {
            suite
                .failed_targets
                .push("the workspace documentation tests failed".to_owned());
        }
    }

    Ok(suite)
}

/// Runs the workspace documentation tests.
///
/// They belong to the suite the campaign claims passes, and they report no
/// per-example result that could be attributed to a ledger row, so they are
/// recorded as one pass or failure.
fn run_documentation_tests(root: &Path) -> Result<bool, String> {
    eprintln!("==> workspace documentation tests");

    let status = Command::new("cargo")
        .current_dir(root)
        .args(["test", "--workspace", "--all-features", "--doc"])
        .status()
        .map_err(|error| format!("could not run the documentation tests: {error}"))?;

    Ok(status.success())
}

/// Reports every accepted scenario the suite did not prove.
fn reconcile(scope: &Scope, suite: &Suite) -> Vec<String> {
    let mut violations = suite.failed_targets.clone();

    for (row, scenarios) in &scope.rows {
        for scenario in scenarios {
            let key = (
                scenario.package.clone(),
                scenario.target.clone(),
                scenario.name.clone(),
            );
            match suite.results.get(&key).map(String::as_str) {
                Some("ok") => {}
                Some(other) => violations.push(format!(
                    "{row}: {}::{} reported {other}",
                    scenario.target, scenario.name
                )),
                None => violations.push(format!(
                    "{row}: {}::{} did not run in package {}",
                    scenario.target, scenario.name, scenario.package
                )),
            }
        }
    }

    violations
}

/// Writes the retained campaign report and returns its path.
fn write_report(
    root: &Path,
    scope: &Scope,
    fixtures: &BTreeMap<String, bool>,
    suite: &Suite,
    violations: &[String],
    manifest: &Value,
    environment: &Value,
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let rows = scope
        .rows
        .iter()
        .map(|(row, scenarios)| {
            json!({
                "row": row,
                "scenarios": scenarios
                    .iter()
                    .map(|scenario| json!({
                        "package": scenario.package,
                        "target": scenario.target,
                        "name": scenario.name,
                        "class": scenario.class,
                        "fixture": scenario.fixture,
                        "result": suite.results.get(&(
                            scenario.package.clone(),
                            scenario.target.clone(),
                            scenario.name.clone(),
                        )),
                    }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let mut outcomes: BTreeMap<&str, usize> = BTreeMap::new();
    for outcome in suite.results.values() {
        *outcomes.entry(outcome.as_str()).or_default() += 1;
    }

    let document = json!({
        "report": "conformance",
        "campaign": "full embedded conformance on the accepted M0-M4 scope",
        "scenario": "full_embedded_conformance_suite_passes_on_the_accepted_scope",
        "postgresql_major_version": expected_matrix_major(),
        "environment": environment,
        "observation": {
            "execution_manifest": manifest,
        },
        "fixtures": fixtures,
        "suite": {
            "targets": suite.targets,
            "tests": suite.results.len(),
            "outcomes": outcomes,
            "documentation_tests_passed": suite.documentation,
        },
        "rows": rows,
        "violations": violations,
        "passed": violations.is_empty(),
        "notes": [
            "Documentation tests run as one target and report no per-example \
             result that could be attributed to a ledger row, so they are \
             recorded as a single pass or failure.",
            "A result of `ignored` is not a pass. The campaign requires every \
             named scenario to report `ok` on a host that supplies its \
             fixture."
        ],
    });

    fs::write(
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

/// One test target the suite runs.
#[derive(Clone)]
struct Target {
    /// The workspace package that owns it.
    package: String,
    /// The target name.
    name: String,
    /// The cargo arguments that select it.
    selector: Vec<String>,
}

/// Everything the suite reported.
#[derive(Default)]
struct Suite {
    /// Package, target, and test path to the outcome libtest reported.
    results: BTreeMap<(String, String, String), String>,
    /// Targets that exited unsuccessfully.
    failed_targets: Vec<String>,
    /// The number of targets that ran.
    targets: usize,
    /// Whether this execution owned the workspace documentation-test obligation.
    ///
    /// Full/canonical runs always carry Some; non-owner CI shards carry None.
    documentation: Option<bool>,
}

/// The committed accepted-scope document.
struct Scope {
    /// Row identifier to the scenarios assigned to it.
    rows: BTreeMap<String, Vec<Scenario>>,
    /// Fixture name to the environment variables it requires.
    fixtures: BTreeMap<String, Vec<String>>,
}

/// One executable scenario the campaign runs.
struct Scenario {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test path libtest reports, including any module prefix.
    name: String,
    /// The ledger evidence class the scenario contributes.
    class: String,
    /// The fixture the scenario needs in order to observe anything.
    fixture: String,
}

impl Scope {
    /// Reads the accepted-scope document from the workspace.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("conformance")
            .join("accepted-scope.json");
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        let document: Value = serde_json::from_str(&source)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

        let mut fixtures = BTreeMap::new();
        for (fixture, variables) in document
            .get("fixtures")
            .and_then(Value::as_object)
            .ok_or_else(|| "the scope document declares no fixtures".to_owned())?
        {
            let variables = variables
                .as_array()
                .ok_or_else(|| format!("fixture {fixture} declares no variable list"))?
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            fixtures.insert(fixture.clone(), variables);
        }

        let mut rows = BTreeMap::new();
        for row in document
            .get("rows")
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope document declares no rows".to_owned())?
        {
            let id = suite::string(row, "id")?;
            let mut scenarios = Vec::new();
            for scenario in row
                .get("scenarios")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("{id} declares no scenario"))?
            {
                scenarios.push(Scenario {
                    package: suite::string(scenario, "package")?,
                    target: suite::string(scenario, "target")?,
                    name: suite::string(scenario, "name")?,
                    class: suite::string(scenario, "class")?,
                    fixture: suite::string(scenario, "fixture")?,
                });
            }
            rows.insert(id, scenarios);
        }

        Ok(Self { rows, fixtures })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::collections::BTreeSet;

    use super::{Scope, partition_targets, required_targets, suite_targets};
    use crate::suite;

    /// Every other M5 campaign's own reconciliation/contract test, plus this
    /// campaign's own. None of them is named by any accepted-scope scenario,
    /// and several of them read a shared evidence document or another
    /// campaign's fixtures — semantic inputs that cannot be added to this
    /// campaign's closure without creating the retention-time self-reference
    /// this narrowing exists to avoid. Regression coverage for the exact
    /// counterexample found in review: `m5_campaign_record` reads
    /// `docs/project/m5-campaign-evidence.md`, which this campaign's own
    /// retention step rewrites with the report's own provenance after the
    /// report is produced.
    const GOVERNANCE_TARGETS: &[&str] = &[
        "m5_campaign_record",
        "m5_cancellation_campaign",
        "m5_conformance_campaign",
        "m5_crash_restore_campaign",
        "m5_performance_campaign",
        "m5_resource_bounds_campaign",
        "m5_security_campaign",
        "m5_soak_campaign",
        "m5_upgrade_campaign",
    ];

    #[test]
    fn required_targets_excludes_every_m5_governance_test() {
        let root = suite::workspace_root().expect("workspace root");
        let scope = Scope::read(&root).expect("accepted-scope.json");
        let required = required_targets(&scope);

        assert!(
            !required.is_empty(),
            "the accepted scope named no required target, so this test checks nothing",
        );

        for governance in GOVERNANCE_TARGETS {
            assert!(
                !required.contains(&("oxide-batch".to_owned(), (*governance).to_owned())),
                "{governance} is a governance test, not an accepted-scope scenario, and must not \
                 be part of the campaign's execution envelope",
            );
        }
    }

    #[test]
    fn two_way_partition_is_an_exact_non_empty_cover() {
        let root = suite::workspace_root().expect("workspace root");
        let scope = Scope::read(&root).expect("accepted-scope.json");
        let targets = suite_targets(&scope).expect("suite_targets");
        let shards = partition_targets(&targets, 2).expect("partition");

        assert_eq!(shards.len(), 2);
        assert!(shards.iter().all(|shard| !shard.is_empty()));

        let expected = targets
            .iter()
            .map(|target| (target.package.clone(), target.name.clone()))
            .collect::<BTreeSet<_>>();
        let observed = shards
            .iter()
            .flatten()
            .map(|target| (target.package.clone(), target.name.clone()))
            .collect::<BTreeSet<_>>();

        assert_eq!(observed, expected);
        assert_eq!(shards.iter().map(Vec::len).sum::<usize>(), targets.len());
    }

    #[test]
    fn suite_targets_resolves_to_exactly_the_required_set() {
        let root = suite::workspace_root().expect("workspace root");
        let scope = Scope::read(&root).expect("accepted-scope.json");
        let required = required_targets(&scope);

        let resolved = suite_targets(&scope).expect("suite_targets");
        let resolved_set = resolved
            .iter()
            .map(|target| (target.package.clone(), target.name.clone()))
            .collect::<BTreeSet<_>>();

        assert_eq!(
            resolved_set, required,
            "suite_targets must resolve to exactly the accepted scope's required set: no target \
             cargo metadata reports may be silently dropped or added",
        );

        for governance in GOVERNANCE_TARGETS {
            assert!(
                !resolved_set.contains(&("oxide-batch".to_owned(), (*governance).to_owned())),
                "{governance} was resolved as a target the campaign runs, which is exactly the \
                 defect this narrowing exists to prevent",
            );
        }
    }
}
