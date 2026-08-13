//! The M5 `PostgreSQL` upgrade campaign runner.
//!
//! The campaign owes three reports: a direct upgrade to schema 3 from schema 1
//! and from schema 2, a runtime that supports schema 2 refusing a database that
//! has been upgraded to schema 3, and a rollback performed by restoring the
//! logical backup taken before the upgrade. Between them they cover five schema
//! paths, and the committed scope document is the denominator that says so.
//!
//! This is a command rather than a test for the reason the other two campaigns
//! are: every scenario it runs returns success without a database, because it
//! prints a skip line and returns. Under `cargo test` that is indistinguishable
//! from evidence. Here the fixtures are resolved first, and a campaign run
//! without them fails before any target starts.
//!
//! Passing tests are not sufficient either, and an upgrade campaign has a
//! sharper version of that problem than most: a report that ran against one
//! source schema and silently skipped the other would be green and would have
//! proved half of what it claims. So each report also writes a machine-readable
//! observation into a directory this runner creates empty, and the runner
//! reconciles the declared schema paths against what those observations
//! actually record — the source and target schema version, what the migration
//! did, what opening the database afterwards did, whether durable state was
//! compared, what the backup and restore did, and the version finally observed.
//! A path with no matching observation, or one whose observation disagrees with
//! the scope, fails the campaign.
//!
//! The scope document is `tests/fixtures/upgrade/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_upgrade_campaign.rs` reconciles it against the
//! accepted plan and gate, so this runner consumes a document that ordinary
//! review has already checked.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "upgrade-campaign.json";

/// The directory the reports write their observations into.
const OBSERVATIONS: &str = "upgrade-observations";

/// The variable that tells a report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_UPGRADE_OBSERVATIONS";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means every report ran on its fixture and every
/// declared schema path was observed doing what the support contract promises.
///
/// # Errors
///
/// Returns the first failure that prevents the campaign from producing a result
/// at all, such as an unreadable scope document or an unwritable report
/// directory.
pub fn run() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;

    let mut violations = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut violations);
    if !violations.is_empty() {
        let report = write_report(&root, &scope, &fixtures, &Runs::default(), &violations)?;
        return Ok(Campaign { violations, report });
    }

    let observations = prepare_observations(&root)?;
    let mut runs = Runs::default();
    for report in &scope.reports {
        eprintln!("==> {} {}", report.target, report.name);
        let run = suite::run_target(
            &root,
            &TargetCommand {
                package: &report.package,
                selector: &["--test".to_owned(), report.target.clone()],
                filters: &["--exact", &report.name],
                environment: &[(OBSERVATIONS_ENV, observations.display().to_string())],
                nocapture: true,
                release: false,
            },
        )?;

        if !run.succeeded {
            runs.failed_targets.push(format!(
                "{} {} exited unsuccessfully",
                report.package, report.target
            ));
        }
        runs.outcomes.insert(
            (report.target.clone(), report.name.clone()),
            run.results.get(&report.name).cloned(),
        );
    }

    runs.observations = read_observations(&observations)?;
    violations.extend(reconcile(&scope, &runs));

    let report = write_report(&root, &scope, &fixtures, &runs, &violations)?;
    Ok(Campaign { violations, report })
}

/// Reports which declared fixtures the environment supplies.
///
/// Every fixture this document declares is needed by something it runs, so an
/// absent one is always a violation. The campaign stops before running a single
/// target, because a report produced without its fixture is the forged pass the
/// campaign exists to rule out.
fn resolve_fixtures(scope: &Scope, violations: &mut Vec<String>) -> BTreeMap<String, bool> {
    let needed = scope
        .reports
        .iter()
        .map(|report| report.fixture.clone())
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
            "the {fixture} fixture is required by the upgrade campaign and is absent: set {}",
            variables.join(", ")
        ));
    }

    resolved
}

/// Creates an empty observation directory and returns it.
///
/// It is emptied rather than reused so a report retained by an earlier run can
/// never be counted as this run's evidence.
fn prepare_observations(root: &Path) -> Result<PathBuf, String> {
    let directory = suite::directory(root).join(OBSERVATIONS);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    Ok(directory)
}

/// Reads every observation the reports retained.
fn read_observations(directory: &Path) -> Result<BTreeMap<String, Value>, String> {
    let mut observations = BTreeMap::new();
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;

    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not read {}: {error}", directory.display()))?
            .path();
        let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        let document = serde_json::from_str(&source)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))?;
        observations.insert(name.to_owned(), document);
    }

    Ok(observations)
}

/// Reports everything the campaign required and did not observe.
fn reconcile(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = runs.failed_targets.clone();

    for report in &scope.reports {
        let key = (report.target.clone(), report.name.clone());
        match runs.outcomes.get(&key).and_then(Option::as_deref) {
            Some("ok") => {}
            Some(other) => violations.push(format!(
                "{}::{} reported {other}",
                report.target, report.name
            )),
            None => violations.push(format!(
                "{}::{} did not run in package {}",
                report.target, report.name, report.package
            )),
        }

        let Some(observation) = runs.observations.get(&report.id) else {
            violations.push(format!(
                "{} ran and retained no observation, so nothing says it did the work",
                report.id
            ));
            continue;
        };
        if observation.get("passed").and_then(Value::as_bool) != Some(true) {
            violations.push(format!(
                "{} retained an observation that did not pass",
                report.id
            ));
        }
        for violation in strings(observation, "violations") {
            violations.push(format!("{}: {violation}", report.id));
        }
    }

    for path in &scope.paths {
        violations.extend(reconcile_path(path, runs));
    }

    violations
}

/// Reports what one declared schema path required and its report did not show.
fn reconcile_path(path: &SchemaPath, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get(&path.report) else {
        // The absent observation is already reported against the report itself.
        return violations;
    };

    let observed = observation
        .get("paths")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| {
            entry.get("source_schema_version").and_then(Value::as_u64) == Some(path.source)
        });

    let Some(observed) = observed else {
        violations.push(format!(
            "the {} path is required and {} observed nothing starting from schema {}",
            path.id, path.report, path.source
        ));
        return violations;
    };

    for (field, expected) in [
        ("target_schema_version", json!(path.target)),
        ("observed_schema_version", json!(path.observed)),
        ("repository_open_result", json!(path.repository_open)),
        ("backup_restore_result", path.backup_restore.clone()),
        ("migration_result", json!("ok")),
        ("durable_state_verified", json!(true)),
        ("passed", json!(true)),
    ] {
        let actual = observed.get(field).unwrap_or(&Value::Null);
        if *actual != expected {
            violations.push(format!(
                "the {} path requires {field} to be {expected} and its report records {actual}",
                path.id
            ));
        }
    }

    for violation in strings(observed, "violations") {
        violations.push(format!("{}: {violation}", path.id));
    }

    violations
}

/// Reads a string array field, treating an absent one as empty.
fn strings(value: &Value, name: &str) -> Vec<String> {
    value
        .get(name)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

/// Writes the retained campaign report and returns its path.
fn write_report(
    root: &Path,
    scope: &Scope,
    fixtures: &BTreeMap<String, bool>,
    runs: &Runs,
    violations: &[String],
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let reports = scope
        .reports
        .iter()
        .map(|report| {
            json!({
                "id": report.id,
                "title": report.title,
                "owes": report.owes,
                "package": report.package,
                "target": report.target,
                "name": report.name,
                "fixture": report.fixture,
                "result": runs
                    .outcomes
                    .get(&(report.target.clone(), report.name.clone()))
                    .cloned()
                    .flatten(),
                "observation": runs.observations.get(&report.id),
            })
        })
        .collect::<Vec<_>>();

    let paths = scope
        .paths
        .iter()
        .map(|path| {
            json!({
                "id": path.id,
                "report": path.report,
                "source_schema_version": path.source,
                "target_schema_version": path.target,
                "repository_open_result": path.repository_open,
                "backup_restore_result": path.backup_restore,
                "observed_schema_version": path.observed,
                "expected": path.expected,
            })
        })
        .collect::<Vec<_>>();

    let document = json!({
        "report": "upgrade",
        "campaign": "M5 PostgreSQL upgrade",
        "scenarios": [
            "schema1_and_schema2_upgrade_directly_to_schema3",
            "schema2_runtime_rejects_schema3",
            "schema3_backup_restores_the_prior_schema",
        ],
        "environment": suite::environment(),
        "fixtures": fixtures,
        "sources": scope.sources,
        "schema2_runtime": scope.runtime,
        "reports": reports,
        "paths": paths,
        "related": scope.related,
        "violations": violations,
        "passed": violations.is_empty(),
        "notes": [
            "Every report is run on its own so its result is attributable, and \
             so a report that needs to build a previous revision of this crate \
             does not do so inside another report's invocation.",
            "A passing report is not sufficient on its own. Each one retains an \
             observation into a directory this runner creates empty, and the \
             runner requires every declared schema path to appear in one, \
             carrying the source and target schema version, the migration \
             result, the repository-open result, the durable-state comparison, \
             the backup and restore result where the path has one, and the \
             version finally observed.",
            "The prior schemas are not reconstructed. Each is installed by \
             running this crate's immutable migration set up to that version \
             and stopping, and the reports refuse to proceed against a fixture \
             that carries a structure a later schema introduced.",
            "The runtime the rejection report is about is built from the last \
             revision before schema 3 existed, because no build of this tree \
             can report a supported schema version of 2. The campaign needs the \
             repository's full history for that; a shallow clone fails it \
             rather than skipping it."
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

/// Everything the campaign's own invocations reported.
#[derive(Default)]
struct Runs {
    /// Target and test name to the outcome libtest reported.
    outcomes: BTreeMap<(String, String), Option<String>>,
    /// Invocations that exited unsuccessfully.
    failed_targets: Vec<String>,
    /// The observation each report retained, by report identifier.
    observations: BTreeMap<String, Value>,
}

/// The committed campaign scope document.
struct Scope {
    /// Fixture name to the environment variables it requires.
    fixtures: BTreeMap<String, Vec<String>>,
    /// The prior schemas the campaign builds fixtures at, as declared.
    sources: Value,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The schema paths the campaign must observe.
    paths: Vec<SchemaPath>,
    /// The revision the rejecting runtime is built from, as declared.
    runtime: Value,
    /// Evidence the campaign keeps and does not run, as declared.
    related: Value,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("upgrade")
            .join("campaign-scope.json");
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

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: suite::string(report, "id")?,
                title: suite::string(report, "title")?,
                owes: suite::string(report, "owes")?,
                package: suite::string(report, "package")?,
                target: suite::string(report, "target")?,
                name: suite::string(report, "name")?,
                fixture: suite::string(report, "fixture")?,
            });
        }

        let mut paths = Vec::new();
        for path in array(&document, "paths")? {
            paths.push(SchemaPath {
                id: suite::string(path, "id")?,
                report: suite::string(path, "report")?,
                source: number(path, "source_schema_version")?,
                target: number(path, "target_schema_version")?,
                observed: number(path, "observed_schema_version")?,
                repository_open: suite::string(path, "repository_open_result")?,
                backup_restore: path
                    .get("backup_restore_result")
                    .cloned()
                    .unwrap_or(Value::Null),
                expected: suite::string(path, "expected")?,
            });
        }

        Ok(Self {
            fixtures,
            sources: document.get("sources").cloned().unwrap_or(Value::Null),
            reports,
            paths,
            runtime: document
                .get("schema2_runtime")
                .cloned()
                .unwrap_or(Value::Null),
            related: document.get("related").cloned().unwrap_or(Value::Null),
        })
    }
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, String> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("the scope document has no {name}"))
}

/// Reads one required unsigned field.
fn number(value: &Value, name: &str) -> Result<u64, String> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a scope entry has no {name}"))
}

/// One report the campaign delivers.
struct Report {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The human-readable title of the report.
    title: String,
    /// The obligation the report discharges.
    owes: String,
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The fixture it needs in order to observe anything.
    fixture: String,
}

/// One schema path the campaign must observe.
struct SchemaPath {
    /// The identifier the report and the runner share.
    id: String,
    /// The report that covers the path.
    report: String,
    /// The schema version the path starts at.
    source: u64,
    /// The schema version the upgrade reaches.
    target: u64,
    /// The version the database the path ends on must record.
    observed: u64,
    /// What opening that database with the current runtime must do.
    repository_open: String,
    /// What the backup and restore must do, where the path has one.
    backup_restore: Value,
    /// What the accepted contract requires the path to show.
    expected: String,
}
