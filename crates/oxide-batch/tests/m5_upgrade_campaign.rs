//! Scope reconciliation for the M5 `PostgreSQL` upgrade campaign.
//!
//! The campaign has the same two halves the conformance and crash-and-restore
//! campaigns have, split for the same reason:
//!
//! - **what the campaign owes, and which scenario proves each part of it.**
//!   That is a reconciliation between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Every scenario it runs needs a real
//!   database and returns green without one, because it skips. That half is
//!   `cargo xtask upgrade`, which requires the fixtures, runs the targets,
//!   requires each declared schema path to have been observed, and writes the
//!   retained report.
//!
//! The scope document is `tests/fixtures/upgrade/campaign-scope.json` at the
//! workspace root. Both halves read it, so the schema paths, the reports, and
//! the historical fixtures are stated once.
//!
//! Two things this file checks are specific to an upgrade campaign and worth
//! naming. The first is that the historical fixtures are historical: the scope
//! may only claim a source schema the immutable migration set actually
//! installs, and the seed for it may not mention a column a later schema
//! introduced. The second is that the runtime the rejection report rejects with
//! is pinned to a revision whose supported schema version really is `2`, read
//! out of that revision rather than asserted about it.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

/// The reports the performance plan's upgrade row requires.
const REQUIRED_REPORTS: &[&str] = &["schema-upgrade", "schema-rejection", "upgrade-rollback"];

/// The scenarios the M5 design gate names for this campaign.
const NAMED_SCENARIOS: &[&str] = &[
    "schema1_and_schema2_upgrade_directly_to_schema3",
    "schema2_runtime_rejects_schema3",
    "schema3_backup_restores_the_prior_schema",
];

/// The schema paths the M5 support contract promises, as the runner names them.
///
/// The list is here rather than only in the scope document on purpose. The
/// document says what the runner requires; this says what review accepted, and
/// a path can only leave the campaign by changing both.
const REQUIRED_PATHS: &[&str] = &[
    "upgrade-from-1",
    "upgrade-from-2",
    "reject-schema-3",
    "rollback-to-1",
    "rollback-to-2",
];

/// The schema version this crate installs, and the only upgrade target.
const TARGET_SCHEMA_VERSION: u64 = 3;

/// The regression test the campaign keeps and does not stand in for.
const KEPT_REGRESSION: &str = "newer_schema_is_rejected_without_guessing_compatibility";

#[test]
fn campaign_scope_matches_the_accepted_upgrade_obligations() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_REPORTS.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign delivers exactly the reports the performance plan's upgrade row requires",
    );
    assert_eq!(
        scope
            .paths
            .iter()
            .map(|path| path.id.as_str())
            .collect::<Vec<_>>(),
        REQUIRED_PATHS,
        "the campaign covers every schema path the M5 support contract promises, in order",
    );
    for path in &scope.paths {
        assert!(
            scope.reports.iter().any(|report| report.id == path.report),
            "{} names {}, which is not a report this campaign delivers",
            path.id,
            path.report,
        );
        assert_eq!(
            path.target, TARGET_SCHEMA_VERSION,
            "{}: the M5 preview installs schema {TARGET_SCHEMA_VERSION} and nothing else is an \
             upgrade target",
            path.id,
        );
    }

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    for scenario in NAMED_SCENARIOS {
        assert!(
            gate.contains(scenario),
            "the design gate must still name {scenario} for the evidence campaigns",
        );
        assert!(
            scope.reports.iter().any(|report| report.name == *scenario),
            "no report in the campaign produces {scenario}",
        );
    }

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Upgrade |"))
        .ok_or_else(|| Failure("the performance plan has no upgrade row".to_owned()))?;
    for owed in [
        "Schema 1 and 2 to schema 3 direct upgrade",
        "newer-schema rejection",
        "restore-based rollback",
    ] {
        assert!(
            row.contains(owed),
            "the performance plan's upgrade row no longer requires {owed}, so the campaign's \
             denominator and the accepted plan disagree",
        );
    }
    for report in &scope.reports {
        assert!(
            row.contains(&report.owes) || gate.contains(&report.owes),
            "{} claims to discharge {}, which neither the plan nor the gate requires",
            report.id,
            report.owes,
        );
    }

    Ok(())
}

#[test]
fn every_declared_scenario_resolves_to_a_test_this_workspace_declares() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    for report in &scope.reports {
        assert!(
            scope.fixtures.contains(&report.fixture),
            "{} needs an undeclared fixture: {}",
            report.name,
            report.fixture,
        );
        let source = workspace_root()
            .join("crates")
            .join(&report.package)
            .join("tests")
            .join(format!("{}.rs", report.target));
        let declared = fs::read_to_string(&source)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.display())))?;
        assert!(
            declared.contains(&format!("fn {}(", report.name)),
            "{} declares no test named {}",
            source.display(),
            report.name,
        );
    }

    // The campaign does not run this one, and it must not be able to disappear
    // while the campaign claims to cover the invariant end to end.
    let regression = workspace_root()
        .join("crates")
        .join("oxide-batch")
        .join("tests")
        .join("postgres_repository.rs");
    assert!(
        fs::read_to_string(&regression)?.contains(&format!("fn {KEPT_REGRESSION}(")),
        "the campaign keeps {KEPT_REGRESSION} as the lower-level regression test and it is gone",
    );

    Ok(())
}

#[test]
fn every_declared_source_schema_is_one_the_migration_set_installs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let migrations = workspace_root()
        .join("crates")
        .join("oxide-batch")
        .join("migrations");

    for source in &scope.sources {
        assert!(
            source.schema_version < TARGET_SCHEMA_VERSION,
            "schema {} is not a source the M5 preview upgrades from",
            source.schema_version,
        );
        let installer = workspace_root().join(&source.installed_by);
        assert!(
            installer.starts_with(&migrations),
            "a source schema must be installed by this crate's immutable migration set, and {} \
             is not in it",
            source.installed_by,
        );
        let sql = fs::read_to_string(&installer)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.installed_by)))?;
        assert!(
            sql.contains(&format!("SET version = {}", source.schema_version))
                || sql.contains(&format!("VALUES (true, {}", source.schema_version)),
            "{} does not install schema {}, so the fixture built from it is not that schema",
            source.installed_by,
            source.schema_version,
        );

        let seed = workspace_root().join(&source.seed);
        let seeded = fs::read_to_string(&seed)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.seed)))?;
        for later in later_columns(source.schema_version) {
            assert!(
                !seeded.contains(later),
                "{} names {later}, which schema {} did not have, so it is not a fixture of that \
                 schema",
                source.seed,
                source.schema_version,
            );
        }
    }

    let declared = scope
        .sources
        .iter()
        .map(|source| source.schema_version)
        .collect::<BTreeSet<_>>();
    let covered = scope
        .paths
        .iter()
        .map(|path| path.source)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        declared, covered,
        "every source schema the campaign declares must be a schema some path exercises, and \
         every path must start from one the campaign declares",
    );

    Ok(())
}

#[test]
fn the_rejecting_runtime_is_a_revision_that_supports_schema_2() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    assert_eq!(
        scope.runtime_supported, 2,
        "the rejection report is about a runtime that supports schema 2",
    );

    let probe = workspace_root().join(&scope.runtime_probe);
    assert!(
        probe.is_file(),
        "the campaign names a probe program it does not have: {}",
        scope.runtime_probe,
    );

    // The supported version is read out of the pinned revision rather than
    // asserted about it. A revision that turned out to support something else
    // would make the report's central claim false while every other check here
    // still passed.
    let source = Command::new("git")
        .current_dir(workspace_root())
        .arg("show")
        .arg(format!(
            "{}:crates/oxide-batch/src/repository/postgres.rs",
            scope.runtime_revision
        ))
        .output()?;
    if !source.status.success() {
        eprintln!(
            "skipped: this repository does not have {}, which a shallow clone produces",
            scope.runtime_revision
        );
        return Ok(());
    }
    let source = String::from_utf8_lossy(&source.stdout);
    assert!(
        source.contains(&format!(
            "const SUPPORTED_SCHEMA_VERSION: u32 = {};",
            scope.runtime_supported
        )),
        "revision {} does not support schema {}, so the rejection report would be about a \
         different runtime than the one it names",
        scope.runtime_revision,
        scope.runtime_supported,
    );

    Ok(())
}

/// Returns the columns a schema later than `version` introduced.
///
/// A seed that mentions one is not a fixture of the schema it claims to be. The
/// database would reject it, but the reconciliation says so in review, where the
/// mistake is cheaper to find than in a campaign run.
fn later_columns(version: u64) -> &'static [&'static str] {
    match version {
        1 => &[
            "step_logical_id",
            "read_retry_count",
            "fault_state_payload",
            "ob_flow_decision",
            "owner_token",
            "hold_actor",
            "ob_step_partition",
        ],
        2 => &[
            "owner_token",
            "hold_actor",
            "ob_step_partition",
            "ob_operator_request",
        ],
        _ => &[],
    }
}

/// The committed campaign scope document.
struct Scope {
    /// The fixture names the document declares.
    fixtures: BTreeSet<String>,
    /// The prior schemas the campaign builds fixtures at.
    sources: Vec<Source>,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The schema paths the campaign must observe.
    paths: Vec<Path>,
    /// The revision the rejecting runtime is built from.
    runtime_revision: String,
    /// The schema version that revision supports.
    runtime_supported: u64,
    /// The probe program that revision runs.
    runtime_probe: String,
}

/// One prior schema the campaign builds a fixture at.
struct Source {
    /// The schema version the fixture is at.
    schema_version: u64,
    /// The migration that installs it.
    installed_by: String,
    /// The committed seed script for it.
    seed: String,
}

/// One report the campaign delivers.
struct Report {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The obligation the report discharges, as the plan or gate words it.
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
struct Path {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The report that covers the path.
    report: String,
    /// The schema version the path starts at.
    source: u64,
    /// The schema version the upgrade reaches.
    target: u64,
}

impl Scope {
    /// Reads and parses the committed scope document.
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("upgrade")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let fixtures = document
            .get("fixtures")
            .and_then(Value::as_object)
            .map(|object| object.keys().cloned().collect())
            .ok_or_else(|| Failure("the scope document declares no fixtures".to_owned()))?;

        let mut sources = Vec::new();
        for source in array(&document, "sources")? {
            sources.push(Source {
                schema_version: number(source, "schema_version")?,
                installed_by: field(source, "installed_by")?,
                seed: field(source, "seed")?,
            });
        }

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: field(report, "id")?,
                owes: field(report, "owes")?,
                package: field(report, "package")?,
                target: field(report, "target")?,
                name: field(report, "name")?,
                fixture: field(report, "fixture")?,
            });
        }

        let mut paths = Vec::new();
        for path in array(&document, "paths")? {
            paths.push(Path {
                id: field(path, "id")?,
                report: field(path, "report")?,
                source: number(path, "source_schema_version")?,
                target: number(path, "target_schema_version")?,
            });
        }

        let runtime = document
            .get("schema2_runtime")
            .ok_or_else(|| Failure("the scope document names no schema-2 runtime".to_owned()))?;

        Ok(Self {
            fixtures,
            sources,
            reports,
            paths,
            runtime_revision: field(runtime, "revision")?,
            runtime_supported: number(runtime, "supported_schema_version")?,
            runtime_probe: field(runtime, "probe")?,
        })
    }
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Reads one canonical document from the workspace.
fn read_document(relative: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(relative))?)
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document.get(name).and_then(Value::as_array).ok_or_else(|| {
        Box::new(Failure(format!("the scope document has no {name}"))) as Box<dyn Error>
    })
}

/// Reads one required string field.
fn field(value: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// Reads one required unsigned field.
fn number(value: &Value, name: &str) -> Result<u64, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// A reconciliation input the campaign could not read.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
