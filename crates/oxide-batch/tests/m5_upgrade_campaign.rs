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
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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

// ---------------------------------------------------------------------
// Historical revision binding. The rejection report builds a real schema-2
// runtime from a pinned commit rather than reconstructing one, and three
// independent places name that commit: the scope document, the runner's
// shared support module, and the execution contract. If any two disagree,
// "the revision the workflow can build," "the revision the campaign says it
// built," and "the revision review accepted" are three different claims
// wearing one hash.
// ---------------------------------------------------------------------

#[test]
fn the_historical_revision_is_bound_across_scope_runner_and_contract() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    let runner_source = read_document("crates/oxide-batch/tests/upgrade/mod.rs")?;
    let runner_revision = runner_source
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("pub const SCHEMA2_RUNTIME_REVISION: &str = \"")?
                .strip_suffix("\";")
        })
        .ok_or_else(|| {
            Failure(
                "crates/oxide-batch/tests/upgrade/mod.rs declares no \
                 SCHEMA2_RUNTIME_REVISION constant in the expected shape"
                    .to_owned(),
            )
        })?;
    assert_eq!(
        scope.runtime_revision, runner_revision,
        "the scope document names {} as the schema-2 runtime revision and the runner's \
         SCHEMA2_RUNTIME_REVISION constant names {runner_revision}; the two must agree or the \
         campaign's denominator and its implementation are about different runtimes",
        scope.runtime_revision,
    );

    let contract: Value = serde_json::from_str(&read_document(
        "tests/fixtures/upgrade/execution-contract.json",
    )?)?;
    let contract_revision = contract
        .pointer("/historical_schema2_runtime/revision")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Failure(
                "execution-contract.json names no historical_schema2_runtime.revision".to_owned(),
            )
        })?;
    assert_eq!(
        scope.runtime_revision, contract_revision,
        "the scope document and execution-contract.json must name the same schema-2 runtime \
         revision",
    );

    let checkout_depth = contract
        .pointer("/checkout/fetch_depth")
        .and_then(Value::as_u64);
    assert_eq!(
        checkout_depth,
        Some(0),
        "the historical revision can only be resolved from full repository history, and the \
         execution contract must declare a full-history checkout",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Semantic closure. The producer records the object identity of every path
// listed in tests/fixtures/upgrade/campaign-semantics.json from inside its
// own checkout, and the offline evidence verifier requires those identities
// to still hold. Neither restates the list here; this proves the closure
// actually covers what the campaign runs and excludes what it must not.
// ---------------------------------------------------------------------

/// Every other M5 campaign's own reconciliation/contract test, plus this
/// campaign's own. None of them is a scenario this campaign runs, and their
/// inclusion in the closure would either create a retention-time
/// self-reference or bind this campaign's evidence to another campaign's
/// fixtures.
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
fn the_semantic_closure_covers_what_the_campaign_runs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let paths = closure_paths()?;

    let mut required_targets = BTreeSet::new();
    for report in &scope.reports {
        required_targets.insert(("oxide-batch".to_owned(), report.target.clone()));
    }
    assert!(
        !required_targets.is_empty(),
        "the campaign scope named no required target, so this test checks nothing",
    );

    for (package, target) in &required_targets {
        let relative = format!("crates/{package}/tests/{target}.rs");
        assert!(
            covered(&paths, &relative),
            "{relative} backs a report in package {package}, and is not covered by any path in \
             the campaign's semantic closure",
        );
    }
    assert!(
        covered(&paths, "crates/oxide-batch/tests/upgrade"),
        "the shared upgrade test-support module, which pins the historical schema-2 runtime \
         revision, is not covered by the campaign's semantic closure",
    );

    for governance in GOVERNANCE_TARGETS {
        assert!(
            !required_targets.contains(&("oxide-batch".to_owned(), (*governance).to_owned())),
            "{governance} is a governance test, not an upgrade scenario, and must not be part of \
             the campaign's required-target set",
        );
    }

    for excluded in [
        "docs/project/m5-campaign-evidence.md",
        "tests/fixtures/soak/campaign-scope.json",
        "tests/fixtures/soak/campaign-semantics.json",
        "tests/fixtures/conformance/accepted-scope.json",
        "tests/fixtures/conformance/campaign-semantics.json",
    ] {
        assert!(
            !paths.iter().any(|path| path == excluded),
            "{excluded} must not be in the upgrade closure: including it would either create a \
             retention-time self-reference or bind upgrade evidence to another campaign's \
             fixtures",
        );
    }

    for required in [
        "crates/oxide-batch/src",
        "crates/oxide-batch/migrations",
        "tests/fixtures/upgrade/campaign-scope.json",
        "tests/fixtures/upgrade/schema-1/seed.sql",
        "tests/fixtures/upgrade/schema-2/seed.sql",
        "tests/fixtures/upgrade/schema-2-runtime/probe.rs",
        "xtask/src/upgrade.rs",
        "xtask/src/evidence.rs",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".github/workflows/m5-upgrade.yml",
        "tests/fixtures/upgrade/execution-contract.json",
        "tests/fixtures/upgrade/run-ci-campaign.sh",
        "tests/fixtures/upgrade/verify-ci-contract.sh",
    ] {
        assert!(
            paths.iter().any(|path| path == required),
            "{required} is not in the campaign's semantic closure, so a change to it would leave \
             retained evidence looking valid when it is evidence of something else",
        );
    }

    assert!(
        !paths.iter().any(|path| path == ".github/workflows/ci.yml"),
        "ci.yml is unrelated to the dedicated upgrade campaign and must not invalidate its \
         evidence",
    );

    for path in &paths {
        assert!(
            workspace_root().join(path).exists(),
            "{path} is declared as campaign semantics and does not exist, so the producer cannot \
             record its object identity",
        );
    }
    Ok(())
}

/// Returns every path the campaign's semantic closure declares.
fn closure_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let closure: Value = serde_json::from_str(&read_document(
        "tests/fixtures/upgrade/campaign-semantics.json",
    )?)?;
    Ok(closure
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the closure declares no categories".to_owned()))?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>())
}

/// Reports whether a repository-relative path is covered by the closure:
/// named exactly, or nested under a closure path that names a directory.
fn covered(paths: &[String], candidate: &str) -> bool {
    paths
        .iter()
        .any(|path| path == candidate || candidate.starts_with(&format!("{path}/")))
}

// ---------------------------------------------------------------------
// Contract-check exactness. `verify-ci-contract.sh` binds the dedicated
// workflow and `run-ci-campaign.sh` by exact git blob identity, and
// separately cross-checks the historical revision across the contract, the
// scope document, and the runner's pinning constant. These tests drive the
// real script against an isolated sandbox copy of those files, so a mutation
// proves the checker's actual behaviour, and never touches the repository
// working tree.
// ---------------------------------------------------------------------

#[test]
fn contract_check_passes_on_the_canonical_workflow_and_script() -> Result<(), Box<dyn Error>> {
    assert!(run_upgrade_contract_check(|_sandbox| Ok(()))?);
    Ok(())
}

#[test]
fn contract_check_fails_on_an_added_trigger() -> Result<(), Box<dyn Error>> {
    let passed = run_upgrade_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-upgrade.yml"),
            "  workflow_dispatch:\n",
            "  schedule:\n    - cron: '0 0 * * *'\n",
        )
    })?;
    assert!(
        !passed,
        "an added trigger must fail the contract check even though every expected trigger is \
         still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_widened_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_upgrade_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-upgrade.yml"),
            "postgres: [\"15\", \"18\"]\n",
            "        include:\n          - postgres: \"16\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional matrix execution point must fail even though the literal \
         postgres: [\"15\", \"18\"] declaration is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_narrowed_checkout() -> Result<(), Box<dyn Error>> {
    let passed = run_upgrade_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-upgrade.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace("fetch-depth: 0", "fetch-depth: 1");
        assert_ne!(
            source, mutated,
            "the fetch-depth literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a narrowed checkout must fail even though every other literal is unchanged: a shallow \
         clone cannot resolve the historical schema-2 runtime revision",
    );
    Ok(())
}

#[test]
fn contract_check_fails_when_the_scope_revision_drifts_from_the_runner()
-> Result<(), Box<dyn Error>> {
    let passed = run_upgrade_contract_check(|sandbox| {
        let scope = sandbox.join("tests/fixtures/upgrade/campaign-scope.json");
        let source = fs::read_to_string(&scope)?;
        let mutated = source.replace("397a38bcada93d961dbb2ca3d9960311a3fb4395", &"0".repeat(40));
        assert_ne!(
            source, mutated,
            "the pinned revision was not found to mutate"
        );
        fs::write(&scope, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a scope document naming a different schema-2 runtime revision than the runner's pinning \
         constant must fail the contract check",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_harmless_comment_byte() -> Result<(), Box<dyn Error>> {
    let passed = run_upgrade_contract_check(|sandbox| {
        append_line(
            &sandbox.join(".github/workflows/m5-upgrade.yml"),
            "# harmless comment",
        )
    })?;
    assert!(
        !passed,
        "exact git blob identity, not heuristic literal parsing, is the retained-evidence \
         boundary: even a harmless trailing comment must fail",
    );
    Ok(())
}

/// Copies the real workflow, script, contract, scope document, and the
/// runner's revision-pinning module into an isolated sandbox, applies
/// `mutate` to that sandbox, then runs the real `verify-ci-contract.sh`
/// against the (possibly mutated) copy and reports whether it exited zero.
fn run_upgrade_contract_check(
    mutate: impl FnOnce(&std::path::Path) -> Result<(), Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    let root = workspace_root();
    let sandbox = Sandbox::new("upgrade-contract-check")?;

    let workflow_dir = sandbox.path().join(".github/workflows");
    fs::create_dir_all(&workflow_dir)?;
    fs::copy(
        root.join(".github/workflows/m5-upgrade.yml"),
        workflow_dir.join("m5-upgrade.yml"),
    )?;

    let fixture_dir = sandbox.path().join("tests/fixtures/upgrade");
    fs::create_dir_all(&fixture_dir)?;
    for name in [
        "execution-contract.json",
        "run-ci-campaign.sh",
        "verify-ci-contract.sh",
        "campaign-scope.json",
    ] {
        fs::copy(
            root.join("tests/fixtures/upgrade").join(name),
            fixture_dir.join(name),
        )?;
    }

    let runner_dir = sandbox.path().join("crates/oxide-batch/tests/upgrade");
    fs::create_dir_all(&runner_dir)?;
    fs::copy(
        root.join("crates/oxide-batch/tests/upgrade/mod.rs"),
        runner_dir.join("mod.rs"),
    )?;

    let checker = fixture_dir.join("verify-ci-contract.sh");
    let mut permissions = fs::metadata(&checker)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&checker, permissions)?;

    mutate(sandbox.path())?;

    let status = Command::new(&checker)
        .arg(".github/workflows/m5-upgrade.yml")
        .current_dir(sandbox.path())
        .status()?;
    Ok(status.success())
}

/// Inserts `insertion` immediately after the first occurrence of `anchor`.
fn insert_after(
    path: &std::path::Path,
    anchor: &str,
    insertion: &str,
) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    let position = contents.find(anchor).ok_or_else(|| {
        Box::new(Failure(format!(
            "no {anchor:?} anchor found in {}",
            path.display()
        ))) as Box<dyn Error>
    })?;
    let insert_at = position + anchor.len();
    let mut mutated = String::with_capacity(contents.len() + insertion.len());
    mutated.push_str(&contents[..insert_at]);
    mutated.push_str(insertion);
    mutated.push_str(&contents[insert_at..]);
    fs::write(path, mutated)?;
    Ok(())
}

/// Appends one line to a file.
fn append_line(path: &std::path::Path, line: &str) -> Result<(), Box<dyn Error>> {
    let mut contents = fs::read_to_string(path)?;
    contents.push('\n');
    contents.push_str(line);
    contents.push('\n');
    fs::write(path, contents)?;
    Ok(())
}

/// A uniquely named temporary directory, removed when it goes out of scope
/// regardless of how the test exits.
struct Sandbox(PathBuf);

impl Sandbox {
    fn new(label: &str) -> Result<Self, Box<dyn Error>> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "oxide-batch-{label}-{}-{unique}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
