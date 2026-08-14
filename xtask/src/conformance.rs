//! The M5 conformance campaign runner.
//!
//! The M5 design gate names
//! `full_embedded_conformance_suite_passes_on_the_accepted_scope` as the
//! evidence the conformance campaign owes. This command is that scenario. It
//! runs exactly the test targets the accepted-scope document assigns a
//! scenario to — no more, no fewer — attributes each result to the target
//! that produced it, and reconciles the document against what actually ran.
//!
//! The target set is derived from the scope document rather than enumerated
//! from `cargo metadata` directly, and that used to be the other way around:
//! every workspace test target that carried the `test` kind ran, and any of
//! them exiting unsuccessfully failed the campaign. That made the campaign's
//! pass/fail gate depend on tests the accepted scope never named — including
//! the other M5 campaigns' own reconciliation tests, several of which read
//! fixtures and the shared evidence record no accepted scenario's semantic
//! closure could name without creating a retention-time self-reference (the
//! record is rewritten with a report's own provenance after the report is
//! produced). `required_targets` is the fix: the campaign's execution surface
//! is exactly the denominator it claims to prove, so a workspace test outside
//! that denominator can change and the campaign's result is unaffected by it,
//! and general Rust CI is still what runs and fails on it.
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
    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;
    let manifest = execution_manifest(&root)?;

    let mut violations = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut violations);
    if !violations.is_empty() {
        let report = write_report(
            &root,
            &scope,
            &fixtures,
            &Suite::default(),
            &violations,
            &manifest,
        )?;
        return Ok(Campaign { violations, report });
    }

    let targets = suite_targets(&scope)?;
    let suite = run_suite(&root, &targets)?;
    violations.extend(reconcile(&scope, &suite));

    let report = write_report(&root, &scope, &fixtures, &suite, &violations, &manifest)?;
    Ok(Campaign { violations, report })
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
/// This is the campaign's execution surface. It is derived from the same
/// document the ledger reconciliation test validates rather than stated
/// again here, for the reason every other derived value in this file is
/// derived rather than restated: a second list is a list that will drift.
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
fn run_suite(root: &Path, targets: &[Target]) -> Result<Suite, String> {
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

    suite.documentation = run_documentation_tests(root)?;
    if !suite.documentation {
        suite
            .failed_targets
            .push("the workspace documentation tests failed".to_owned());
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
        "environment": suite::environment(),
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
    /// Whether the workspace documentation tests passed.
    documentation: bool,
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
    use std::collections::BTreeSet;

    use super::{Scope, required_targets, suite_targets};
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
                 be part of the campaign's execution surface",
            );
        }
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
