//! The M5 conformance campaign runner.
//!
//! The M5 design gate names
//! `full_embedded_conformance_suite_passes_on_the_accepted_scope` as the
//! evidence the conformance campaign owes. This command is that scenario. It
//! runs every test target in the workspace, attributes each result to the
//! target that produced it, and reconciles the accepted-scope document in
//! `tests/fixtures/conformance/accepted-scope.json` against what actually ran.
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

    let mut violations = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut violations);
    if !violations.is_empty() {
        let report = write_report(&root, &scope, &fixtures, &Suite::default(), &violations)?;
        return Ok(Campaign { violations, report });
    }

    let targets = suite_targets()?;
    let suite = run_suite(&root, &targets)?;
    violations.extend(reconcile(&scope, &suite));

    let report = write_report(&root, &scope, &fixtures, &suite, &violations)?;
    Ok(Campaign { violations, report })
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

/// Enumerates every test target in the workspace with its package and kind.
///
/// The list comes from the workspace metadata rather than from a build,
/// because a build is not needed to know what exists and building the whole
/// workspace only to rebuild it per package wastes the larger part of the run.
fn suite_targets() -> Result<Vec<Target>, String> {
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
        "environment": suite::environment(),
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
