//! The M5 crash and restore campaign runner.
//!
//! The campaign owes three reports: process kill at each phase of the commit
//! protocol, P-013 restart after many chunks, and a logical backup restore. It
//! also owes a denominator that includes the durable write protocols M2-M4
//! already proved, which it reuses rather than rewrites.
//!
//! This is a command rather than a test for the reason the conformance campaign
//! is: every scenario it runs returns success without a database, because it
//! prints a skip line and returns. Under `cargo test` that is indistinguishable
//! from evidence. Here the fixtures are resolved first, and a campaign run
//! without them fails before any target starts.
//!
//! Passing tests are not sufficient either. A scenario could report `ok` having
//! skipped, so each report also writes a machine-readable observation into a
//! directory this runner creates empty and inspects afterwards. A phase with no
//! observation, an observation that records no `SIGKILL`, or an observation
//! carrying a violation fails the campaign. That is what makes a forged pass
//! impossible rather than merely unlikely.
//!
//! The scope document is `tests/fixtures/crash-restore/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_crash_restore_campaign.rs` reconciles it
//! against the accepted plan and gate, so this runner consumes a document that
//! ordinary review has already checked.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "crash-restore-campaign.json";

/// The directory the scenarios write their observations into.
const OBSERVATIONS: &str = "crash-restore-observations";

/// The variable that tells a scenario where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_CRASH_RESTORE_OBSERVATIONS";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means every report ran on its fixture, every
/// declared commit phase killed a live process and recovered without a forged
/// status, and every reused M2-M4 scenario still passes.
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
    for scenario in scope.scenarios() {
        eprintln!("==> {} {}", scenario.target, scenario.name);
        let run = suite::run_target(
            &root,
            &TargetCommand {
                package: &scenario.package,
                selector: &["--test".to_owned(), scenario.target.clone()],
                filters: &["--exact", &scenario.name],
                environment: &[(OBSERVATIONS_ENV, observations.display().to_string())],
                nocapture: true,
                release: false,
            },
        )?;

        let outcome = run.results.get(&scenario.name).cloned();
        if !run.succeeded {
            runs.failed_targets.push(format!(
                "{} {} exited unsuccessfully",
                scenario.package, scenario.target
            ));
        }
        runs.outcomes
            .insert((scenario.target.clone(), scenario.name.clone()), outcome);
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
        .scenarios()
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
            "the {fixture} fixture is required by the crash and restore campaign and is \
             absent: set {}",
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

/// Reads every observation the scenarios retained.
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

    for scenario in scope.scenarios() {
        let key = (scenario.target.clone(), scenario.name.clone());
        match runs.outcomes.get(&key).and_then(Option::as_deref) {
            Some("ok") => {}
            Some(other) => violations.push(format!(
                "{}::{} reported {other}",
                scenario.target, scenario.name
            )),
            None => violations.push(format!(
                "{}::{} did not run in package {}",
                scenario.target, scenario.name, scenario.package
            )),
        }
    }

    for report in &scope.reports {
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
        for violation in observation
            .get("violations")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            violations.push(format!("{}: {violation}", report.id));
        }
    }

    for phase in &scope.phases {
        let Some(observation) = runs.observations.get(&phase.report) else {
            continue;
        };
        let observed = observation
            .get("phases")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|entry| entry.get("phase").and_then(Value::as_str) == Some(phase.id.as_str()));

        let Some(observed) = observed else {
            violations.push(format!(
                "the {} phase is required and its report observed nothing for it",
                phase.id
            ));
            continue;
        };
        let signal = observed
            .get("termination")
            .and_then(|termination| termination.get("signal"))
            .and_then(Value::as_str);
        if signal != Some(phase.termination.as_str()) {
            violations.push(format!(
                "the {} phase must end in {} and the report records {signal:?}",
                phase.id, phase.termination
            ));
        }
        if observed.get("passed").and_then(Value::as_bool) != Some(true) {
            violations.push(format!("the {} phase did not pass", phase.id));
        }
    }

    violations
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
                "scenario": scenario_json(&report.scenario, runs),
                "observation": runs.observations.get(&report.id),
            })
        })
        .collect::<Vec<_>>();

    let phases = scope
        .phases
        .iter()
        .map(|phase| {
            json!({
                "id": phase.id,
                "protocol": phase.protocol,
                "report": phase.report,
                "termination": phase.termination,
                "expected": phase.expected,
            })
        })
        .collect::<Vec<_>>();

    let reused = scope
        .reused
        .iter()
        .map(|scenario| {
            let mut entry = scenario_json(scenario, runs);
            if let Some(map) = entry.as_object_mut() {
                map.insert("protocol".to_owned(), json!(scenario.protocol));
                map.insert("milestone".to_owned(), json!(scenario.milestone));
            }
            entry
        })
        .collect::<Vec<_>>();

    let document = json!({
        "report": "crash-restore",
        "campaign": "M5 crash and restore",
        "scenarios": [
            "process_kill_at_each_commit_phase_recovers_without_a_forged_status",
            "restart_after_many_chunks_matches_an_uninterrupted_run",
            "logical_backup_restores_the_durable_state_and_the_job_restarts_on_it",
        ],
        "environment": suite::environment(),
        "fixtures": fixtures,
        "reports": reports,
        "phases": phases,
        "reused": reused,
        "violations": violations,
        "passed": violations.is_empty(),
        "notes": [
            "Every scenario is run on its own so its result is attributable: \
             several campaign scenarios re-execute their own test binary, and a \
             shared invocation could not say which one produced a line.",
            "A passing scenario is not sufficient on its own. Each report also \
             retains an observation into a directory this runner creates empty, \
             and a missing observation, a phase that did not end in SIGKILL, or \
             a recorded violation fails the campaign.",
            "The reused M2-M4 scenarios are run rather than cited. The campaign \
             does not rewrite the evidence those milestones delivered, and it \
             does not take their passing on trust either."
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

/// Renders one scenario and the outcome the campaign observed for it.
fn scenario_json(scenario: &Scenario, runs: &Runs) -> Value {
    json!({
        "package": scenario.package,
        "target": scenario.target,
        "name": scenario.name,
        "fixture": scenario.fixture,
        "result": runs
            .outcomes
            .get(&(scenario.target.clone(), scenario.name.clone()))
            .cloned()
            .flatten(),
    })
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
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The commit-protocol phases the campaign kills a process in.
    phases: Vec<Phase>,
    /// The M2-M4 scenarios the campaign reuses rather than rewrites.
    reused: Vec<Scenario>,
}

impl Scope {
    /// Returns every scenario the campaign runs, delivered and reused alike.
    fn scenarios(&self) -> impl Iterator<Item = &Scenario> {
        self.reports
            .iter()
            .map(|report| &report.scenario)
            .chain(&self.reused)
    }

    /// Reads the campaign scope document from the workspace.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("crash-restore")
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
                scenario: Scenario {
                    package: suite::string(report, "package")?,
                    target: suite::string(report, "target")?,
                    name: suite::string(report, "name")?,
                    fixture: suite::string(report, "fixture")?,
                    protocol: "campaign report".to_owned(),
                    milestone: "M5".to_owned(),
                },
            });
        }

        let mut phases = Vec::new();
        for phase in array(&document, "phases")? {
            phases.push(Phase {
                id: suite::string(phase, "id")?,
                protocol: suite::string(phase, "protocol")?,
                report: suite::string(phase, "report")?,
                termination: suite::string(phase, "termination")?,
                expected: suite::string(phase, "expected")?,
            });
        }

        let mut reused = Vec::new();
        for scenario in array(&document, "reused")? {
            reused.push(Scenario {
                package: suite::string(scenario, "package")?,
                target: suite::string(scenario, "target")?,
                name: suite::string(scenario, "name")?,
                fixture: suite::string(scenario, "fixture")?,
                protocol: suite::string(scenario, "protocol")?,
                milestone: suite::string(scenario, "milestone")?,
            });
        }

        Ok(Self {
            fixtures,
            reports,
            phases,
            reused,
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

/// One report the campaign delivers.
struct Report {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The human-readable title of the report.
    title: String,
    /// The obligation the report discharges.
    owes: String,
    /// The scenario that produces it.
    scenario: Scenario,
}

/// One phase of a durable write protocol a process is killed in.
struct Phase {
    /// The identifier the report and the implementation share.
    id: String,
    /// The durable write protocol the phase belongs to.
    protocol: String,
    /// The report that covers the phase.
    report: String,
    /// How the process is required to die.
    termination: String,
    /// What the accepted contract requires the durable state to say.
    expected: String,
}

/// One executable scenario the campaign runs or reuses.
struct Scenario {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The fixture it needs in order to observe anything.
    fixture: String,
    /// The durable write protocol it covers.
    protocol: String,
    /// The milestone that delivered it.
    milestone: String,
}
