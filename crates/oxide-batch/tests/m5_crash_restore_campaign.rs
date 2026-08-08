//! Scope reconciliation for the M5 crash and restore campaign.
//!
//! The campaign has the same two halves the conformance campaign has, split for
//! the same reason:
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
//!   `cargo xtask crash-restore`, which requires the fixtures, runs the
//!   targets, requires the per-phase observations, and writes the retained
//!   report.
//!
//! The scope document is `tests/fixtures/crash-restore/campaign-scope.json` at
//! the workspace root. Both halves read it, so the phase set, the reports, and
//! the reused M2-M4 evidence are stated once.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

/// The commit-protocol phases this campaign must kill a process in.
///
/// The list is here rather than only in the scope document on purpose. The
/// document says what the runner requires; this says what review accepted, and
/// a phase can only leave the campaign by changing both.
const REQUIRED_PHASES: &[&str] = &[
    "business-written",
    "state-provided",
    "progress-blocked",
    "commit-in-flight",
    "commit-acknowledged",
];

/// The reports the performance plan's crash and restore row requires.
const REQUIRED_REPORTS: &[&str] = &[
    "commit-phase-process-kill",
    "p013-restart-after-many-chunks",
    "logical-backup-restore",
];

/// The scenario the M5 design gate names for this campaign.
const NAMED_SCENARIO: &str = "process_kill_at_each_commit_phase_recovers_without_a_forged_status";

#[test]
fn campaign_scope_matches_the_accepted_crash_and_restore_obligations() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .phases
            .iter()
            .map(|phase| phase.id.as_str())
            .collect::<Vec<_>>(),
        REQUIRED_PHASES,
        "the campaign kills a process in every phase of the commit protocol, in commit order",
    );
    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_REPORTS.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign delivers exactly the reports the performance plan requires",
    );
    for phase in &scope.phases {
        assert!(
            scope.reports.iter().any(|report| report.id == phase.report),
            "{} names {}, which is not a report this campaign delivers",
            phase.id,
            phase.report,
        );
        assert_eq!(
            phase.termination, "SIGKILL",
            "{}: the campaign kills a live process rather than asking one to exit",
            phase.id,
        );
    }

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    assert!(
        gate.contains(NAMED_SCENARIO),
        "the design gate must still name {NAMED_SCENARIO} for the evidence campaigns",
    );
    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Crash and restore |"))
        .ok_or_else(|| Failure("the performance plan has no crash and restore row".to_owned()))?;
    for owed in [
        "P-013",
        "process-kill at each commit phase",
        "logical backup",
    ] {
        assert!(
            row.contains(owed),
            "the performance plan's crash and restore row no longer requires {owed}, so the \
             campaign's denominator and the accepted plan disagree",
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

    for scenario in scope
        .reports
        .iter()
        .map(|report| &report.scenario)
        .chain(&scope.reused)
    {
        assert!(
            scope.fixtures.contains(&scenario.fixture),
            "{} needs an undeclared fixture: {}",
            scenario.name,
            scenario.fixture,
        );
        let source = workspace_root()
            .join("crates")
            .join(&scenario.package)
            .join("tests")
            .join(format!("{}.rs", scenario.target));
        let declared = fs::read_to_string(&source)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.display())))?;
        assert!(
            declared.contains(&format!("fn {}(", scenario.name)),
            "{} declares no test named {}",
            source.display(),
            scenario.name,
        );
    }

    let phases = fs::read_to_string(
        workspace_root()
            .join("crates")
            .join("oxide-batch")
            .join("tests")
            .join("postgres_commit_phase_process_kill.rs"),
    )?;
    for phase in REQUIRED_PHASES {
        assert!(
            phases.contains(&format!("\"{phase}\"")),
            "the process-kill target no longer implements the {phase} phase",
        );
    }

    Ok(())
}

#[test]
fn reused_evidence_covers_every_durable_write_protocol_m2_to_m4_delivered()
-> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let protocols = scope
        .reused
        .iter()
        .map(|scenario| scenario.protocol.as_str())
        .collect::<BTreeSet<_>>();

    for protocol in [
        "chunk commit",
        "retry reservation",
        "skip commit",
        "flow decision",
        "partition aggregation",
        "split join",
        "unknown commit outcome",
    ] {
        assert!(
            protocols.contains(protocol),
            "the campaign reuses no M2-M4 evidence for the {protocol} protocol, so its \
             denominator is narrower than the durable writes the milestones delivered",
        );
    }

    Ok(())
}

/// The committed campaign scope document.
struct Scope {
    /// The fixture names the document declares.
    fixtures: BTreeSet<String>,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The commit-protocol phases the campaign kills a process in.
    phases: Vec<Phase>,
    /// The M2-M4 scenarios the campaign reuses rather than rewrites.
    reused: Vec<Scenario>,
}

/// One report the campaign delivers.
struct Report {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The obligation the report discharges, as the plan or gate words it.
    owes: String,
    /// The scenario that produces it.
    scenario: Scenario,
}

/// One phase of a durable write protocol a process is killed in.
struct Phase {
    /// The identifier the report and the implementation share.
    id: String,
    /// The report that covers the phase.
    report: String,
    /// How the process is required to die.
    termination: String,
}

/// One executable scenario the campaign runs or reuses.
struct Scenario {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The durable write protocol it covers.
    protocol: String,
    /// The fixture it needs in order to observe anything.
    fixture: String,
}

impl Scope {
    /// Reads and parses the committed scope document.
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("crash-restore")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let fixtures = document
            .get("fixtures")
            .and_then(Value::as_object)
            .map(|object| object.keys().cloned().collect())
            .ok_or_else(|| Failure("the scope document declares no fixtures".to_owned()))?;

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: field(report, "id")?,
                owes: field(report, "owes")?,
                scenario: Scenario {
                    package: field(report, "package")?,
                    target: field(report, "target")?,
                    name: field(report, "name")?,
                    protocol: "campaign report".to_owned(),
                    fixture: field(report, "fixture")?,
                },
            });
        }

        let mut phases = Vec::new();
        for phase in array(&document, "phases")? {
            phases.push(Phase {
                id: field(phase, "id")?,
                report: field(phase, "report")?,
                termination: field(phase, "termination")?,
            });
        }

        let mut reused = Vec::new();
        for scenario in array(&document, "reused")? {
            reused.push(Scenario {
                package: field(scenario, "package")?,
                target: field(scenario, "target")?,
                name: field(scenario, "name")?,
                protocol: field(scenario, "protocol")?,
                fixture: field(scenario, "fixture")?,
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

/// A reconciliation input the campaign could not read.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
