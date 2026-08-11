//! Scope reconciliation for the M5 `PostgreSQL` cancellation campaign.
//!
//! The campaign has the two halves every other M5 campaign has, split for the
//! same reason:
//!
//! - **what the campaign owes, and what proves it.** That is a reconciliation
//!   between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Its reports need a real database and
//!   return green without one, because they skip. That half is
//!   `cargo xtask cancellation`.
//!
//! ## The asymmetry this campaign has and the others do not
//!
//! Every delivered M5 campaign so far could be reconciled against a scenario
//! name the design gate fixes. Cancellation cannot: the gate's named-scenario
//! table lists ten scenario IDs for the evidence campaigns and none of them is
//! a cancellation scenario. Conformance, crash, upgrade, security, resource
//! bounds and soak each have at least one; cancellation, performance and
//! reference workload have none.
//!
//! That is recorded rather than repaired. Adding a scenario ID to the gate
//! would mean editing a closed decision record to make a downstream campaign's
//! own reconciliation stronger, which is the wrong direction of travel — and
//! [`the_design_gate_still_names_no_cancellation_scenario`] asserts the gap
//! rather than asserting it away, so the day someone does add one, this test
//! fails and the omission below gets revisited deliberately.
//!
//! The consequence is that the reconciliation against the *plan* has to carry
//! more weight here than it does elsewhere, because the plan is the only
//! accepted document that says what this campaign owes. So
//! [`the_plan_row_still_requires_every_observation_the_campaign_declares`] and
//! [`every_measurement_the_plan_requires_is_declared`] are checked in both
//! directions: the campaign may not drop an obligation the plan states, and it
//! may not claim one the plan does not.
//!
//! ## Why the deadline set is reconciled at all
//!
//! P-014 owes the count of unjoined tasks *at each deadline*, and "each" is
//! only meaningful against a fixed set. A campaign that quietly reduced its
//! deadline set to one would produce a report of exactly the same shape, and
//! [`the_declared_deadlines_are_a_set`] is what stops that. It also requires
//! the held-task counts to differ between points, because a set of deadlines
//! that all held the same number of tasks cannot distinguish a coordinator
//! reporting what it owns from one reporting a constant.

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

/// The reports the campaign delivers, in the order the scope declares them.
const REQUIRED_REPORTS: &[&str] = &[
    "operator-stop",
    "phase-separation",
    "deadline-unjoined",
    "restart-after-cancellation",
];

/// The scenario names the campaign's reports run under.
///
/// Fixed here as well as in the scope because the design gate fixes no name for
/// this campaign, so nothing above the scope document would catch a rename.
const REQUIRED_SCENARIOS: &[&str] = &[
    "operator_stop_reaches_a_durable_terminal_status",
    "cancellation_latency_is_measured_separately_per_phase",
    "drain_reports_unjoined_tasks_at_every_declared_deadline",
    "restart_after_cancellation_resumes_without_rerunning_committed_work",
];

/// The target every report runs in.
const REQUIRED_TARGET: &str = "postgres_cancellation";

/// The M4 measurement this campaign builds on and must not consume.
const M4_MEASUREMENT: &str = "p014_cancellation_and_shutdown_latency";

/// The observations the accepted plan's P-014 rows require, in its own words.
///
/// A campaign that took three of these would look complete in its own report,
/// so each is required here rather than inferred from what a report happens to
/// record.
const REQUIRED_OBSERVATIONS: &[&str] = &[
    "request-to-intake-stop",
    "request-to-durable-terminal",
    "unjoined-count-per-deadline",
    "phase-separated-latency",
];

/// The phases the accepted plan requires measured separately.
const REQUIRED_PHASES: &[&str] = &["async", "blocking", "transaction"];

/// The accepted bound on a shutdown or task-join deadline, in milliseconds.
///
/// Read from the delivered contract rather than chosen: `MIN_SHUTDOWN_DEADLINE`
/// and `MAX_SHUTDOWN_DEADLINE` in `crates/oxide-batch/src/shutdown.rs`. A
/// declared deadline outside this range is not a configuration, it is a value
/// the framework would refuse to construct.
const MIN_DEADLINE_MILLIS: u64 = 1_000;
/// The upper end of the same accepted bound.
const MAX_DEADLINE_MILLIS: u64 = 60 * 60 * 1_000;

/// The accepted bound on the stop poll interval, in milliseconds.
const MIN_STOP_POLL_MILLIS: u64 = 100;
/// The upper end of the same accepted bound.
const MAX_STOP_POLL_MILLIS: u64 = 60 * 1_000;

#[test]
fn campaign_scope_delivers_exactly_the_declared_reports() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;

    let ids = array(&scope, "reports")?
        .iter()
        .filter_map(|report| report.get("id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        ids, REQUIRED_REPORTS,
        "the campaign delivers exactly the reports its denominator declares",
    );

    let names = array(&scope, "reports")?
        .iter()
        .filter_map(|report| report.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        names, REQUIRED_SCENARIOS,
        "a report was renamed, and the design gate names no cancellation scenario that would \
         have caught it",
    );

    for report in array(&scope, "reports")? {
        assert_eq!(
            report.get("target").and_then(Value::as_str),
            Some(REQUIRED_TARGET),
            "every cancellation report runs in the same target",
        );
        assert_eq!(
            report.get("against_database").and_then(Value::as_bool),
            Some(true),
            "a cancellation report that is not run against a database is not this campaign",
        );
        assert_eq!(
            report.get("fixture").and_then(Value::as_str),
            Some("postgres-cancellation"),
            "every report must declare the fixture the runner resolves before running anything",
        );
    }
    Ok(())
}

#[test]
fn every_declared_scenario_exists_in_the_workspace() -> Result<(), Box<dyn Error>> {
    let source = read_document(&format!("crates/oxide-batch/tests/{REQUIRED_TARGET}.rs"))?;
    for scenario in REQUIRED_SCENARIOS {
        assert!(
            source.contains(&format!("fn {scenario}(")),
            "{scenario} is declared in the campaign's denominator and no such test exists, so the \
             campaign would report it as never having run",
        );
    }
    Ok(())
}

#[test]
fn the_plan_row_still_requires_every_observation_the_campaign_declares()
-> Result<(), Box<dyn Error>> {
    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Cancellation |"))
        .ok_or_else(|| Failure("the performance plan has no cancellation row".to_owned()))?;

    for owed in [
        "P-014",
        "request-to-intake-stop",
        "request-to-durable-terminal",
        "unjoined tasks",
        "each deadline",
    ] {
        assert!(
            row.contains(owed),
            "the performance plan's cancellation row no longer requires {owed}, so the \
             campaign's denominator and the accepted plan disagree",
        );
    }
    Ok(())
}

#[test]
fn every_measurement_the_plan_requires_is_declared() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let declared = array(&scope, "observations")
        .or_else(|_| {
            scope
                .get("observations")
                .and_then(|observations| observations.get("required"))
                .and_then(Value::as_array)
                .ok_or_else(|| Failure("the scope declares no required observations".to_owned()))
        })?
        .iter()
        .filter_map(|entry| entry.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for required in REQUIRED_OBSERVATIONS {
        assert!(
            declared.iter().any(|id| id == required),
            "{required} is required by the accepted plan and the campaign declares no observation \
             for it",
        );
    }

    // The other direction. Every declared observation must be owed by a report
    // the campaign actually runs, or the denominator contains an obligation
    // nothing can discharge.
    let report_ids = array(&scope, "reports")?
        .iter()
        .filter_map(|report| report.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for entry in scope
        .get("observations")
        .and_then(|observations| observations.get("required"))
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
    {
        let id = entry.get("id").and_then(Value::as_str).unwrap_or_default();
        let measured_by = entry
            .get("measured_by")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            measured_by == "every database report" || report_ids.iter().any(|r| r == measured_by),
            "{id} is owed by {measured_by}, which is not a report this campaign runs",
        );
    }
    Ok(())
}

#[test]
fn the_plan_still_requires_the_phases_measured_separately() -> Result<(), Box<dyn Error>> {
    let plan = read_document("docs/engineering/performance-plan.md")?;
    assert!(
        plan.contains("async, blocking, and transaction phases")
            || plan.contains("async, blocking, and\n  transaction phases")
            || plan.contains("async, blocking, and"),
        "the accepted plan no longer separates the cancellation phases, so this campaign's phase \
         mapping is measuring something the plan does not ask for",
    );

    let scope = read_scope()?;
    let mapped = scope
        .get("phases")
        .and_then(|phases| phases.get("stop_timing"))
        .and_then(|timing| timing.get("mapping"))
        .and_then(Value::as_array)
        .ok_or_else(|| Failure("the scope declares no phase mapping".to_owned()))?
        .iter()
        .filter_map(|entry| entry.get("plan_phase").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for phase in REQUIRED_PHASES {
        assert!(
            mapped.iter().any(|mapped| mapped == phase),
            "the accepted plan requires the {phase} phase measured separately and the campaign \
             maps no delivered mechanism onto it",
        );
    }
    Ok(())
}

#[test]
fn the_declared_deadlines_are_a_set() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let points = scope
        .get("deadlines")
        .and_then(|deadlines| deadlines.get("points"))
        .and_then(Value::as_array)
        .ok_or_else(|| Failure("the scope declares no deadline points".to_owned()))?;

    assert!(
        points.len() >= 2,
        "P-014 owes the unjoined count at each deadline, and a single deadline cannot \
         distinguish a coordinator that reports what it owns from one that reports a constant",
    );

    let mut millis = Vec::new();
    let mut held = Vec::new();
    for point in points {
        let value = point
            .get("millis")
            .and_then(Value::as_u64)
            .ok_or_else(|| Failure("a deadline point declares no millis".to_owned()))?;
        assert!(
            (MIN_DEADLINE_MILLIS..=MAX_DEADLINE_MILLIS).contains(&value),
            "a deadline of {value} ms is outside the accepted 1 s..=1 h bound the framework \
             enforces, so the campaign declares a deadline it could not construct",
        );
        millis.push(value);
        held.push(
            point
                .get("held_tasks")
                .and_then(Value::as_u64)
                .ok_or_else(|| Failure("a deadline point declares no held_tasks".to_owned()))?,
        );
    }

    let mut unique = millis.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        millis.len(),
        "two declared deadlines have the same value, so one of them proves nothing the other \
         does not",
    );

    let mut unique_held = held.clone();
    unique_held.sort_unstable();
    unique_held.dedup();
    assert_eq!(
        unique_held.len(),
        held.len(),
        "two declared deadlines hold the same number of tasks, so a coordinator that reported \
         that constant would satisfy both",
    );

    // The accepted default has to be among them. It is the deadline a
    // deployment gets without configuring one, so a campaign that measured
    // only cheap deadlines would be reporting the configuration it chose
    // rather than the one the framework ships.
    assert!(
        points.iter().any(|point| {
            point.get("accepted_constant").and_then(Value::as_str)
                == Some("DEFAULT_SHUTDOWN_DEADLINE")
        }),
        "no declared deadline corresponds to the accepted default, which is the deadline most \
         deployments will actually experience",
    );
    Ok(())
}

#[test]
fn the_declared_workload_is_inside_the_accepted_bounds() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let workload = scope
        .get("workload")
        .ok_or_else(|| Failure("the scope declares no workload".to_owned()))?;

    let interval = workload
        .get("stop_poll_interval_millis")
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure("the workload declares no stop poll interval".to_owned()))?;
    assert!(
        (MIN_STOP_POLL_MILLIS..=MAX_STOP_POLL_MILLIS).contains(&interval),
        "a stop poll interval of {interval} ms is outside the accepted 100 ms..=60 s bound, so \
         the campaign declares an interval the framework would refuse",
    );

    // The pool has to be the derived requirement exactly. A campaign that gave
    // itself spare connections would absorb a checkout leak rather than
    // exhausting the pool, and a cancelled attempt failing to return its
    // connections is one of the leaked-work outcomes P-014 is about.
    let budget = workload
        .get("worker_budget")
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure("the workload declares no worker budget".to_owned()))?;
    let pool = workload
        .get("pool_size")
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure("the workload declares no pool size".to_owned()))?;
    assert_eq!(
        pool,
        budget + 1,
        "the pool must be the derived requirement exactly, so a connection a cancelled attempt \
         did not return exhausts it rather than being absorbed",
    );
    Ok(())
}

#[test]
fn the_latency_obligation_is_declared_observational() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    assert_eq!(
        scope
            .get("latency_status")
            .and_then(|status| status.get("status"))
            .and_then(Value::as_str),
        Some("observational"),
        "no accepted document states a cancellation budget, so the campaign must declare its \
         latencies observational rather than gating",
    );

    // The plan says what a budget would require, and the campaign must not have
    // quietly become one. If the plan ever gains a numeric cancellation budget
    // this test is the thing that fails, which is the intended prompt to
    // revisit the campaign rather than to keep reporting observationally.
    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Cancellation |"))
        .ok_or_else(|| Failure("the performance plan has no cancellation row".to_owned()))?;
    assert!(
        !row.contains(" ms") && !row.contains(" µs") && !row.contains(" seconds"),
        "the accepted plan's cancellation row now names a duration, so the campaign's \
         observational latency status needs revisiting against it",
    );
    Ok(())
}

#[test]
fn the_m4_measurement_is_retained_rather_than_replaced() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let related = array(&scope, "related")?;
    let m4 = related
        .iter()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some(M4_MEASUREMENT))
        .ok_or_else(|| {
            Failure(format!(
                "the campaign does not record {M4_MEASUREMENT} as related evidence"
            ))
        })?;
    assert_eq!(
        m4.get("run_by_this_campaign").and_then(Value::as_bool),
        Some(false),
        "the M4 measurement is the baseline this campaign builds on; moving it under a \
         PostgreSQL fixture and calling the result production-preview evidence is the cheapest \
         way to appear to deliver this campaign",
    );

    let measurement = read_document("docs/engineering/measurements/m4/p-014.json")?;
    assert!(
        measurement.contains("request_to_durable_terminal_micros"),
        "the retained M4 P-014 measurement no longer records the latency this campaign extends \
         to PostgreSQL",
    );
    Ok(())
}

#[test]
fn the_design_gate_still_names_no_cancellation_scenario() -> Result<(), Box<dyn Error>> {
    // Asserted rather than asserted away. The gate's named-scenario table has no
    // cancellation entry, which is why this campaign's names are fixed by its
    // own denominator and by the test above. If a scenario is ever added to the
    // gate, this fails and the campaign's reconciliation should be strengthened
    // to reconcile against it instead of against nothing.
    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    for scenario in REQUIRED_SCENARIOS {
        assert!(
            !gate.contains(scenario),
            "the design gate now names {scenario}; the campaign should reconcile against the \
             gate rather than against its own denominator alone, and the recorded gap in \
             tests/fixtures/cancellation/campaign-scope.json needs updating",
        );
    }

    let scope = read_scope()?;
    assert!(
        scope.get("design_gate_scenario_gap").is_some(),
        "the campaign must record why it reconciles against no design-gate scenario, rather than \
         leaving the omission to be discovered",
    );
    Ok(())
}

#[test]
fn the_campaign_declares_what_it_does_not_establish() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let out_of_scope = array(&scope, "out_of_scope")?
        .iter()
        .filter_map(|entry| entry.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for required in [
        // The accepted plan puts forced worker loss in the crash campaign
        // explicitly, so a cancellation campaign that claimed it would be
        // claiming another campaign's result.
        "forced-worker-loss",
        // The plan names broker and remote worker phases among those a
        // cancellation test separates, and M5 has neither.
        "broker-and-remote-worker-phases",
        // No accepted budget exists and this campaign does not create one.
        "cancellation-latency-budget",
    ] {
        assert!(
            out_of_scope.iter().any(|id| id == required),
            "the campaign must record {required} as out of scope rather than leaving a reader to \
             infer that it was covered",
        );
    }
    Ok(())
}

#[test]
fn the_semantic_closure_covers_what_the_campaign_runs() -> Result<(), Box<dyn Error>> {
    let closure = read_json("tests/fixtures/cancellation/campaign-semantics.json")?;
    let paths = closure
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the closure declares no categories".to_owned()))?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for required in [
        // The code under test.
        "crates/oxide-batch/src",
        "crates/oxide-batch-repository/src",
        // The report and its mechanics.
        "crates/oxide-batch/tests/postgres_cancellation.rs",
        "crates/oxide-batch/tests/cancellation",
        // The denominator, including the deadline set.
        "tests/fixtures/cancellation/campaign-scope.json",
        // The verifier, whose verdicts are part of the result.
        "xtask/src/cancellation.rs",
        // The resolved dependency graph: this campaign measures durations and
        // the async runtime's scheduler and timer are pinned here.
        "Cargo.lock",
        // How CI runs it.
        ".github/workflows/ci.yml",
    ] {
        assert!(
            paths.iter().any(|path| path == required),
            "{required} is not in the campaign's semantic closure, so a change to it would leave \
             retained evidence looking valid when it is evidence of something else",
        );
    }

    for path in &paths {
        assert!(
            workspace_root().join(path).exists(),
            "{path} is declared as campaign semantics and does not exist, so the producer cannot \
             record its object identity",
        );
    }
    Ok(())
}

/// Reads the committed campaign scope document.
fn read_scope() -> Result<Value, Box<dyn Error>> {
    read_json("tests/fixtures/cancellation/campaign-scope.json")
}

/// Reads and parses one JSON document relative to the workspace root.
fn read_json(path: &str) -> Result<Value, Box<dyn Error>> {
    Ok(serde_json::from_str(&read_document(path)?)?)
}

/// Reads one document relative to the workspace root.
fn read_document(path: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(path))?)
}

/// Reads a required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| Box::new(Failure(format!("the scope declares no {name}"))) as Box<dyn Error>)
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A reconciliation failure.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
