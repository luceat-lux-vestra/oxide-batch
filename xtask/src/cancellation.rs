//! The M5 cancellation campaign runner.
//!
//! The campaign owes P-014: request-to-intake-stop and request-to-durable-
//! terminal latency, with the count of unjoined tasks at each deadline. It
//! delivers that as four reports — the durable operator path, the phases
//! measured separately, the unjoined counts at every declared deadline, and
//! restart after a cancelled attempt — and this runner is the half that decides
//! whether they proved it.
//!
//! It is a command rather than a test for the reason every other M5 campaign
//! runner is: all four reports return success without a database, because they
//! print a skip line and return. Under `cargo test` that is indistinguishable
//! from evidence. Here the fixture is resolved first, and a campaign run
//! without it fails before any target starts.
//!
//! Passing tests are not sufficient either, and this campaign has a failure
//! mode particular to it. Its headline numbers are *durations*, and a duration
//! is the easiest observation in the whole M5 set to produce without doing the
//! work: a report that measured nothing can retain a zero, a report that
//! measured the wrong interval retains a plausible number, and both look
//! exactly like evidence. So the runner requires the substance:
//!
//! - every observation the committed denominator declares was taken by the
//!   report that owes it, so the campaign cannot shrink by a report quietly
//!   measuring less;
//! - every declared deadline was run both ways — tasks that finish and tasks
//!   held past it — and the unjoined count reported at each equals the number
//!   of tasks the report actually held. This is the campaign's real assertion,
//!   and it is what a constant-reporting coordinator fails;
//! - the durations are *structurally possible*: present, non-negative, and
//!   ordered so that intake stopped no later than the durable terminal.
//!
//! What it deliberately does **not** do is compare any duration against a
//! limit. No accepted document states a cancellation budget. The committed
//! scope records that as `latency_status: observational` and this runner
//! enforces the same rule from the other side: a fast run and a slow run reach
//! the same verdict, and only a missing, impossible, or misordered measurement
//! fails. Inventing a threshold here would publish a release commitment nobody
//! accepted, and importing the M4 in-memory figures — microseconds, against a
//! repository with no commit and no poll interval — would fail every healthy
//! `PostgreSQL` run.
//!
//! It also requires each report to name the `PostgreSQL` major it ran against.
//! A matrix point is invisible in a connection string, so an observation from
//! one supported major would otherwise reconcile perfectly inside a run of
//! another.
//!
//! The scope document is `tests/fixtures/cancellation/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_cancellation_campaign.rs` reconciles it against
//! the accepted plan and the design gate, so this runner consumes a document
//! that ordinary review has already checked from both sides.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "cancellation-campaign.json";

/// The directory the reports write their observations into.
const OBSERVATIONS: &str = "cancellation-observations";

/// The variable that tells a report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_CANCELLATION_OBSERVATIONS";

/// The phases the accepted plan requires measured separately, and exactly
/// these: the campaign's own reconciliation test asserts the plan still names
/// this set (see `the_plan_still_requires_the_phases_measured_separately` in
/// `crates/oxide-batch/tests/m5_cancellation_campaign.rs`), and the committed
/// scope declares no ordering between them, so this checks set membership
/// rather than position.
const REQUIRED_PHASES: &[&str] = &["async", "blocking", "transaction"];

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
/// declared observation was taken, every declared deadline was run both ways
/// with the right unjoined count, and the accepted cancellation and recovery
/// contract held.
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
        .filter_map(|report| report.fixture.clone())
        .collect::<BTreeSet<_>>();

    let mut resolved = BTreeMap::new();
    for (fixture, variables) in &scope.fixtures {
        let missing = variables
            .iter()
            .filter(|variable| !env::var(variable).is_ok_and(|value| !value.is_empty()))
            .cloned()
            .collect::<Vec<_>>();
        resolved.insert(fixture.clone(), missing.is_empty());

        if missing.is_empty() || !needed.contains(fixture) {
            continue;
        }
        violations.push(format!(
            "the {fixture} fixture is required by the cancellation campaign and is incomplete: \
             set {}",
            missing.join(", ")
        ));
    }

    resolved
}

/// Creates an empty observation directory and returns it.
///
/// Emptied rather than reused, so a report retained by an earlier run can never
/// be counted as this run's evidence.
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
            // An ignored result is the skip this campaign exists to rule out,
            // and it is named as such rather than reported as a generic
            // non-pass: a reader who sees "ignored" should know immediately
            // that the fixture, not the framework, is what went wrong.
            Some("ignored") => violations.push(format!(
                "{}::{} was skipped, so it produced no evidence",
                report.target, report.name
            )),
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
        if report.against_database {
            violations.extend(reconcile_matrix_point(&report.id, observation));
        }
    }

    violations.extend(reconcile_required_observations(scope, runs));
    violations.extend(reconcile_latency(runs));
    violations.extend(reconcile_deadlines(scope, runs));
    violations.extend(reconcile_recovery(runs));

    violations
}

/// Requires a database report to name the matrix point it ran at.
///
/// Presence of the major and the full server version is required
/// unconditionally, whether or not `OXIDEBATCH_CAMPAIGN_MATRIX` is set: a
/// report retained without them is invisible to every check downstream that
/// keys off the major, including the cross-report consensus in
/// [`postgres_major`]. The variable, when set, additionally requires the
/// observed major to equal the matrix point the campaign was told it ran at.
fn reconcile_matrix_point(id: &str, observation: &Value) -> Vec<String> {
    verify_matrix_identity(id, expected_matrix_major().as_deref(), observation)
}

/// Extracts the `PostgreSQL` major the campaign was configured to run at.
///
/// `OXIDEBATCH_CAMPAIGN_MATRIX` names a matrix point such as `postgres-15`;
/// only the major after the last hyphen is meaningful to a database report.
fn expected_matrix_major() -> Option<String> {
    let raw = env::var(suite::MATRIX)
        .ok()
        .filter(|value| !value.is_empty())?;
    Some(
        raw.rsplit_once('-')
            .map_or_else(|| raw.clone(), |(_, major)| major.to_owned()),
    )
}

/// Checks one database report's recorded identity against an optional
/// expected major.
///
/// Pure and independent of the environment, so it is exercised directly by
/// unit tests rather than through a process-global variable: presence of
/// `postgres_major_version` and `server_version` is required regardless of
/// `expected`, and `expected`, when given, must equal the observed major.
fn verify_matrix_identity(id: &str, expected: Option<&str>, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    let major = non_empty_str(observation, "postgres_major_version");
    if major.is_none() {
        violations.push(format!(
            "{id} retained no PostgreSQL major version, so this campaign cannot tell which \
             matrix point it ran against"
        ));
    }
    if non_empty_str(observation, "server_version").is_none() {
        violations.push(format!(
            "{id} retained no PostgreSQL server version, so this campaign cannot tell which \
             matrix point it ran against"
        ));
    }

    if let (Some(expected), Some(major)) = (expected, major)
        && major != expected
    {
        violations.push(format!(
            "{id} ran against PostgreSQL {major} and this campaign run is {expected}"
        ));
    }

    violations
}

/// Reads a non-empty string field from an observation.
fn non_empty_str<'a>(document: &'a Value, name: &str) -> Option<&'a str> {
    document
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

/// Requires every observation the denominator declares to have been taken.
///
/// This is what stops the campaign shrinking. Each entry names the report that
/// owes it, and the check is that the report's observation actually carries a
/// value at the path the observation corresponds to — not merely that the
/// report passed, which it would do while measuring nothing.
fn reconcile_required_observations(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();

    for required in &scope.required_observations {
        // "every database report" is a property of all of them rather than of
        // one, and is already checked per report by the matrix reconciliation.
        if required.measured_by == "every database report" {
            continue;
        }
        let Some(observation) = runs.observations.get(&required.measured_by) else {
            violations.push(format!(
                "{} is owed by {} and that report retained no observation",
                required.id, required.measured_by
            ));
            continue;
        };
        let Some(path) = observation_path(&required.id) else {
            violations.push(format!(
                "{} is declared as a required observation and this runner does not know where to \
                 look for it, so the denominator and the verifier disagree",
                required.id
            ));
            continue;
        };
        if !has_value(observation, path) {
            violations.push(format!(
                "{} is owed by {} and its observation carries nothing at {}",
                required.id,
                required.measured_by,
                path.join(".")
            ));
        }
    }

    violations
}

/// Where each declared observation lives inside a retained observation.
///
/// Kept here, beside the checks, rather than in the scope document. The
/// document is the campaign's denominator and is read by three consumers; a
/// JSON pointer is an implementation detail of one of them. What the document
/// does fix is the *set* of observations, and an entry this function does not
/// recognise is a violation rather than a pass, so the two cannot drift apart
/// silently in either direction.
fn observation_path(id: &str) -> Option<&'static [&'static str]> {
    Some(match id {
        "cancellation-request" => &["cancellation_request", "path"],
        "request-to-intake-stop" => &["latency", "request_to_intake_stop_micros"],
        "request-to-durable-terminal" => &["latency", "request_to_durable_terminal_micros"],
        "process-request-to-intake-stop" => &["process_intake", "request_to_intake_stop_micros"],
        "phase-separated-latency" => &["phases"],
        // Both are discharged by the same array, because the drain report
        // records the completing and expiring halves of each deadline in one
        // entry. They stay separate obligations in the denominator: the
        // reconciliation below checks the two halves separately, and only the
        // presence check shares a path.
        "drain-completion" | "unjoined-count-per-deadline" => &["deadlines"],
        "checkpoint-preservation" => &["checkpoint", "preserved"],
        "recovery-after-cancellation" => &["restart", "new_execution"],
        _ => return None,
    })
}

/// Requires the measured durations to be present and structurally possible.
///
/// Deliberately not a comparison against a budget. Every check here is one a
/// correct measurement passes at any speed: present, a number, non-negative by
/// construction, and ordered so intake stopped no later than the terminal.
fn reconcile_latency(runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();

    if let Some(observation) = runs.observations.get("operator-stop") {
        let intake = number_at(observation, &["latency", "request_to_intake_stop_micros"]);
        let terminal = number_at(
            observation,
            &["latency", "request_to_durable_terminal_micros"],
        );
        match (intake, terminal) {
            (Some(intake), Some(terminal)) if intake > terminal => violations.push(format!(
                "operator-stop measured {intake} µs to intake stop and {terminal} µs to the \
                 durable terminal, so the terminal preceded intake stopping"
            )),
            (None, _) => violations.push(
                "operator-stop retained no request-to-intake-stop measurement, which is one of \
                 the two latencies P-014 names"
                    .to_owned(),
            ),
            (_, None) => violations.push(
                "operator-stop retained no request-to-durable-terminal measurement, which is one \
                 of the two latencies P-014 names"
                    .to_owned(),
            ),
            _ => {}
        }
        if observation
            .get("latency")
            .and_then(|latency| latency.get("status"))
            .and_then(Value::as_str)
            != Some("observational")
        {
            violations.push(
                "operator-stop did not record its latencies as observational, and this campaign \
                 has no accepted budget to record them against"
                    .to_owned(),
            );
        }
    }

    if let Some(observation) = runs.observations.get("phase-separation") {
        let phases = observation
            .get("phases")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // The accepted plan requires exactly the async, blocking, and
        // transaction phases, each measured once. A bare count check accepts a
        // report that duplicated one phase and dropped another, or that
        // measured an unrecognised phase in place of a required one, as long as
        // the total still came to three or more; this requires the observed set
        // to equal the required set.
        violations.extend(reconcile_phase_set(&phases));
        for phase in &phases {
            let name = phase
                .get("phase")
                .and_then(Value::as_str)
                .unwrap_or("an unnamed phase");
            if number_at(phase, &["request_to_durable_terminal_micros"]).is_none() {
                violations.push(format!(
                    "the {name} phase retained no request-to-durable-terminal measurement"
                ));
            }
            if phase.get("batch_status").and_then(Value::as_str) != Some("STOPPED") {
                violations.push(format!(
                    "the {name} phase reached {} rather than the accepted STOPPED terminal status",
                    phase
                        .get("batch_status")
                        .and_then(Value::as_str)
                        .unwrap_or("an unrecorded status")
                ));
            }
            violations.extend(reconcile_phase_cancellation_point(name, phase));
        }
        if number_at(
            observation,
            &["process_intake", "request_to_intake_stop_micros"],
        )
        .is_none()
        {
            violations.push(
                "phase-separation retained no process-path intake-stop measurement".to_owned(),
            );
        }
    }

    violations
}

/// Requires the phase-separation report's phase set to equal exactly
/// [`REQUIRED_PHASES`], each named once.
///
/// A report can satisfy an entry-count check by duplicating one phase in
/// place of a missing one, by measuring a phase the plan does not name, or by
/// carrying more entries than the required three, and every per-entry check
/// below this one (duration, terminal status, cancellation point) would still
/// pass on the duplicated or unrecognised entries. So the set is checked
/// directly, against every way it can diverge from what is required: an entry
/// with no usable name, a required phase never observed, a required phase
/// observed more than once, and any observed phase the required set does not
/// name. The scope declares no order among the three, so this is set
/// membership, not a sequence comparison.
fn reconcile_phase_set(phases: &[Value]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    let mut unnamed = 0u64;

    for phase in phases {
        match non_empty_str(phase, "phase") {
            Some(name) => *counts.entry(name).or_insert(0) += 1,
            None => unnamed += 1,
        }
    }

    if unnamed > 0 {
        violations.push(format!(
            "phase-separation reported {unnamed} phase entry(s) with no phase name, and every \
             entry must name which of the async, blocking, or transaction phases it measured"
        ));
    }

    let missing = REQUIRED_PHASES
        .iter()
        .filter(|required| !counts.contains_key(*required))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        violations.push(format!(
            "phase-separation did not report the {} phase(s) the accepted plan requires \
             measured separately",
            missing.join(", ")
        ));
    }

    for required in REQUIRED_PHASES {
        if let Some(count) = counts.get(required)
            && *count > 1
        {
            violations.push(format!(
                "phase-separation reported the {required} phase {count} times, and the accepted \
                 plan requires each phase measured once"
            ));
        }
    }

    let unexpected = counts
        .keys()
        .filter(|name| !REQUIRED_PHASES.contains(*name))
        .copied()
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        violations.push(format!(
            "phase-separation reported the {} phase(s), which the accepted plan does not name \
             among the phases measured separately",
            unexpected.join(", ")
        ));
    }

    violations
}

/// Requires one phase-separation phase to carry evidence that a target-phase
/// worker was actually in flight when the cancellation was requested, rather
/// than merely asserting the phase's own `passed` flag.
///
/// The phase-separation report is retained separately from this runner, so a
/// producer that regresses back to timing-only evidence — or a hand-edited
/// retained JSON — has to be caught here rather than trusted because the
/// report once contained the right code. Both required fields must be present
/// and `true`; `false`, missing, or a non-boolean value are all violations,
/// which is what makes this fail closed rather than fail open on an
/// unrecognised shape.
fn reconcile_phase_cancellation_point(name: &str, phase: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let point = phase.get("cancellation_point");

    if point
        .and_then(|point| point.get("target_phase_entered_before_request"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!(
            "the {name} phase retained no evidence that a target-phase worker entered the phase \
             before the cancellation was requested"
        ));
    }
    if point
        .and_then(|point| point.get("target_worker_in_flight_at_request"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!(
            "the {name} phase retained no evidence that a target-phase worker was in flight at \
             the moment the cancellation was requested"
        ));
    }

    violations
}

/// Reconciles one declared deadline against what the report observed at it.
///
/// Split out of [`reconcile_deadlines`] so that the outer function reads as the
/// sequence it is — every declared deadline, then escalation, then the reverse
/// direction — rather than as one block in which the per-deadline rules and the
/// whole-set rules are interleaved.
fn reconcile_one_deadline(deadline: &Deadline, reported: &[Value]) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(entry) = reported
        .iter()
        .find(|entry| entry.get("deadline").and_then(Value::as_str) == Some(&deadline.id))
    else {
        return vec![format!(
            "the {} deadline is declared and the report observed no unjoined count at it",
            deadline.id
        )];
    };

    if number_at(entry, &["deadline_millis"]) != Some(deadline.millis) {
        violations.push(format!(
            "the {} deadline is declared at {} ms and the report ran it at {}",
            deadline.id,
            deadline.millis,
            describe(number_at(entry, &["deadline_millis"]))
        ));
    }
    if number_at(entry, &["held_tasks"]) != Some(deadline.held_tasks) {
        violations.push(format!(
            "the {} deadline declares {} held task(s) and the report held {}",
            deadline.id,
            deadline.held_tasks,
            describe(number_at(entry, &["held_tasks"]))
        ));
    }

    violations.extend(reconcile_completing_half(deadline, entry));
    violations.extend(reconcile_expiring_half(deadline, entry));
    violations
}

/// Requires the half whose tasks finished to report nothing unjoined.
fn reconcile_completing_half(deadline: &Deadline, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    if entry
        .get("completing")
        .and_then(|completing| completing.get("drain_complete"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!(
            "the completing drain at the {} deadline did not join every owned task",
            deadline.id
        ));
    }
    if number_at(entry, &["completing", "unjoined_tasks"]) != Some(0) {
        violations.push(format!(
            "the completing drain at the {} deadline reported {} unjoined, and a drain whose \
             tasks finished owes zero",
            deadline.id,
            describe(number_at(entry, &["completing", "unjoined_tasks"]))
        ));
    }
    violations
}

/// Requires the half whose tasks were held to report every one of them.
fn reconcile_expiring_half(deadline: &Deadline, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let expiring = number_at(entry, &["expiring", "unjoined_tasks"]);
    if expiring != Some(deadline.held_tasks) {
        violations.push(format!(
            "the {} deadline held {} task(s) past it and the drain reported {} unjoined",
            deadline.id,
            deadline.held_tasks,
            describe(expiring)
        ));
    }
    if entry
        .get("expiring")
        .and_then(|expiring| expiring.get("drain_complete"))
        .and_then(Value::as_bool)
        != Some(false)
    {
        violations.push(format!(
            "the {} deadline held tasks past it and the drain reported itself complete",
            deadline.id
        ));
    }
    // Waiting ended by expiry, so escalation must not be claimed.
    if entry
        .get("expiring")
        .and_then(|expiring| expiring.get("escalated"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        violations.push(format!(
            "the {} deadline ended its wait by expiry and the drain reported escalation",
            deadline.id
        ));
    }
    // The per-phase breakdown has to account for every unjoined task, or
    // the count and the attribution are two different answers.
    let attributed = entry
        .get("expiring")
        .and_then(|expiring| expiring.get("phases"))
        .and_then(Value::as_array)
        .map(|phases| {
            phases
                .iter()
                .filter_map(|phase| phase.get("count").and_then(Value::as_u64))
                .sum::<u64>()
        });
    if attributed != expiring {
        violations.push(format!(
            "the {} deadline reported {} unjoined and attributed {} to phases",
            deadline.id,
            describe(expiring),
            describe(attributed)
        ));
    }

    violations
}

/// Requires every declared deadline to have been run both ways with the right
/// unjoined count.
///
/// This is the campaign's substantive assertion. A coordinator that reports a
/// constant satisfies one half or the other and cannot satisfy both across
/// deadlines whose held-task counts differ, which is why the denominator
/// declares different counts and why both halves are required here.
fn reconcile_deadlines(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get("deadline-unjoined") else {
        return vec![
            "deadline-unjoined retained no observation, so no deadline was reconciled".to_owned(),
        ];
    };

    let reported = observation
        .get("deadlines")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let seen = reported
        .iter()
        .filter_map(|entry| entry.get("deadline").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    for deadline in &scope.deadlines {
        violations.extend(reconcile_one_deadline(deadline, &reported));
    }

    // A report that ran a deadline the denominator does not declare is as much
    // a disagreement as one that skipped a declared deadline.
    for extra in seen {
        if !scope.deadlines.iter().any(|deadline| deadline.id == extra) {
            violations.push(format!(
                "the report observed a {extra} deadline that the denominator does not declare"
            ));
        }
    }

    // Escalation ends waiting the other way and owes the same count.
    let escalation_held = number_at(observation, &["escalation", "held_tasks"]);
    if escalation_held != Some(scope.escalation_held_tasks) {
        violations.push(format!(
            "escalation declares {} held task(s) and the report held {}",
            scope.escalation_held_tasks,
            describe(escalation_held)
        ));
    }
    if number_at(observation, &["escalation", "unjoined_tasks"])
        != Some(scope.escalation_held_tasks)
    {
        violations.push(format!(
            "escalation held {} task(s) and the drain reported {} unjoined",
            scope.escalation_held_tasks,
            describe(number_at(observation, &["escalation", "unjoined_tasks"]))
        ));
    }
    if observation
        .get("escalation")
        .and_then(|escalation| escalation.get("escalated"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(
            "waiting was ended by a second request and the drain did not report escalation"
                .to_owned(),
        );
    }

    violations
}

/// Requires the accepted recovery contract to have held after a cancellation.
///
/// A cancellation that leaves an unrestartable execution is not a successful
/// cancellation, so this is a correctness check rather than a measurement.
fn reconcile_recovery(runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get("restart-after-cancellation") else {
        return vec![
            "restart-after-cancellation retained no observation, so nothing says a cancelled \
             attempt can still be restarted"
                .to_owned(),
        ];
    };

    let committed_before_cancellation = observation
        .get("cancelled_attempt")
        .and_then(|attempt| attempt.get("committed_partitions"))
        .and_then(Value::as_u64);
    if committed_before_cancellation.unwrap_or(0) == 0 {
        violations.push(
            "the cancelled attempt committed no partitions before it was cancelled, so a \
             restart that re-ran nothing proves nothing was preserved"
                .to_owned(),
        );
    }

    for (path, requirement) in [
        (
            ["restart", "same_instance"],
            "the restart created a new job instance rather than a new attempt of the same one",
        ),
        (
            ["restart", "new_execution"],
            "the restart reused the cancelled job execution rather than creating a new one",
        ),
    ] {
        if observation
            .get(path[0])
            .and_then(|value| value.get(path[1]))
            .and_then(Value::as_bool)
            != Some(true)
        {
            violations.push(requirement.to_owned());
        }
    }

    let rerun = observation
        .get("restart")
        .and_then(|restart| restart.get("committed_partitions_re_run"))
        .and_then(Value::as_array)
        .map(Vec::len);
    if rerun != Some(0) {
        violations.push(format!(
            "the restart re-ran {} partition(s) the cancelled attempt had already committed",
            describe(rerun.map(|count| count as u64))
        ));
    }

    if observation
        .get("restart")
        .and_then(|restart| restart.get("batch_status"))
        .and_then(Value::as_str)
        != Some("COMPLETED")
    {
        violations.push(
            "the restart after a cancelled attempt did not reach the COMPLETED terminal status"
                .to_owned(),
        );
    }

    violations
}

/// Reads a number at a path inside an observation.
fn number_at(document: &Value, path: &[&str]) -> Option<u64> {
    let mut cursor = document;
    for step in path {
        cursor = cursor.get(step)?;
    }
    cursor.as_u64()
}

/// Reports whether a non-null value exists at a path inside an observation.
fn has_value(document: &Value, path: &[&str]) -> bool {
    let mut cursor = document;
    for step in path {
        match cursor.get(step) {
            Some(next) => cursor = next,
            None => return false,
        }
    }
    !cursor.is_null()
}

/// Renders an optional number for a violation line.
fn describe(value: Option<u64>) -> String {
    value.map_or_else(|| "nothing".to_owned(), |value| value.to_string())
}

/// Reads a string array out of an observation.
fn strings(value: &Value, name: &str) -> Vec<String> {
    value
        .get(name)
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    entry
                        .as_str()
                        .map_or_else(|| entry.to_string(), str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Writes the campaign's own report.
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

    let (manifest, manifest_violations) = execution_manifest(runs);
    let (major, major_violations) = postgres_major(&scope.reports, runs);
    let mut violations = violations.to_vec();
    violations.extend(manifest_violations);
    violations.extend(major_violations);
    let violations = violations.as_slice();

    let document = json!({
        "campaign": scope.campaign,
        "issue": scope.issue,
        "workload": "P-014",
        "passed": violations.is_empty(),
        "violations": violations,
        "environment": suite::environment(),
        "postgresql_major_version": major,
        "observation": { "execution_manifest": manifest },
        "latency_status": {
            "status": "observational",
            "note": "No accepted document states a cancellation latency budget, so this campaign \
                     measures and reports the two latencies P-014 names and compares neither \
                     against a limit. A fast run and a slow run reach the same verdict here; only \
                     a missing, impossible, or misordered measurement fails.",
        },
        "fixtures": fixtures,
        "reports": scope
            .reports
            .iter()
            .map(|report| {
                json!({
                    "id": report.id,
                    "package": report.package,
                    "target": report.target,
                    "name": report.name,
                    "fixture": report.fixture,
                    "against_database": report.against_database,
                    "outcome": runs
                        .outcomes
                        .get(&(report.target.clone(), report.name.clone()))
                        .and_then(Clone::clone),
                    "observation": runs.observations.get(&report.id),
                })
            })
            .collect::<Vec<_>>(),
        "declared_deadlines": scope
            .deadlines
            .iter()
            .map(|deadline| {
                json!({
                    "id": deadline.id,
                    "millis": deadline.millis,
                    "held_tasks": deadline.held_tasks,
                })
            })
            .collect::<Vec<_>>(),
        "declared_escalation_held_tasks": scope.escalation_held_tasks,
        "required_observations": scope
            .required_observations
            .iter()
            .map(|required| {
                json!({ "id": required.id, "measured_by": required.measured_by })
            })
            .collect::<Vec<_>>(),
    });

    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&document)
                .map_err(|error| format!("could not render the campaign report: {error}"))?
        ),
    )
    .map_err(|error| format!("could not write {}: {error}", path.display()))?;
    Ok(path)
}

/// Hoists the execution manifest the reports recorded to the campaign report.
///
/// Each report records the object identity of the campaign's declared closure
/// from inside its own checkout, and the evidence verifier reads that manifest
/// from the top level of the retained campaign report — that is what binds the
/// retained bytes to the tree they were produced on.
///
/// All four reports run in one campaign against one tree, so their manifests
/// are expected to be identical, and this requires it rather than assuming it.
/// Two reports that disagreed would mean the campaign ran across a tree that
/// changed underneath it, which makes every observation in it a statement about
/// no particular revision — a violation rather than something to average.
fn execution_manifest(runs: &Runs) -> (Value, Vec<String>) {
    let mut manifests = runs
        .observations
        .iter()
        .filter_map(|(id, observation)| {
            observation
                .get("execution_manifest")
                .map(|manifest| (id.clone(), manifest.clone()))
        })
        .collect::<Vec<_>>();

    let Some((first_id, first)) = manifests.pop() else {
        return (
            Value::Null,
            vec![
                "no report recorded an execution manifest, so nothing says which tree the \
                 campaign ran against"
                    .to_owned(),
            ],
        );
    };

    let mut violations = Vec::new();
    for (id, manifest) in &manifests {
        if manifest != &first {
            violations.push(format!(
                "{id} and {first_id} recorded different execution manifests, so the campaign ran \
                 against a tree that changed underneath it"
            ));
        }
    }
    (first, violations)
}

/// Hoists the `PostgreSQL` major the reports ran against.
///
/// Recorded at the top level of the campaign report because that is where the
/// evidence verifier reads it when checking that a retained report is filed
/// under the major it actually ran against. Computed from the reports the
/// scope declares `against_database` rather than by filtering whichever
/// observations happen to carry a major: a report that retained none must stop
/// the hoist rather than be silently excluded from the consensus it is
/// supposed to be part of. Every database report records its own, and they
/// must agree: two reports of one campaign run against different servers would
/// make the campaign a result about neither.
fn postgres_major(reports: &[Report], runs: &Runs) -> (Option<String>, Vec<String>) {
    let mut majors = BTreeSet::new();
    for report in reports.iter().filter(|report| report.against_database) {
        let major = runs
            .observations
            .get(&report.id)
            .and_then(|observation| non_empty_str(observation, "postgres_major_version"));
        let Some(major) = major else {
            // The missing identity is already reported by
            // `reconcile_matrix_point` for this report; here it only has to
            // stop the hoist from completing on a partial consensus.
            return (None, Vec::new());
        };
        majors.insert(major.to_owned());
    }

    match majors.len() {
        1 => (majors.into_iter().next(), Vec::new()),
        0 => (
            None,
            vec![
                "no report recorded the PostgreSQL major it ran against, so the evidence cannot \
                 be filed under a matrix point"
                    .to_owned(),
            ],
        ),
        _ => (
            None,
            vec![format!(
                "the reports ran against {} different PostgreSQL majors ({}), so this campaign \
                 run is a result about none of them",
                majors.len(),
                majors.into_iter().collect::<Vec<_>>().join(", ")
            )],
        ),
    }
}

/// Every target this campaign ran and everything they retained.
#[derive(Default)]
struct Runs {
    /// Targets that exited unsuccessfully, as human-readable lines.
    failed_targets: Vec<String>,
    /// What libtest reported for each declared report.
    outcomes: BTreeMap<(String, String), Option<String>>,
    /// The observation each report retained, by report identifier.
    observations: BTreeMap<String, Value>,
}

/// The committed campaign denominator.
struct Scope {
    campaign: String,
    issue: String,
    fixtures: BTreeMap<String, Vec<String>>,
    reports: Vec<Report>,
    deadlines: Vec<Deadline>,
    escalation_held_tasks: u64,
    required_observations: Vec<Required>,
}

/// One report the campaign runs.
struct Report {
    id: String,
    package: String,
    target: String,
    name: String,
    fixture: Option<String>,
    against_database: bool,
}

/// One declared deadline and the number of tasks held past it.
struct Deadline {
    id: String,
    millis: u64,
    held_tasks: u64,
}

/// One observation the denominator requires and the report that owes it.
struct Required {
    id: String,
    measured_by: String,
}

impl Scope {
    /// Reads the committed scope document.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("cancellation")
            .join("campaign-scope.json");
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        let document: Value = serde_json::from_str(&source)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

        let mut fixtures = BTreeMap::new();
        for (name, variables) in document
            .get("fixtures")
            .and_then(Value::as_object)
            .ok_or_else(|| "the scope declares no fixtures".to_owned())?
        {
            fixtures.insert(
                name.clone(),
                variables
                    .as_array()
                    .map(|entries| {
                        entries
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default(),
            );
        }

        let reports = array(&document, "reports")?
            .iter()
            .map(|entry| {
                Ok(Report {
                    id: suite::string(entry, "id")?,
                    package: suite::string(entry, "package")?,
                    target: suite::string(entry, "target")?,
                    name: suite::string(entry, "name")?,
                    fixture: entry
                        .get("fixture")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    against_database: entry
                        .get("against_database")
                        .and_then(Value::as_bool)
                        .unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if reports.is_empty() {
            return Err("the scope declares no reports".to_owned());
        }

        let deadlines_document = document
            .get("deadlines")
            .ok_or_else(|| "the scope declares no deadlines".to_owned())?;
        let deadlines = deadlines_document
            .get("points")
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope declares no deadline points".to_owned())?
            .iter()
            .map(|entry| {
                Ok(Deadline {
                    id: suite::string(entry, "id")?,
                    millis: number(entry, "millis")?,
                    held_tasks: number(entry, "held_tasks")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if deadlines.is_empty() {
            return Err(
                "the scope declares an empty deadline set, so P-014's \"each deadline\" is no \
                 deadline"
                    .to_owned(),
            );
        }

        let escalation = deadlines_document
            .get("escalation")
            .ok_or_else(|| "the scope declares no escalation observation".to_owned())?;

        let required_observations = document
            .get("observations")
            .and_then(|observations| observations.get("required"))
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope declares no required observations".to_owned())?
            .iter()
            .map(|entry| {
                Ok(Required {
                    id: suite::string(entry, "id")?,
                    measured_by: suite::string(entry, "measured_by")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(Self {
            campaign: suite::string(&document, "campaign")?,
            issue: suite::string(&document, "issue")?,
            fixtures,
            reports,
            deadlines,
            escalation_held_tasks: number(escalation, "held_tasks")?,
            required_observations,
        })
    }
}

/// Reads a required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, String> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("the scope declares no {name}"))
}

/// Reads a required unsigned field.
fn number(document: &Value, name: &str) -> Result<u64, String> {
    document
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a scope entry has no numeric {name}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        Report, Runs, non_empty_str, postgres_major, reconcile_phase_cancellation_point,
        reconcile_phase_set, reconcile_recovery, verify_matrix_identity,
    };

    /// A fully identified database observation: present, non-empty major and
    /// server version, agreeing with `major`.
    fn identified(major: &str, server: &str) -> serde_json::Value {
        json!({
            "postgres_major_version": major,
            "server_version": server,
        })
    }

    #[test]
    fn identity_passes_when_matrix_env_is_unset_and_identity_is_recorded() {
        let observation = identified("15", "15.4 (Debian 15.4-1)");
        assert!(verify_matrix_identity("db-report", None, &observation).is_empty());
    }

    #[test]
    fn identity_fails_closed_on_missing_major_without_matrix_env() {
        let observation = json!({ "server_version": "15.4" });
        let violations = verify_matrix_identity("db-report", None, &observation);
        assert!(
            !violations.is_empty(),
            "a missing major must fail even with no expected matrix point configured"
        );
    }

    #[test]
    fn identity_fails_closed_on_missing_server_version_without_matrix_env() {
        let observation = json!({ "postgres_major_version": "15" });
        let violations = verify_matrix_identity("db-report", None, &observation);
        assert!(
            !violations.is_empty(),
            "a missing server version must fail even with no expected matrix point configured"
        );
    }

    #[test]
    fn identity_fails_on_missing_major_with_matrix_env_set() {
        let observation = json!({ "server_version": "15.4" });
        let violations = verify_matrix_identity("db-report", Some("15"), &observation);
        assert!(!violations.is_empty());
    }

    #[test]
    fn identity_fails_on_missing_server_version_with_matrix_env_set() {
        let observation = json!({ "postgres_major_version": "15" });
        let violations = verify_matrix_identity("db-report", Some("15"), &observation);
        assert!(!violations.is_empty());
    }

    #[test]
    fn identity_fails_when_observed_major_disagrees_with_expected() {
        let observation = identified("18", "18.0");
        let violations = verify_matrix_identity("db-report", Some("15"), &observation);
        assert!(!violations.is_empty());
    }

    #[test]
    fn identity_passes_when_observed_major_matches_expected() {
        let observation = identified("15", "15.4");
        assert!(verify_matrix_identity("db-report", Some("15"), &observation).is_empty());
    }

    #[test]
    fn identity_treats_empty_strings_as_absent() {
        let observation = json!({ "postgres_major_version": "", "server_version": "" });
        let violations = verify_matrix_identity("db-report", None, &observation);
        assert_eq!(
            violations.len(),
            2,
            "an empty string is not a recorded identity"
        );
    }

    #[test]
    fn non_empty_str_rejects_blank_and_absent_fields() {
        let observation = json!({ "a": "", "b": "x" });
        assert_eq!(non_empty_str(&observation, "a"), None);
        assert_eq!(non_empty_str(&observation, "b"), Some("x"));
        assert_eq!(non_empty_str(&observation, "missing"), None);
    }

    /// Builds a minimal against-database report the way the committed scope
    /// declares its four.
    fn db_report(id: &str) -> Report {
        Report {
            id: id.to_owned(),
            package: "oxide-batch".to_owned(),
            target: "postgres_cancellation".to_owned(),
            name: id.to_owned(),
            fixture: Some("postgres-cancellation".to_owned()),
            against_database: true,
        }
    }

    #[test]
    fn postgres_major_hoists_when_every_report_agrees() {
        let reports = vec![db_report("operator-stop"), db_report("phase-separation")];
        let mut runs = Runs::default();
        runs.observations.insert(
            "operator-stop".to_owned(),
            identified("15", "15.4 (Debian 15.4-1)"),
        );
        runs.observations.insert(
            "phase-separation".to_owned(),
            identified("15", "15.4 (Debian 15.4-1)"),
        );

        let (major, violations) = postgres_major(&reports, &runs);
        assert_eq!(major.as_deref(), Some("15"));
        assert!(violations.is_empty());
    }

    #[test]
    fn postgres_major_refuses_mixed_majors_across_reports() {
        let reports = vec![db_report("operator-stop"), db_report("phase-separation")];
        let mut runs = Runs::default();
        runs.observations.insert(
            "operator-stop".to_owned(),
            identified("15", "15.4 (Debian 15.4-1)"),
        );
        runs.observations.insert(
            "phase-separation".to_owned(),
            identified("18", "18.0 (Debian 18.0-1)"),
        );

        let (major, violations) = postgres_major(&reports, &runs);
        assert!(major.is_none());
        assert!(!violations.is_empty());
    }

    #[test]
    fn postgres_major_does_not_hoist_a_partial_consensus() {
        // One report never recorded a major at all. Its own reconciliation
        // fails it separately (`verify_matrix_identity`); the point checked
        // here is that the other report's agreeing major cannot be hoisted as
        // if it spoke for both.
        let reports = vec![db_report("operator-stop"), db_report("phase-separation")];
        let mut runs = Runs::default();
        runs.observations.insert(
            "operator-stop".to_owned(),
            identified("15", "15.4 (Debian 15.4-1)"),
        );
        runs.observations
            .insert("phase-separation".to_owned(), json!({}));

        let (major, _violations) = postgres_major(&reports, &runs);
        assert!(
            major.is_none(),
            "a report with no recorded major must not be silently dropped from the consensus"
        );
    }

    /// A restart observation shaped like `restart_after_cancellation`'s, with
    /// the given prior commit count and every other check satisfied.
    fn restart_observation(committed_partitions: u64) -> serde_json::Value {
        json!({
            "cancelled_attempt": { "committed_partitions": committed_partitions },
            "restart": {
                "same_instance": true,
                "new_execution": true,
                "committed_partitions_re_run": [],
                "batch_status": "COMPLETED",
            },
        })
    }

    #[test]
    fn recovery_rejects_a_restart_with_no_prior_committed_work() {
        let mut runs = Runs::default();
        runs.observations.insert(
            "restart-after-cancellation".to_owned(),
            restart_observation(0),
        );
        let violations = reconcile_recovery(&runs);
        assert!(
            !violations.is_empty(),
            "zero committed partitions before cancellation makes zero rerun vacuous, not evidence"
        );
    }

    #[test]
    fn recovery_accepts_a_restart_with_prior_committed_work() {
        let mut runs = Runs::default();
        runs.observations.insert(
            "restart-after-cancellation".to_owned(),
            restart_observation(1),
        );
        let violations = reconcile_recovery(&runs);
        assert!(violations.is_empty());
    }

    /// A phase entry carrying both required cancellation-point booleans,
    /// shaped like `phase_separation`'s producer emits.
    fn phase_with_cancellation_point(entered: bool, in_flight: bool) -> serde_json::Value {
        json!({
            "phase": "async",
            "cancellation_point": {
                "target_phase_entered_before_request": entered,
                "target_worker_in_flight_at_request": in_flight,
            },
        })
    }

    #[test]
    fn phase_cancellation_point_passes_when_both_observations_are_true() {
        let phase = phase_with_cancellation_point(true, true);
        assert!(reconcile_phase_cancellation_point("async", &phase).is_empty());
    }

    #[test]
    fn phase_cancellation_point_fails_closed_when_entry_evidence_is_missing() {
        let phase = json!({
            "phase": "async",
            "cancellation_point": {
                "target_worker_in_flight_at_request": true,
            },
        });
        let violations = reconcile_phase_cancellation_point("async", &phase);
        assert!(
            !violations.is_empty(),
            "a missing entry observation must fail even when the in-flight observation is present"
        );
    }

    #[test]
    fn phase_cancellation_point_fails_closed_when_entry_evidence_is_false() {
        let phase = phase_with_cancellation_point(false, true);
        let violations = reconcile_phase_cancellation_point("async", &phase);
        assert!(
            !violations.is_empty(),
            "a false entry observation must fail, not be treated as absent-and-ignored"
        );
    }

    #[test]
    fn phase_cancellation_point_fails_closed_when_in_flight_evidence_is_missing() {
        let phase = json!({
            "phase": "blocking",
            "cancellation_point": {
                "target_phase_entered_before_request": true,
            },
        });
        let violations = reconcile_phase_cancellation_point("blocking", &phase);
        assert!(
            !violations.is_empty(),
            "a missing in-flight observation must fail even when entry was observed"
        );
    }

    #[test]
    fn phase_cancellation_point_fails_closed_when_in_flight_evidence_is_false() {
        let phase = phase_with_cancellation_point(true, false);
        let violations = reconcile_phase_cancellation_point("transaction", &phase);
        assert!(
            !violations.is_empty(),
            "a false in-flight observation must fail, not be treated as absent-and-ignored"
        );
    }

    #[test]
    fn phase_cancellation_point_fails_closed_when_the_whole_object_is_absent() {
        let phase = json!({ "phase": "async" });
        let violations = reconcile_phase_cancellation_point("async", &phase);
        assert_eq!(
            violations.len(),
            2,
            "an entirely missing cancellation_point object must fail both checks, not be treated \
             as an unrecognised-but-tolerated shape"
        );
    }

    /// A phase entry with the given name and nothing else — enough to
    /// exercise [`reconcile_phase_set`], which looks only at `phase`.
    fn named_phase(name: &str) -> serde_json::Value {
        json!({ "phase": name })
    }

    #[test]
    fn phase_set_passes_with_exactly_the_required_three() {
        let phases = vec![
            named_phase("async"),
            named_phase("blocking"),
            named_phase("transaction"),
        ];
        assert!(reconcile_phase_set(&phases).is_empty());
    }

    #[test]
    fn phase_set_fails_closed_on_a_duplicated_phase() {
        // async twice, transaction never: an entry-count check alone would
        // pass this, because it still totals three entries.
        let phases = vec![
            named_phase("async"),
            named_phase("async"),
            named_phase("blocking"),
        ];
        let violations = reconcile_phase_set(&phases);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("async") && violation.contains("2 times")),
            "a duplicated async phase must be diagnosed as a duplicate: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("transaction")),
            "the missing transaction phase must also be reported: {violations:?}"
        );
    }

    #[test]
    fn phase_set_fails_closed_on_a_missing_phase() {
        let phases = vec![named_phase("async"), named_phase("blocking")];
        let violations = reconcile_phase_set(&phases);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("transaction")),
            "a missing transaction phase must be diagnosed by name: {violations:?}"
        );
    }

    #[test]
    fn phase_set_fails_closed_on_an_unknown_phase_in_place_of_a_required_one() {
        // Three entries total, same as a passing run, but one of them is not
        // a phase the plan names and the plan's transaction phase never
        // appears.
        let phases = vec![
            named_phase("async"),
            named_phase("blocking"),
            named_phase("unknown"),
        ];
        let violations = reconcile_phase_set(&phases);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("transaction")),
            "the never-observed transaction phase must be reported missing: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("unknown")),
            "the unrecognised phase must be reported by name: {violations:?}"
        );
    }

    #[test]
    fn phase_set_fails_closed_on_an_extra_unrecognised_phase() {
        let phases = vec![
            named_phase("async"),
            named_phase("blocking"),
            named_phase("transaction"),
            named_phase("unknown"),
        ];
        let violations = reconcile_phase_set(&phases);
        assert_eq!(
            violations.len(),
            1,
            "the three required phases are each present exactly once, so the extra unrecognised \
             phase should be the only violation: {violations:?}"
        );
        assert!(violations[0].contains("unknown"));
    }

    #[test]
    fn phase_set_fails_closed_on_an_unnamed_phase_entry() {
        let phases = vec![
            named_phase("async"),
            named_phase("blocking"),
            named_phase("transaction"),
            json!({}),
        ];
        let violations = reconcile_phase_set(&phases);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("no phase name")),
            "an entry with no phase name must be diagnosed rather than silently ignored: \
             {violations:?}"
        );
    }

    #[test]
    fn phase_set_fails_closed_on_an_empty_phase_name() {
        let phases = vec![
            named_phase("async"),
            named_phase("blocking"),
            named_phase("transaction"),
            named_phase(""),
        ];
        let violations = reconcile_phase_set(&phases);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("no phase name")),
            "an empty phase name must be treated as absent, not as a fourth phase: {violations:?}"
        );
    }
}
