//! The M5 soak campaign runner.
//!
//! The campaign owes P-015 across repeated launch, shutdown, restart, and
//! recovery cycles on `PostgreSQL`, reporting task, connection, handle, and
//! memory growth over a declared duration. It delivers that as one report, and
//! this runner is the half that decides whether the report proved it.
//!
//! It is a command rather than a test for the reason the other campaigns are:
//! the report returns success without a database, because it prints a skip line
//! and returns. Under `cargo test` that is indistinguishable from evidence.
//! Here the fixture is resolved first, and a campaign run without it fails
//! before the target starts.
//!
//! A passing test is not sufficient either, and a soak has a failure mode the
//! other campaigns do not have. Every other campaign's obligation exists
//! independently of its run — a ledger row, a commit phase, a schema path, a
//! declared ceiling — so a report either covered it or did not. A soak's
//! obligation is *a period*, and a soak that ran three cycles and a soak that
//! ran three hundred produce reports of identical shape, both green. Worse, the
//! shorter one produces flatter series and therefore a *more* convincing
//! result. So this runner reads the committed denominator itself rather than
//! trusting the report's summary of it, and requires:
//!
//! - the declared warmup and measured windows to have actually run, with a
//!   sample per cycle and at least the declared minimum of measured samples;
//! - the workload the report ran to be the workload the denominator declares,
//!   down to the partition count, the worker budget, and the pool size, since a
//!   soak of a smaller workload is a different campaign with the same name;
//! - the lifecycle to have actually happened every cycle: a fault injected, a
//!   restart, a recovery, and a completed drain, once per cycle, rather than a
//!   run that repeated a plain launch;
//! - every declared correctness obligation to have been decided and to hold,
//!   because resource flatness over a workload that stopped doing the work is
//!   not a result;
//! - every declared growth rule to have been decided and to have passed, and
//!   every measured sample to carry every declared observation, because a rule
//!   whose metric was never sampled passes by default;
//! - the final drain to have joined every owned task and closed the pool.
//!
//! It also requires the report to name the `PostgreSQL` major it ran against. A
//! matrix point is invisible in a connection string, so an observation from one
//! supported major would otherwise reconcile perfectly inside a run of another.
//!
//! The scope document is `tests/fixtures/soak/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_soak_campaign.rs` reconciles it against the
//! accepted plan and the design gate, so this runner consumes a document that
//! ordinary review has already checked from the other side.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::soak_evidence;
use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "soak-campaign.json";

/// The directory the report writes its observation into.
const OBSERVATIONS: &str = "soak-observations";

/// The variable that tells the report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_SOAK_OBSERVATIONS";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means the report ran on its fixture, over the
/// declared window, doing the declared work, and decided every correctness
/// obligation and every growth rule the campaign declares.
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
        let report = write_report(&root, &scope, &fixtures, &Run::default(), &violations)?;
        return Ok(Campaign { violations, report });
    }

    let observations = prepare_observations(&root)?;
    let mut run = Run::default();

    eprintln!("==> {} {}", scope.report.target, scope.report.name);
    let target = suite::run_target(
        &root,
        &TargetCommand {
            package: &scope.report.package,
            selector: &["--test".to_owned(), scope.report.target.clone()],
            filters: &["--exact", &scope.report.name],
            environment: &[(OBSERVATIONS_ENV, observations.display().to_string())],
            nocapture: true,
        },
    )?;
    run.succeeded = target.succeeded;
    run.outcome = target.results.get(&scope.report.name).cloned();
    run.observation = read_observation(&observations)?;

    violations.extend(reconcile(&scope, &run, matrix_point().as_deref()));
    let report = write_report(&root, &scope, &fixtures, &run, &violations)?;
    Ok(Campaign { violations, report })
}

/// Reports which declared fixtures the environment supplies.
///
/// The campaign's one report needs a database, so an absent fixture is always a
/// violation and the campaign stops before running anything: a soak produced
/// without a database is the forged pass this runner exists to rule out.
fn resolve_fixtures(scope: &Scope, violations: &mut Vec<String>) -> BTreeMap<String, bool> {
    let mut resolved = BTreeMap::new();
    for (fixture, variables) in &scope.fixtures {
        let missing = variables
            .iter()
            .filter(|variable| !env::var(variable).is_ok_and(|value| !value.is_empty()))
            .cloned()
            .collect::<Vec<_>>();
        resolved.insert(fixture.clone(), missing.is_empty());

        if missing.is_empty() || fixture != &scope.report.fixture {
            continue;
        }
        violations.push(format!(
            "the {fixture} fixture is required by the soak campaign and is incomplete: set {}",
            missing.join(", "),
        ));
    }
    resolved
}

/// Creates an empty observation directory and returns it.
///
/// Emptied rather than reused so a report retained by an earlier run can never
/// be counted as this run's evidence. That matters more here than elsewhere: a
/// soak takes minutes, and a stale report is exactly what a truncated rerun
/// would leave behind.
fn prepare_observations(root: &Path) -> Result<PathBuf, String> {
    let directory = suite::directory(root).join(OBSERVATIONS);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    Ok(directory)
}

/// Reads the observation the report retained, if it retained one.
fn read_observation(directory: &Path) -> Result<Option<Value>, String> {
    let path = directory.join("soak.json");
    if !path.exists() {
        return Ok(None);
    }
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    serde_json::from_str(&source)
        .map(Some)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))
}

/// Returns the supported-matrix major this run covers, where one is declared.
///
/// Resolved from the environment once, at the edge, and passed inward. The
/// reconciliation itself reads no environment: a check that consults ambient
/// state cannot be tested, and this one silently disagreed with its own unit
/// test the first time the campaign ran on a matrix point other than the one
/// the test's fixture named.
fn matrix_point() -> Option<String> {
    env::var(suite::MATRIX)
        .ok()
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .rsplit_once('-')
                .map_or(value.clone(), |(_, major)| major.to_owned())
        })
}

/// Reports everything the campaign required and did not observe.
fn reconcile(scope: &Scope, run: &Run, matrix: Option<&str>) -> Vec<String> {
    let mut violations = Vec::new();

    if !run.succeeded {
        violations.push(format!(
            "{} {} exited unsuccessfully",
            scope.report.package, scope.report.target,
        ));
    }
    match run.outcome.as_deref() {
        Some("ok") => {}
        Some(other) => violations.push(format!(
            "{}::{} reported {other}",
            scope.report.target, scope.report.name,
        )),
        None => violations.push(format!(
            "{}::{} did not run in package {}",
            scope.report.target, scope.report.name, scope.report.package,
        )),
    }

    let Some(observation) = &run.observation else {
        violations.push(
            "the soak report ran and retained no observation, so nothing says it did the work"
                .to_owned(),
        );
        return violations;
    };

    if observation.get("passed").and_then(Value::as_bool) != Some(true) {
        violations.push("the soak report retained an observation that did not pass".to_owned());
    }
    for violation in strings(observation, "violations") {
        violations.push(format!("soak: {violation}"));
    }

    violations.extend(reconcile_matrix_point(observation, matrix));
    violations.extend(reconcile_window(scope, observation));
    violations.extend(reconcile_workload(scope, observation));

    // The order below is the trust graph, and it is not interchangeable. Every
    // step after the chronology check indexes into the samples and the journal
    // — the correctness baseline is "the first measured cycle" and the memory
    // verdict is read from each window's first and last reading, and both are
    // positions. The memory rule is the more exposed of the two: it consults
    // exactly four readings out of six hundred and thirty-two, so a single
    // reordered entry that happens to land on an endpoint moves the verdict
    // outright while leaving every value in the series intact. A run whose
    // chronology does not hold is therefore not evaluated further: its later
    // verdicts would be answers about a different arrangement of the same data.
    let cycles = match soak_evidence::read_cycles(observation) {
        Ok(cycles) => cycles,
        Err(error) => {
            violations.push(format!("the per-cycle journal could not be read: {error}"));
            return violations;
        }
    };
    let window = soak_evidence::Window {
        warmup: scope.warmup_cycles,
        measured: scope.measured_cycles,
    };
    let sequence = soak_evidence::reconcile_sequence(&window, observation, &cycles);
    if !sequence.is_empty() {
        violations.extend(sequence);
        return violations;
    }

    let workload = soak_evidence::Workload {
        partitions_per_cycle: scope.partitions_per_cycle,
        worker_budget: scope.worker_budget,
    };
    let lifecycle = soak_evidence::fold_lifecycle(&cycles);
    violations.extend(soak_evidence::reconcile_lifecycle(
        &lifecycle,
        observation,
        &cycles,
    ));
    violations.extend(reconcile_declared_workload(scope, &lifecycle));

    let recomputed = soak_evidence::recompute_correctness(&workload, &cycles);
    violations.extend(soak_evidence::reconcile_correctness(
        &scope.correctness,
        &recomputed,
        observation,
    ));

    if observation
        .pointer("/campaign/pool_readings")
        .and_then(Value::as_u64)
        .is_none_or(|readings| readings == 0)
    {
        violations.push(
            "the report took no pool reading while the cycles ran, so every peak occupancy in it \
             is a zero that means nothing was measured"
                .to_owned(),
        );
    }
    violations.extend(reconcile_observations(scope, observation));
    violations.extend(reconcile_growth(scope, observation));
    violations.extend(reconcile_final_drain(observation));

    violations
}

/// Requires the folded lifecycle to be the workload the campaign declares.
///
/// The journal is authority over the summary, and the scope is authority over
/// both: a run that consistently did less than the declared workload would fold
/// and summarise in perfect agreement.
fn reconcile_declared_workload(scope: &Scope, lifecycle: &soak_evidence::Lifecycle) -> Vec<String> {
    let mut violations = Vec::new();
    let total = scope.warmup_cycles + scope.measured_cycles;
    let expected = total * scope.partitions_per_cycle + total;

    if lifecycle.cycles != total {
        violations.push(format!(
            "the campaign declares {total} cycles and the journal contains {}",
            lifecycle.cycles,
        ));
    }
    for (what, folded) in [
        ("faults", lifecycle.faults),
        ("restarts", lifecycle.restarts),
        ("recoveries", lifecycle.recoveries),
        ("completed drains", lifecycle.drains),
    ] {
        if folded != total {
            violations.push(format!(
                "{total} cycles ran and the journal contains {folded} {what}"
            ));
        }
    }
    // Every cycle invokes each partition once and the injected one twice, so
    // the workload implies its own execution count exactly.
    if lifecycle.partition_executions != expected {
        violations.push(format!(
            "the declared workload implies {expected} partition executions and the journal \
             contains {}",
            lifecycle.partition_executions,
        ));
    }
    violations
}

/// Requires the report to name the matrix point the campaign ran at.
fn reconcile_matrix_point(observation: &Value, expected: Option<&str>) -> Vec<String> {
    let Some(expected) = expected else {
        return Vec::new();
    };
    let observed = observation
        .get("postgres_major_version")
        .and_then(Value::as_str);

    if observed == Some(expected) {
        return Vec::new();
    }
    vec![format!(
        "the soak ran against PostgreSQL {} and this campaign run is {expected}",
        observed.unwrap_or("an unrecorded version"),
    )]
}

/// Requires the declared window to have actually run and been sampled.
///
/// This is the obligation the campaign turns on. A soak's result is only as
/// good as its period, and a shorter run produces a flatter series and a more
/// convincing report, so the period is required from the denominator rather
/// than read off the run.
fn reconcile_window(scope: &Scope, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    for (field, declared) in [
        ("warmup_cycles", scope.warmup_cycles),
        ("measured_cycles", scope.measured_cycles),
    ] {
        let reported = campaign_number(observation, field);
        if reported != Some(declared) {
            violations.push(format!(
                "the campaign declares {declared} {field} and the report ran {}",
                describe(reported),
            ));
        }
    }

    let completed = campaign_number(observation, "completed_cycles");
    let total = scope.warmup_cycles + scope.measured_cycles;
    if completed != Some(total) {
        violations.push(format!(
            "the campaign declares {total} cycles and the report completed {}",
            describe(completed),
        ));
    }

    let samples = observation
        .get("samples")
        .and_then(Value::as_array)
        .map_or(0, Vec::len) as u64;
    if samples != total {
        violations.push(format!(
            "{total} cycles were declared and {samples} samples were retained; a growth rule is \
             only as good as the series it was decided from",
        ));
    }

    let measured = phase_count(observation, "measured");
    if measured < scope.minimum_measured_samples {
        violations.push(format!(
            "the campaign requires at least {} measured samples and the report retained \
             {measured}",
            scope.minimum_measured_samples,
        ));
    }
    let warmup = phase_count(observation, "warmup");
    if warmup != scope.warmup_cycles {
        violations.push(format!(
            "the campaign declares {} warmup samples and the report marked {warmup}; warmup that \
             can be widened after the fact is a way to exclude accumulation rather than startup",
            scope.warmup_cycles,
        ));
    }

    violations
}

/// Requires the workload run to be the workload declared.
///
/// A soak of a smaller workload is a different campaign wearing this one's
/// name, and nothing in a resource series says which workload produced it.
fn reconcile_workload(scope: &Scope, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    for (field, declared) in [
        ("partitions_per_cycle", scope.partitions_per_cycle),
        ("worker_budget", scope.worker_budget),
        ("launches_per_cycle", scope.launches_per_cycle),
        ("owned_tasks_per_drain", scope.owned_tasks_per_drain),
    ] {
        let reported = campaign_number(observation, field);
        if reported != Some(declared) {
            violations.push(format!(
                "the campaign declares {field} of {declared} and the report ran {}",
                describe(reported),
            ));
        }
    }

    let pool = observation
        .pointer("/environment/pool/size")
        .and_then(Value::as_u64);
    if pool != Some(scope.pool_size) {
        violations.push(format!(
            "the campaign declares a pool of {} connections and the report opened {}",
            scope.pool_size,
            describe(pool),
        ));
    }

    violations
}

/// Requires every sample to carry every observation the campaign declares.
///
/// A metric that stopped being sampled leaves its rule deciding an absence, and
/// an absence is not a flat line.
fn reconcile_observations(scope: &Scope, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let samples = observation
        .get("samples")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for declared in &scope.observations {
        let missing = samples
            .iter()
            .filter(|sample| {
                sample
                    .pointer(&format!("/metrics/{declared}"))
                    .and_then(Value::as_i64)
                    .is_none()
            })
            .count();
        if missing != 0 {
            violations.push(format!(
                "{declared} is a declared observation and {missing} of {} samples do not carry it",
                samples.len(),
            ));
        }
    }

    violations
}

/// Requires every declared growth rule to be decided and to have passed.
fn reconcile_growth(scope: &Scope, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let verdicts = observation
        .pointer("/growth/rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut by_id: BTreeMap<String, Value> = BTreeMap::new();
    for verdict in &verdicts {
        let Some(id) = verdict.get("id").and_then(Value::as_str) else {
            violations.push("the report decided a rule with no identity".to_owned());
            continue;
        };
        if by_id.insert(id.to_owned(), verdict.clone()).is_some() {
            violations.push(format!(
                "the report decided {id} more than once, so which verdict the campaign passed on \
                 depends on which one is read"
            ));
        }
    }

    for rule in &scope.rules {
        let Some(verdict) = by_id.get(&rule.id) else {
            violations.push(format!(
                "the campaign declares the {} growth rule and the report decided nothing for it",
                rule.id,
            ));
            continue;
        };
        if verdict.get("decided").and_then(Value::as_bool) != Some(true) {
            violations.push(format!(
                "the {} rule was not decided: {}",
                rule.id,
                verdict
                    .get("explanation")
                    .and_then(Value::as_str)
                    .unwrap_or("the report gave no reason"),
            ));
            continue;
        }
        if verdict.get("rule").and_then(Value::as_str) != Some(rule.decides.as_str()) {
            violations.push(format!(
                "the campaign declares {} for {} and the report applied {}",
                rule.decides,
                rule.id,
                verdict
                    .get("rule")
                    .and_then(Value::as_str)
                    .unwrap_or("no stated rule"),
            ));
        }

        let measured = measured_series(observation, &rule.metric);
        violations.extend(reconcile_series(rule, verdict, measured.as_deref()));
        let Some(measured) = measured else {
            continue;
        };
        if (measured.len() as u64) < scope.minimum_measured_samples {
            violations.push(format!(
                "the {} rule was decided from {} readings and the campaign requires at least {}",
                rule.id,
                measured.len(),
                scope.minimum_measured_samples,
            ));
        }

        // The verdict itself is recomputed here, from the samples, by this
        // program. The report's own boolean is then required to agree with it —
        // a report that marked a leaking series as passing fails on the
        // recomputation rather than being taken at its word.
        let baseline = baseline_reading(observation, &rule.metric);
        let warmup = warmup_series(observation, &rule.metric);
        let recomputed = decide(
            rule,
            &measured,
            baseline,
            scope.pool_size,
            warmup.as_deref(),
        );
        let claimed_pass = verdict.get("passed").and_then(Value::as_bool);
        if claimed_pass != Some(recomputed.passed) {
            violations.push(format!(
                "the {} rule reports passed={} and recomputing it from the measured samples gives \
                 {}: {}",
                rule.id,
                claimed_pass.map_or_else(|| "nothing".to_owned(), |value| value.to_string()),
                recomputed.passed,
                recomputed.explanation,
            ));
        }
        if !recomputed.passed {
            violations.push(format!("{}: {}", rule.id, recomputed.explanation));
        }
    }

    for id in by_id.keys() {
        if !scope.rules.iter().any(|rule| &rule.id == id) {
            violations.push(format!(
                "the report decided a {id} rule, which the campaign scope does not declare",
            ));
        }
    }

    violations
}

/// Requires a verdict's series to be the one the retained samples carry.
///
/// A verdict decided from readings nobody else can see is not checkable, so the
/// two are compared element by element rather than by length.
fn reconcile_series(rule: &Rule, verdict: &Value, measured: Option<&[i64]>) -> Vec<String> {
    let claimed = verdict
        .get("series")
        .and_then(Value::as_array)
        .map(|values| values.iter().map(Value::as_i64).collect::<Vec<_>>());
    match (measured, claimed) {
        (Some(measured), Some(claimed)) => {
            if claimed.len() == measured.len()
                && claimed
                    .iter()
                    .zip(measured)
                    .all(|(claimed, measured)| *claimed == Some(*measured))
            {
                return Vec::new();
            }
            vec![format!(
                "the {} rule was decided from a series of {} reading(s) that is not the {} the \
                 measured samples carry for {}",
                rule.id,
                claimed.len(),
                measured.len(),
                rule.metric,
            )]
        }
        (None, _) => vec![format!(
            "the {} rule is decided from {}, which the measured samples do not all carry",
            rule.id, rule.metric,
        )],
        (_, None) => vec![format!(
            "the {} rule carries no series to check against the measured samples",
            rule.id,
        )],
    }
}

/// Rebuilds one metric's measured series from the retained samples.
///
/// This is the authoritative series. A verdict carries its own copy, and the
/// two are required to be identical, but the samples are what the campaign
/// retained and what a reader can check.
fn measured_series(observation: &Value, metric: &str) -> Option<Vec<i64>> {
    let samples = observation.get("samples").and_then(Value::as_array)?;
    samples
        .iter()
        .filter(|sample| sample.get("phase").and_then(Value::as_str) == Some("measured"))
        .map(|sample| {
            sample
                .pointer(&format!("/metrics/{metric}"))
                .and_then(Value::as_i64)
        })
        .collect()
}

/// Rebuilds one metric's warmup series from the retained samples.
fn warmup_series(observation: &Value, metric: &str) -> Option<Vec<i64>> {
    let samples = observation.get("samples").and_then(Value::as_array)?;
    samples
        .iter()
        .filter(|sample| sample.get("phase").and_then(Value::as_str) == Some("warmup"))
        .map(|sample| {
            sample
                .pointer(&format!("/metrics/{metric}"))
                .and_then(Value::as_i64)
        })
        .collect()
}

/// Reads one metric's post-warmup baseline from the retained samples.
///
/// The baseline the campaign declares is the last warmup sample: the first
/// reading taken with the pool open, the arenas sized, and the runtime started.
fn baseline_reading(observation: &Value, metric: &str) -> Option<i64> {
    observation
        .get("samples")
        .and_then(Value::as_array)?
        .iter()
        .rfind(|sample| sample.get("phase").and_then(Value::as_str) == Some("warmup"))?
        .pointer(&format!("/metrics/{metric}"))
        .and_then(Value::as_i64)
}

/// One rule's recomputed decision and the sentence that explains it.
struct Decision {
    passed: bool,
    explanation: String,
}

/// Applies one declared rule to one measured series.
///
/// This is a second implementation of the rule the report applies, deliberately
/// kept independent of it. The report and this program share the declared rule
/// and the samples; they do not share the boolean, and they do not share the
/// code that produces it. Two implementations of one documented algorithm
/// disagreeing is a finding, which is the point.
fn decide(
    rule: &Rule,
    series: &[i64],
    baseline: Option<i64>,
    capacity: u64,
    warmup: Option<&[i64]>,
) -> Decision {
    match rule.decides.as_str() {
        "every-measured-sample-equals-zero" => {
            let offenders = series.iter().filter(|value| **value != 0).count();
            Decision {
                passed: offenders == 0,
                explanation: format!(
                    "{} was non-zero at {offenders} of {} measured boundaries",
                    rule.metric,
                    series.len(),
                ),
            }
        }
        "no-measured-sample-above-baseline" => {
            let Some(baseline) = baseline else {
                return Decision {
                    passed: false,
                    explanation: format!(
                        "{} is decided against the post-warmup baseline and no warmup sample \
                         carries {}",
                        rule.id, rule.metric,
                    ),
                };
            };
            let worst = series.iter().copied().max().unwrap_or(baseline);
            Decision {
                passed: worst <= baseline,
                explanation: format!(
                    "{} settled at {baseline} after warmup and the measured window reached {worst}",
                    rule.metric,
                ),
            }
        }
        "no-measured-sample-above-configured-capacity" => {
            let capacity = i64::try_from(capacity).unwrap_or(i64::MAX);
            let worst = series.iter().copied().max().unwrap_or_default();
            Decision {
                passed: worst <= capacity,
                explanation: format!(
                    "{} reached {worst} against a configured capacity of {capacity}",
                    rule.metric,
                ),
            }
        }
        "warmup-relative-rate-decay" => decide_decay(rule, series, warmup),
        other => Decision {
            passed: false,
            explanation: format!(
                "the {} rule asks for {other}, which this runner does not know how to recompute, \
                 so its verdict cannot be checked",
                rule.id,
            ),
        },
    }
}

/// Decides the convergence rule from the warmup and measured rates.
fn decide_decay(rule: &Rule, series: &[i64], warmup: Option<&[i64]>) -> Decision {
    let Some(decay) = rule.decay_percent else {
        return Decision {
            passed: false,
            explanation: format!(
                "the {} rule is decided on a decay and the campaign declares none",
                rule.id,
            ),
        };
    };
    let Some(warmup) = warmup else {
        return Decision {
            passed: false,
            explanation: format!(
                "the {} rule is decided against the warmup rate and no warmup series carried {}",
                rule.id, rule.metric,
            ),
        };
    };
    let (Some(early), Some(late)) = (rate(warmup), rate(series)) else {
        return Decision {
            passed: false,
            explanation: format!(
                "the {} rule is decided from a rate per cycle, and a window of {} warmup and {} \
                 measured samples spans too few cycle intervals to have one",
                rule.id,
                warmup.len(),
                series.len(),
            ),
        };
    };
    let passed = if early <= 0 {
        late <= 0
    } else {
        late.saturating_mul(100) <= early.saturating_mul(decay)
    };
    Decision {
        passed,
        explanation: format!(
            "{} grew at {early} millionths of a KiB per cycle across warmup and {late} across the \
             measured window, against a rule that the measured rate must be at most {decay}% of \
             the warmup rate",
            rule.metric,
        ),
    }
}

/// Returns the mean growth rate of a series, in millionths per cycle.
///
/// This is the runner's own reading of the definition the campaign scope
/// states: the rise from the window's first endpoint to its last, divided by
/// the number of cycle intervals between them. It is written from that
/// definition rather than shared with the campaign that produced the report,
/// because a recomputation that imported the producer's arithmetic would agree
/// with the producer by construction and could not contradict it. The two
/// implementations are held to the same declared vectors instead.
///
/// A window of `n` samples spans `n - 1` intervals. Fewer than two samples span
/// none, and no rate exists; `None` says so, rather than a zero that would be
/// indistinguishable from a genuinely flat window.
fn rate(series: &[i64]) -> Option<i64> {
    let intervals = match series.len() {
        0 | 1 => return None,
        length => i64::try_from(length - 1).unwrap_or(i64::MAX),
    };
    let (first, last) = (series[0], series[series.len() - 1]);
    Some(last.saturating_sub(first).saturating_mul(1_000_000) / intervals)
}

/// Requires the final drain to have joined everything and closed the pool.
fn reconcile_final_drain(observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    let result = observation
        .pointer("/final_drain/drain/result")
        .and_then(Value::as_str);
    if result != Some("complete") {
        violations.push(format!(
            "the final drain reported {} rather than joining every owned task",
            result.unwrap_or("nothing"),
        ));
    }
    let unjoined = observation
        .pointer("/final_drain/drain/unjoined_tasks")
        .and_then(Value::as_i64);
    if unjoined != Some(0) {
        violations.push(format!(
            "the final drain left {} unjoined task(s)",
            describe(unjoined.and_then(|count| u64::try_from(count).ok())),
        ));
    }
    if observation
        .pointer("/final_drain/repository_closed")
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(
            "the final drain did not close the repository, so the campaign never observed the \
             pool being given back"
                .to_owned(),
        );
    }
    // The authoritative connection evidence is taken while the pool is still
    // open, because a closed pool has no occupancy to read. An absent reading
    // is a violation rather than a pass: "nothing was checked out" and "nobody
    // looked" are different findings, and only one of them is evidence.
    match observation
        .pointer("/final_drain/pre_close_pool/in_use")
        .map(serde_json::Value::as_i64)
    {
        Some(Some(0)) => {}
        Some(Some(count)) => violations.push(format!(
            "{count} connection(s) were still checked out when the pool closed",
        )),
        Some(None) => violations
            .push("the final drain recorded a pool checkout state that is not a number".to_owned()),
        None => violations.push(
            "the final drain did not record the pool checkout state before closing, so nothing \
             in this report says the connections were given back"
                .to_owned(),
        ),
    }

    // The database's own view after the close, as corroboration on a different
    // side of the socket. Required to be present for the same reason.
    match observation
        .pointer("/final_drain/post_close/database_backends")
        .map(serde_json::Value::as_i64)
    {
        Some(Some(0)) => {}
        Some(Some(count)) => violations.push(format!(
            "the server still reported {count} backend(s) for this application after the pool \
             closed",
        )),
        Some(None) => violations
            .push("the final drain recorded a backend count that is not a number".to_owned()),
        None => violations.push(
            "the final drain did not record the server's backend count after closing".to_owned(),
        ),
    }

    violations
}

/// Counts the retained samples of one phase.
fn phase_count(observation: &Value, phase: &str) -> u64 {
    observation
        .get("samples")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|sample| sample.get("phase").and_then(Value::as_str) == Some(phase))
        .count() as u64
}

/// Reads one number out of the report's campaign summary.
fn campaign_number(observation: &Value, name: &str) -> Option<u64> {
    observation
        .pointer(&format!("/campaign/{name}"))
        .and_then(Value::as_u64)
}

/// Renders an absent number as words rather than as `None`.
fn describe(value: Option<u64>) -> String {
    value.map_or_else(|| "none it recorded".to_owned(), |value| value.to_string())
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
#[allow(
    clippy::too_many_lines,
    reason = "the retained record is one document, and splitting its construction would scatter \
              the fields the evidence contract names"
)]
fn write_report(
    root: &Path,
    scope: &Scope,
    fixtures: &BTreeMap<String, bool>,
    run: &Run,
    violations: &[String],
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let observation = run.observation.as_ref();
    let document = json!({
        "report": "soak",
        "campaign": "M5 PostgreSQL soak",
        "scenarios": [scope.report.name],
        "required_scenarios": [scope.report.name],
        "observed_scenarios": if run.outcome.as_deref() == Some("ok") && observation.is_some() {
            vec![scope.report.name.clone()]
        } else {
            Vec::new()
        },
        "environment": suite::environment(),
        "postgresql_version": observation.and_then(|value| value.get("server_version").cloned()),
        "postgresql_major_version": observation
            .and_then(|value| value.get("postgres_major_version").cloned()),
        "fixtures": fixtures,
        "claim": scope.claim,
        "declared_window": {
            "warmup_cycles": scope.warmup_cycles,
            "measured_cycles": scope.measured_cycles,
            "minimum_measured_samples": scope.minimum_measured_samples,
            "partitions_per_cycle": scope.partitions_per_cycle,
            "worker_budget": scope.worker_budget,
            "pool_size": scope.pool_size,
            "launches_per_cycle": scope.launches_per_cycle,
            "owned_tasks_per_drain": scope.owned_tasks_per_drain,
        },
        "observed_window": observation.and_then(|value| value.get("campaign").cloned()),
        "declared_observations": scope.observations,
        "declared_correctness": scope.correctness,
        "declared_growth_rules": scope
            .rules
            .iter()
            .map(|rule| json!({
                "id": rule.id,
                "metric": rule.metric,
                "rule": rule.decides,
                "decay_percent": rule.decay_percent,
            }))
            .collect::<Vec<_>>(),
        "report_result": run.outcome,
        "observation": observation,
        "out_of_scope": scope.excluded,
        "related": scope.related,
        "violations": violations,
        "passed": violations.is_empty(),
        "result": if violations.is_empty() { "passed" } else { "failed" },
        "notes": [
            "The report is run on its own so its result is attributable, and \
             the fixture is resolved before it starts, because a soak that \
             skipped for want of a database returns success.",
            "A passing report is not sufficient, and a soak has a failure mode \
             the other campaigns do not. Every other campaign's obligation \
             exists independently of its run — a ledger row, a commit phase, a \
             schema path, a declared ceiling — so a report either covered it or \
             did not. A soak's obligation is a period, and a soak that ran \
             three cycles and one that ran three hundred produce reports of \
             identical shape. The shorter one produces the flatter series and \
             therefore the more convincing result. So this runner reads the \
             committed denominator and requires the declared warmup and \
             measured windows to have run, with a sample per cycle.",
            "It also requires the workload to be the declared one, down to the \
             partition count, worker budget, and pool size, and requires the \
             lifecycle to have happened once per cycle: a fault injected, a \
             restart, a recovery, and a completed drain. A run that repeated a \
             plain launch would otherwise satisfy every window requirement and \
             produce the same flat series.",
            "Every declared correctness obligation must have been decided and \
             must hold before any resource number counts, because resource \
             flatness over a workload that stopped doing the work is not a \
             result. Every declared growth rule must have been decided, must \
             have applied the rule the campaign declares, must carry the series \
             it was decided from, and must have passed.",
            "What a passing run establishes is bounded by the declared window \
             and the declared workload. It is not a claim that no leak exists \
             over an unbounded period, and the durable history the database \
             accumulates on purpose is recorded beside the process series \
             rather than counted as growth.",
            "The report is required to name the PostgreSQL major it ran \
             against, because a matrix point is invisible in a connection \
             string and an observation from one supported major would otherwise \
             reconcile perfectly inside a run of another.",
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

/// The campaign's own invocation and what it reported.
#[derive(Default)]
struct Run {
    /// Whether the target process exited successfully.
    succeeded: bool,
    /// The outcome libtest reported for the scenario.
    outcome: Option<String>,
    /// The observation the report retained.
    observation: Option<Value>,
}

/// The committed campaign scope document.
struct Scope {
    /// Fixture name to the environment variables it requires.
    fixtures: BTreeMap<String, Vec<String>>,
    /// The one report the campaign delivers.
    report: Report,
    /// What a passing run does and does not establish, as declared.
    claim: String,
    /// Cycles run before any sample is eligible for a growth rule.
    warmup_cycles: u64,
    /// Cycles run inside the measured window.
    measured_cycles: u64,
    /// The fewest measured samples the campaign may pass with.
    minimum_measured_samples: u64,
    /// Partitions offered in each cycle.
    partitions_per_cycle: u64,
    /// Concurrent partition workers each cycle admits.
    worker_budget: u64,
    /// Connections the repository pool is opened with.
    pool_size: u64,
    /// Launches each cycle performs.
    launches_per_cycle: u64,
    /// Tasks each cycle's coordinator owns and must join.
    owned_tasks_per_drain: u64,
    /// The per-sample observations the campaign declares.
    observations: Vec<String>,
    /// The per-cycle durable obligations the campaign declares.
    correctness: Vec<String>,
    /// The growth rules the campaign declares.
    rules: Vec<Rule>,
    /// The campaign boundaries, as declared.
    excluded: Value,
    /// Evidence the campaign records and does not run, as declared.
    related: Value,
}

/// One growth rule, as declared.
struct Rule {
    /// The identity the report's verdict and this requirement share.
    id: String,
    /// The per-sample metric the rule is decided from.
    metric: String,
    /// How the metric's measured series decides the rule.
    decides: String,
    /// How far the late growth rate must fall below the early one, in percent,
    /// for a rule decided on convergence rather than on a level.
    decay_percent: Option<i64>,
}

/// The report the campaign delivers.
struct Report {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The fixture it needs.
    fixture: String,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("soak")
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

        let reports = array(&document, "reports")?;
        let report = reports
            .first()
            .ok_or_else(|| "the scope document declares no report".to_owned())?;
        if reports.len() != 1 {
            return Err(format!(
                "the soak campaign delivers one report and the scope declares {}",
                reports.len(),
            ));
        }
        let report = Report {
            package: suite::string(report, "package")?,
            target: suite::string(report, "target")?,
            name: suite::string(report, "name")?,
            fixture: suite::string(report, "fixture")?,
        };

        let workload = document
            .get("workload")
            .ok_or_else(|| "the scope document declares no workload".to_owned())?;
        let window = document
            .get("window")
            .ok_or_else(|| "the scope document declares no window".to_owned())?;
        let sampling = window
            .get("sampling")
            .ok_or_else(|| "the window declares no sampling".to_owned())?;
        let growth = document
            .get("growth_rules")
            .ok_or_else(|| "the scope document declares no growth rules".to_owned())?;

        let mut rules = Vec::new();
        for rule in array(growth, "rules")? {
            rules.push(Rule {
                id: suite::string(rule, "id")?,
                metric: suite::string(rule, "metric")?,
                decides: suite::string(rule, "rule")?,
                decay_percent: rule.get("decay_percent").and_then(Value::as_i64),
            });
        }

        let correctness = document
            .get("correctness")
            .ok_or_else(|| "the scope document declares no correctness checks".to_owned())?;
        let correctness = array(correctness, "checks")?
            .iter()
            .map(|check| suite::string(check, "id"))
            .collect::<Result<Vec<_>, _>>()?;
        let observations = array(&document, "observations")?
            .iter()
            .map(|observation| suite::string(observation, "id"))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .collect();

        Ok(Self {
            fixtures,
            report,
            claim: suite::string(&document, "claim")?,
            warmup_cycles: number(window, "warmup_cycles")?,
            measured_cycles: number(window, "measured_cycles")?,
            minimum_measured_samples: number(sampling, "minimum_measured_samples")?,
            partitions_per_cycle: number(workload, "partitions_per_cycle")?,
            worker_budget: number(workload, "worker_budget")?,
            pool_size: number(workload, "pool_size")?,
            launches_per_cycle: number(workload, "launches_per_cycle")?,
            owned_tasks_per_drain: number(workload, "owned_tasks_per_drain")?,
            observations,
            correctness,
            rules,
            excluded: document.get("out_of_scope").cloned().unwrap_or(Value::Null),
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

/// Reads one required count.
fn number(document: &Value, name: &str) -> Result<u64, String> {
    document
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("the scope document has no {name}"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use serde_json::{Value, json};

    use super::{Report, Rule, Scope};

    /// Builds a scope small enough to reason about, shaped like the real one.
    fn scope() -> Scope {
        Scope {
            fixtures: [("postgres-soak".to_owned(), vec!["URL".to_owned()])]
                .into_iter()
                .collect(),
            report: Report {
                package: "oxide-batch".to_owned(),
                target: "postgres_soak".to_owned(),
                name: "soak_reports_no_task_connection_handle_or_memory_growth".to_owned(),
                fixture: "postgres-soak".to_owned(),
            },
            claim: "bounded by the declared window".to_owned(),
            warmup_cycles: 2,
            measured_cycles: 12,
            minimum_measured_samples: 12,
            partitions_per_cycle: 16,
            worker_budget: 4,
            pool_size: 5,
            launches_per_cycle: 2,
            owned_tasks_per_drain: 4,
            observations: vec!["resident_kib".to_owned()],
            correctness: [
                "final-job-status",
                "final-step-status",
                "execution-counts",
                "partition-count",
                "partition-key-set",
                "partition-terminal-state",
                "restart-position",
                "committed-work-reused",
                "no-duplicate-durable-work",
                "no-missing-durable-work",
                "failure-not-forged",
                "recovery-semantics",
                "no-worker-outlives-its-parent",
                "drain-complete",
                "constant-repository-work",
            ]
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
            rules: vec![
                Rule {
                    id: "resident-memory-converges".to_owned(),
                    metric: "resident_kib".to_owned(),
                    decides: "warmup-relative-rate-decay".to_owned(),
                    decay_percent: Some(25),
                },
                Rule {
                    id: "connections-are-returned".to_owned(),
                    metric: "pool_connections_in_use".to_owned(),
                    decides: "every-measured-sample-equals-zero".to_owned(),
                    decay_percent: None,
                },
            ],
            excluded: Value::Null,
            related: Value::Null,
        }
    }

    /// Measured readings in the fixture window.
    ///
    /// Twelve rather than a handful. The convergence rule itself would be
    /// decided by two, but the mutations below have to be able to disturb a
    /// series without landing on one of its endpoints, and the correctness and
    /// chronology attacks need somewhere in the middle to attack. The
    /// campaign's own window is six hundred for a different reason, recorded
    /// with the window in the scope document.
    const MEASURED: usize = 12;

    /// The warmup readings: a process still settling steeply.
    ///
    /// The convergence rule compares the measured rate against this one, so a
    /// fixture whose warmup did not grow would leave the rule nothing to decay
    /// from and every series would fail it.
    const WARMUP: [i64; 2] = [100, 200];

    /// A converging resident series: it rises early and flattens.
    const RESIDENT: [i64; MEASURED] = [200, 204, 208, 212, 214, 215, 216, 216, 216, 216, 216, 216];

    /// A straight line — a leak of a fixed amount every cycle.
    const LEAKING: [i64; MEASURED] = [
        200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300,
    ];

    /// Flat through the window and rising only at the end.
    const LATE_RISE: [i64; MEASURED] = [200, 200, 200, 200, 200, 200, 200, 200, 200, 300, 400, 500];

    /// Partitions each fixture cycle offers.
    const PARTITIONS: usize = 16;

    /// The partition the fixture's fault is injected into.
    fn injected() -> String {
        format!("partition-{:04}", PARTITIONS - 1)
    }

    /// Builds one cycle's journal entry for a healthy run.
    ///
    /// The full lossless shape the report writes, because every attack below is
    /// a mutation of a valid run and a fixture that omitted a field would make
    /// the attacks pass for the wrong reason.
    fn cycle(index: usize, measured: bool) -> Value {
        let keys = (0..PARTITIONS)
            .map(|partition| format!("partition-{partition:04}"))
            .collect::<Vec<_>>();
        let counts = json!({
            "read": 0, "processed": 0, "written": 0,
            "filtered": 0, "committed": 0, "rolled_back": 0,
        });
        let partitions = keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    json!({
                        "status": "Completed",
                        "exit_status": "ExitStatus { code: ExitCode(\"COMPLETED\") }",
                        "counts": counts,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let invocations = keys
            .iter()
            .map(|key| {
                let runs = if *key == injected() { 2 } else { 1 };
                (key.clone(), json!(runs))
            })
            .collect::<serde_json::Map<_, _>>();

        json!({
            "cycle": index,
            "phase": if measured { "measured" } else { "warmup" },
            "elapsed_millis": 250,
            "failed_attempt": {
                "outcome": "Failed(Tasklet(Error))",
                "durable_status": "Failed",
                "injected_partition": injected(),
                "partitions_committed": keys[..PARTITIONS - 1].to_vec(),
                "fault_wait_expired": false,
            },
            "restart": {
                "new_execution_on_same_instance": true,
                "partitions_re_run": [injected()],
            },
            "terminal": {
                "outcome": "Completed",
                "job_status": "Completed",
                "job_exit_status": "ExitStatus { code: ExitCode(\"COMPLETED\") }",
                "parent_status": "Completed",
                "parent_exit_status": "ExitStatus { code: ExitCode(\"COMPLETED\") }",
                "parent_counts": counts,
                "step_executions": 2,
                "partitions": partitions,
            },
            "invocations": invocations,
            "repository_transactions": 108,
            "worker_peak_occupancy": 2,
            "worker_residue": 0,
            "drain": { "result": "complete", "unjoined_tasks": 0, "panicked_tasks": 0 },
            "durable_history_growth": {
                "instances": 1, "executions": 2, "step_executions": 19, "partitions": 32,
            },
        })
    }

    /// Builds the observation a healthy run of that scope would retain.
    fn observation() -> Value {
        let total = MEASURED + 2;
        let samples = (0..total)
            .map(|index| {
                let measured = index >= 2;
                json!({
                    "cycle": index,
                    "phase": if measured { "measured" } else { "warmup" },
                    "metrics": {
                        "resident_kib": if measured { RESIDENT[index - 2] } else { WARMUP[index] },
                        "pool_connections_in_use": 0,
                    },
                })
            })
            .collect::<Vec<_>>();
        let cycles = (0..total)
            .map(|index| cycle(index, index >= 2))
            .collect::<Vec<_>>();
        let checks = [
            "final-job-status",
            "final-step-status",
            "execution-counts",
            "partition-count",
            "partition-key-set",
            "partition-terminal-state",
            "restart-position",
            "committed-work-reused",
            "no-duplicate-durable-work",
            "no-missing-durable-work",
            "failure-not-forged",
            "recovery-semantics",
            "no-worker-outlives-its-parent",
            "drain-complete",
            "constant-repository-work",
        ]
        .iter()
        .map(|id| json!({ "id": id, "holds": true, "failing_cycles": [] }))
        .collect::<Vec<_>>();

        json!({
            "passed": true,
            "violations": [],
            "postgres_major_version": "18",
            "environment": { "pool": { "size": 5 } },
            "campaign": {
                "warmup_cycles": 2,
                "measured_cycles": MEASURED,
                "completed_cycles": total,
                "partitions_per_cycle": PARTITIONS,
                "worker_budget": 4,
                "launches_per_cycle": 2,
                "owned_tasks_per_drain": 4,
                "faults_injected": total,
                "restarts": total,
                "recoveries": total,
                "drains_completed": total,
                "partitions_executed": total * (PARTITIONS + 1),
                "pool_readings": 900,
            },
            "samples": samples,
            "cycles": cycles,
            "correctness": { "passed": true, "checks": checks },
            "growth": {
                "rules": [
                    {
                        "id": "resident-memory-converges",
                        "metric": "resident_kib",
                        "rule": "warmup-relative-rate-decay",
                        "decided": true,
                        "passed": true,
                        "series": RESIDENT,
                    },
                    {
                        "id": "connections-are-returned",
                        "metric": "pool_connections_in_use",
                        "rule": "every-measured-sample-equals-zero",
                        "decided": true,
                        "passed": true,
                        "series": vec![0; MEASURED],
                    },
                ],
            },
            "final_drain": {
                "pre_close_pool": { "connections": 5, "idle": 5, "in_use": 0 },
                "drain": { "result": "complete", "unjoined_tasks": 0 },
                "repository_closed": true,
                "post_close": { "database_backends": 0 },
            },
        })
    }

    /// Reconciles one observation against the scope above.
    ///
    /// The matrix point is passed rather than read from the environment, which
    /// is the point of it being a parameter: these tests run inside the
    /// conformance campaign, which sets `OXIDEBATCH_CAMPAIGN_MATRIX` to
    /// whichever major that job is covering.
    fn reconcile(observation: Value) -> Vec<String> {
        reconcile_at("18", observation)
    }

    /// Reconciles one observation as a run of a named matrix point.
    fn reconcile_at(matrix: &str, observation: Value) -> Vec<String> {
        super::reconcile(
            &scope(),
            &super::Run {
                succeeded: true,
                outcome: Some("ok".to_owned()),
                observation: Some(observation),
            },
            Some(matrix),
        )
    }

    /// Replaces one field of the observation, addressed by JSON pointer.
    fn with(pointer: &str, value: Value) -> Value {
        let mut observation = observation();
        *observation.pointer_mut(pointer).expect(pointer) = value;
        observation
    }

    #[test]
    fn a_healthy_run_reconciles() {
        assert_eq!(reconcile(observation()), Vec::<String>::new());
    }

    #[test]
    fn a_report_that_ran_against_another_major_is_rejected() {
        // A matrix point is invisible in a connection string, so an observation
        // from one supported major would otherwise reconcile inside a run of
        // another. The fixture names 18.
        let violations = reconcile_at("15", observation());
        assert!(
            violations.iter().any(|violation| violation
                .contains("ran against PostgreSQL 18 and this campaign run is 15")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_report_that_skipped_is_not_evidence() {
        let violations = super::reconcile(
            &scope(),
            &super::Run {
                succeeded: true,
                outcome: Some("ok".to_owned()),
                observation: None,
            },
            Some("18"),
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("retained no observation")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_shorter_window_is_not_the_declared_one() {
        // The failure this campaign is most exposed to: a run that did less
        // produces a flatter series and a more convincing report.
        let violations = reconcile(with("/campaign/measured_cycles", json!(2)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("declares 12 measured_cycles")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_run_with_fewer_samples_than_cycles_is_rejected() {
        let mut observation = observation();
        observation["samples"]
            .as_array_mut()
            .expect("samples")
            .pop();
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("13 samples were retained")),
            "{violations:?}",
        );
    }

    #[test]
    fn widening_warmup_after_the_fact_is_rejected() {
        let mut observation = observation();
        observation["samples"][2]["phase"] = json!("warmup");
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("declares 2 warmup samples")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_smaller_workload_is_a_different_campaign() {
        let violations = reconcile(with("/campaign/partitions_per_cycle", json!(2)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("partitions_per_cycle of 16")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_duplicate_sample_cycle() {
        let mut observation = observation();
        observation["samples"][7]["cycle"] = json!(6);
        observation["cycles"][7]["cycle"] = json!(6);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("cycle 6 is journalled more than once")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_missing_sample_cycle() {
        let mut observation = observation();
        observation["samples"]
            .as_array_mut()
            .expect("samples")
            .remove(5);
        observation["cycles"]
            .as_array_mut()
            .expect("cycles")
            .remove(5);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the declared window is 14 cycles")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_reordered_samples() {
        // Serialization order is not authority: the recorded index is, and it
        // has to agree with the position. A swapped pair moves which readings
        // are the window's endpoints without changing any value in it.
        let mut observation = observation();
        let samples = observation["samples"].as_array_mut().expect("samples");
        samples.swap(4, 9);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("are not in the order they were taken")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_reordered_cycles() {
        let mut observation = observation();
        let cycles = observation["cycles"].as_array_mut().expect("cycles");
        cycles.swap(3, 10);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("are not in the order they ran")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_shifted_warmup_boundary() {
        // Relabelling one measured cycle as warmup excludes it from every
        // growth rule while leaving the counts intact.
        let mut observation = observation();
        observation["samples"][2]["phase"] = json!("warmup");
        observation["cycles"][2]["phase"] = json!("warmup");
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the declared window makes it measured")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_wrong_cycle_phase() {
        let mut observation = observation();
        observation["samples"][3]["phase"] = json!("cooldown");
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("not a phase this campaign has")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_sample_cycle_phase_disagreement() {
        let mut observation = observation();
        observation["cycles"][1]["phase"] = json!("measured");
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("journalled as measured and sampled as")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_duplicate_cycle_evidence() {
        let mut observation = observation();
        let cycles = observation["cycles"].as_array_mut().expect("cycles");
        let duplicate = cycles[4].clone();
        cycles.push(duplicate);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the declared window is 14 cycles")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_broken_chronology_stops_evaluation() {
        // Nothing below the chronology check is meaningful once positions have
        // moved, so the run is not evaluated further rather than producing
        // verdicts about a different arrangement of the same data.
        let mut observation = observation();
        observation["cycles"]
            .as_array_mut()
            .expect("cycles")
            .swap(3, 10);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .all(|violation| !violation.contains("resident-memory-converges")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_forged_lifecycle_totals() {
        // The summary is a claim about the journal, so a summary that
        // disagrees with the journal is caught by the fold rather than read.
        for field in [
            "faults_injected",
            "restarts",
            "recoveries",
            "drains_completed",
            "partitions_executed",
        ] {
            let violations = reconcile(with(&format!("/campaign/{field}"), json!(0)));
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(&format!("summarises {field} as Some(0)"))),
                "{field}: {violations:?}",
            );
        }
    }

    #[test]
    fn rejects_raw_lifecycle_violation() {
        // A cycle that launched and never restarted. Every total still folds
        // and summarises in agreement, because the fold counts what the
        // journal says — so the lifecycle is required of each cycle, not of
        // the totals.
        let mut observation = observation();
        observation["cycles"][8]["restart"]["new_execution_on_same_instance"] = json!(false);
        observation["cycles"][8]["restart"]["partitions_re_run"] = json!([]);
        observation["campaign"]["recoveries"] = json!(13);
        observation["campaign"]["restarts"] = json!(13);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation
                    .contains("cycle 8 did not restart onto the same instance")),
            "{violations:?}",
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("14 cycles ran and the journal contains 13")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_metric_that_stopped_being_sampled_is_rejected() {
        let mut observation = observation();
        observation["samples"][4]["metrics"] = json!({});
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("resident_kib is a declared observation")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_declared_rule_with_no_verdict_is_rejected() {
        let violations = reconcile(with("/growth/rules", json!([])));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("decided nothing for it")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_verdict_decided_from_too_few_readings_is_rejected() {
        // Fewer measured samples than the campaign declares. The window checks
        // fire too; this asserts the growth rule refuses to be decided from
        // them independently of that.
        let mut observation = observation();
        let samples = observation["samples"].as_array_mut().expect("samples");
        samples.truncate(5);
        for rule in observation["growth"]["rules"]
            .as_array_mut()
            .expect("rules")
            .iter_mut()
        {
            let series = rule["series"].as_array_mut().expect("series");
            series.truncate(3);
        }
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the declared window is 14 cycles")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_verdict_that_applied_a_different_rule_is_rejected() {
        let violations = reconcile(with("/growth/rules/0/rule", json!("looks-fine-to-me")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the report applied looks-fine-to-me")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_failed_growth_rule_fails_the_campaign() {
        let violations = reconcile(with("/growth/rules/0/passed", json!(false)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("reports passed=false")),
            "{violations:?}",
        );
    }

    #[test]
    fn accepts_declared_rate_decay() {
        // The healthy shape on its own, so the rejections below cannot all be
        // passing for an unrelated reason.
        let violations = reconcile(observation());
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("resident-memory-converges")),
            "{violations:?}",
        );
    }

    /// Rewrites the observation so a metric carries the given measured series.
    fn with_series(metric: &str, rule: usize, series: [i64; MEASURED]) -> Value {
        let mut observation = observation();
        for (index, value) in series.iter().enumerate() {
            observation["samples"][index + 2]["metrics"][metric] = json!(value);
        }
        observation["growth"]["rules"][rule]["series"] = json!(series);
        observation
    }

    #[test]
    fn rejects_passed_true_for_leaking_series() {
        // The forged green this whole recomputation exists for: a straight
        // line reported as passing. The runner rebuilds the verdict from the
        // samples and disagrees.
        let violations = reconcile(with_series("resident_kib", 0, LEAKING));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("reports passed=true")
                    && violation.contains("gives false")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_flat_early_but_rising_late_memory() {
        // Zero early rate admits nothing above zero, or a process that only
        // starts growing halfway through the window would pass.
        let violations = reconcile(with_series("resident_kib", 0, LATE_RISE));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("resident-memory-converges")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_forged_pass_on_an_exact_count_rule() {
        let mut leaked = [0; MEASURED];
        leaked[7] = 1;
        let violations = reconcile(with_series("pool_connections_in_use", 1, leaked));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("connections-are-returned")
                    && violation.contains("non-zero at 1")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_growth_series_different_from_measured_samples() {
        // The verdict keeps a flattering series while the samples say
        // otherwise. Element-by-element comparison catches it.
        let mut observation = observation();
        observation["samples"][3]["metrics"]["resident_kib"] = json!(9999);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("is not the")
                    && violation.contains("measured samples carry")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_wrong_decay_threshold() {
        // A report that applied a laxer decay than the campaign declares.
        let violations = reconcile(with(
            "/growth/rules/0/rule",
            json!("warmup-relative-rate-decay-90"),
        ));
        assert!(
            violations
                .iter()
                .any(|violation| violation
                    .contains("the report applied warmup-relative-rate-decay-90")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_rule_duplicate_ids() {
        let mut observation = observation();
        let duplicate = observation["growth"]["rules"][0].clone();
        observation["growth"]["rules"]
            .as_array_mut()
            .expect("rules")
            .push(duplicate);
        let violations = reconcile(observation);
        assert!(
            violations.iter().any(|violation| violation.contains(
                "decided resident-memory-converges more than \
                                                     once"
            ) || violation.contains("more than once")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_forged_correctness_pass() {
        // The journal shows an obligation violated and the report holds it.
        let mut observation = observation();
        observation["cycles"][6]["terminal"]["job_status"] = json!("Failed");
        let violations = reconcile(observation);
        assert!(
            violations.iter().any(|violation| violation
                .contains("the report holds final-job-status to be Some(true)")
                && violation.contains("[6]")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_raw_cycle_that_violates_obligation_despite_pass_summary() {
        // Same journal violation, with the report's own summary also green.
        let mut observation = observation();
        observation["cycles"][9]["drain"]["unjoined_tasks"] = json!(1);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("drain-complete does not hold in cycle(s) [9]")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_wrong_failing_cycles() {
        // The obligation genuinely fails, and the report names the wrong cycle.
        let mut observation = observation();
        observation["cycles"][5]["worker_residue"] = json!(1);
        for check in observation["correctness"]["checks"]
            .as_array_mut()
            .expect("checks")
        {
            if check["id"] == json!("no-worker-outlives-its-parent") {
                check["holds"] = json!(false);
                check["failing_cycles"] = json!([11]);
            }
        }
        observation["correctness"]["passed"] = json!(false);
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("as failing in Some([11])")
                    && violation.contains("[5]")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_duplicate_correctness_id_even_when_last_is_true() {
        // Last-wins reading is the forgery: a false entry followed by a true
        // one for the same obligation.
        let mut observation = observation();
        let checks = observation["correctness"]["checks"]
            .as_array_mut()
            .expect("checks");
        checks.insert(
            0,
            json!({
                "id": "drain-complete", "holds": false, "failing_cycles": [3],
            }),
        );
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("decided drain-complete more than once")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_unexpected_correctness_id() {
        let mut observation = observation();
        observation["correctness"]["checks"]
            .as_array_mut()
            .expect("checks")
            .push(json!({ "id": "invented-obligation", "holds": true, "failing_cycles": [] }));
        let violations = reconcile(observation);
        assert!(
            violations.iter().any(|violation| violation.contains(
                "decided invented-obligation, which the campaign scope does not declare"
            )),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_missing_correctness_id() {
        let mut observation = observation();
        observation["correctness"]["checks"]
            .as_array_mut()
            .expect("checks")
            .retain(|check| check["id"] != json!("recovery-semantics"));
        let violations = reconcile(observation);
        assert!(
            violations.iter().any(|violation| violation.contains(
                "declares the recovery-semantics obligation and the report decided \
                          nothing"
            ) || violation
                .contains("recovery-semantics obligation and the report decided")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_incomplete_final_drain_fails_the_campaign() {
        let violations = reconcile(with("/final_drain/drain/result", json!("incomplete")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("the final drain reported incomplete")),
            "{violations:?}",
        );
    }

    #[test]
    fn accepts_zero_checkout_final_drain() {
        // The healthy shape, stated on its own so the rejections below cannot
        // all be passing for some unrelated reason.
        let violations = reconcile(observation());
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("checked out")
                    || violation.contains("backend")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_checked_out_connection_after_final_drain() {
        let violations = reconcile(with("/final_drain/pre_close_pool/in_use", json!(1)));
        assert!(
            violations.iter().any(|violation| violation
                .contains("1 connection(s) were still checked out when the pool closed")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_missing_final_connection_evidence() {
        // The hole this closes: an absent reading is not a zero. A report that
        // never looked and one that looked and found nothing checked out are
        // different findings, and only the second is evidence.
        let mut observation = observation();
        observation["final_drain"]
            .as_object_mut()
            .expect("final_drain")
            .remove("pre_close_pool");
        let violations = reconcile(observation);
        assert!(
            violations.iter().any(|violation| violation
                .contains("did not record the pool checkout state before closing")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_null_final_connection_evidence() {
        let violations = reconcile(with("/final_drain/pre_close_pool", json!(null)));
        assert!(
            violations.iter().any(|violation| violation
                .contains("did not record the pool checkout state before closing")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_unreadable_final_connection_evidence() {
        let violations = reconcile(with("/final_drain/pre_close_pool/in_use", json!("none")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("not a number")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_a_repository_that_was_not_closed() {
        let violations = reconcile(with("/final_drain/repository_closed", json!(false)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("did not close the repository")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_surviving_backends_after_the_pool_closed() {
        let violations = reconcile(with("/final_drain/post_close/database_backends", json!(3)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("still reported 3 backend(s)")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_missing_backend_evidence_after_the_pool_closed() {
        let mut observation = observation();
        observation["final_drain"]
            .as_object_mut()
            .expect("final_drain")
            .remove("post_close");
        let violations = reconcile(observation);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("did not record the server's backend count")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_run_that_took_no_pool_reading_is_rejected() {
        let violations = reconcile(with("/campaign/pool_readings", json!(0)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("took no pool reading")),
            "{violations:?}",
        );
    }
}

/// The declared rate vectors, applied to this runner's own arithmetic.
///
/// This runner exists to disagree with the report when the report is wrong, so
/// it recomputes the memory verdict rather than reading it. That only works if
/// the two arithmetics are genuinely separate, and they are: neither calls the
/// other and neither shares a helper with it. Separateness alone, though, does
/// not make them independent — both were written from the same description, and
/// when that description was ambiguous about the denominator both made the same
/// mistake and agreed with each other about a statistic neither was computing
/// correctly.
///
/// `tests/fixtures/soak/rate-vectors.json` is the repair. It states the
/// statistic and its answers outside both implementations, so agreement between
/// them is no longer self-certifying: each is checked against the declaration,
/// not against the other.
#[cfg(test)]
mod rate_vectors {
    #![allow(clippy::expect_used, clippy::panic)]

    use std::fs;

    use serde_json::Value;

    use super::{Rule, decide_decay, rate};

    /// Reads the declared vector document from the committed fixture.
    fn document() -> Value {
        let path = super::super::suite::workspace_root()
            .expect("the workspace root resolves")
            .join("tests")
            .join("fixtures")
            .join("soak")
            .join("rate-vectors.json");
        let text = fs::read_to_string(&path).expect("the declared rate vectors are committed");
        serde_json::from_str(&text).expect("the declared rate vectors parse")
    }

    /// Reads one declared series.
    fn series(vector: &Value, name: &str) -> Vec<i64> {
        vector[name]
            .as_array()
            .expect("the vector declares the series")
            .iter()
            .map(|value| value.as_i64().expect("a reading is an integer"))
            .collect()
    }

    /// The rule under test, at the decay the vectors declare.
    fn rule(decay: i64) -> Rule {
        Rule {
            id: "resident-memory-converges".to_owned(),
            metric: "resident_kib".to_owned(),
            decides: "warmup-relative-rate-decay".to_owned(),
            decay_percent: Some(decay),
        }
    }

    #[test]
    fn declared_rates_match_this_implementation() {
        let document = document();
        for vector in document["vectors"].as_array().expect("vectors") {
            let id = vector["id"].as_str().expect("the vector is named");
            for name in ["warmup", "measured"] {
                let declared = vector[format!("{name}_rate_micro")].as_i64();
                let computed = rate(&series(vector, name));
                assert_eq!(
                    computed, declared,
                    "{id}: the {name} window's declared rate is {declared:?} and this runner \
                     computes {computed:?}",
                );
            }
        }
    }

    #[test]
    fn declared_verdicts_match_this_implementation() {
        let document = document();
        let decay = document["decay_percent"].as_i64().expect("decay percent");
        for vector in document["vectors"].as_array().expect("vectors") {
            let id = vector["id"].as_str().expect("the vector is named");
            let expected = vector["passes"].as_bool().expect("the verdict is declared");
            let warmup = series(vector, "warmup");
            let decision = decide_decay(&rule(decay), &series(vector, "measured"), Some(&warmup));
            assert_eq!(
                decision.passed, expected,
                "{id}: the declared verdict is {expected} and this runner decided {} — {}",
                decision.passed, decision.explanation,
            );
        }
    }

    #[test]
    fn a_constant_leak_rates_the_same_in_windows_of_any_length() {
        let document = document();
        let decay = document["decay_percent"].as_i64().expect("decay percent");
        for case in document["constant_growth_property"]["cases"]
            .as_array()
            .expect("cases")
        {
            let slope = case["slope"].as_i64().expect("slope");
            let ramp = |samples: u64, origin: i64| -> Vec<i64> {
                let mut readings = Vec::new();
                let mut value = origin;
                for _ in 0..samples {
                    readings.push(value);
                    value += slope;
                }
                readings
            };
            let warmup = ramp(case["warmup_samples"].as_u64().expect("warmup"), 1_000);
            let measured = ramp(case["measured_samples"].as_u64().expect("measured"), 7_777);

            // A leak adds the same amount every cycle. Its rate is therefore a
            // property of the leak and not of the window watching it, and the
            // ratio of two such rates is one however differently sized the two
            // windows are. A sample-count denominator breaks that: it scales
            // each window's rate by its own length.
            let (early, late) = (rate(&warmup), rate(&measured));
            assert_eq!(
                early,
                Some(slope * 1_000_000),
                "warmup of {} samples",
                warmup.len()
            );
            assert_eq!(
                late, early,
                "windows of different length, same constant rise"
            );

            let decision = decide_decay(&rule(decay), &measured, Some(&warmup));
            assert!(
                !decision.passed,
                "a constant leak must fail at {decay}% — {}",
                decision.explanation,
            );
        }
    }
}
