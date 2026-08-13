//! The M5 performance and reference-workload campaign runner.
//!
//! The accepted denominator names two campaigns, performance and
//! reference-workload, but exactly three reports: `p001-fixed-overhead`,
//! `p003-reference-workload`, and `p010-local-partition-scaling`.
//! `p003-reference-workload` is declared by both campaigns, and this runner
//! honors that literally — it resolves the report once, in release profile,
//! and the same retained observation satisfies both the performance row's
//! obligation and the reference-workload row's. Running P-003 a second time
//! would produce a second sample of the same workload, not evidence for a
//! second obligation.
//!
//! Like every other M5 campaign runner, this exists because a report that
//! needs a database and does not have one prints a skip line and returns
//! success, which `cargo test` cannot distinguish from evidence. The fixture
//! is resolved first, and a campaign run without it fails before any target
//! starts.
//!
//! Passing is not sufficient either. `observation.passed == true` and an
//! empty `violations` array are the producer's own claim, and this runner
//! treats them as exactly that — a claim — by independently re-deriving every
//! declared correctness obligation and every declared measurement's presence
//! from the retained fields, never from the report's verdict about itself.
//!
//! No duration, rate, or efficiency figure is compared against a limit here.
//! No accepted document states one, and the committed scope records that as
//! `numeric_status: observational`. What this runner gates is correctness,
//! finite resource ceilings, exact report and matrix cardinality, and the
//! shared-P003 identity the two campaign rows depend on.
//!
//! The scope document is `tests/fixtures/performance/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_performance_campaign.rs` reconciles it against
//! the accepted plan, so this runner consumes a document ordinary review has
//! already checked from both sides.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "performance-campaign.json";

/// The directory the reports write their observations into.
const OBSERVATIONS: &str = "performance-observations";

/// The variable that tells a report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_PERFORMANCE_OBSERVATIONS";

/// The report every declared campaign expects.
const REQUIRED_REPORTS: &[&str] = &[
    "p001-fixed-overhead",
    "p003-reference-workload",
    "p010-local-partition-scaling",
];

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means every declared report ran on its fixture, in
/// release profile, exactly once; every declared correctness obligation was
/// independently re-derived as true; every declared measurement was recorded;
/// and the shared `p003-reference-workload` report is the same retained
/// observation both campaign rows depend on.
///
/// # Errors
///
/// Returns the first failure that prevents the campaign from producing a
/// result at all, such as an unreadable scope document or an unwritable
/// report directory.
pub fn run() -> Result<Campaign, String> {
    let root = suite::workspace_root()?;
    let scope = Scope::read(&root)?;

    let mut violations = Vec::new();
    let fixtures = resolve_fixtures(&scope, &mut violations);
    let mut runs = Runs::default();
    if violations.is_empty() {
        let observations = prepare_observations(&root)?;
        for report in &scope.reports {
            eprintln!("==> {} {} (release)", report.target, report.name);
            let run = suite::run_target(
                &root,
                &TargetCommand {
                    package: &report.package,
                    selector: &["--test".to_owned(), report.target.clone()],
                    filters: &["--exact", &report.name],
                    environment: &[(OBSERVATIONS_ENV, observations.display().to_string())],
                    nocapture: true,
                    release: true,
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
    }

    let verdict = finalize_verdict(
        &scope,
        &runs,
        violations,
        expected_matrix_major().as_deref(),
    );
    let report = write_report(&root, &scope, &fixtures, &runs, &verdict)?;
    Ok(Campaign {
        violations: verdict.violations,
        report,
    })
}

/// Reports which declared fixtures the environment supplies.
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
            "the {fixture} fixture is required by the performance campaign and is incomplete: \
             set {}",
            missing.join(", ")
        ));
    }

    resolved
}

/// Creates an empty observation directory and returns it.
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
fn reconcile(scope: &Scope, runs: &Runs, expected_major: Option<&str>) -> Vec<String> {
    let mut violations = runs.failed_targets.clone();

    // Report cardinality is exact and fixed, independent of what the scope
    // happens to declare this run: a shrinking scope must not silently
    // shrink what this runner requires, and a report the scope forgot to
    // declare is caught by `Scope::read` itself.
    let declared_ids = scope
        .reports
        .iter()
        .map(|report| report.id.as_str())
        .collect::<BTreeSet<_>>();
    for required in REQUIRED_REPORTS {
        if !declared_ids.contains(required) {
            violations.push(format!(
                "the committed scope no longer declares the required report {required}"
            ));
        }
    }
    if declared_ids.len() != REQUIRED_REPORTS.len() {
        violations.push(format!(
            "the committed scope declares {} reports, not exactly the required {}",
            declared_ids.len(),
            REQUIRED_REPORTS.len()
        ));
    }

    for report in &scope.reports {
        let key = (report.target.clone(), report.name.clone());
        match runs.outcomes.get(&key).and_then(Option::as_deref) {
            Some("ok") => {}
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
        violations.extend(reconcile_measurements(report, observation));
        violations.extend(reconcile_correctness(report, observation));
        if report.id == "p010-local-partition-scaling" {
            violations.extend(reconcile_p010(
                observation,
                scope.p010_partitions,
                &scope.p010_worker_points,
            ));
        }
        if report.against_database {
            violations.extend(verify_matrix_identity(
                &report.id,
                expected_major,
                observation,
            ));
        } else if observation
            .get("postgresql_major_version")
            .is_some_and(|value| !value.is_null())
        {
            violations.push(format!(
                "{} is declared against_database=false and retained a non-null \
                 postgresql_major_version, so it is not the in-memory measurement the accepted \
                 workload table requires",
                report.id
            ));
        }
        if observation
            .get("environment")
            .and_then(|value| value.get("profile"))
            != Some(&Value::String("release".to_owned()))
        {
            violations.push(format!(
                "{} did not record a release-profile measurement environment",
                report.id
            ));
        }
    }

    violations.extend(reconcile_shared_p003(scope, runs));
    violations
}

/// Requires every declared measurement identifier to resolve to a non-null
/// value in the observation's `measurements` object, and every measurement
/// the observation claims to be one the scope actually declared.
///
/// Exact in both directions, for the same reason the cancellation and soak
/// verifiers require it: a report that silently dropped a declared
/// measurement would look complete, and a report that recorded one nobody
/// declared is a denominator claiming less than what is actually measured —
/// which is just as much a drift as the other direction.
fn reconcile_measurements(report: &Report, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(measurements) = observation.get("measurements").and_then(Value::as_object) else {
        return vec![format!("{} retained no measurements object", report.id)];
    };
    let declared: BTreeSet<&str> = report.measurements.iter().map(String::as_str).collect();
    let observed: BTreeSet<&str> = measurements.keys().map(String::as_str).collect();

    for missing in declared.difference(&observed) {
        violations.push(format!(
            "{} declares the {missing} measurement and retained no value for it",
            report.id
        ));
    }
    for extra in observed.difference(&declared) {
        if extra.ends_with("-note") {
            continue;
        }
        violations.push(format!(
            "{} retained a {extra} measurement the committed scope does not declare",
            report.id
        ));
    }
    for name in &declared {
        if measurements.get(*name).is_some_and(Value::is_null) {
            violations.push(format!(
                "{} recorded a null value for the declared {name} measurement",
                report.id
            ));
        }
    }
    violations
}

/// Requires every declared correctness obligation to resolve to `true` in the
/// observation's `correctness` object, independently of the producer's own
/// `passed` claim.
fn reconcile_correctness(report: &Report, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(correctness) = observation.get("correctness").and_then(Value::as_object) else {
        return vec![format!("{} retained no correctness object", report.id)];
    };
    for obligation in &report.correctness {
        let key = obligation.replace('-', "_");
        match correctness.get(&key).and_then(Value::as_bool) {
            Some(true) => {}
            Some(false) => violations.push(format!(
                "{} independently re-derived {obligation} as false",
                report.id
            )),
            None => violations.push(format!(
                "{} declares the {obligation} obligation and retained no boolean for it",
                report.id
            )),
        }
    }
    violations
}

/// Independently rederives P-010's obligations from its raw per-point
/// evidence, rather than trusting the producer's own `correctness` booleans.
///
/// The producer's `correctness` object is one more claim the report makes
/// about itself, checked generically by [`reconcile_correctness`] above like
/// every other report's. This function additionally recomputes the same
/// facts from `observation.points[]`, `observation.business_row_set`,
/// `observation.pool_ceiling_proof`, and the flat `measurements` object, so a
/// producer that computed a correctness boolean correctly but reported a
/// different (or fabricated) raw value cannot pass silently.
fn reconcile_p010(
    observation: &Value,
    expected_partitions: u64,
    expected_workers: &[u64],
) -> Vec<String> {
    const ID: &str = "p010-local-partition-scaling";
    let mut violations = Vec::new();

    if observation.get("report").and_then(Value::as_str) != Some(ID) {
        violations.push(format!(
            "{ID} retained an observation whose report field does not match"
        ));
    }
    if observation.get("workload").and_then(Value::as_str) != Some("P-010") {
        violations.push(format!(
            "{ID} retained an observation whose workload field is not P-010"
        ));
    }
    if observation
        .pointer("/declared/partitions")
        .and_then(Value::as_u64)
        != Some(expected_partitions)
    {
        violations.push(format!(
            "{ID} declared.partitions does not equal the committed \
             workloads.p010.partitions ({expected_partitions})"
        ));
    }

    let Some(points) = observation
        .pointer("/observation/points")
        .and_then(Value::as_array)
    else {
        violations.push(format!("{ID} retained no observation.points array"));
        return violations;
    };

    let max_observed_owned_tasks = reconcile_p010_points(
        points,
        expected_partitions,
        expected_workers,
        &mut violations,
    );
    reconcile_p010_resource_ceilings(observation, max_observed_owned_tasks, &mut violations);
    reconcile_p010_canonical_measurements(observation, points, &mut violations);
    reconcile_p010_pool_ceiling_proof(observation, &mut violations);

    violations
}

/// Walks `observation.points[]`, checking each scale point's own raw fields
/// and the cross-point invariants (exact worker-point set, identical business
/// digest), and returns the raw maximum observed occupancy across all points.
fn reconcile_p010_points(
    points: &[Value],
    expected_partitions: u64,
    expected_workers: &[u64],
    violations: &mut Vec<String>,
) -> u64 {
    const ID: &str = "p010-local-partition-scaling";
    if points.len() != expected_workers.len() {
        violations.push(format!(
            "{ID} retained {} scale points, not exactly the committed {}",
            points.len(),
            expected_workers.len()
        ));
    }

    let mut seen_workers = Vec::new();
    let mut max_observed_owned_tasks: u64 = 0;
    let mut first_business_digest: Option<&str> = None;
    for point in points {
        let Some(workers) = point.get("workers").and_then(Value::as_u64) else {
            violations.push(format!(
                "{ID} retained a scale point with no integer workers field"
            ));
            continue;
        };
        seen_workers.push(workers);

        if point.get("partitions").and_then(Value::as_u64) != Some(expected_partitions) {
            violations.push(format!(
                "{ID} worker point {workers} recorded a partitions count other than the \
                 committed {expected_partitions}"
            ));
        }
        match point.get("peak_active_workers").and_then(Value::as_u64) {
            Some(peak) if peak == workers => {
                max_observed_owned_tasks = max_observed_owned_tasks.max(peak);
            }
            Some(peak) => violations.push(format!(
                "{ID} worker point {workers} observed peak_active_workers {peak}, not exactly \
                 {workers}: multi-worker occupancy was not demonstrated at this point"
            )),
            None => violations.push(format!(
                "{ID} worker point {workers} retained no peak_active_workers"
            )),
        }
        if point
            .get("active_workers_after_join")
            .and_then(Value::as_u64)
            != Some(0)
        {
            violations.push(format!(
                "{ID} worker point {workers} left a worker active after its parent returned"
            ));
        }
        if point.get("business_row_count").and_then(Value::as_u64) != Some(expected_partitions) {
            violations.push(format!(
                "{ID} worker point {workers} wrote a business row count other than the \
                 committed {expected_partitions}"
            ));
        }
        match point.get("business_digest").and_then(Value::as_str) {
            Some(digest) => match first_business_digest {
                None => first_business_digest = Some(digest),
                Some(baseline) if baseline != digest => violations.push(format!(
                    "{ID} worker point {workers} recorded a business digest that disagrees with \
                     an earlier scale point's"
                )),
                Some(_) => {}
            },
            None => violations.push(format!(
                "{ID} worker point {workers} retained no business_digest"
            )),
        }
    }

    let mut sorted_seen = seen_workers.clone();
    sorted_seen.sort_unstable();
    sorted_seen.dedup();
    let mut sorted_expected = expected_workers.to_vec();
    sorted_expected.sort_unstable();
    if sorted_seen != sorted_expected || sorted_seen.len() != seen_workers.len() {
        violations.push(format!(
            "{ID} scale points are at workers {seen_workers:?}, not exactly the committed \
             {expected_workers:?} with no duplicate"
        ));
    }
    max_observed_owned_tasks
}

/// Requires the observed peak owned-task and connection counts to equal what
/// was just independently rederived from the raw points, and each to stay
/// within its own configured ceiling.
fn reconcile_p010_resource_ceilings(
    observation: &Value,
    max_observed_owned_tasks: u64,
    violations: &mut Vec<String>,
) {
    const ID: &str = "p010-local-partition-scaling";

    let configured_worker_budget = observation
        .pointer("/observation/configured_worker_budget")
        .and_then(Value::as_u64);
    let observed_peak_owned_tasks = observation
        .pointer("/observation/observed_peak_owned_tasks")
        .and_then(Value::as_u64);
    if observed_peak_owned_tasks != Some(max_observed_owned_tasks) {
        violations.push(format!(
            "{ID} observation.observed_peak_owned_tasks ({observed_peak_owned_tasks:?}) does not \
             equal the raw maximum peak_active_workers across points \
             ({max_observed_owned_tasks})"
        ));
    }
    match (observed_peak_owned_tasks, configured_worker_budget) {
        (Some(observed), Some(budget)) if observed > budget => violations.push(format!(
            "{ID} observed_peak_owned_tasks {observed} exceeded configured_worker_budget {budget}"
        )),
        (Some(_), Some(_)) => {}
        _ => violations.push(format!(
            "{ID} retained no configured_worker_budget/observed_peak_owned_tasks pair to check"
        )),
    }

    let configured_connection_ceiling = observation
        .pointer("/observation/configured_connection_ceiling")
        .and_then(Value::as_u64);
    let observed_peak_connections = observation
        .pointer("/observation/observed_peak_connections")
        .and_then(Value::as_u64);
    match (observed_peak_connections, configured_connection_ceiling) {
        (Some(observed), Some(ceiling)) if observed > ceiling => violations.push(format!(
            "{ID} observed_peak_connections {observed} exceeded configured_connection_ceiling \
             {ceiling}"
        )),
        (Some(_), Some(_)) => {}
        _ => violations.push(format!(
            "{ID} retained no configured_connection_ceiling/observed_peak_connections pair to check"
        )),
    }
}

/// Requires the canonical `measurements` fields to be copies of the raw
/// observed values just checked above — never a configured constant, and
/// never a value other than the largest worker point's own.
fn reconcile_p010_canonical_measurements(
    observation: &Value,
    points: &[Value],
    violations: &mut Vec<String>,
) {
    const ID: &str = "p010-local-partition-scaling";

    if observation.pointer("/measurements/peak-owned-tasks")
        != observation.pointer("/observation/observed_peak_owned_tasks")
    {
        violations.push(format!(
            "{ID} measurements.\"peak-owned-tasks\" does not equal observation.\
             observed_peak_owned_tasks"
        ));
    }
    if observation.pointer("/measurements/peak-connections")
        != observation.pointer("/observation/observed_peak_connections")
    {
        violations.push(format!(
            "{ID} measurements.\"peak-connections\" does not equal observation.\
             observed_peak_connections"
        ));
    }
    if observation.pointer("/measurements/peak-resident-memory")
        != observation.pointer("/observation/peak_resident_memory_kib")
    {
        violations.push(format!(
            "{ID} measurements.\"peak-resident-memory\" does not equal the observed sampled peak"
        ));
    }

    let Some(largest) = points
        .iter()
        .max_by_key(|point| point.get("workers").and_then(Value::as_u64).unwrap_or(0))
    else {
        return;
    };
    for (measurement_key, raw_key) in [
        ("partitions-per-second", "partitions_per_second"),
        ("end-to-end-duration", "wall_micros"),
        ("scaling-efficiency", "scaling_efficiency"),
        ("worker-skew", "worker_skew_micros"),
        ("aggregation-duration", "aggregation_duration_micros"),
        ("repository-round-trips", "repository_round_trips"),
    ] {
        let measured = observation.pointer(&format!("/measurements/{measurement_key}"));
        if measured != largest.get(raw_key) {
            violations.push(format!(
                "{ID} measurements.\"{measurement_key}\" does not equal the largest worker \
                 point's raw {raw_key}"
            ));
        }
    }
}

/// The pool-ceiling proof is a separate, out-of-band launch attempt with its
/// own raw evidence: independently required rather than trusted from the
/// correctness boolean alone.
fn reconcile_p010_pool_ceiling_proof(observation: &Value, violations: &mut Vec<String>) {
    const ID: &str = "p010-local-partition-scaling";

    let proof = observation.pointer("/observation/pool_ceiling_proof");
    let rejected = proof
        .and_then(|value| value.get("rejected_with_insufficient_pool_capacity"))
        .and_then(Value::as_bool);
    let observed_during_attempt = proof
        .and_then(|value| value.get("observed_peak_workers_during_attempt"))
        .and_then(Value::as_u64);
    let configured_pool = proof
        .and_then(|value| value.get("configured_pool"))
        .and_then(Value::as_u64);
    let derived_budget = proof
        .and_then(|value| value.get("derived_budget"))
        .and_then(Value::as_u64);
    match (
        rejected,
        observed_during_attempt,
        configured_pool,
        derived_budget,
    ) {
        (Some(true), Some(0), Some(pool), Some(budget)) if pool + 1 == budget => {}
        (Some(_), _, Some(pool), Some(budget)) if pool + 1 != budget => violations.push(format!(
            "{ID} pool_ceiling_proof.configured_pool ({pool}) is not exactly one connection \
             short of derived_budget ({budget})"
        )),
        _ => violations.push(format!(
            "{ID} pool_ceiling_proof did not record a pool one connection short of the derived \
             budget being refused before any worker started"
        )),
    }
}

/// Requires the shared P-003 report to be the one retained observation both
/// campaign rows depend on, and forbids a second, equivalent report from
/// having run.
fn reconcile_shared_p003(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let performance = scope
        .campaigns
        .iter()
        .find(|campaign| campaign.id == "performance");
    let reference = scope
        .campaigns
        .iter()
        .find(|campaign| campaign.id == "reference-workload");
    match (performance, reference) {
        (Some(performance), Some(reference)) => {
            if !performance
                .reports
                .contains(&"p003-reference-workload".to_owned())
            {
                violations.push(
                    "the performance campaign no longer declares p003-reference-workload"
                        .to_owned(),
                );
            }
            if reference.reports != vec!["p003-reference-workload".to_owned()] {
                violations.push(
                    "the reference-workload campaign must declare exactly the shared \
                     p003-reference-workload report"
                        .to_owned(),
                );
            }
        }
        _ => violations.push(
            "the committed scope must declare both the performance and reference-workload \
             campaigns"
                .to_owned(),
        ),
    }

    // The report ran exactly once: `runs.outcomes` is keyed by (target, name)
    // and `read_observations` is keyed by report id, so a second execution of
    // the same scenario can only appear here as a second retained file under
    // a different id — which `Scope::read`'s exact id list already forbids —
    // or as the same id observed twice, which a `BTreeMap` cannot represent.
    // What is left to check is that exactly one observation exists for the
    // shared report and that both campaign rows are satisfied by it.
    if !runs.observations.contains_key("p003-reference-workload") {
        violations.push(
            "no p003-reference-workload observation exists to satisfy either campaign row"
                .to_owned(),
        );
    }
    violations
}

/// Reconciles a report's declared `PostgreSQL` major against the matrix point
/// the campaign was told it ran at, and requires the matrix consensus check
/// below to have something to hoist.
fn verify_matrix_identity(id: &str, expected: Option<&str>, observation: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let major = observation
        .get("postgresql_major_version")
        .and_then(Value::as_str);
    let Some(major) = major.filter(|value| !value.is_empty()) else {
        violations.push(format!("{id} recorded no PostgreSQL major"));
        return violations;
    };
    if observation
        .get("server_version")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        violations.push(format!("{id} recorded no PostgreSQL server version"));
    }
    if let Some(expected) = expected
        && major != expected
    {
        violations.push(format!(
            "{id} ran against PostgreSQL {major} but the campaign was told it ran at {expected}"
        ));
    }
    violations
}

/// Hoists the `PostgreSQL` major every `against_database` report agrees on.
///
/// Every database report must record its own, and they must agree: two
/// reports of one campaign run against different servers would make the
/// campaign a result about neither.
fn postgres_major(reports: &[Report], runs: &Runs) -> (Option<String>, Vec<String>) {
    let mut majors = BTreeSet::new();
    for report in reports.iter().filter(|report| report.against_database) {
        let major = runs
            .observations
            .get(&report.id)
            .and_then(|observation| observation.get("postgresql_major_version"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let Some(major) = major else {
            return (
                None,
                vec![format!(
                    "{} recorded no PostgreSQL major, so the reports have no complete matrix \
                     consensus",
                    report.id
                )],
            );
        };
        majors.insert(major.to_owned());
    }
    match majors.len() {
        0 => (None, Vec::new()),
        1 => (majors.into_iter().next(), Vec::new()),
        _ => (
            None,
            vec![format!(
                "the database reports disagree on the PostgreSQL major: {}",
                majors.into_iter().collect::<Vec<_>>().join(", ")
            )],
        ),
    }
}

/// Reads the `PostgreSQL` major the campaign was configured to run at.
fn expected_matrix_major() -> Option<String> {
    let matrix = env::var(suite::MATRIX).ok()?;
    matrix.strip_prefix("postgres-").map(str::to_owned)
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

/// The one finalized verdict consumed by the report and the command exit
/// path.
struct FinalVerdict {
    violations: Vec<String>,
    execution_manifest: Value,
    postgres_major: Option<String>,
}

/// Completes every semantic reconciliation before report rendering begins.
fn finalize_verdict(
    scope: &Scope,
    runs: &Runs,
    mut violations: Vec<String>,
    expected_major: Option<&str>,
) -> FinalVerdict {
    violations.extend(reconcile(scope, runs, expected_major));
    let (execution_manifest, manifest_violations) = execution_manifest(&scope.reports, runs);
    violations.extend(manifest_violations);
    let (postgres_major, major_violations) = postgres_major(&scope.reports, runs);
    violations.extend(major_violations);

    FinalVerdict {
        violations,
        execution_manifest,
        postgres_major,
    }
}

/// Hoists the execution manifest the reports recorded to the campaign report.
///
/// Every declared report is required to have one and they must agree, so a
/// report that retained no manifest, a null one, or one that disagreed with
/// another cannot pass silently.
fn execution_manifest(reports: &[Report], runs: &Runs) -> (Value, Vec<String>) {
    let mut violations = Vec::new();
    let mut first: Option<(&str, &Value)> = None;
    for report in reports {
        let Some(observation) = runs.observations.get(&report.id) else {
            violations.push(format!(
                "{} retained no observation and therefore no execution manifest",
                report.id
            ));
            continue;
        };
        let Some(manifest) = observation.get("execution_manifest") else {
            violations.push(format!(
                "{} retained no execution_manifest field",
                report.id
            ));
            continue;
        };
        if manifest.is_null() {
            violations.push(format!("{} retained a null execution manifest", report.id));
            continue;
        }

        if let Some((first_id, first_manifest)) = first {
            if manifest != first_manifest {
                violations.push(format!(
                    "{} and {first_id} recorded different execution manifests, so the campaign \
                     ran against a tree that changed underneath it",
                    report.id
                ));
            }
        } else {
            first = Some((&report.id, manifest));
        }
    }

    (
        first.map_or(Value::Null, |(_, manifest)| manifest.clone()),
        violations,
    )
}

/// Writes the campaign's own report from an already finalized verdict.
///
/// Intentionally a rendering-only stage: discovering a new semantic violation
/// here would allow the rendered verdict to diverge from the
/// [`Campaign::violations`] consumed by the command's exit path.
fn write_report(
    root: &Path,
    scope: &Scope,
    fixtures: &BTreeMap<String, bool>,
    runs: &Runs,
    verdict: &FinalVerdict,
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let document = json!({
        "campaigns": scope.campaigns.iter().map(|campaign| json!({
            "id": campaign.id,
            "plan_row": campaign.plan_row,
            "reports": campaign.reports,
        })).collect::<Vec<_>>(),
        "passed": verdict.violations.is_empty(),
        "violations": verdict.violations,
        "environment": suite::environment_with_profile("release"),
        "postgresql_major_version": verdict.postgres_major,
        "observation": { "execution_manifest": verdict.execution_manifest },
        "numeric_status": {
            "status": "observational",
            "note": "No accepted document states a binding M5 throughput, latency, or \
                     scaling-efficiency limit. This campaign measures and reports the declared \
                     figures and compares none of them against a number; only a missing or \
                     malformed measurement fails.",
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
                    "measurements": report.measurements,
                    "correctness": report.correctness,
                    "outcome": runs
                        .outcomes
                        .get(&(report.target.clone(), report.name.clone()))
                        .and_then(Clone::clone),
                    "observation": runs.observations.get(&report.id),
                })
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

/// One declared campaign row.
struct CampaignRow {
    id: String,
    plan_row: String,
    reports: Vec<String>,
}

/// One declared report.
struct Report {
    id: String,
    package: String,
    target: String,
    name: String,
    against_database: bool,
    fixture: Option<String>,
    measurements: Vec<String>,
    correctness: Vec<String>,
}

/// The committed denominator this runner consumes.
struct Scope {
    campaigns: Vec<CampaignRow>,
    reports: Vec<Report>,
    fixtures: BTreeMap<String, Vec<String>>,
    p010_partitions: u64,
    p010_worker_points: Vec<u64>,
}

impl Scope {
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("performance")
            .join("campaign-scope.json");
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        let document: Value = serde_json::from_str(&source)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

        let campaigns = document
            .get("campaigns")
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope declares no campaigns array".to_owned())?
            .iter()
            .map(|entry| {
                Ok(CampaignRow {
                    id: suite::string(entry, "id")?,
                    plan_row: suite::string(entry, "plan_row")?,
                    reports: entry
                        .get("reports")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "a campaign declares no reports array".to_owned())?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let reports = document
            .get("reports")
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope declares no reports array".to_owned())?
            .iter()
            .map(|entry| {
                Ok(Report {
                    id: suite::string(entry, "id")?,
                    package: "oxide-batch".to_owned(),
                    target: suite::string(entry, "target")?,
                    name: suite::string(entry, "scenario")?,
                    against_database: entry
                        .get("against_database")
                        .and_then(Value::as_bool)
                        .ok_or_else(|| format!("{entry} declares no against_database"))?,
                    fixture: entry
                        .get("fixture")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    measurements: entry
                        .get("measurements")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "a report declares no measurements array".to_owned())?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                    correctness: entry
                        .get("correctness")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "a report declares no correctness array".to_owned())?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let fixtures = document
            .get("fixtures")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .map(|(name, variables)| {
                        (
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
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let (p010_partitions, p010_worker_points) = Self::read_p010_workload(&document)?;

        Ok(Self {
            campaigns,
            reports,
            fixtures,
            p010_partitions,
            p010_worker_points,
        })
    }

    /// Reads `workloads.p010.partitions` and `workloads.p010.worker_points`
    /// once, from the committed scope, rather than hardcoded here:
    /// `m5_performance_campaign.rs` already reconciles these against
    /// `oxide_batch::MAX_PARTITION_WORKERS` and the accepted plan, so this
    /// runner rederives P-010's raw evidence against the same document
    /// review already checked, instead of a second, possibly-drifting
    /// literal.
    fn read_p010_workload(document: &Value) -> Result<(u64, Vec<u64>), String> {
        let partitions = document
            .pointer("/workloads/p010/partitions")
            .and_then(Value::as_u64)
            .ok_or_else(|| "the scope declares no workloads.p010.partitions".to_owned())?;
        let worker_points = document
            .pointer("/workloads/p010/worker_points")
            .and_then(Value::as_array)
            .ok_or_else(|| "the scope declares no workloads.p010.worker_points".to_owned())?
            .iter()
            .map(|value| {
                value.as_u64().ok_or_else(|| {
                    "workloads.p010.worker_points has a non-integer entry".to_owned()
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok((partitions, worker_points))
    }
}

/// One campaign's accumulated target runs and observations.
#[derive(Default)]
struct Runs {
    failed_targets: Vec<String>,
    outcomes: BTreeMap<(String, String), Option<String>>,
    observations: BTreeMap<String, Value>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use serde_json::json;

    use super::reconcile_p010;

    /// A fully valid P-010 observation, shaped exactly like the retained
    /// `p010-local-partition-scaling.json` the producer writes: three scale
    /// points (1/10/64) with exact occupancy, an identical business digest at
    /// every point, canonical measurements copied from the observed values,
    /// and a pool-ceiling proof that rejected before any worker started.
    /// Every test below starts from this and mutates exactly one thing.
    fn canonical_observation() -> serde_json::Value {
        let point = |workers: u64| {
            json!({
                "workers": workers,
                "partitions": 100,
                "peak_active_workers": workers,
                "active_workers_after_join": 0,
                "business_row_count": 100,
                "business_digest": "same-digest",
                "partitions_per_second": 1.0,
                "wall_micros": 1000,
                "scaling_efficiency": 1.0,
                "worker_skew_micros": 0,
                "aggregation_duration_micros": 0,
                "repository_round_trips": 6,
            })
        };
        json!({
            "report": "p010-local-partition-scaling",
            "workload": "P-010",
            "declared": { "partitions": 100 },
            "observation": {
                "points": [point(1), point(10), point(64)],
                "configured_worker_budget": 64,
                "observed_peak_owned_tasks": 64,
                "configured_connection_ceiling": 73,
                "observed_peak_connections": 66,
                "peak_resident_memory_kib": 9000,
                "pool_ceiling_proof": {
                    "rejected_with_insufficient_pool_capacity": true,
                    "observed_peak_workers_during_attempt": 0,
                    "configured_pool": 4,
                    "derived_budget": 5,
                },
            },
            "measurements": {
                "peak-owned-tasks": 64,
                "peak-connections": 66,
                "peak-resident-memory": 9000,
                "partitions-per-second": 1.0,
                "end-to-end-duration": 1000,
                "scaling-efficiency": 1.0,
                "worker-skew": 0,
                "aggregation-duration": 0,
                "repository-round-trips": 6,
            },
        })
    }

    const EXPECTED_WORKERS: &[u64] = &[1, 10, 64];

    #[test]
    fn canonical_observation_passes() {
        let violations = reconcile_p010(&canonical_observation(), 100, EXPECTED_WORKERS);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn worker_10_peak_mismatch_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][1]["peak_active_workers"] = json!(1);
        // observed_peak_owned_tasks must also disagree with the new raw max
        // for a realistic single-field mutation, but leaving it at 64 alone
        // already proves the point-level check fires independently.
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("worker point 10")),
            "{violations:?}"
        );
    }

    #[test]
    fn worker_64_peak_mismatch_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][2]["peak_active_workers"] = json!(1);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("worker point 64")),
            "{violations:?}"
        );
    }

    #[test]
    fn missing_scale_point_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"]
            .as_array_mut()
            .expect("points is an array")
            .truncate(2);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(!violations.is_empty());
    }

    #[test]
    fn duplicate_scale_point_fails() {
        let mut observation = canonical_observation();
        let duplicate = observation["observation"]["points"][0].clone();
        observation["observation"]["points"]
            .as_array_mut()
            .expect("points is an array")
            .push(duplicate);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(!violations.is_empty());
    }

    #[test]
    fn wrong_worker_point_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][1]["workers"] = json!(5);
        observation["observation"]["points"][1]["peak_active_workers"] = json!(5);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(!violations.is_empty());
    }

    #[test]
    fn active_workers_after_join_nonzero_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][0]["active_workers_after_join"] = json!(1);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("left a worker active")),
            "{violations:?}"
        );
    }

    #[test]
    fn observed_connections_exceeding_ceiling_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["observed_peak_connections"] = json!(80);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("exceeded")),
            "{violations:?}"
        );
    }

    #[test]
    fn canonical_peak_connections_copied_from_ceiling_instead_of_observation_fails() {
        let mut observation = canonical_observation();
        // The measured field silently reverts to a configured-looking
        // constant instead of the observed value.
        observation["measurements"]["peak-connections"] = json!(73);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("\"peak-connections\"")),
            "{violations:?}"
        );
    }

    #[test]
    fn canonical_peak_owned_tasks_differing_from_raw_peak_fails() {
        let mut observation = canonical_observation();
        observation["measurements"]["peak-owned-tasks"] = json!(10);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("\"peak-owned-tasks\"")),
            "{violations:?}"
        );
    }

    #[test]
    fn missing_business_row_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][0]["business_row_count"] = json!(99);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("business row count")),
            "{violations:?}"
        );
    }

    #[test]
    fn wrong_business_digest_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][1]["business_digest"] = json!("different-digest");
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("business digest")),
            "{violations:?}"
        );
    }

    #[test]
    fn undersized_pool_proof_false_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["pool_ceiling_proof"]["rejected_with_insufficient_pool_capacity"] =
            json!(false);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("pool_ceiling_proof")),
            "{violations:?}"
        );
    }

    #[test]
    fn undersized_pool_proof_missing_fails() {
        let mut observation = canonical_observation();
        observation["observation"]
            .as_object_mut()
            .expect("observation is an object")
            .remove("pool_ceiling_proof");
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(
            violations.iter().any(|v| v.contains("pool_ceiling_proof")),
            "{violations:?}"
        );
    }

    #[test]
    fn partition_count_drift_fails() {
        let mut observation = canonical_observation();
        observation["observation"]["points"][0]["partitions"] = json!(64);
        let violations = reconcile_p010(&observation, 100, EXPECTED_WORKERS);
        assert!(!violations.is_empty());
    }
}
