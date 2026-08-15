//! The M5 resource-bound campaign runner.
//!
//! The campaign owes a declared-ceiling proof for every queue, retry cache,
//! page, buffer, worker assignment, and result set the framework owns, with
//! backpressure propagation under stress. It delivers that as four reports —
//! worker assignments, bounded query paths, bounded payloads, and bounded
//! shedding — and this runner is the half that decides whether they proved it.
//!
//! It is a command rather than a test for the reason the other campaigns are:
//! three of the four reports return success without a database, because they
//! print a skip line and return. Under `cargo test` that is indistinguishable
//! from evidence. Here the fixtures are resolved first, and a campaign run
//! without them fails before any target starts.
//!
//! Passing tests are not sufficient either, and a resource campaign has a
//! failure mode the others do not. A bound can be *reported* as holding by a
//! run that never approached it: a worker budget of `64` whose observed peak
//! was `3`, a page bound checked against four rows, a queue that was never
//! filled and therefore dropped nothing. All three are green, and none is
//! evidence about a ceiling. So each report retains a machine-readable
//! observation into a directory this runner creates empty, and the runner
//! requires the substance rather than the outcome:
//!
//! - every resource the committed denominator lists was observed by some
//!   report, so a campaign cannot shrink by a report quietly covering less;
//! - the ceiling each report says it checked is the ceiling the denominator
//!   declares, which the in-process reconciliation has already checked against
//!   the constants the code holds;
//! - every resource whose occupancy is live reached its ceiling exactly, and
//!   every resource that sheds was actually offered an overload and actually
//!   shed;
//! - the stressed run's durable record equals the sequential baseline's, since
//!   a concurrency result that changes a durable observation is invalid
//!   regardless of its throughput.
//!
//! It also requires the database reports to name the `PostgreSQL` major they
//! ran against. A matrix point is invisible in a connection string, so an
//! observation from one supported major would otherwise reconcile perfectly
//! inside a run of another.
//!
//! The scope document is `tests/fixtures/resource-bounds/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_resource_bounds_campaign.rs` reconciles it
//! against the accepted plan, the capacity budgets, and the bounds this
//! workspace declares, so this runner consumes a document that ordinary review
//! has already checked from both sides.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "resource-bounds-campaign.json";

/// The directory the reports write their observations into.
const OBSERVATIONS: &str = "resource-observations";

/// The variable that tells a report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_RESOURCE_OBSERVATIONS";

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
/// declared resource was observed, every live ceiling was reached rather than
/// merely respected, and the stressed run left the baseline's durable record.
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
        let report = write_report(
            &root,
            &scope,
            &fixtures,
            &Runs::default(),
            &violations,
            &Value::Null,
        )?;
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
                release: false,
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
    let (execution_manifest, manifest_violations) = execution_manifest(&scope.reports, &runs);
    violations.extend(manifest_violations);

    let report = write_report(
        &root,
        &scope,
        &fixtures,
        &runs,
        &violations,
        &execution_manifest,
    )?;
    Ok(Campaign { violations, report })
}

/// Hoists the execution manifest the reports recorded to the campaign report.
///
/// Every declared report is required to have one and they must agree, so a
/// report that retained no manifest, a null one, or one that disagreed with
/// another cannot pass silently. The manifest itself is recorded by each
/// report from inside its own test process — see
/// `crates/oxide-batch/tests/resource_bounds/mod.rs`'s `execution_manifest`
/// for the three database reports, and
/// `crates/oxide-batch-cli/tests/m5_resource_bound_shedding.rs`'s own copy for
/// the shedding report, which runs in a different workspace crate — because
/// that is the tree the campaign actually ran against; this function only
/// requires the declared reports to agree on it.
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
            "the {fixture} fixture is required by the resource-bound campaign and is incomplete: \
             set {}",
            missing.join(", ")
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
            let expected = env::var(suite::MATRIX)
                .ok()
                .filter(|value| !value.is_empty());
            violations.extend(reconcile_matrix_point(
                &report.id,
                observation,
                expected.as_deref(),
            ));
        }
    }

    let (evidence, identity_violations) = collect_evidence(scope, runs);
    violations.extend(identity_violations);
    violations.extend(reconcile_denominator(scope, &evidence));
    violations.extend(reconcile_stress(scope, &evidence));
    violations.extend(reconcile_equivalence(scope, runs));

    violations
}

/// Requires a database report to name the matrix point the campaign ran at.
///
/// The matrix point is invisible in a connection string, so without this an
/// observation produced against one supported major would reconcile perfectly
/// inside a run of another.
fn reconcile_matrix_point(id: &str, observation: &Value, expected: Option<&str>) -> Vec<String> {
    let Some(expected) = expected else {
        return Vec::new();
    };
    let expected = expected
        .rsplit_once('-')
        .map_or(expected.to_owned(), |(_, major)| major.to_owned());
    let observed = observation
        .get("postgres_major_version")
        .and_then(Value::as_str);

    if observed == Some(expected.as_str()) {
        return Vec::new();
    }
    vec![format!(
        "{id} ran against PostgreSQL {} and this campaign run is {expected}",
        observed.unwrap_or("an unrecorded version"),
    )]
}

/// Checks one resource's evidence identity against the denominator, and
/// records the claim so a second one is rejected as a duplicate.
///
/// Raw evidence identity is the tuple `(resource, expected report)`, not the
/// resource name alone: a resource this document does not declare, one
/// recorded by a report other than the one the denominator names for it, and
/// one already claimed by an earlier observation are each rejected rather
/// than silently accepted, misattributed, or overwritten. `claimed_by` is an
/// ordinary map checked with `contains_key` before every insert for exactly
/// that reason — a bare `insert` would let a second observation win silently.
///
/// Returns the violation, when the identity does not hold.
fn check_identity(
    scope: &Scope,
    claimed_by: &mut BTreeMap<String, String>,
    resource: &str,
    id: &str,
) -> Option<String> {
    let Some(expected) = scope
        .resources
        .iter()
        .find(|candidate| candidate.name == resource)
    else {
        return Some(format!(
            "{resource} appears in the {id} report's evidence and is not one of the campaign's \
             declared resources",
        ));
    };
    if expected.report != id {
        return Some(format!(
            "{resource} is declared to be proved by the {} report and its evidence was recorded \
             by {id} instead",
            expected.report,
        ));
    }
    if claimed_by.contains_key(resource) {
        return Some(format!(
            "{resource} was recorded more than once, by more than one observation",
        ));
    }
    claimed_by.insert(resource.to_owned(), id.to_owned());
    None
}

/// Gathers what every report said about every resource it touched.
///
/// A report records a resource in one of four places: as a saturated or
/// refused resource with its offered load and observed peak, as one durable
/// instance-key rejection, as a construction cell at or past a ceiling, or as
/// a swept table entry. The first two carry evidence this runner reconciles
/// numerically; construction and swept-table entries carry evidence this
/// runner reconciles as an accept-at-the-ceiling and refuse-one-past-it pair.
/// Every entry's identity is checked against the denominator as it is
/// gathered, so an undeclared resource, a resource recorded by the wrong
/// report, or a resource claimed twice is a violation rather than silent
/// coverage.
fn collect_evidence(scope: &Scope, runs: &Runs) -> (Evidence, Vec<String>) {
    let mut evidence = Evidence::default();
    let mut violations = Vec::new();
    let mut claimed_by: BTreeMap<String, String> = BTreeMap::new();

    for (id, observation) in &runs.observations {
        let numeric = observation
            .get("resources")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(observation.get("instance_key"));
        for entry in numeric {
            let Some(resource) = entry.get("resource").and_then(Value::as_str) else {
                continue;
            };
            if let Some(violation) = check_identity(scope, &mut claimed_by, resource, id) {
                violations.push(violation);
            } else {
                evidence.covered.insert(resource.to_owned());
                evidence
                    .observed
                    .insert(resource.to_owned(), (id.clone(), entry.clone()));
            }
        }

        let mut cells: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let constructions = ["construction", "cells"].into_iter().flat_map(|array| {
            observation
                .get(array)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        });
        for entry in constructions {
            let Some(resource) = entry.get("resource").and_then(Value::as_str) else {
                continue;
            };
            cells
                .entry(resource.to_owned())
                .or_default()
                .push(entry.clone());
        }
        for (resource, cells) in cells {
            if let Some(violation) = check_identity(scope, &mut claimed_by, &resource, id) {
                violations.push(violation);
            } else {
                evidence.covered.insert(resource.clone());
                evidence.construction.insert(resource, (id.clone(), cells));
            }
        }
    }

    (evidence, violations)
}

/// Requires every declared resource to have been observed, at its ceiling.
///
/// A resource whose only evidence is a pair of construction cells is proved
/// numerically by the pair rather than by a single reconcilable entry: the
/// declared ceiling must have been accepted exactly, and one past it must
/// have been refused. Neither side alone is non-vacuous proof — an all-accept
/// or all-refuse report would say nothing about where the ceiling actually
/// falls.
fn reconcile_denominator(scope: &Scope, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();

    for resource in &scope.resources {
        if !evidence.covered.contains(&resource.name) {
            violations.push(format!(
                "{} is a resource this campaign is answerable for and no report observed it, so \
                 the campaign proved less than its denominator says",
                resource.name,
            ));
            continue;
        }

        if let Some((report, entry)) = evidence.observed.get(&resource.name) {
            if entry.get("passed").and_then(Value::as_bool) == Some(false) {
                violations.push(format!("{} failed in the {report} report", resource.name));
            }

            // The ceiling the report says it checked has to be the one the
            // denominator declares. Without this, a report could quietly
            // narrow a bound and still reconcile.
            let Some(declared) = resource.ceiling else {
                continue;
            };
            let observed = entry
                .get("declared_ceiling")
                .or_else(|| entry.get("configured_ceiling"))
                .and_then(Value::as_i64);
            if observed != Some(declared) {
                violations.push(format!(
                    "{} is declared with a ceiling of {declared} and the {report} report \
                     checked {observed:?}",
                    resource.name,
                ));
            }
            continue;
        }

        let Some((report, cells)) = evidence.construction.get(&resource.name) else {
            continue;
        };
        let Some(declared) = resource.ceiling else {
            continue;
        };
        let accepted_at_ceiling = cells.iter().any(|cell| {
            cell.get("value").and_then(Value::as_i64) == Some(declared)
                && cell.get("accepted").and_then(Value::as_bool) == Some(true)
                && cell.get("expected").and_then(Value::as_bool) == Some(true)
        });
        let refused_past_ceiling = cells.iter().any(|cell| {
            cell.get("value")
                .and_then(Value::as_i64)
                .is_some_and(|value| value > declared)
                && cell.get("accepted").and_then(Value::as_bool) == Some(false)
                && cell.get("expected").and_then(Value::as_bool) == Some(false)
        });
        if !accepted_at_ceiling {
            violations.push(format!(
                "{} is declared with a ceiling of {declared} and the {report} report recorded \
                 no construction accepted exactly at it",
                resource.name,
            ));
        }
        if !refused_past_ceiling {
            violations.push(format!(
                "{} is declared with a ceiling of {declared} and the {report} report recorded \
                 no construction refused one past it",
                resource.name,
            ));
        }
    }

    violations
}

/// Requires every ceiling the campaign says must be reached to have been.
fn reconcile_stress(scope: &Scope, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();

    for requirement in &scope.stress {
        let Some((report, entry)) = evidence.observed.get(&requirement.resource) else {
            violations.push(format!(
                "{} must be reached under stress and no report recorded an offered load for it",
                requirement.resource,
            ));
            continue;
        };
        if report != &requirement.report {
            violations.push(format!(
                "{} is declared to be stressed by the {} report and its evidence was recorded \
                 by {report} instead",
                requirement.resource, requirement.report,
            ));
            continue;
        }
        let ceiling = entry
            .get("configured_ceiling")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let offered = entry
            .get("offered_load")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let peak = entry
            .get("observed_peak_occupancy")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let drops = entry
            .get("drops")
            .and_then(Value::as_i64)
            .unwrap_or_default();

        match requirement.requires.as_str() {
            "peak-equals-ceiling" => {
                if peak != ceiling {
                    violations.push(format!(
                        "{} has a ceiling of {ceiling} and the {report} report observed a peak of \
                         {peak}; a ceiling a run never reached is not evidence that it holds",
                        requirement.resource,
                    ));
                }
                if offered <= ceiling {
                    violations.push(format!(
                        "{} was offered {offered} against a ceiling of {ceiling}, so the run had \
                         nothing to hold back",
                        requirement.resource,
                    ));
                }
            }
            "offered-exceeds-ceiling" => {
                if offered <= ceiling {
                    violations.push(format!(
                        "{} sheds under overload and was offered {offered} against a ceiling of \
                         {ceiling}, so it was never overloaded",
                        requirement.resource,
                    ));
                }
                if drops <= 0 {
                    violations.push(format!(
                        "{} was offered more than it holds and shed nothing",
                        requirement.resource,
                    ));
                }
                if peak > ceiling {
                    violations.push(format!(
                        "{} held {peak} against a ceiling of {ceiling}",
                        requirement.resource,
                    ));
                }
            }
            "history-exceeds-page" | "candidates-exceed-batch" => {
                if offered <= ceiling {
                    violations.push(format!(
                        "{} was asked for {ceiling} against {offered} available, so a path that \
                         returned everything would have satisfied the bound",
                        requirement.resource,
                    ));
                }
                if peak > ceiling {
                    violations.push(format!(
                        "{} returned {peak} against a bound of {ceiling}",
                        requirement.resource,
                    ));
                }
            }
            "rejected-before-any-durable-write" => {
                violations.extend(reconcile_closed_rejection(&requirement.resource, entry));
            }
            other => violations.push(format!(
                "the {} stress requirement asks for {other}, which this runner does not know how \
                 to check",
                requirement.resource,
            )),
        }
    }

    violations
}

/// Requires a fail-closed rejection to have left nothing behind.
///
/// A refusal that had already created an instance or an execution is a partial
/// launch behind an error, which is the failure this obligation exists for.
fn reconcile_closed_rejection(resource: &str, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    if entry
        .get("rejections")
        .and_then(Value::as_i64)
        .is_none_or(|count| count <= 0)
    {
        violations.push(format!(
            "{resource} must be refused before any child starts and the report recorded no \
             rejection",
        ));
    }

    let residue = entry.pointer("/detail/residue_after_refusal");
    let Some(residue) = residue.and_then(Value::as_object) else {
        violations.push(format!(
            "{resource} was refused and the report does not say what the refusal left in the \
             database, so nothing distinguishes a closed failure from a partial launch",
        ));
        return violations;
    };
    if residue.is_empty() {
        violations.push(format!(
            "{resource} was refused and no table was inspected afterwards",
        ));
    }
    for (table, rows) in residue {
        if rows.as_i64() != Some(0) {
            violations.push(format!(
                "{resource} was refused and left {rows} row(s) in {table}",
            ));
        }
    }

    violations
}

/// Requires the stressed runs to have matched their sequential baselines.
///
/// The producer's own `passed` summary is not trusted as root evidence: this
/// re-derives pass or fail from the raw `fields_compared` and
/// `must_not_observe` entries, requiring the exact set of fields the scope
/// declares (missing or duplicated is a failure, and an undeclared extra
/// field cannot mask a missing required one), each required field's own
/// `agrees` literally `true`, and each declared regression's own `observed`
/// literally `false`.
fn reconcile_equivalence(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();

    for comparison in &scope.equivalence {
        let Some(observation) = runs.observations.get(&comparison.report) else {
            // The absent observation is already reported against the report.
            continue;
        };
        let Some(equivalence) = observation.get("durable_equivalence") else {
            violations.push(format!(
                "the {} report ran work concurrently and recorded no comparison against a \
                 sequential baseline",
                comparison.report,
            ));
            continue;
        };
        for violation in strings(equivalence, "violations") {
            violations.push(format!("{}: {violation}", comparison.report));
        }

        let mut compared: BTreeMap<String, bool> = BTreeMap::new();
        for field in equivalence
            .get("fields_compared")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(name) = field.get("field").and_then(Value::as_str) else {
                continue;
            };
            if compared.contains_key(name) {
                violations.push(format!(
                    "the {} comparison recorded {name} more than once",
                    comparison.report,
                ));
                continue;
            }
            compared.insert(
                name.to_owned(),
                field.get("agrees").and_then(Value::as_bool) == Some(true),
            );
        }
        for required in &comparison.must_agree_on {
            match compared.get(required) {
                None => violations.push(format!(
                    "the {} comparison is required to agree on {required} and compared no such \
                     field",
                    comparison.report,
                )),
                Some(false) => violations.push(format!(
                    "the {} comparison recorded {required} as disagreeing, and a concurrency \
                     result that changes a durable observation is invalid regardless of its \
                     throughput",
                    comparison.report,
                )),
                Some(true) => {}
            }
        }

        let mut observed_conditions: BTreeMap<String, bool> = BTreeMap::new();
        for entry in equivalence
            .get("must_not_observe")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(condition) = entry.get("condition").and_then(Value::as_str) else {
                continue;
            };
            let Some(observed) = entry.get("observed").and_then(Value::as_bool) else {
                continue;
            };
            if observed_conditions.contains_key(condition) {
                violations.push(format!(
                    "the {} comparison recorded {condition} more than once",
                    comparison.report,
                ));
                continue;
            }
            observed_conditions.insert(condition.to_owned(), observed);
        }
        for condition in &comparison.must_not_observe {
            match observed_conditions.get(condition) {
                None => violations.push(format!(
                    "the {} comparison is required to report on {condition} and recorded no \
                     such condition",
                    comparison.report,
                )),
                Some(true) => violations.push(format!(
                    "the {} comparison observed {condition}, which the campaign requires it not \
                     to",
                    comparison.report,
                )),
                Some(false) => {}
            }
        }
    }

    violations
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
    runs: &Runs,
    violations: &[String],
    execution_manifest: &Value,
) -> Result<PathBuf, String> {
    let directory = suite::directory(root);
    fs::create_dir_all(&directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let path = directory.join(REPORT);

    let (evidence, _) = collect_evidence(scope, runs);
    let reports = scope
        .reports
        .iter()
        .map(|report| {
            json!({
                "id": report.id,
                "title": report.title,
                "owes": report.owes,
                "package": report.package,
                "target": report.target,
                "name": report.name,
                "fixture": report.fixture,
                "result": runs
                    .outcomes
                    .get(&(report.target.clone(), report.name.clone()))
                    .cloned()
                    .flatten(),
                "observation": runs.observations.get(&report.id),
            })
        })
        .collect::<Vec<_>>();

    // The resource ledger is the point of the record: one row per declared
    // resource, saying which report proved it, what ceiling was checked, how
    // much was offered, and how much the framework held.
    let ledger = scope
        .resources
        .iter()
        .map(|resource| {
            let observed = evidence.observed.get(&resource.name);
            json!({
                "resource": resource.name,
                "class": resource.class,
                "overload_policy": resource.policy,
                "declared_ceiling": resource.ceiling,
                "proving_report": resource.report,
                "observed_by": observed.map(|(report, _)| report.clone()),
                "covered": evidence.covered.contains(&resource.name),
                "postgres": resource.postgres,
                "observation": observed.map(|(_, entry)| entry.clone()),
            })
        })
        .collect::<Vec<_>>();

    let any = runs
        .observations
        .values()
        .find(|observation| observation.get("postgres_major_version").is_some());

    let document = json!({
        "report": "resource-bounds",
        "campaign": "M5 PostgreSQL resource bounds",
        "scenarios": scope
            .reports
            .iter()
            .map(|report| report.name.clone())
            .collect::<Vec<_>>(),
        "required_scenarios": scope
            .reports
            .iter()
            .map(|report| report.name.clone())
            .collect::<Vec<_>>(),
        "observed_scenarios": scope
            .reports
            .iter()
            .filter(|report| {
                runs.outcomes
                    .get(&(report.target.clone(), report.name.clone()))
                    .and_then(Option::as_deref)
                    == Some("ok")
                    && runs.observations.contains_key(&report.id)
            })
            .map(|report| report.name.clone())
            .collect::<Vec<_>>(),
        "environment": suite::environment(),
        "observation": { "execution_manifest": execution_manifest },
        "postgresql_version": any.and_then(|observation| observation.get("server_version").cloned()),
        "postgresql_major_version": any
            .and_then(|observation| observation.get("postgres_major_version").cloned()),
        "fixtures": fixtures,
        "resource_classes": scope.classes,
        "overload_policies": scope.policies,
        "declared_resources": scope.resources.len(),
        "observed_resources": evidence.covered.len(),
        "resource_ledger": ledger,
        "out_of_scope": scope.excluded,
        "stress": scope.stress_document,
        "durable_equivalence": scope
            .equivalence
            .iter()
            .map(|comparison| json!({
                "report": comparison.report,
                "observation": runs
                    .observations
                    .get(&comparison.report)
                    .and_then(|observation| observation.get("durable_equivalence").cloned()),
            }))
            .collect::<Vec<_>>(),
        "reports": reports,
        "related": scope.related,
        "violations": violations,
        "passed": violations.is_empty(),
        "result": if violations.is_empty() { "passed" } else { "failed" },
        "notes": [
            "Every report is run on its own so its result is attributable.",
            "A passing report is not sufficient on its own, and a resource \
             campaign has a failure mode the other campaigns do not: a bound \
             can be reported as holding by a run that never approached it. A \
             worker budget of 64 whose observed peak was 3, a page bound \
             checked against four rows, and a queue that was never filled are \
             all green and none is evidence about a ceiling. So each report \
             retains an observation into a directory this runner creates empty, \
             and the runner requires every declared resource to have been \
             observed, every live ceiling to have been reached exactly, and \
             every shedding resource to have been offered an overload and to \
             have shed.",
            "The stressed run is required to have left the same durable record \
             as the sequential baseline, because a concurrency result that \
             changes a durable observation is invalid regardless of its \
             throughput.",
            "Each database report is required to name the PostgreSQL major it \
             ran against, because a matrix point is invisible in a connection \
             string and an observation from one supported major would otherwise \
             reconcile perfectly inside a run of another.",
            "The denominator this reconciles against is checked from the other \
             side by an ordinary cargo test, which parses every library crate \
             and fails when a constant declared under the repository's bound \
             declaration convention is neither proved nor argued out of scope. \
             That convention is what the guarantee is bounded by: a ceiling \
             written as a bare literal or named outside it is ruled out by \
             review rather than by the scan.",
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

/// What the reports said about the resources they touched.
#[derive(Default)]
struct Evidence {
    /// Every resource some report mentioned at all.
    covered: BTreeSet<String>,
    /// The reconcilable entry for a resource, and the report that wrote it.
    observed: BTreeMap<String, (String, Value)>,
    /// The construction cells for a resource proved only that way, and the
    /// report that wrote them.
    construction: BTreeMap<String, (String, Vec<Value>)>,
}

/// The committed campaign scope document.
struct Scope {
    /// Fixture name to the environment variables it requires.
    fixtures: BTreeMap<String, Vec<String>>,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The resources the campaign is answerable for.
    resources: Vec<Resource>,
    /// The resource classes the accepted plan names.
    classes: Value,
    /// The overload policies the campaign distinguishes.
    policies: Value,
    /// The ceilings the campaign must reach rather than respect.
    stress: Vec<StressRequirement>,
    /// The stress obligations as declared, for the retained report.
    stress_document: Value,
    /// The durable comparisons the campaign requires.
    equivalence: Vec<Comparison>,
    /// The bounds the campaign argues are not framework resources.
    excluded: Value,
    /// Evidence the campaign keeps and does not run, as declared.
    related: Value,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("resource-bounds")
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
                package: suite::string(report, "package")?,
                target: suite::string(report, "target")?,
                name: suite::string(report, "name")?,
                fixture: report
                    .get("fixture")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                against_database: report
                    .get("database_report")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }

        let mut resources = Vec::new();
        for resource in array(&document, "resources")? {
            resources.push(Resource {
                name: suite::string(resource, "resource")?,
                class: suite::string(resource, "class")?,
                policy: suite::string(resource, "policy")?,
                report: suite::string(resource, "report")?,
                ceiling: resource.get("ceiling").and_then(Value::as_i64),
                postgres: resource
                    .get("postgres")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }

        let stress_document = document
            .get("stress")
            .cloned()
            .ok_or_else(|| "the scope document declares no stress obligations".to_owned())?;
        let stress = read_stress(&stress_document)?;
        let equivalence = read_equivalence(&document)?;

        let contract = document
            .get("support_contract")
            .ok_or_else(|| "the scope document declares no support contract".to_owned())?;

        Ok(Self {
            fixtures,
            reports,
            resources,
            classes: contract
                .get("resource_classes")
                .cloned()
                .unwrap_or(Value::Null),
            policies: contract
                .get("overload_policies")
                .cloned()
                .unwrap_or(Value::Null),
            stress,
            stress_document,
            equivalence,
            excluded: document.get("out_of_scope").cloned().unwrap_or(Value::Null),
            related: document.get("related").cloned().unwrap_or(Value::Null),
        })
    }
}

/// Reads the ceilings the campaign must reach rather than respect.
fn read_stress(document: &Value) -> Result<Vec<StressRequirement>, String> {
    let mut stress = Vec::new();
    for requirement in array(document, "requirements")? {
        stress.push(StressRequirement {
            resource: suite::string(requirement, "resource")?,
            report: suite::string(requirement, "report")?,
            requires: suite::string(requirement, "requires")?,
        });
    }
    Ok(stress)
}

/// Reads the durable comparisons between baseline and stressed runs.
fn read_equivalence(document: &Value) -> Result<Vec<Comparison>, String> {
    let equivalence = document.get("durable_equivalence").ok_or_else(|| {
        "the scope document declares no durable equivalence obligations".to_owned()
    })?;
    let mut comparisons = Vec::new();
    for comparison in array(equivalence, "comparisons")? {
        comparisons.push(Comparison {
            report: suite::string(comparison, "report")?,
            must_agree_on: comparison
                .get("must_agree_on")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            must_not_observe: comparison
                .get("must_not_observe")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        });
    }
    Ok(comparisons)
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
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The fixture it needs, when it needs one.
    fixture: Option<String>,
    /// Whether it ran against a database and must name the major it used.
    against_database: bool,
}

/// One bounded resource the campaign is answerable for.
struct Resource {
    /// The identity the reports and the denominator share.
    name: String,
    /// The class of the accepted plan's list it belongs to.
    class: String,
    /// The overload semantics it contracts for.
    policy: String,
    /// The report that proves it.
    report: String,
    /// The declared ceiling, when it is a number.
    ceiling: Option<i64>,
    /// Whether the proof is required to be on `PostgreSQL`.
    postgres: bool,
}

/// One obligation to reach a ceiling rather than stay under it.
struct StressRequirement {
    /// The resource that must be reached.
    resource: String,
    /// The report the raw observation for this requirement must come from.
    report: String,
    /// What reaching it means for this resource's policy.
    requires: String,
}

/// One durable comparison between a baseline and a stressed run.
struct Comparison {
    /// The report that owes the comparison.
    report: String,
    /// The fields the two runs must agree on.
    must_agree_on: Vec<String>,
    /// The regressions the two runs must not exhibit.
    must_not_observe: Vec<String>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use std::collections::BTreeMap;

    use serde_json::{Value, json};

    use super::{
        Comparison, Report, Resource, Runs, Scope, StressRequirement, collect_evidence,
        execution_manifest, reconcile_denominator, reconcile_equivalence, reconcile_matrix_point,
        reconcile_stress,
    };

    /// A scope shaped like the committed `campaign-scope.json`, without the
    /// filesystem: every reconciliation function under test reads only the
    /// fields built here. Three resources cover the three ways this campaign
    /// proves one: `resource-a` is a saturated worker-style ceiling proved by
    /// `report-a`, `resource-b` is proved only by construction cells in
    /// `report-a`, and `resource-c` is a shedding queue proved by `report-b`.
    /// `resource-d` is the fail-closed resource the rejection test needs.
    fn scope() -> Scope {
        Scope {
            fixtures: BTreeMap::new(),
            reports: vec![report("report-a"), report("report-b")],
            resources: vec![
                resource(
                    "resource-a",
                    "worker-assignment",
                    "bounded-concurrency",
                    "report-a",
                    Some(10),
                    true,
                ),
                resource(
                    "resource-b",
                    "buffer",
                    "fail-closed",
                    "report-a",
                    Some(5),
                    false,
                ),
                resource(
                    "resource-c",
                    "queue",
                    "bounded-shedding",
                    "report-b",
                    Some(20),
                    false,
                ),
                resource("resource-d", "queue", "fail-closed", "report-a", None, true),
            ],
            classes: Value::Null,
            policies: Value::Null,
            stress: vec![
                StressRequirement {
                    resource: "resource-a".to_owned(),
                    report: "report-a".to_owned(),
                    requires: "peak-equals-ceiling".to_owned(),
                },
                StressRequirement {
                    resource: "resource-c".to_owned(),
                    report: "report-b".to_owned(),
                    requires: "offered-exceeds-ceiling".to_owned(),
                },
                StressRequirement {
                    resource: "resource-d".to_owned(),
                    report: "report-a".to_owned(),
                    requires: "rejected-before-any-durable-write".to_owned(),
                },
            ],
            stress_document: Value::Null,
            equivalence: vec![Comparison {
                report: "report-a".to_owned(),
                must_agree_on: vec!["field-x".to_owned()],
                must_not_observe: vec!["regression-y".to_owned()],
            }],
            excluded: Value::Null,
            related: Value::Null,
        }
    }

    fn report(id: &str) -> Report {
        Report {
            id: id.to_owned(),
            title: id.to_owned(),
            owes: id.to_owned(),
            package: "oxide-batch".to_owned(),
            target: id.to_owned(),
            name: id.to_owned(),
            fixture: None,
            against_database: true,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn resource(
        name: &str,
        class: &str,
        policy: &str,
        report: &str,
        ceiling: Option<i64>,
        postgres: bool,
    ) -> Resource {
        Resource {
            name: name.to_owned(),
            class: class.to_owned(),
            policy: policy.to_owned(),
            report: report.to_owned(),
            ceiling,
            postgres,
        }
    }

    fn runs_with(observations: &[(&str, Value)]) -> Runs {
        let mut runs = Runs::default();
        for (id, observation) in observations {
            runs.observations
                .insert((*id).to_owned(), observation.clone());
        }
        runs
    }

    /// An observation for `report-a` that satisfies every requirement
    /// `scope()` declares of it: `resource-a` saturated exactly at its
    /// ceiling, `resource-b` proved by an accept-at-ceiling and
    /// refuse-one-past-it construction pair, `resource-d` refused before any
    /// row exists, and a durable-equivalence comparison that agrees on
    /// `field-x` and does not observe `regression-y`.
    fn valid_report_a() -> Value {
        json!({
            "passed": true,
            "resources": [
                {
                    "resource": "resource-a",
                    "declared_ceiling": 10,
                    "configured_ceiling": 10,
                    "offered_load": 25,
                    "observed_peak_occupancy": 10,
                    "drops": 0,
                    "passed": true,
                },
                {
                    "resource": "resource-d",
                    "declared_ceiling": Value::Null,
                    "rejections": 1,
                    "detail": { "residue_after_refusal": { "ob_job_instance": 0 } },
                    "passed": true,
                },
            ],
            "construction": [
                { "resource": "resource-b", "case": "at the ceiling", "value": 5, "accepted": true, "expected": true },
                { "resource": "resource-b", "case": "one past", "value": 6, "accepted": false, "expected": false },
            ],
            "durable_equivalence": {
                "fields_compared": [{ "field": "field-x", "agrees": true }],
                "must_not_observe": [{ "condition": "regression-y", "observed": false }],
                "violations": [],
            },
            "execution_manifest": manifest("deadbeef"),
            "violations": [],
        })
    }

    /// An observation for `report-b` that satisfies every requirement
    /// `scope()` declares of it: `resource-c` offered well past its ceiling
    /// and shedding the excess.
    fn valid_report_b() -> Value {
        json!({
            "passed": true,
            "resources": [
                {
                    "resource": "resource-c",
                    "declared_ceiling": 20,
                    "configured_ceiling": 20,
                    "offered_load": 50,
                    "observed_peak_occupancy": 20,
                    "drops": 30,
                    "passed": true,
                },
            ],
            "execution_manifest": manifest("deadbeef"),
            "violations": [],
        })
    }

    fn manifest(commit: &str) -> Value {
        json!({ "execution_commit": commit, "objects": { "Cargo.lock": "abc123" } })
    }

    fn any_violation<'a>(violations: &'a [String], needle: &str) -> Option<&'a String> {
        violations
            .iter()
            .find(|violation| violation.contains(needle))
    }

    #[test]
    fn a_valid_pair_of_observations_reconciles_clean() {
        let scope = scope();
        let runs = runs_with(&[
            ("report-a", valid_report_a()),
            ("report-b", valid_report_b()),
        ]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        assert!(reconcile_denominator(&scope, &evidence).is_empty());
        assert!(reconcile_stress(&scope, &evidence).is_empty());
        assert!(reconcile_equivalence(&scope, &runs).is_empty());
    }

    #[test]
    fn a_required_resource_deleted_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .retain(|entry| entry.get("resource").and_then(Value::as_str) != Some("resource-a"));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-a").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn an_undeclared_resource_added_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .push(json!({
                "resource": "resource-z",
                "declared_ceiling": 1,
                "configured_ceiling": 1,
                "offered_load": 1,
                "observed_peak_occupancy": 1,
                "drops": 0,
                "passed": true,
            }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (_, violations) = collect_evidence(&scope, &runs);
        assert!(
            any_violation(&violations, "resource-z").is_some_and(
                |violation| violation.contains("not one of the campaign's declared resources")
            ),
            "{violations:?}"
        );
    }

    #[test]
    fn a_same_count_bogus_resource_substituted_for_a_removed_one_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        let resources = report_a["resources"]
            .as_array_mut()
            .expect("resources array");
        resources
            .retain(|entry| entry.get("resource").and_then(Value::as_str) != Some("resource-a"));
        resources.push(json!({
            "resource": "resource-z",
            "declared_ceiling": 10,
            "configured_ceiling": 10,
            "offered_load": 25,
            "observed_peak_occupancy": 10,
            "drops": 0,
            "passed": true,
        }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        // The count of resources this report claims is unchanged, so only an
        // exact-identity check — not a count check — catches the swap.
        assert!(
            any_violation(&identity_violations, "resource-z").is_some(),
            "{identity_violations:?}"
        );
        let denominator_violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&denominator_violations, "resource-a").is_some(),
            "{denominator_violations:?}"
        );
    }

    #[test]
    fn a_duplicated_resource_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        let duplicate = report_a["resources"][0].clone();
        report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .push(duplicate);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (_, violations) = collect_evidence(&scope, &runs);
        assert!(
            any_violation(&violations, "resource-a")
                .is_some_and(|violation| violation.contains("more than once")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_resource_recorded_under_the_wrong_report_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        let moved = report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .remove(0);
        assert_eq!(moved["resource"], "resource-a");
        let mut report_b = valid_report_b();
        report_b["resources"]
            .as_array_mut()
            .expect("resources array")
            .push(moved);
        let runs = runs_with(&[("report-a", report_a), ("report-b", report_b)]);
        let (_, violations) = collect_evidence(&scope, &runs);
        assert!(
            any_violation(&violations, "resource-a")
                .is_some_and(|violation| violation.contains("recorded by report-b instead")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_stress_resource_report_mismatch_is_rejected() {
        let mut scope = scope();
        scope.stress[0].report = "report-b".to_owned();
        let runs = runs_with(&[
            ("report-a", valid_report_a()),
            ("report-b", valid_report_b()),
        ]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-a")
                .is_some_and(|violation| violation.contains("recorded by report-a instead")),
            "{violations:?}"
        );
    }

    #[test]
    fn offered_at_or_below_the_ceiling_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"][0]["offered_load"] = json!(10);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "nothing to hold back").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_peak_below_a_reachable_ceiling_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"][0]["observed_peak_occupancy"] = json!(3);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "never reached").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_peak_above_the_ceiling_is_rejected() {
        let scope = scope();
        let mut report_b = valid_report_b();
        report_b["resources"][0]["observed_peak_occupancy"] = json!(21);
        let runs = runs_with(&[("report-a", valid_report_a()), ("report-b", report_b)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-c")
                .is_some_and(|violation| violation.contains("held 21")),
            "{violations:?}"
        );
    }

    #[test]
    fn shedding_under_overload_with_zero_drops_is_rejected() {
        let scope = scope();
        let mut report_b = valid_report_b();
        report_b["resources"][0]["drops"] = json!(0);
        let runs = runs_with(&[("report-a", valid_report_a()), ("report-b", report_b)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "shed nothing").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_fail_closed_partial_write_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"][1]["detail"]["residue_after_refusal"]["ob_job_instance"] = json!(1);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-d")
                .is_some_and(|violation| violation.contains("left 1 row")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_required_durable_equivalence_field_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["fields_compared"] = json!([]);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "field-x")
                .is_some_and(|violation| violation.contains("compared no such field")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_disagreeing_durable_equivalence_field_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["fields_compared"][0]["agrees"] = json!(false);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "field-x")
                .is_some_and(|violation| violation.contains("disagreeing")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_comparison_field_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        let duplicate = report_a["durable_equivalence"]["fields_compared"][0].clone();
        report_a["durable_equivalence"]["fields_compared"]
            .as_array_mut()
            .expect("fields_compared array")
            .push(duplicate);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "more than once").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn an_observed_forbidden_regression_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["must_not_observe"][0]["observed"] = json!(true);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "regression-y")
                .is_some_and(|violation| violation.contains("requires it not to")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_must_not_observe_condition_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["must_not_observe"] = json!([]);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "regression-y")
                .is_some_and(|violation| violation.contains("recorded no such condition")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_construction_only_resource_missing_the_refused_side_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .retain(|cell| cell.get("case").and_then(Value::as_str) != Some("one past"));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b")
                .is_some_and(|violation| violation.contains("refused one past it")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_construction_only_resource_missing_the_accepted_side_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .retain(|cell| cell.get("case").and_then(Value::as_str) != Some("at the ceiling"));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b")
                .is_some_and(|violation| violation.contains("accepted exactly at it")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_execution_manifest_is_rejected() {
        let reports = vec![report("report-a"), report("report-b")];
        let mut report_b = valid_report_b();
        report_b
            .as_object_mut()
            .expect("object")
            .remove("execution_manifest");
        let runs = runs_with(&[("report-a", valid_report_a()), ("report-b", report_b)]);
        let (_, violations) = execution_manifest(&reports, &runs);
        assert!(
            any_violation(&violations, "report-b")
                .is_some_and(|violation| violation.contains("execution_manifest")),
            "{violations:?}"
        );
    }

    #[test]
    fn disagreeing_execution_manifests_are_rejected() {
        let reports = vec![report("report-a"), report("report-b")];
        let mut report_b = valid_report_b();
        report_b["execution_manifest"] = manifest("cafef00d");
        let runs = runs_with(&[("report-a", valid_report_a()), ("report-b", report_b)]);
        let (_, violations) = execution_manifest(&reports, &runs);
        assert!(
            any_violation(&violations, "different execution manifests").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn agreeing_execution_manifests_hoist_cleanly() {
        let reports = vec![report("report-a"), report("report-b")];
        let runs = runs_with(&[
            ("report-a", valid_report_a()),
            ("report-b", valid_report_b()),
        ]);
        let (hoisted, violations) = execution_manifest(&reports, &runs);
        assert!(violations.is_empty(), "{violations:?}");
        assert_eq!(hoisted, manifest("deadbeef"));
    }

    #[test]
    fn a_postgresql_major_mismatch_is_rejected() {
        let observation = json!({ "postgres_major_version": "15" });
        let violations = reconcile_matrix_point("report-a", &observation, Some("postgres-18"));
        assert!(any_violation(&violations, "18").is_some(), "{violations:?}");
    }
}
