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
///
/// A resource may legitimately appear in both categories at once — a report
/// can offer a numeric, ceiling-checked entry for the same resource its
/// construction cells also bound at the boundary — so identity is tracked
/// separately per category rather than in one shared map: what must not
/// happen is two different observations both claiming to be *the* numeric
/// entry for a resource, or two different observations both claiming to be
/// its construction cells, not a resource being evidenced in more than one
/// way by the report the denominator already names for it. Every entry's
/// identity is still checked against the denominator as it is gathered, so
/// an undeclared resource, a resource recorded by the wrong report, or a
/// resource claimed twice within one category is a violation rather than
/// silent coverage.
fn collect_evidence(scope: &Scope, runs: &Runs) -> (Evidence, Vec<String>) {
    let mut evidence = Evidence::default();
    let mut violations = Vec::new();
    let mut claimed_numeric: BTreeMap<String, String> = BTreeMap::new();
    let mut claimed_construction: BTreeMap<String, String> = BTreeMap::new();

    for (id, observation) in &runs.observations {
        evidence.reports.insert(id.clone(), observation.clone());
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
            if let Some(violation) = check_identity(scope, &mut claimed_numeric, resource, id) {
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
                violations.push(format!(
                    "{id} recorded a construction cell with no resource identity"
                ));
                continue;
            };
            cells
                .entry(resource.to_owned())
                .or_default()
                .push(entry.clone());
        }
        for (resource, cells) in cells {
            if let Some(violation) = check_identity(scope, &mut claimed_construction, &resource, id)
            {
                violations.push(violation);
            } else {
                evidence.covered.insert(resource.clone());
                evidence.construction.insert(resource, (id.clone(), cells));
            }
        }
    }

    (evidence, violations)
}

/// Requires every declared resource to have been observed, and every proof
/// kind it declares to be satisfied by its raw evidence.
///
/// Numeric and construction evidence are not mutually exclusive: a resource
/// with both is checked on both, because a report could otherwise narrow a
/// hard ceiling behind a stress-only saturation proof, or vice versa. Each
/// declared proof kind is dispatched to its own check; a kind with no
/// matching evidence is a violation, and a resource that declares no proof
/// kinds fails to parse at all (`Scope::read` requires at least one).
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
            if let Some(declared) = resource.ceiling {
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
            }
        }

        for proof in &resource.proofs {
            violations.extend(match proof.as_str() {
                "construction-boundary" => reconcile_construction_boundary(resource, evidence),
                "range-boundary" => reconcile_range_boundary(resource, evidence),
                "subject-boundary" => reconcile_subject_boundary(resource, evidence),
                "derived-capacity" => reconcile_derived_capacity(resource, evidence),
                "dual-budget-boundary" => reconcile_dual_budget_boundary(resource, evidence),
                "search-bounded-construction" => {
                    reconcile_search_bounded_construction(resource, evidence)
                }
                "upper-bound-only" => reconcile_upper_bound_only(resource, evidence),
                "truncation" => reconcile_truncation(resource, evidence),
                "refusal-past-ceiling" => reconcile_refusal_past_ceiling(resource, evidence),
                "fail-closed-residue" => reconcile_fail_closed_residue(resource, evidence),
                // Checked elsewhere: stress-saturation by reconcile_stress
                // against scope.stress, durable-round-trip by the producer's
                // own byte-identity assertion before it would ever retain a
                // passing observation.
                "stress-saturation" | "durable-round-trip" => Vec::new(),
                other => vec![format!(
                    "{} declares a proof kind ({other}) this runner does not know how to check",
                    resource.name,
                )],
            });
        }
        violations.extend(reconcile_modality_coverage(resource, evidence));
    }

    violations
}

/// The proof kinds whose evidence lives in construction cells, and the ones
/// whose evidence lives in a numeric entry. A kind absent from both lists —
/// `stress-saturation` and `durable-round-trip` — is checked elsewhere
/// against a different raw shape entirely (`reconcile_stress`, and the
/// producer's own byte-identity assertion, respectively).
const CONSTRUCTION_PROOF_KINDS: &[&str] = &[
    "construction-boundary",
    "range-boundary",
    "subject-boundary",
    "search-bounded-construction",
    "upper-bound-only",
    "fail-closed-residue",
];
const NUMERIC_PROOF_KINDS: &[&str] = &[
    "stress-saturation",
    "derived-capacity",
    "dual-budget-boundary",
    "truncation",
    "refusal-past-ceiling",
    "upper-bound-only",
];

/// Requires every raw evidence category a resource's report actually
/// recorded to be one some declared proof kind consumes.
///
/// The reverse direction — a declared proof kind with no matching evidence —
/// is already required by each proof kind's own dispatch above; this is the
/// direction that was missing: construction cells or a numeric entry
/// recorded for a resource whose declared proofs read neither would sit in
/// the raw report unverified by anything, which is indistinguishable from a
/// stray or mistaken observation. Only the coarse construction/numeric shape
/// is checked, not each of the eleven finer-grained proof kind names,
/// because a raw cell does not self-identify as `range-boundary` versus
/// `construction-boundary` versus `subject-boundary` — that distinction is
/// the scope's declaration, not something the evidence shape carries on its
/// own.
///
/// A numeric entry the producer marks `"summarizes_construction": true` is
/// exempted from the numeric side of this check: several reports compute a
/// numeric rollup or supplementary observation directly from a resource's
/// own construction cells purely for the retained report's readability, and
/// that rollup is not independent evidence any more than `passed` or
/// `violations` are — it is a derived summary field, and requiring it to
/// name its own proof kind would be requiring construction-only resources to
/// declare a numeric proof kind they do not have. The construction cells it
/// summarizes are exactly what the construction side of this same check
/// still requires to be accounted for.
fn reconcile_modality_coverage(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    if evidence.construction.contains_key(&resource.name)
        && !resource
            .proofs
            .iter()
            .any(|proof| CONSTRUCTION_PROOF_KINDS.contains(&proof.as_str()))
    {
        violations.push(format!(
            "{} recorded construction cells and declares no proof kind that requires them",
            resource.name,
        ));
    }
    if let Some((_, entry)) = evidence.observed.get(&resource.name)
        && entry
            .get("summarizes_construction")
            .and_then(Value::as_bool)
            != Some(true)
        && !resource
            .proofs
            .iter()
            .any(|proof| NUMERIC_PROOF_KINDS.contains(&proof.as_str()))
    {
        violations.push(format!(
            "{} recorded a numeric entry and declares no proof kind that requires it",
            resource.name,
        ));
    }
    violations
}

/// Requires the declared ceiling accepted exactly, and one past it refused
/// exactly. Neither side alone is non-vacuous proof — an all-accept or
/// all-refuse report says nothing about where the ceiling actually falls.
fn reconcile_construction_boundary(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(declared) = resource.ceiling else {
        violations.push(format!(
            "{} declares a construction-boundary proof and has no numeric ceiling to check it \
             against",
            resource.name,
        ));
        return violations;
    };
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        // A numeric entry proving stress-saturation does not stand in for
        // this: construction-boundary declared for a resource requires its
        // own construction cells regardless of what else was recorded for it,
        // so a numeric-only observation is still a missing proof here.
        violations.push(format!(
            "{} declares a construction-boundary proof and no report recorded any construction \
             cells for it",
            resource.name,
        ));
        return violations;
    };
    let Some(past) = declared.checked_add(1) else {
        violations.push(format!(
            "{} has a declared ceiling of {declared} that overflows one past it, so \
             construction-boundary cannot be checked",
            resource.name,
        ));
        return violations;
    };
    violations.extend(reconcile_exact_cell(
        &resource.name,
        report,
        cells,
        declared,
        "accepted",
        declared,
        &resource.unit,
        "accepted exactly at it",
    ));
    violations.extend(reconcile_exact_cell(
        &resource.name,
        report,
        cells,
        past,
        "refused",
        declared,
        &resource.unit,
        &format!("refused at exactly {past}"),
    ));
    for extra in &resource.additional_boundaries {
        violations.extend(reconcile_exact_cell(
            &resource.name,
            report,
            cells,
            extra.value,
            &extra.expected,
            declared,
            &resource.unit,
            &extra.label,
        ));
    }
    let allowed: Vec<(i64, &str)> = [(declared, "accepted"), (past, "refused")]
        .into_iter()
        .chain(
            resource
                .additional_boundaries
                .iter()
                .map(|extra| (extra.value, extra.expected.as_str())),
        )
        .collect();
    violations.extend(reject_undeclared_cells(
        &resource.name,
        report,
        cells,
        &allowed,
    ));
    violations
}

/// Rejects any cell whose `(value, expected-side)` identity is not one of
/// `allowed`. A construction-boundary, range-boundary, or subject-boundary
/// resource's raw evidence may carry only the exact cells its own
/// denominator declares — a plausible-looking extra cell that was never
/// declared is exactly as unproven as a missing required one, because
/// nothing here says what it was meant to show.
fn reject_undeclared_cells(
    resource: &str,
    report: &str,
    cells: &[Value],
    allowed: &[(i64, &str)],
) -> Vec<String> {
    let mut violations = Vec::new();
    for cell in cells {
        let Some(value) = cell.get("value").and_then(Value::as_i64) else {
            violations.push(format!("the {report} report recorded a malformed construction cell for {resource} with no numeric value"));
            continue;
        };
        let Some(side) = cell.get("expected").and_then(Value::as_str) else {
            violations.push(format!("the {report} report recorded a malformed construction cell for {resource} with no expected side"));
            continue;
        };
        let Some(observed) = cell.get("observed").and_then(Value::as_str) else {
            violations.push(format!("the {report} report recorded a malformed construction cell for {resource} with no observed side"));
            continue;
        };
        if cell.get("case").and_then(Value::as_str).is_none() {
            violations.push(format!("the {report} report recorded a construction cell for {resource} with no boundary case identity"));
        }
        if observed != side {
            violations.push(format!("the {report} report recorded a construction cell for {resource} expected {side} but observed {observed}"));
        }
        if !allowed
            .iter()
            .any(|(allowed_value, allowed_side)| *allowed_value == value && *allowed_side == side)
        {
            violations.push(format!(
                "the {report} report recorded a construction cell for {resource} at {value}, \
                 expected {side}, which is not one of its declared root cells",
            ));
        }
    }
    violations
}

/// Requires exactly one cell at `value`/`side`, and cross-checks its own
/// `declared_ceiling` field against `declared` when the raw cell carries one.
///
/// Missing (`0` matches) and duplicated (`>1` matches) both fail: a boundary
/// proved twice is not stronger evidence than proved once, and a duplicate
/// can mask a missing required cell the same way an undeclared substitution
/// can — an exact count is the only way to tell a genuine second proof from
/// either failure mode. Not every raw cell carries a `declared_ceiling`
/// field today (only the resources this producer already emits one for do),
/// so the cross-check applies only when the field is present rather than
/// requiring it universally.
fn reconcile_exact_cell(
    resource: &str,
    report: &str,
    cells: &[Value],
    value: i64,
    side: &str,
    declared: i64,
    unit: &str,
    label: &str,
) -> Vec<String> {
    let matches = matching_cells(cells, value, side);
    let mut violations = Vec::new();
    match matches.len() {
        0 => violations.push(format!(
            "{resource} is declared with a ceiling of {declared} and the {report} report \
             recorded no construction {label}",
        )),
        1 => {
            match matches[0].get("declared_ceiling").and_then(Value::as_i64) {
                Some(observed) if observed != declared => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded a declared bound of {observed}, and the denominator declares {declared}",
                )),
                Some(_) => {}
                None => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded no declared bound to check against {declared}",
                )),
            }
            match matches[0].get("unit").and_then(Value::as_str) {
                Some(observed) if observed != unit => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded a unit of {observed}, and the denominator declares {unit}",
                )),
                Some(_) => {}
                None => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded no unit to check against {unit}",
                )),
            }
        }
        n => violations.push(format!(
            "{resource}'s {label} cell was recorded {n} times in the {report} report, and \
             exactly one is required",
        )),
    }
    violations
}

/// Finds every cell exactly matching `value`, `expected == side`, and
/// `observed == side`.
fn matching_cells<'a>(cells: &'a [Value], value: i64, side: &str) -> Vec<&'a Value> {
    cells
        .iter()
        .filter(|cell| {
            cell.get("value").and_then(Value::as_i64) == Some(value)
                && cell.get("expected").and_then(Value::as_str) == Some(side)
                && cell.get("observed").and_then(Value::as_str) == Some(side)
        })
        .collect()
}

/// Requires exactly one cell at `value`/`side`, and cross-checks its own
/// `declared_ceiling` and `unit` fields against `declared`/`unit`.
///
/// Used by range-boundary and subject-boundary, both of which sweep more
/// than one boundary value on a single resource (a minimum and a maximum; one
/// ceiling per subject or dimension), so a bare numeric match is not enough
/// to know which fact a cell actually proves — unlike plain
/// construction-boundary's single ceiling, where the value alone is
/// unambiguous. Both fields are required on every cell this function checks,
/// not merely cross-checked when present: a range-boundary or
/// subject-boundary resource's raw evidence with no unit is exactly the gap
/// this closes. Missing (`0` matches) and duplicated (`>1` matches) both
/// fail, for the same reason `reconcile_exact_cell` requires an exact count.
#[allow(clippy::too_many_arguments)]
fn reconcile_dimensioned_cell(
    resource: &str,
    report: &str,
    cells: &[Value],
    value: i64,
    side: &str,
    declared: i64,
    unit: &str,
    label: &str,
) -> Vec<String> {
    let matches = matching_cells(cells, value, side);
    let mut violations = Vec::new();
    match matches.len() {
        0 => violations.push(format!(
            "{resource} is declared with a bound of {declared} {unit} and the {report} report \
             recorded no construction {label}",
        )),
        1 => {
            match matches[0].get("declared_ceiling").and_then(Value::as_i64) {
                Some(observed) if observed != declared => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded a declared \
                     bound of {observed}, and the denominator declares {declared}",
                )),
                Some(_) => {}
                None => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded no declared \
                     bound to check against {declared}",
                )),
            }
            match matches[0].get("unit").and_then(Value::as_str) {
                Some(observed) if observed != unit => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded a unit of \
                     {observed}, and the denominator declares {unit}",
                )),
                Some(_) => {}
                None => violations.push(format!(
                    "{resource}'s {label} cell in the {report} report recorded no unit to \
                     check against {unit}",
                )),
            }
        }
        n => violations.push(format!(
            "{resource}'s {label} cell was recorded {n} times in the {report} report, and \
             exactly one is required",
        )),
    }
    violations
}

/// Requires all four sides of an inclusive minimum/maximum range: accepted at
/// the minimum, refused one unit below it, accepted at the maximum, refused
/// one unit past it, all in the unit the denominator declares.
fn reconcile_range_boundary(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(bounds) = &resource.bounds else {
        violations.push(format!(
            "{} declares a range-boundary proof and has no bounds declared to check it against",
            resource.name,
        ));
        return violations;
    };
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        violations.push(format!(
            "{} declares a range-boundary proof and no report recorded construction cells for \
             it",
            resource.name,
        ));
        return violations;
    };
    let (Some(below_minimum), Some(past_maximum)) =
        (bounds.minimum.checked_sub(1), bounds.maximum.checked_add(1))
    else {
        violations.push(format!(
            "{} has a minimum of {} and a maximum of {} that overflow one step past their \
             boundary, so range-boundary cannot be checked",
            resource.name, bounds.minimum, bounds.maximum,
        ));
        return violations;
    };
    for (value, side, declared, label) in [
        (
            bounds.minimum,
            "accepted",
            bounds.minimum,
            "accepted exactly at the minimum",
        ),
        (
            below_minimum,
            "refused",
            bounds.minimum,
            "refused one unit below the minimum",
        ),
        (
            bounds.maximum,
            "accepted",
            bounds.maximum,
            "accepted exactly at the maximum",
        ),
        (
            past_maximum,
            "refused",
            bounds.maximum,
            "refused one unit past the maximum",
        ),
    ] {
        violations.extend(reconcile_dimensioned_cell(
            &resource.name,
            report,
            cells,
            value,
            side,
            declared,
            &bounds.unit,
            label,
        ));
    }
    let allowed = [
        (bounds.minimum, "accepted"),
        (below_minimum, "refused"),
        (bounds.maximum, "accepted"),
        (past_maximum, "refused"),
    ];
    violations.extend(reject_undeclared_cells(
        &resource.name,
        report,
        cells,
        &allowed,
    ));
    violations
}

/// Requires the exact declared subject set, each with its own
/// non-placeholder ceiling accepted exactly and refused one unit past it, in
/// its own declared unit.
///
/// Reused beyond bounded-identifier-text for any resource whose evidence
/// proves more than one independent named bound on itself: `subject` here
/// means "the name of one of the several things this resource's evidence
/// must independently prove," not only a caller-supplied identifier type.
fn reconcile_subject_boundary(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        violations.push(format!(
            "{} declares a subject-boundary proof and no report recorded construction cells for \
             it",
            resource.name,
        ));
        return violations;
    };

    let mut by_subject: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for cell in cells {
        let Some(subject) = cell.get("subject").and_then(Value::as_str) else {
            let value = cell.get("value").and_then(Value::as_i64);
            violations.push(format!(
                "the {report} report recorded a construction cell for {} at {value:?} with no \
                 subject, and every cell this resource's evidence carries must be attributed to \
                 one of its declared subjects",
                resource.name,
            ));
            continue;
        };
        by_subject.entry(subject).or_default().push(cell.clone());
    }

    let declared: BTreeMap<&str, (i64, &str)> = resource
        .subjects
        .iter()
        .map(|subject| {
            (
                subject.subject.as_str(),
                (subject.ceiling, subject.unit.as_str()),
            )
        })
        .collect();

    for subject in by_subject.keys() {
        if !declared.contains_key(subject) {
            violations.push(format!(
                "{subject} appears in the {report} report's {} evidence and is not one of its \
                 declared subjects",
                resource.name,
            ));
        }
    }

    for (subject, &(ceiling, unit)) in &declared {
        let Some(subject_cells) = by_subject.get(subject) else {
            violations.push(format!(
                "{subject} is a declared subject of {} and no report recorded evidence for it",
                resource.name,
            ));
            continue;
        };
        let Some(past) = ceiling.checked_add(1) else {
            violations.push(format!(
                "{subject} has a declared ceiling of {ceiling} that overflows one unit past it",
            ));
            continue;
        };
        violations.extend(reconcile_dimensioned_cell(
            subject,
            report,
            subject_cells,
            ceiling,
            "accepted",
            ceiling,
            unit,
            "accepted exactly at its ceiling",
        ));
        violations.extend(reconcile_dimensioned_cell(
            subject,
            report,
            subject_cells,
            past,
            "refused",
            ceiling,
            unit,
            "refused one unit past its ceiling",
        ));
        let allowed = [(ceiling, "accepted"), (past, "refused")];
        violations.extend(reject_undeclared_cells(
            subject,
            report,
            subject_cells,
            &allowed,
        ));
    }

    violations
}

/// Re-derives `repository-connection-capacity`'s required connection count
/// from the raw `concurrent_children` field as `concurrent_children +
/// parent_connections`, rather than trusting the report's own arithmetic.
fn reconcile_derived_capacity(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((report, entry)) = evidence.observed.get(&resource.name) else {
        violations.push(format!(
            "{} declares a derived-capacity proof and no report recorded a numeric entry for it",
            resource.name,
        ));
        return violations;
    };
    let Some(parent) = resource.parent_connections else {
        violations.push(format!(
            "{} declares a derived-capacity proof and no parent_connections value to derive from",
            resource.name,
        ));
        return violations;
    };
    if resource.bound_kind.as_deref() != Some("derived") {
        violations.push(format!(
            "{} declares a derived-capacity proof and its bound_kind is not \"derived\"",
            resource.name,
        ));
    }
    let Some(children) = entry
        .pointer("/detail/concurrent_children")
        .and_then(Value::as_i64)
    else {
        violations.push(format!(
            "{} declares a derived-capacity proof and the {report} report recorded no \
             concurrent_children detail to re-derive from",
            resource.name,
        ));
        return violations;
    };
    let Some(required) = children.checked_add(parent) else {
        violations.push(format!(
            "{} has a concurrent_children of {children} that overflows adding the parent \
             connection",
            resource.name,
        ));
        return violations;
    };
    let recorded = entry
        .pointer("/detail/required_connections")
        .and_then(Value::as_i64);
    if recorded != Some(required) {
        violations.push(format!(
            "{} re-derives to a required connection count of {required} from {children} \
             concurrent children plus {parent}, and the {report} report recorded {recorded:?}",
            resource.name,
        ));
    }
    violations
}

/// Independently re-verifies `concurrent-split-branches`'s two nested runs:
/// the budgeted run's own peak-equals-budget relation, and the ceiling run's
/// own proof that the declared ceiling is reachable — not just the resource's
/// top-level `configured_ceiling`/`peak` fields, which only cover the
/// budgeted run.
fn reconcile_dual_budget_boundary(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((report, entry)) = evidence.observed.get(&resource.name) else {
        violations.push(format!(
            "{} declares a dual-budget-boundary proof and no report recorded a numeric entry \
             for it",
            resource.name,
        ));
        return violations;
    };
    let Some(declared) = resource.ceiling else {
        return violations;
    };

    let budgeted = entry.pointer("/detail/budgeted_run");
    let budget = budgeted
        .and_then(|run| run.get("budget"))
        .and_then(Value::as_i64);
    let budgeted_offered = budgeted
        .and_then(|run| run.get("offered"))
        .and_then(Value::as_i64);
    let budgeted_peak = budgeted
        .and_then(|run| run.get("peak"))
        .and_then(Value::as_i64);
    if budgeted.is_none() {
        violations.push(format!(
            "{} declares a dual-budget-boundary proof and the {report} report recorded no \
             budgeted_run detail",
            resource.name,
        ));
    } else {
        if budgeted_peak != budget {
            violations.push(format!(
                "{}'s budgeted run has a budget of {budget:?} and a peak of {budgeted_peak:?}",
                resource.name,
            ));
        }
        if budgeted_offered
            .zip(budget)
            .is_none_or(|(offered, budget)| offered <= budget)
        {
            violations.push(format!(
                "{}'s budgeted run offered {budgeted_offered:?} against a budget of \
                 {budget:?}, so it had nothing to hold back",
                resource.name,
            ));
        }
    }

    let ceiling_run = entry.pointer("/detail/ceiling_run");
    let ceiling_budget = ceiling_run
        .and_then(|run| run.get("budget"))
        .and_then(Value::as_i64);
    let ceiling_offered = ceiling_run
        .and_then(|run| run.get("offered"))
        .and_then(Value::as_i64);
    let ceiling_peak = ceiling_run
        .and_then(|run| run.get("peak"))
        .and_then(Value::as_i64);
    if ceiling_run.is_none() {
        violations.push(format!(
            "{} declares a dual-budget-boundary proof and the {report} report recorded no \
             ceiling_run detail",
            resource.name,
        ));
    } else {
        if ceiling_budget != Some(declared) {
            violations.push(format!(
                "{}'s ceiling run is budgeted at {ceiling_budget:?} and the declared ceiling is \
                 {declared}",
                resource.name,
            ));
        }
        if ceiling_offered.is_none_or(|offered| offered < declared) {
            violations.push(format!(
                "{}'s ceiling run offered {ceiling_offered:?} against a declared ceiling of \
                 {declared}, so the ceiling was not reachable",
                resource.name,
            ));
        }
        if ceiling_peak != Some(declared) {
            violations.push(format!(
                "{}'s ceiling run has a declared ceiling of {declared} and a peak of \
                 {ceiling_peak:?}",
                resource.name,
            ));
        }
    }

    violations
}

/// `definition-manifest` only: the canonical manifest ceiling binds before
/// the node ceiling does, so the accepted side is the largest chain a search
/// found that still fits the byte ceiling, and the refused side is one
/// chain-length past it rather than one byte past it, which a refused graph
/// has no manifest to measure.
fn reconcile_search_bounded_construction(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(declared) = resource.ceiling else {
        return violations;
    };
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        violations.push(format!(
            "{} declares a search-bounded-construction proof and no report recorded \
             construction cells for it",
            resource.name,
        ));
        return violations;
    };
    let accepted: Vec<&Value> = cells
        .iter()
        .filter(|cell| {
            cell.get("value")
                .and_then(Value::as_i64)
                .is_some_and(|value| value <= declared)
                && cell.get("expected").and_then(Value::as_str) == Some("accepted")
                && cell.get("observed").and_then(Value::as_str) == Some("accepted")
        })
        .collect();
    let refused: Vec<&Value> = cells
        .iter()
        .filter(|cell| {
            cell.get("expected").and_then(Value::as_str) == Some("refused")
                && cell.get("observed").and_then(Value::as_str) == Some("refused")
        })
        .collect();
    match accepted.len() {
        0 => violations.push(format!(
            "{} is declared with a ceiling of {declared} bytes and the {report} report recorded \
             no construction accepted at or under it",
            resource.name,
        )),
        1 => {}
        n => violations.push(format!(
            "{} had a construction cell accepted at or under its ceiling recorded {n} times in \
             the {report} report, and exactly one is required",
            resource.name,
        )),
    }
    match refused.len() {
        0 => violations.push(format!(
            "{} is declared with a ceiling of {declared} bytes and the {report} report recorded \
             no construction refused past the largest chain that fits",
            resource.name,
        )),
        1 => {}
        n => violations.push(format!(
            "{}'s refused-past-the-largest-chain-that-fits cell was recorded {n} times in the \
             {report} report, and exactly one is required",
            resource.name,
        )),
    }
    if accepted.len() + refused.len() != cells.len() {
        violations.push(format!(
            "the {report} report recorded a construction cell for {} that is neither its accepted search cell nor its refused search cell",
            resource.name,
        ));
    }
    if let (Some(accepted), Some(refused)) = (accepted.first(), refused.first()) {
        for cell in [*accepted, *refused] {
            if cell.get("declared_ceiling").and_then(Value::as_i64) != Some(declared) {
                violations.push(format!(
                    "{} search evidence does not carry declared_ceiling={declared}",
                    resource.name
                ));
            }
            if cell.get("case").and_then(Value::as_str).is_none() {
                violations.push(format!(
                    "{} search evidence carries no case identity",
                    resource.name
                ));
            }
        }
        let accepted_value = accepted.get("value").and_then(Value::as_i64);
        let refused_value = refused.get("value").and_then(Value::as_i64);
        let accepted_unit = accepted.get("unit").and_then(Value::as_str);
        let refused_unit = refused.get("unit").and_then(Value::as_str);
        match resource.name.as_str() {
            "retry-cache-bytes" => {
                if accepted_unit != Some("bytes") || refused_unit != Some("bytes") {
                    violations.push("retry-cache-bytes search cells must both be bytes".to_owned());
                }
                if refused_value != declared.checked_add(1) {
                    violations.push(format!(
                        "retry-cache-bytes refusal must be exactly {} bytes",
                        declared + 1
                    ));
                }
            }
            "definition-nodes" => {
                if accepted_unit != Some("nodes") || refused_unit != Some("nodes") {
                    violations.push("definition-nodes search cells must both be nodes".to_owned());
                }
                if refused_value != declared.checked_add(1) {
                    violations.push(format!(
                        "definition-nodes refusal must be exactly {} nodes",
                        declared + 1
                    ));
                }
            }
            "definition-manifest" => {
                if accepted_unit != Some("bytes") || refused_unit != Some("nodes") {
                    violations.push("definition-manifest must record accepted bytes and refused chain nodes explicitly".to_owned());
                }
                let raw = evidence.reports.get(report);
                let largest_chain = raw
                    .and_then(|v| v.pointer("/definition_bound/largest_accepted_chain"))
                    .and_then(Value::as_i64);
                let largest_bytes = raw
                    .and_then(|v| v.pointer("/definition_bound/largest_accepted_manifest_bytes"))
                    .and_then(Value::as_i64);
                if accepted_value != largest_bytes {
                    violations.push("definition-manifest accepted cell is not the recorded largest accepted manifest byte count".to_owned());
                }
                if largest_chain.and_then(|v| v.checked_add(1)) != refused_value {
                    violations.push("definition-manifest refused chain is not exactly one node past the largest accepted chain".to_owned());
                }
                if largest_bytes.is_none_or(|v| v > declared) {
                    violations.push("definition-manifest largest accepted manifest does not fit its declared byte ceiling".to_owned());
                }
                if raw
                    .and_then(|v| v.pointer("/definition_bound/binding_bound"))
                    .and_then(Value::as_str)
                    != Some("definition-manifest")
                {
                    violations.push("definition-manifest report does not identify the manifest as the binding bound".to_owned());
                }
            }
            _ => {}
        }
    }
    violations
}

/// A ceiling no accepted M5 input reaches, because a different bound always
/// binds first or the resource composes from parts that never sum close to
/// it. There is no refusal to require; the observed value beside the ceiling
/// is enough, from either a numeric entry or a construction accept cell.
fn reconcile_upper_bound_only(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    if evidence.observed.contains_key(&resource.name) {
        // The generic declared_ceiling/configured_ceiling match above already
        // requires the observed value to be recorded beside the ceiling.
        return Vec::new();
    }
    let mut violations = Vec::new();
    let Some(declared) = resource.ceiling else {
        return violations;
    };
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        violations.push(format!(
            "{} declares an upper-bound-only proof and no report recorded any evidence for it",
            resource.name,
        ));
        return violations;
    };
    let accepted: Vec<&Value> = cells
        .iter()
        .filter(|cell| {
            cell.get("value")
                .and_then(Value::as_i64)
                .is_some_and(|value| value <= declared)
                && cell.get("expected").and_then(Value::as_str) == Some("accepted")
                && cell.get("observed").and_then(Value::as_str) == Some("accepted")
        })
        .collect();
    match accepted.len() {
        0 => violations.push(format!(
            "{} is declared upper-bound-only with a ceiling of {declared} and the {report} \
             report recorded no construction accepted at or under it",
            resource.name,
        )),
        1 => {}
        n => violations.push(format!(
            "{} had a construction cell accepted at or under its ceiling recorded {n} times in \
             the {report} report, and exactly one is required",
            resource.name,
        )),
    }
    if accepted.len() != cells.len() {
        violations.push(format!(
            "the {report} report recorded a construction cell for {} that is not its \
             accepted-at-or-under-the-ceiling cell, and upper-bound-only declares no refusal to \
             account for one",
            resource.name,
        ));
    }
    violations
}

/// `instance-key-input` only: a fail-closed resource proved by refusal alone,
/// with no construction accept cell at the ceiling. Requires the recorded
/// offer to actually have exceeded the declared ceiling and the report's own
/// `refused` flag to be true — an offer at or under the ceiling, or a refusal
/// flag left false, means the ceiling was never actually exercised.
fn reconcile_refusal_past_ceiling(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(declared) = resource.ceiling else {
        return violations;
    };
    let Some((report, entry)) = evidence.observed.get(&resource.name) else {
        violations.push(format!(
            "{} declares a refusal-past-ceiling proof and no report recorded a numeric entry \
             for it",
            resource.name,
        ));
        return violations;
    };
    let offered = entry
        .get("offered_load")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if offered <= declared {
        violations.push(format!(
            "{} is declared with a ceiling of {declared} and the {report} report offered \
             {offered}, which never exceeded it",
            resource.name,
        ));
    }
    if entry.get("refused").and_then(Value::as_bool) != Some(true) {
        violations.push(format!(
            "{} was offered past its declared ceiling and the {report} report did not record a \
             refusal",
            resource.name,
        ));
    }
    violations
}

/// `repository-connection-capacity` only: requires at least one construction
/// cell refused, independently of the stress-saturation requirement's own
/// database-level residue check. The two are not the same proof: this one is
/// the in-memory budget constructor itself refusing a pool one short of the
/// derivation, before any database round trip; `rejected-before-any-durable-write`
/// is the full end-to-end check that the refusal left no row behind.
fn reconcile_fail_closed_residue(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((report, cells)) = evidence.construction.get(&resource.name) else {
        violations.push(format!(
            "{} declares a fail-closed-residue proof and no report recorded any construction \
             cells for it",
            resource.name,
        ));
        return violations;
    };
    let refused = cells.iter().any(|cell| {
        cell.get("expected").and_then(Value::as_str) == Some("refused")
            && cell.get("observed").and_then(Value::as_str) == Some("refused")
    });
    if !refused {
        violations.push(format!(
            "{} declares a fail-closed-residue proof and the {report} report recorded no \
             construction cell refused",
            resource.name,
        ));
    }
    violations
}

/// `explorer-response` only: `offered_load` is a row count here, not bytes,
/// so it is not reconciled against the byte ceiling. The proof instead reads
/// the traversal's own truncation marker and the observed byte peak.
fn reconcile_truncation(resource: &Resource, evidence: &Evidence) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(declared) = resource.ceiling else {
        return violations;
    };
    let Some((report, entry)) = evidence.observed.get(&resource.name) else {
        violations.push(format!(
            "{} declares a truncation proof and no report recorded a numeric entry for it",
            resource.name,
        ));
        return violations;
    };
    let peak = entry
        .get("observed_peak_occupancy")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if peak > declared {
        violations.push(format!(
            "{} held {peak} against a declared ceiling of {declared}",
            resource.name,
        ));
    }
    let truncated = entry
        .get("truncated_pages")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if truncated <= 0 {
        violations.push(format!(
            "{} recorded no truncated pages in the {report} report, so its bound was never \
             actually exercised",
            resource.name,
        ));
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
                let declared_rule = scope
                    .resources
                    .iter()
                    .find(|candidate| candidate.name == requirement.resource)
                    .and_then(|candidate| candidate.shedding_rule.as_deref());
                violations.extend(reconcile_offered_exceeds_ceiling(
                    &requirement.resource,
                    entry,
                    ceiling,
                    offered,
                    peak,
                    drops,
                    declared_rule,
                ));
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

/// Requires a shedding-policy resource offered past its ceiling to have
/// actually shed, held no more than its ceiling, and accounted for every
/// offered unit exactly under the relation its own declared shedding rule
/// contracts for.
#[allow(clippy::too_many_arguments)]
fn reconcile_offered_exceeds_ceiling(
    resource: &str,
    entry: &Value,
    ceiling: i64,
    offered: i64,
    peak: i64,
    drops: i64,
    declared_rule: Option<&str>,
) -> Vec<String> {
    let mut violations = Vec::new();
    if offered <= ceiling {
        violations.push(format!(
            "{resource} sheds under overload and was offered {offered} against a ceiling of \
             {ceiling}, so it was never overloaded",
        ));
    }
    if drops <= 0 {
        violations.push(format!(
            "{resource} was offered more than it holds and shed nothing",
        ));
    }
    if peak > ceiling {
        violations.push(format!(
            "{resource} held {peak} against a ceiling of {ceiling}"
        ));
    }
    let observed_rule = entry.get("shedding_rule").and_then(Value::as_str);
    match (declared_rule, observed_rule) {
        (Some(declared), Some(observed)) if declared != observed => {
            violations.push(format!(
                "{resource} is declared with the {declared} shedding rule and the report \
                 recorded {observed} instead",
            ));
        }
        (Some(_), None) => violations.push(format!(
            "{resource} declares a shedding rule and the report recorded none",
        )),
        _ => {}
    }
    violations.extend(reconcile_shed_accounting(
        resource,
        entry,
        offered,
        drops,
        declared_rule.or(observed_rule),
    ));
    violations
}

/// Requires exact accounting for a shedding-policy resource: every unit
/// offered is either retained or discarded, and a gap between the two is loss
/// the report failed to attribute rather than shedding.
///
/// Two relations are known: `drop-newest` and `evict-oldest` both retain
/// exactly what a ceiling-sized buffer holds, so what is not retained is
/// discarded outright. `collapse-to-reserved-series` instead carves one slot
/// out of the ceiling for a shared reserved series that every excess unit
/// collapses into: that slot is retained but did not come from a distinct
/// offered unit being kept, so one fewer offered unit is truly new and one
/// more is attributed to the reserved series than the flat relation would
/// count.
fn reconcile_shed_accounting(
    resource: &str,
    entry: &Value,
    offered: i64,
    drops: i64,
    rule: Option<&str>,
) -> Vec<String> {
    let retained = entry
        .get("retained")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let expected = match rule {
        Some("collapse-to-reserved-series") => retained
            .checked_sub(1)
            .and_then(|distinct| offered.checked_sub(distinct)),
        _ => offered.checked_sub(retained),
    };
    match expected {
        Some(expected) if expected == drops => Vec::new(),
        Some(expected) => vec![format!(
            "{resource} offered {offered} and retained {retained}, so {expected} should have \
             been discarded, and the report recorded {drops}",
        )],
        None => vec![format!(
            "{resource} retained {retained} against an offered load of {offered}, which \
             retains more than was offered",
        )],
    }
}

/// Requires a fail-closed rejection to have left nothing behind.
///
/// A refusal that had already created an instance or an execution is a partial
/// launch behind an error, which is the failure this obligation exists for.
fn reconcile_closed_rejection(resource: &str, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    let children = entry
        .pointer("/detail/concurrent_children")
        .and_then(Value::as_i64);
    let required = entry
        .pointer("/detail/required_connections")
        .and_then(Value::as_i64);
    let short_pool = entry.pointer("/detail/short_pool").and_then(Value::as_i64);
    match (children, required, short_pool) {
        (Some(children), Some(required), Some(short_pool)) => {
            if children.checked_add(1) != Some(required) {
                violations.push(format!("{resource} recorded {children} concurrent children but required_connections={required}, not children + 1"));
            }
            if required.checked_sub(1) != Some(short_pool) {
                violations.push(format!("{resource} recorded required_connections={required} but short_pool={short_pool}, not exactly one short"));
            }
            if entry.get("connection_capacity").and_then(Value::as_i64) != Some(short_pool) {
                violations.push(format!("{resource} short-pool evidence does not bind connection_capacity to {short_pool}"));
            }
            if entry.pointer("/detail/sufficient_pool").and_then(Value::as_i64) != Some(required) {
                violations.push(format!("{resource} did not prove a pool of exactly {required} is the sufficient side of the boundary"));
            }
        }
        _ => violations.push(format!("{resource} recorded no complete concurrent_children/required_connections/short_pool derivation")),
    }

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

        violations.extend(reconcile_agreement_fields(
            &comparison.report,
            equivalence,
            &comparison.must_agree_on,
        ));
        violations.extend(reconcile_forbidden_conditions(
            &comparison.report,
            equivalence,
            &comparison.must_not_observe,
        ));
    }

    violations
}

/// Requires `fields_compared` to be the exact set `must_agree_on` declares: a
/// required field missing or disagreeing fails, and a compared field not
/// declared also fails, so an undeclared extra cannot mask a missing
/// required one.
fn reconcile_agreement_fields(
    report: &str,
    equivalence: &Value,
    must_agree_on: &[String],
) -> Vec<String> {
    let mut violations = Vec::new();
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
                "the {report} comparison recorded {name} more than once"
            ));
            continue;
        }
        compared.insert(
            name.to_owned(),
            field.get("agrees").and_then(Value::as_bool) == Some(true),
        );
    }
    for required in must_agree_on {
        match compared.get(required) {
            None => violations.push(format!(
                "the {report} comparison is required to agree on {required} and compared no \
                 such field",
            )),
            Some(false) => violations.push(format!(
                "the {report} comparison recorded {required} as disagreeing, and a concurrency \
                 result that changes a durable observation is invalid regardless of its \
                 throughput",
            )),
            Some(true) => {}
        }
    }
    for name in compared.keys() {
        if !must_agree_on.iter().any(|required| required == name) {
            violations.push(format!(
                "the {report} comparison compared {name}, which is not one of its declared \
                 must_agree_on fields",
            ));
        }
    }
    violations
}

/// Requires `must_not_observe` conditions to be reported on exactly: a
/// required condition missing or observed true fails, and a reported
/// condition not declared also fails.
fn reconcile_forbidden_conditions(
    report: &str,
    equivalence: &Value,
    must_not_observe: &[String],
) -> Vec<String> {
    let mut violations = Vec::new();
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
                "the {report} comparison recorded {condition} more than once",
            ));
            continue;
        }
        observed_conditions.insert(condition.to_owned(), observed);
    }
    for condition in must_not_observe {
        match observed_conditions.get(condition) {
            None => violations.push(format!(
                "the {report} comparison is required to report on {condition} and recorded no \
                 such condition",
            )),
            Some(true) => violations.push(format!(
                "the {report} comparison observed {condition}, which the campaign requires it \
                 not to",
            )),
            Some(false) => {}
        }
    }
    for name in observed_conditions.keys() {
        if !must_not_observe.iter().any(|condition| condition == name) {
            violations.push(format!(
                "the {report} comparison reported on {name}, which is not one of its declared \
                 must_not_observe conditions",
            ));
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
                "informational_symbols": resource
                    .informational_symbols
                    .iter()
                    .map(|symbol| json!({ "symbol": symbol.symbol, "reason": symbol.reason }))
                    .collect::<Vec<_>>(),
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
    /// Complete raw observations by report, for relations spanning more than one cell.
    reports: BTreeMap<String, Value>,
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
            resources.push(read_resource(resource)?);
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

/// Reads one declared resource, including its proof kinds and whichever of
/// `bounds`, `bound_kind`, `parent_connections`, or `subjects` its proof
/// kinds require.
fn read_resource(resource: &Value) -> Result<Resource, String> {
    let proofs = resource
        .get("proofs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if proofs.is_empty() {
        return Err(format!(
            "{} declares no proof kinds",
            resource
                .get("resource")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>"),
        ));
    }
    let name = suite::string(resource, "resource")?;
    let bounds = read_bounds(resource, &name)?;
    let subjects = read_subjects(resource, &name)?;
    let informational_symbols = read_informational_symbols(resource, &name)?;
    let additional_boundaries = read_additional_boundaries(resource, &name)?;
    Ok(Resource {
        class: suite::string(resource, "class")?,
        policy: suite::string(resource, "policy")?,
        report: suite::string(resource, "report")?,
        unit: suite::string(resource, "unit")?,
        ceiling: resource.get("ceiling").and_then(Value::as_i64),
        postgres: resource
            .get("postgres")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        proofs,
        bounds,
        bound_kind: resource
            .get("bound_kind")
            .and_then(Value::as_str)
            .map(str::to_owned),
        parent_connections: resource.get("parent_connections").and_then(Value::as_i64),
        subjects,
        shedding_rule: resource
            .get("shedding_rule")
            .and_then(Value::as_str)
            .map(str::to_owned),
        informational_symbols,
        additional_boundaries,
        name,
    })
}

/// Reads the extra root construction cells a construction-boundary
/// resource's report legitimately records beyond its base ceiling
/// accept/refuse pair, failing closed on a missing value, side, or label.
fn read_additional_boundaries(
    resource: &Value,
    name: &str,
) -> Result<Vec<AdditionalBoundary>, String> {
    let mut additional = Vec::new();
    for entry in resource
        .get("additional_boundaries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let value = entry
            .get("value")
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("{name} declares an additional boundary with no value"))?;
        let expected = entry
            .get("expected")
            .and_then(Value::as_str)
            .filter(|side| *side == "accepted" || *side == "refused")
            .ok_or_else(|| {
                format!(
                    "{name} declares an additional boundary with no \"accepted\" or \"refused\" \
                     expected side",
                )
            })?
            .to_owned();
        let label = entry
            .get("label")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{name} declares an additional boundary with no label"))?
            .to_owned();
        additional.push(AdditionalBoundary {
            value,
            expected,
            label,
        });
    }
    Ok(additional)
}

/// Reads a range-boundary resource's inclusive minimum/maximum, failing
/// closed on a missing value or a unit missing or disagreeing between sides.
fn read_bounds(resource: &Value, name: &str) -> Result<Option<Bounds>, String> {
    let Some(bounds) = resource.get("bounds") else {
        return Ok(None);
    };
    let minimum = bounds
        .pointer("/minimum/value")
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("{name} declares bounds with no minimum.value"))?;
    let maximum = bounds
        .pointer("/maximum/value")
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("{name} declares bounds with no maximum.value"))?;
    let minimum_unit = bounds.pointer("/minimum/unit").and_then(Value::as_str);
    let maximum_unit = bounds.pointer("/maximum/unit").and_then(Value::as_str);
    let (Some(minimum_unit), Some(maximum_unit)) = (minimum_unit, maximum_unit) else {
        return Err(format!(
            "{name} declares bounds with no unit on one or both sides"
        ));
    };
    if minimum_unit != maximum_unit {
        return Err(format!(
            "{name} declares a minimum unit of {minimum_unit} and a maximum unit of \
             {maximum_unit}, which must be the same unit",
        ));
    }
    Ok(Some(Bounds {
        minimum,
        maximum,
        unit: minimum_unit.to_owned(),
    }))
}

/// Reads a subject-boundary resource's per-subject denominator, failing
/// closed on a missing subject name, ceiling, or unit.
fn read_subjects(resource: &Value, name: &str) -> Result<Vec<Subject>, String> {
    let mut subjects = Vec::new();
    for entry in resource
        .get("subjects")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let subject = entry
            .get("subject")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{name} declares a subject with no subject name"))?
            .to_owned();
        let ceiling = entry
            .get("ceiling")
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("{name}'s {subject} subject declares no numeric ceiling"))?;
        let unit = entry
            .get("unit")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{name}'s {subject} subject declares no unit"))?
            .to_owned();
        subjects.push(Subject {
            subject,
            ceiling,
            unit,
        });
    }
    Ok(subjects)
}

/// Reads the symbols this resource classifies as a default or capacity hint
/// rather than a ceiling, failing closed on a missing symbol or reason.
fn read_informational_symbols(
    resource: &Value,
    name: &str,
) -> Result<Vec<InformationalSymbol>, String> {
    let mut informational_symbols = Vec::new();
    for entry in resource
        .get("informational_symbols")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        informational_symbols.push(InformationalSymbol {
            symbol: entry
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{name} declares an informational symbol with no symbol"))?
                .to_owned(),
            reason: entry
                .get("reason")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{name} declares an informational symbol with no reason"))?
                .to_owned(),
        });
    }
    Ok(informational_symbols)
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
    /// Canonical unit the denominator states this resource in.
    unit: String,
    /// The declared ceiling, when it is a number.
    ceiling: Option<i64>,
    /// Whether the proof is required to be on `PostgreSQL`.
    postgres: bool,
    /// The proof kinds this resource's raw evidence must satisfy exactly.
    proofs: Vec<String>,
    /// The inclusive minimum/maximum range for a range-boundary resource.
    bounds: Option<Bounds>,
    /// How a resource with no declared ceiling is bounded instead, when it is
    /// not a range: `"derived"` for `repository-connection-capacity`.
    bound_kind: Option<String>,
    /// The parent's own share of a derived connection-capacity resource.
    parent_connections: Option<i64>,
    /// The per-subject denominator for a subject-boundary resource. Reused
    /// beyond bounded-identifier-text for any resource whose evidence must
    /// prove more than one independent named bound — `durable-state-envelope`
    /// and `cli-configuration-document` each sweep two or three dimensions of
    /// themselves this way rather than one ceiling.
    subjects: Vec<Subject>,
    /// The overload-shedding relation a bounded-shedding resource contracts
    /// for, when its policy is `"bounded-shedding"`.
    shedding_rule: Option<String>,
    /// Symbols that match the bound-declaration convention but name a default
    /// or capacity hint rather than an independently enforced ceiling: no
    /// refusal exists to prove past them, so they are classified here rather
    /// than given a proof obligation the code has nothing to satisfy it with.
    informational_symbols: Vec<InformationalSymbol>,
    /// Extra root construction cells a construction-boundary resource's own
    /// report legitimately records beyond the declared-ceiling accept/refuse
    /// pair — an interior edge case such as "zero workers" or "an empty
    /// page" — each declared explicitly so the exact-set check can tell a
    /// reviewed, intentional cell from an undeclared or bogus one.
    additional_boundaries: Vec<AdditionalBoundary>,
}

/// One declared root cell a construction-boundary resource's evidence
/// carries beyond its base ceiling accept/refuse pair.
struct AdditionalBoundary {
    value: i64,
    expected: String,
    label: String,
}

/// An inclusive minimum/maximum range, with the unit both sides are stated
/// in. A range-boundary resource's raw evidence must carry the same unit on
/// every cell, or the boundary values it proves are not comparable to what is
/// declared here.
struct Bounds {
    minimum: i64,
    maximum: i64,
    unit: String,
}

/// One subject a subject-boundary resource sweeps, with its own real ceiling
/// and the unit that ceiling and its raw evidence are both stated in.
#[allow(clippy::struct_field_names)]
struct Subject {
    subject: String,
    ceiling: i64,
    unit: String,
}

/// A symbol classified as a default or capacity hint rather than a ceiling:
/// real per the bound-declaration convention, but with no refusal boundary
/// this campaign's harness can construct past.
struct InformationalSymbol {
    symbol: String,
    reason: String,
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
        AdditionalBoundary, Bounds, Comparison, Report, Resource, Runs, Scope, StressRequirement,
        Subject, collect_evidence, execution_manifest, reconcile_denominator,
        reconcile_equivalence, reconcile_matrix_point, reconcile_stress,
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
                    &["construction-boundary", "stress-saturation"],
                ),
                resource(
                    "resource-b",
                    "buffer",
                    "fail-closed",
                    "report-a",
                    Some(5),
                    false,
                    &["construction-boundary"],
                ),
                resource(
                    "resource-c",
                    "queue",
                    "bounded-shedding",
                    "report-b",
                    Some(20),
                    false,
                    &["stress-saturation"],
                ),
                resource(
                    "resource-d",
                    "queue",
                    "fail-closed",
                    "report-a",
                    None,
                    true,
                    &["stress-saturation"],
                ),
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
        proofs: &[&str],
    ) -> Resource {
        Resource {
            name: name.to_owned(),
            class: class.to_owned(),
            policy: policy.to_owned(),
            report: report.to_owned(),
            unit: "units".to_owned(),
            ceiling,
            postgres,
            proofs: proofs.iter().map(|proof| (*proof).to_owned()).collect(),
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
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
                    "connection_capacity": 8,
                    "rejections": 1,
                    "detail": {
                        "concurrent_children": 8,
                        "required_connections": 9,
                        "short_pool": 8,
                        "sufficient_pool": 9,
                        "residue_after_refusal": { "ob_job_instance": 0 }
                    },
                    "passed": true,
                },
            ],
            "construction": [
                { "resource": "resource-a", "case": "at the ceiling", "declared_ceiling": 10, "unit": "units", "value": 10, "expected": "accepted", "observed": "accepted" },
                { "resource": "resource-a", "case": "one past", "declared_ceiling": 10, "unit": "units", "value": 11, "expected": "refused", "observed": "refused" },
                { "resource": "resource-b", "case": "at the ceiling", "declared_ceiling": 5, "unit": "units", "value": 5, "expected": "accepted", "observed": "accepted" },
                { "resource": "resource-b", "case": "one past", "declared_ceiling": 5, "unit": "units", "value": 6, "expected": "refused", "observed": "refused" },
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
                    "retained": 20,
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
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
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
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .retain(|entry| entry.get("resource").and_then(Value::as_str) != Some("resource-a"));
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
                .is_some_and(|violation| violation.contains("refused at exactly 6")),
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

    /// A scope with exactly one report and one resource, for a proof-kind
    /// check whose evidence shape does not depend on anything `scope()`'s
    /// four resources already cover.
    fn minimal_scope(resource: Resource) -> Scope {
        Scope {
            fixtures: BTreeMap::new(),
            reports: vec![report("report-x")],
            resources: vec![resource],
            classes: Value::Null,
            policies: Value::Null,
            stress: Vec::new(),
            stress_document: Value::Null,
            equivalence: Vec::new(),
            excluded: Value::Null,
            related: Value::Null,
        }
    }

    fn range_boundary_resource() -> Resource {
        Resource {
            name: "resource-range".to_owned(),
            class: "queue".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: None,
            postgres: false,
            proofs: vec!["range-boundary".to_owned()],
            bounds: Some(Bounds {
                minimum: 100,
                maximum: 1000,
                unit: "widgets".to_owned(),
            }),
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    fn range_cell(value: i64, declared: i64, expected: &str) -> Value {
        json!({
            "resource": "resource-range",
            "case": "boundary",
            "declared_ceiling": declared,
            "unit": "widgets",
            "value": value,
            "expected": expected,
            "observed": expected,
        })
    }

    #[test]
    fn a_complete_range_boundary_reconciles_clean() {
        let scope = minimal_scope(range_boundary_resource());
        let observation = json!({
            "construction": [
                range_cell(100, 100, "accepted"),
                range_cell(99, 100, "refused"),
                range_cell(1000, 1000, "accepted"),
                range_cell(1001, 1000, "refused"),
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn a_range_boundary_missing_the_minimum_side_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let observation = json!({
            "construction": [
                range_cell(1000, 1000, "accepted"),
                range_cell(1001, 1000, "refused"),
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "accepted exactly at the minimum").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_range_boundary_cell_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let observation = json!({
            "construction": [
                range_cell(100, 100, "accepted"),
                range_cell(100, 100, "accepted"),
                range_cell(99, 100, "refused"),
                range_cell(1000, 1000, "accepted"),
                range_cell(1001, 1000, "refused"),
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-range")
                .is_some_and(|violation| violation.contains("recorded 2 times")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_same_count_bogus_range_cell_substitution_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        // The minimum-accept cell is missing, and a bogus cell at an
        // unrelated value fills its place — the total cell count is
        // unchanged, so only exact-identity matching (not a count check)
        // catches the swap.
        let observation = json!({
            "construction": [
                range_cell(500, 100, "accepted"),
                range_cell(99, 100, "refused"),
                range_cell(1000, 1000, "accepted"),
                range_cell(1001, 1000, "refused"),
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "accepted exactly at the minimum").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_range_boundary_cell_with_the_wrong_unit_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let mut cells = vec![
            range_cell(100, 100, "accepted"),
            range_cell(99, 100, "refused"),
            range_cell(1000, 1000, "accepted"),
            range_cell(1001, 1000, "refused"),
        ];
        cells[0]["unit"] = json!("gadgets");
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-range")
                .is_some_and(|violation| violation.contains("recorded a unit of")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_range_boundary_cell_with_no_unit_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let mut cells = vec![
            range_cell(100, 100, "accepted"),
            range_cell(99, 100, "refused"),
            range_cell(1000, 1000, "accepted"),
            range_cell(1001, 1000, "refused"),
        ];
        cells[0].as_object_mut().expect("object").remove("unit");
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-range")
                .is_some_and(|violation| violation.contains("recorded no unit")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_range_boundary_cell_with_a_mismatched_declared_bound_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let mut cells = vec![
            range_cell(100, 100, "accepted"),
            range_cell(99, 100, "refused"),
            range_cell(1000, 1000, "accepted"),
            range_cell(1001, 1000, "refused"),
        ];
        cells[2]["declared_ceiling"] = json!(999);
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-range")
                .is_some_and(|violation| violation.contains("recorded a declared bound of")),
            "{violations:?}"
        );
    }

    fn subject_boundary_resource() -> Resource {
        Resource {
            name: "resource-subjects".to_owned(),
            class: "buffer".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: None,
            postgres: false,
            proofs: vec!["subject-boundary".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: vec![
                Subject {
                    subject: "alpha".to_owned(),
                    ceiling: 10,
                    unit: "bytes".to_owned(),
                },
                Subject {
                    subject: "beta".to_owned(),
                    ceiling: 20,
                    unit: "bytes".to_owned(),
                },
            ],
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    fn subject_cell(subject: &str, ceiling: i64, value: i64, expected: &str) -> Value {
        json!({
            "resource": "resource-subjects",
            "subject": subject,
            "case": "boundary",
            "declared_ceiling": ceiling,
            "unit": "bytes",
            "value": value,
            "expected": expected,
            "observed": expected,
        })
    }

    fn valid_subject_cells() -> Vec<Value> {
        vec![
            subject_cell("alpha", 10, 10, "accepted"),
            subject_cell("alpha", 10, 11, "refused"),
            subject_cell("beta", 20, 20, "accepted"),
            subject_cell("beta", 20, 21, "refused"),
        ]
    }

    #[test]
    fn a_complete_subject_boundary_reconciles_clean() {
        let scope = minimal_scope(subject_boundary_resource());
        let observation = json!({ "construction": valid_subject_cells() });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn an_undeclared_subject_is_rejected() {
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells.push(subject_cell("gamma", 5, 5, "accepted"));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "gamma")
                .is_some_and(|violation| violation.contains("not one of its declared subjects")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_declared_subject_is_rejected() {
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells.retain(|cell| cell.get("subject").and_then(Value::as_str) != Some("beta"));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "beta")
                .is_some_and(|violation| violation.contains("no report recorded evidence")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_identifier_cell_is_rejected() {
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells.push(subject_cell("alpha", 10, 10, "accepted"));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "alpha")
                .is_some_and(|violation| violation.contains("recorded 2 times")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_subjectless_construction_cell_is_rejected() {
        // A cell with no `subject` field cannot be attributed to any
        // declared subject, so it is not silently ignored: it is itself a
        // violation, on top of every declared subject's own pair still
        // being required in full.
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells.push(json!({
            "resource": "resource-subjects",
            "case": "boundary",
            "declared_ceiling": 10,
            "unit": "bytes",
            "value": 10,
            "expected": "accepted",
            "observed": "accepted",
        }));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "no subject").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_subject_boundary_cell_with_the_wrong_unit_is_rejected() {
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells[0]["unit"] = json!("characters");
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "alpha")
                .is_some_and(|violation| violation.contains("recorded a unit of")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_subject_boundary_cell_with_a_mismatched_declared_ceiling_is_rejected() {
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells[0]["declared_ceiling"] = json!(999);
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "alpha")
                .is_some_and(|violation| violation.contains("recorded a declared bound of")),
            "{violations:?}"
        );
    }

    fn derived_capacity_resource() -> Resource {
        Resource {
            name: "resource-derived".to_owned(),
            class: "worker-assignment".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: None,
            postgres: true,
            proofs: vec!["derived-capacity".to_owned()],
            bounds: None,
            bound_kind: Some("derived".to_owned()),
            parent_connections: Some(1),
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn a_correct_derivation_reconciles_clean() {
        let scope = minimal_scope(derived_capacity_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-derived",
                "detail": { "concurrent_children": 7, "required_connections": 8 },
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn a_required_connections_count_that_does_not_match_its_own_derivation_is_rejected() {
        let scope = minimal_scope(derived_capacity_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-derived",
                "detail": { "concurrent_children": 7, "required_connections": 9 },
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-derived").is_some(),
            "{violations:?}"
        );
    }

    fn dual_budget_resource() -> Resource {
        Resource {
            name: "resource-dual".to_owned(),
            class: "worker-assignment".to_owned(),
            policy: "bounded-concurrency".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(8),
            postgres: true,
            proofs: vec!["dual-budget-boundary".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn a_ceiling_run_that_never_reached_the_declared_ceiling_is_rejected() {
        let scope = minimal_scope(dual_budget_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-dual",
                "declared_ceiling": 8,
                "detail": {
                    "budgeted_run": { "budget": 4, "offered": 10, "peak": 4 },
                    "ceiling_run": { "budget": 8, "offered": 10, "peak": 6 },
                },
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-dual").is_some(),
            "{violations:?}"
        );
    }

    fn search_bounded_resource() -> Resource {
        Resource {
            name: "resource-search".to_owned(),
            class: "buffer".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(1000),
            postgres: false,
            proofs: vec!["search-bounded-construction".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn a_search_bounded_accept_above_the_declared_ceiling_is_rejected() {
        let scope = minimal_scope(search_bounded_resource());
        let observation = json!({
            "construction": [
                {
                    "resource": "resource-search",
                    "case": "the largest chain any bound admits",
                    "value": 1001,
                    "expected": "accepted",
                    "observed": "accepted",
                },
                {
                    "resource": "resource-search",
                    "case": "one node past the largest chain that fits",
                    "value": 900,
                    "expected": "refused",
                    "observed": "refused",
                },
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "no construction accepted at or under it").is_some(),
            "{violations:?}"
        );
    }

    fn upper_bound_only_resource() -> Resource {
        Resource {
            name: "resource-upper".to_owned(),
            class: "buffer".to_owned(),
            policy: "bounded-truncation".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(4096),
            postgres: false,
            proofs: vec!["upper-bound-only".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn an_upper_bound_only_resource_with_no_accepted_evidence_at_or_under_it_is_rejected() {
        let scope = minimal_scope(upper_bound_only_resource());
        let observation = json!({
            "construction": [{
                "resource": "resource-upper",
                "case": "the transitions of the largest chain any bound admits",
                "value": 5000,
                "expected": "accepted",
                "observed": "accepted",
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "no construction accepted at or under it").is_some(),
            "{violations:?}"
        );
    }

    fn truncation_resource() -> Resource {
        Resource {
            name: "resource-truncated".to_owned(),
            class: "result-set".to_owned(),
            policy: "bounded-truncation".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(262_144),
            postgres: true,
            proofs: vec!["truncation".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn a_truncation_proof_with_no_truncated_pages_is_rejected() {
        let scope = minimal_scope(truncation_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-truncated",
                "configured_ceiling": 262_144,
                "observed_peak_occupancy": 262_144,
                "truncated_pages": 0,
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "never actually exercised").is_some(),
            "{violations:?}"
        );
    }

    fn refusal_past_ceiling_resource() -> Resource {
        Resource {
            name: "resource-refused".to_owned(),
            class: "buffer".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(1_048_576),
            postgres: true,
            proofs: vec!["refusal-past-ceiling".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    #[test]
    fn a_refusal_past_ceiling_resource_with_the_refused_flag_false_is_rejected() {
        let scope = minimal_scope(refusal_past_ceiling_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-refused",
                "configured_ceiling": 1_048_576,
                "offered_load": 1_048_577,
                "refused": false,
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "did not record a refusal").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_refusal_past_ceiling_resource_offered_at_or_under_the_ceiling_is_rejected() {
        let scope = minimal_scope(refusal_past_ceiling_resource());
        let observation = json!({
            "resources": [{
                "resource": "resource-refused",
                "configured_ceiling": 1_048_576,
                "offered_load": 1_048_576,
                "refused": true,
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "never exceeded it").is_some(),
            "{violations:?}"
        );
    }

    #[test]
    fn a_numeric_pass_with_no_construction_evidence_still_fails_construction_boundary() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .retain(|cell| cell.get("resource").and_then(Value::as_str) != Some("resource-a"));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-a").is_some_and(|violation| violation.contains(
                "no report recorded any construction \
                                                               cells"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn a_construction_refusal_two_past_the_ceiling_instead_of_one_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .iter_mut()
            .filter(|cell| cell.get("resource").and_then(Value::as_str) == Some("resource-b"))
            .for_each(|cell| {
                if cell.get("case").and_then(Value::as_str) == Some("one past") {
                    cell["value"] = json!(7);
                }
            });
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b")
                .is_some_and(|violation| violation.contains("refused at exactly 6")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_construction_cell_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        let duplicate = report_a["construction"]
            .as_array()
            .expect("construction array")
            .iter()
            .find(|cell| {
                cell.get("resource").and_then(Value::as_str) == Some("resource-b")
                    && cell.get("case").and_then(Value::as_str) == Some("at the ceiling")
            })
            .cloned()
            .expect("resource-b's accept cell");
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .push(duplicate);
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b")
                .is_some_and(|violation| violation.contains("recorded 2 times")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_same_count_bogus_construction_cell_substitution_is_rejected() {
        // resource-b's accept cell is removed, and a bogus cell at an
        // unrelated value fills its place — the total construction-cell
        // count for resource-b is unchanged, so only exact-identity
        // matching, not a count check, catches the swap.
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .iter_mut()
            .filter(|cell| cell.get("resource").and_then(Value::as_str) == Some("resource-b"))
            .for_each(|cell| {
                if cell.get("case").and_then(Value::as_str) == Some("at the ceiling") {
                    cell["value"] = json!(3);
                }
            });
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
    fn a_construction_boundary_extra_undeclared_cell_is_rejected() {
        // resource-b declares no additional_boundaries, so a third,
        // otherwise-plausible-looking cell is not tolerated: the root cell
        // set is exact, not a minimum.
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["construction"]
            .as_array_mut()
            .expect("construction array")
            .push(json!({
                "resource": "resource-b",
                "case": "an interior value nobody declared",
                "value": 2,
                "expected": "refused",
                "observed": "refused",
            }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b").is_some_and(|violation| violation.contains(
                "not one of its declared root \
                                                               cells"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn a_declared_additional_boundary_missing_is_rejected() {
        let scope = minimal_scope(Resource {
            name: "resource-with-extra".to_owned(),
            class: "buffer".to_owned(),
            policy: "fail-closed".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(10),
            postgres: false,
            proofs: vec!["construction-boundary".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: None,
            informational_symbols: Vec::new(),
            additional_boundaries: vec![AdditionalBoundary {
                value: 0,
                expected: "refused".to_owned(),
                label: "zero".to_owned(),
            }],
        });
        let observation = json!({
            "construction": [
                {
                    "resource": "resource-with-extra",
                    "case": "at the ceiling",
                    "declared_ceiling": 10,
                    "unit": "units",
                    "value": 10,
                    "expected": "accepted",
                    "observed": "accepted",
                },
                {
                    "resource": "resource-with-extra",
                    "case": "one past the ceiling",
                    "declared_ceiling": 10,
                    "unit": "units",
                    "value": 11,
                    "expected": "refused",
                    "observed": "refused",
                },
            ],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-with-extra")
                .is_some_and(|violation| violation.contains("recorded no construction zero")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_range_boundary_extra_undeclared_cell_is_rejected() {
        let scope = minimal_scope(range_boundary_resource());
        let mut cells = vec![
            range_cell(100, 100, "accepted"),
            range_cell(99, 100, "refused"),
            range_cell(1000, 1000, "accepted"),
            range_cell(1001, 1000, "refused"),
        ];
        cells.push(range_cell(500, 100, "accepted"));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-range").is_some_and(|violation| violation
                .contains(
                    "not one of its declared root \
                                                               cells"
                )),
            "{violations:?}"
        );
    }

    #[test]
    fn a_subject_boundary_extra_undeclared_cell_for_a_declared_subject_is_rejected() {
        // "alpha" already has its own correct pair; a third, plausible-value
        // cell for the same declared subject is still not tolerated.
        let scope = minimal_scope(subject_boundary_resource());
        let mut cells = valid_subject_cells();
        cells.push(subject_cell("alpha", 10, 5, "refused"));
        let observation = json!({ "construction": cells });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "alpha").is_some_and(|violation| violation.contains(
                "not one of its declared root \
                                                               cells"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn undeclared_construction_evidence_for_a_numeric_only_resource_is_rejected() {
        // resource-c declares only stress-saturation; construction cells
        // recorded for it anyway are not silently tolerated as bonus
        // evidence — they are unaccounted-for raw evidence.
        let scope = scope();
        let mut report_b = valid_report_b();
        report_b["construction"] = json!([{
            "resource": "resource-c",
            "case": "an unexpected construction cell",
            "value": 1,
            "expected": "accepted",
            "observed": "accepted",
        }]);
        let runs = runs_with(&[("report-a", valid_report_a()), ("report-b", report_b)]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-c").is_some_and(|violation| violation.contains(
                "declares no proof kind that \
                                                               requires them"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn undeclared_numeric_evidence_for_a_construction_only_resource_is_rejected() {
        // resource-b declares only construction-boundary; a numeric entry
        // recorded for it anyway is not silently tolerated.
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .push(json!({
                "resource": "resource-b",
                "configured_ceiling": 5,
                "offered_load": 1,
                "observed_peak_occupancy": 1,
                "drops": 0,
                "passed": true,
            }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-b").is_some_and(|violation| violation.contains(
                "declares no proof kind that \
                                                               requires it"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn a_summarizes_construction_numeric_entry_does_not_require_its_own_proof_kind() {
        // A numeric entry marked summarizes_construction is a derived
        // rollup of the construction cells already checked separately, not
        // independent evidence — it must not force a construction-only
        // resource to also declare a numeric-consuming proof kind.
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["resources"]
            .as_array_mut()
            .expect("resources array")
            .push(json!({
                "resource": "resource-b",
                "configured_ceiling": 5,
                "offered_load": 1,
                "observed_peak_occupancy": 1,
                "drops": 0,
                "passed": true,
                "summarizes_construction": true,
            }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let (evidence, identity_violations) = collect_evidence(&scope, &runs);
        assert!(identity_violations.is_empty(), "{identity_violations:?}");
        let violations = reconcile_denominator(&scope, &evidence);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn an_undeclared_compared_field_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["fields_compared"]
            .as_array_mut()
            .expect("fields_compared array")
            .push(json!({ "field": "field-z", "agrees": true }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "field-z").is_some_and(|violation| violation.contains(
                "not one of its declared \
                                                               must_agree_on fields"
            )),
            "{violations:?}"
        );
    }

    #[test]
    fn an_undeclared_reported_condition_is_rejected() {
        let scope = scope();
        let mut report_a = valid_report_a();
        report_a["durable_equivalence"]["must_not_observe"]
            .as_array_mut()
            .expect("must_not_observe array")
            .push(json!({ "condition": "regression-z", "observed": false }));
        let runs = runs_with(&[("report-a", report_a), ("report-b", valid_report_b())]);
        let violations = reconcile_equivalence(&scope, &runs);
        assert!(
            any_violation(&violations, "regression-z").is_some_and(|violation| violation.contains(
                "not one of its declared \
                                                               must_not_observe conditions"
            )),
            "{violations:?}"
        );
    }

    fn shedding_rule_resource() -> Resource {
        Resource {
            name: "resource-shed".to_owned(),
            class: "queue".to_owned(),
            policy: "bounded-shedding".to_owned(),
            report: "report-x".to_owned(),
            unit: "units".to_owned(),
            ceiling: Some(200),
            postgres: false,
            proofs: vec!["stress-saturation".to_owned()],
            bounds: None,
            bound_kind: None,
            parent_connections: None,
            subjects: Vec::new(),
            shedding_rule: Some("collapse-to-reserved-series".to_owned()),
            informational_symbols: Vec::new(),
            additional_boundaries: Vec::new(),
        }
    }

    fn shedding_scope() -> Scope {
        let mut scope = minimal_scope(shedding_rule_resource());
        scope.stress.push(StressRequirement {
            resource: "resource-shed".to_owned(),
            report: "report-x".to_owned(),
            requires: "offered-exceeds-ceiling".to_owned(),
        });
        scope
    }

    #[test]
    fn a_shedding_rule_that_does_not_match_the_report_is_rejected() {
        let scope = shedding_scope();
        let observation = json!({
            "resources": [{
                "resource": "resource-shed",
                "configured_ceiling": 200,
                "offered_load": 400,
                "observed_peak_occupancy": 200,
                "retained": 200,
                "drops": 200,
                "shedding_rule": "drop-newest",
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-shed")
                .is_some_and(|violation| violation.contains("recorded drop-newest instead")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_collapse_to_reserved_series_accounting_using_the_flat_relation_is_rejected() {
        let scope = shedding_scope();
        let observation = json!({
            "resources": [{
                "resource": "resource-shed",
                "configured_ceiling": 200,
                "offered_load": 400,
                "observed_peak_occupancy": 200,
                "retained": 200,
                "drops": 200,
                "shedding_rule": "collapse-to-reserved-series",
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(
            any_violation(&violations, "resource-shed")
                .is_some_and(|violation| violation.contains("201 should have been discarded")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_collapse_to_reserved_series_accounting_with_the_reserved_slot_offset_reconciles_clean() {
        let scope = shedding_scope();
        let observation = json!({
            "resources": [{
                "resource": "resource-shed",
                "configured_ceiling": 200,
                "offered_load": 400,
                "observed_peak_occupancy": 200,
                "retained": 200,
                "drops": 201,
                "shedding_rule": "collapse-to-reserved-series",
            }],
        });
        let runs = runs_with(&[("report-x", observation)]);
        let (evidence, _) = collect_evidence(&scope, &runs);
        let violations = reconcile_stress(&scope, &evidence);
        assert!(violations.is_empty(), "{violations:?}");
    }
}
