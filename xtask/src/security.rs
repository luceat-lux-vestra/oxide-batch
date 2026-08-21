//! The M5 `PostgreSQL` security campaign runner.
//!
//! The campaign owes three reports: that `verify-full` TLS is what the
//! supported production configuration gives and that nothing weaker is reached
//! by falling back, that no privilege class can exceed itself, and that no
//! prohibited value class reaches a diagnostic surface.
//!
//! This is a command rather than a test for the reason the other campaigns are:
//! two of the three reports return success without a database, because they
//! print a skip line and return. Under `cargo test` that is indistinguishable
//! from evidence. Here the fixtures are resolved first, and a campaign run
//! without them fails before any target starts.
//!
//! Passing tests are not sufficient either. A security report has a sharper
//! version of that problem than most, because the shape of the thing it proves
//! is negative: a report that connected once and never attempted the refusals,
//! or a matrix that filled in one class, or a sweep that injected nothing,
//! would all pass and prove nothing. So each report writes a machine-readable
//! observation into a directory this runner creates empty, and the runner
//! requires the substance rather than the outcome — that the TLS report refused
//! an untrusted authority, a mismatched name, and a server offering no TLS, and
//! left no unencrypted session behind; that every privilege class was observed
//! on both sides of its boundary, through the path an operator uses, with every
//! refusal carrying the code the server uses for want of privilege; and that
//! the sweep covered every surface and every value class and found nothing.
//!
//! It also requires the reports to have run where the campaign says they ran.
//! A matrix point is invisible in a connection string, so a report from one
//! `PostgreSQL` major dropped into a run of another would otherwise reconcile
//! perfectly.
//!
//! The scope document is `tests/fixtures/security/campaign-scope.json`.
//! `crates/oxide-batch/tests/m5_security_campaign.rs` reconciles it against the
//! accepted plan and gate, so this runner consumes a document that ordinary
//! review has already checked.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::suite::{self, TargetCommand};

/// The report this campaign retains.
const REPORT: &str = "security-campaign.json";

/// The directory the reports write their observations into.
const OBSERVATIONS: &str = "security-observations";

/// The variable that tells a report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_SECURITY_OBSERVATIONS";

/// The `SQLSTATE` every refused privilege attempt must carry.
const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// One campaign run and everything it observed.
pub struct Campaign {
    /// Every reconciliation failure, as a human-readable line.
    pub violations: Vec<String>,
    /// Where the raw evidence was written.
    pub report: PathBuf,
}

/// Runs the campaign and writes its report.
///
/// An empty violation list means every report ran on its fixture and every
/// property the support contract promises was observed rather than assumed.
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
    let mut runs = Runs::default();
    if violations.is_empty() {
        let observations = prepare_observations(&root)?;
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
    }

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
/// `crates/oxide-batch/tests/security/mod.rs`'s `execution_manifest` for the
/// two database reports, and `crates/oxide-batch-cli/tests/m5_redaction_sweep.rs`'s
/// own copy for the redaction sweep, which runs in a different workspace
/// crate — because that is the tree the campaign actually ran against; this
/// function only requires the declared reports to agree on it.
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
            "the {fixture} fixture is required by the security campaign and is incomplete: set {}",
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
            violations.extend(reconcile_matrix_point(&report.id, observation));
        }
    }

    violations.extend(reconcile_tls(scope, runs));
    violations.extend(reconcile_classes(scope, runs));
    violations.extend(reconcile_redaction(scope, runs));

    violations
}

/// Requires a database report to name the matrix point the campaign ran at.
///
/// The matrix point is invisible in a connection string, so without this an
/// observation produced against one supported major would reconcile perfectly
/// inside a run of another.
fn reconcile_matrix_point(id: &str, observation: &Value) -> Vec<String> {
    let Some(expected) = env::var(suite::MATRIX)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Vec::new();
    };
    let expected = expected
        .rsplit_once('-')
        .map_or(expected.clone(), |(_, major)| major.to_owned());
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

/// Reports what the TLS report required and did not show.
#[allow(
    clippy::too_many_lines,
    reason = "the exact attempt set, the required connection, and every required refusal are one \
              reconciliation against one observation, and splitting it would scatter the checks \
              that must agree on the same attempt list"
)]
fn reconcile_tls(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get(&scope.tls.report) else {
        // The absent observation is already reported against the report itself.
        return violations;
    };

    if observation.get("tls_mode").and_then(Value::as_str) != Some(scope.tls_mode.as_str()) {
        violations.push(format!(
            "the supported transport is {} and the TLS report recorded {:?}",
            scope.tls_mode,
            observation.get("tls_mode")
        ));
    }

    let attempts = observation
        .get("attempts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    // The exact required attempt set, and nothing else. A missing attempt is
    // already caught below, by name; what this catches is different: an extra
    // attempt the scope does not require, or the same attempt recorded twice,
    // either of which could let a duplicate stand in for a missing one without
    // the count ever looking wrong.
    let required_ids = std::iter::once(scope.tls.connects.as_str())
        .chain(scope.tls.refusals.iter().map(|refusal| refusal.id.as_str()))
        .collect::<BTreeSet<_>>();
    let mut observed_ids = BTreeMap::new();
    for attempt in &attempts {
        let id = attempt.get("id").and_then(Value::as_str).unwrap_or("");
        *observed_ids.entry(id.to_owned()).or_insert(0_u32) += 1;
    }
    for (id, count) in &observed_ids {
        if *count > 1 {
            violations.push(format!(
                "the {id} attempt was recorded {count} times, which could let a duplicate stand \
                 in for a missing one"
            ));
        }
        if !required_ids.contains(id.as_str()) {
            violations.push(format!(
                "the report recorded an attempt named {id}, which the scope does not require"
            ));
        }
    }
    if attempts.len() != required_ids.len() {
        violations.push(format!(
            "the TLS report must record exactly {} attempts and recorded {}",
            required_ids.len(),
            attempts.len()
        ));
    }

    let connected = attempts.iter().find(|attempt| {
        attempt.get("id").and_then(Value::as_str) == Some(scope.tls.connects.as_str())
    });
    match connected.and_then(|attempt| attempt.pointer("/observed/result")) {
        Some(Value::String(result)) if result == "connected" => {
            let encrypted = connected
                .and_then(|attempt| attempt.pointer("/observed/encrypted_sessions"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if encrypted == 0 {
                violations.push(
                    "the TLS report connected and the server reported no encrypted session for \
                     it, so nothing says the session was actually protected"
                        .to_owned(),
                );
            }
        }
        other => violations.push(format!(
            "the supported configuration must connect as {} and the report recorded {other:?}",
            scope.tls.connects,
        )),
    }

    for refusal in &scope.tls.refusals {
        let attempt = attempts
            .iter()
            .find(|attempt| attempt.get("id").and_then(Value::as_str) == Some(refusal.id.as_str()));
        let Some(attempt) = attempt else {
            violations.push(format!(
                "the {} refusal is required and the TLS report attempted nothing under that name",
                refusal.id
            ));
            continue;
        };
        if attempt.pointer("/observed/result").and_then(Value::as_str) != Some("refused") {
            violations.push(format!(
                "the {} attempt must be refused and the report recorded {:?}",
                refusal.id,
                attempt.pointer("/observed/result")
            ));
        }
        let observed_class = attempt
            .pointer("/observed/failure_class")
            .and_then(Value::as_str);
        if observed_class != Some(refusal.failure_class.as_str()) {
            violations.push(format!(
                "the {} attempt must be refused as {} and the transport refused it as {:?}",
                refusal.id, refusal.failure_class, observed_class
            ));
        }
        if attempt
            .pointer("/observed/plaintext_sessions")
            .and_then(Value::as_u64)
            != Some(0)
        {
            violations.push(format!(
                "the {} attempt was followed by an unencrypted session, which is the fallback \
                 the supported mode exists to prevent",
                refusal.id
            ));
        }
    }

    if observation
        .get("residual_sessions_plaintext")
        .and_then(Value::as_u64)
        != Some(0)
    {
        violations
            .push("the TLS report left an unencrypted session behind when it finished".to_owned());
    }

    violations
}

/// Reports what the privilege matrix required and did not show.
#[allow(
    clippy::too_many_lines,
    reason = "the undeclared-class check, the duplicate-cell check, and the exact per-class cell \
              counts all reconcile the same matrix against the same denominator, and splitting \
              them would scatter checks that have to see the whole matrix to be exact"
)]
fn reconcile_classes(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get("least-privilege-roles") else {
        return violations;
    };

    if observation
        .get("public_grants")
        .and_then(Value::as_array)
        .is_none_or(|grants| !grants.is_empty())
    {
        violations.push(
            "PUBLIC still holds a privilege in the metadata schema, so no grant the matrix \
             checks is a boundary"
                .to_owned(),
        );
    }

    let attributes = observation
        .get("class_attributes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let matrix = observation
        .get("matrix")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let declared_classes = scope
        .classes
        .iter()
        .map(|class| class.name.as_str())
        .collect::<BTreeSet<_>>();

    // A class attribute or a cell filed under a name the scope does not
    // declare cannot be evidence of anything the campaign owes: nothing
    // requires it, and nothing reconciles it against a boundary.
    for entry in &attributes {
        if let Some(name) = entry.get("class").and_then(Value::as_str)
            && !declared_classes.contains(name)
        {
            violations.push(format!(
                "the matrix reports cluster-level attributes for {name}, which the scope does not \
                 declare as a privilege class"
            ));
        }
    }
    for cell in &matrix {
        if let Some(name) = cell.get("class").and_then(Value::as_str)
            && !declared_classes.contains(name)
        {
            violations.push(format!(
                "the matrix records a cell for {name}, which the scope does not declare as a \
                 privilege class"
            ));
        }
    }

    // No two cells may share an identity. A duplicate is not redundant
    // evidence: it is exactly the shape a missing cell would take if the
    // count were checked by "at least one" instead of exactly, so it is
    // caught before the per-class counts are checked at all.
    let mut cell_identities = BTreeMap::new();
    for cell in &matrix {
        let identity = (
            cell.get("class").and_then(Value::as_str).unwrap_or(""),
            cell.get("operation").and_then(Value::as_str).unwrap_or(""),
            cell.get("surface").and_then(Value::as_str).unwrap_or(""),
        );
        *cell_identities.entry(identity).or_insert(0_u32) += 1;
    }
    for ((class, operation, surface), count) in &cell_identities {
        if *count > 1 {
            violations.push(format!(
                "the {class} class's {operation} cell on the {surface} surface was recorded \
                 {count} times"
            ));
        }
    }

    for class in &scope.classes {
        let held = attributes
            .iter()
            .find(|entry| entry.get("class").and_then(Value::as_str) == Some(class.name.as_str()));
        let Some(held) = held else {
            violations.push(format!(
                "the {} class is required and the matrix reported no cluster-level attributes \
                 for it",
                class.name
            ));
            continue;
        };
        for attribute in ["superuser", "createdb", "createrole", "replication"] {
            if held.get(attribute).and_then(Value::as_bool) != Some(false) {
                violations.push(format!(
                    "the {} class holds the cluster-level {attribute} privilege",
                    class.name
                ));
            }
        }

        let cells = matrix
            .iter()
            .filter(|cell| cell.get("class").and_then(Value::as_str) == Some(class.name.as_str()))
            .collect::<Vec<_>>();

        // Exact, not "at least one": a matrix that filled in one cell per side
        // and stopped would pass a presence check and fail this one.
        for (expected, required) in [
            ("allowed", class.allowed_cells),
            ("forbidden", class.forbidden_cells),
        ] {
            let observed = cells
                .iter()
                .filter(|cell| cell.get("expected").and_then(Value::as_str) == Some(expected))
                .count() as u64;
            if observed != required {
                violations.push(format!(
                    "the {} class must record exactly {required} {expected} cells and the matrix \
                     recorded {observed}",
                    class.name
                ));
            }
        }
        if !cells.iter().any(|cell| {
            cell.get("surface").and_then(Value::as_str) == Some("service-path")
                && cell.get("expected").and_then(Value::as_str) == Some("allowed")
        }) {
            violations.push(format!(
                "the {} class proved nothing through the path an operator would use",
                class.name
            ));
        }
    }

    // The whole matrix, not only each class's slice of it: a cell filed under
    // no declared class would inflate the total without appearing in any
    // per-class count above.
    if matrix.len() as u64 != scope.role_matrix_total_cells {
        violations.push(format!(
            "the role matrix must record exactly {} cells in total and recorded {}",
            scope.role_matrix_total_cells,
            matrix.len()
        ));
    }

    // The exact identity set the committed role-matrix denominator declares,
    // not merely its shape. The checks above are counts, and a count is not
    // an identity: a report that removed one legitimate boundary and added a
    // same-class, same-surface, same-expected bogus cell in its place would
    // keep every count above unchanged and pass all of them. This is the
    // check that catches that substitution.
    let observed_identities = matrix
        .iter()
        .map(|cell| {
            (
                cell.get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                cell.get("class")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                cell.get("surface")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                cell.get("expected")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            )
        })
        .collect::<BTreeSet<_>>();
    for missing in scope.role_matrix_cells.difference(&observed_identities) {
        violations.push(format!(
            "the role matrix denominator declares {missing:?} and the observation does not \
             record it"
        ));
    }
    for undeclared in observed_identities.difference(&scope.role_matrix_cells) {
        violations.push(format!(
            "the role matrix records {undeclared:?}, which the committed denominator at \
             tests/fixtures/security/role-matrix.json does not declare"
        ));
    }

    // A cell's declared side must be what the server actually did, not only
    // what the identity check above found present. A cell recorded as
    // allowed whose server outcome was not a success, or one recorded as
    // forbidden whose server outcome was not a refusal, is not evidence of
    // either side of the boundary.
    for cell in &matrix {
        let id = cell.get("id").and_then(Value::as_str).unwrap_or("<no id>");
        let observed = cell.get("observed").and_then(Value::as_str);
        match cell.get("expected").and_then(Value::as_str) {
            Some("allowed") if observed != Some("succeeded") => violations.push(format!(
                "{id} is declared allowed and the server recorded {observed:?}"
            )),
            Some("forbidden") if observed != Some("refused") => violations.push(format!(
                "{id} is declared forbidden and the server recorded {observed:?}"
            )),
            _ => {}
        }
    }

    // A refusal that was not a privilege refusal is not evidence of a boundary.
    // A constraint violation and a missing table both merely fail.
    for cell in &matrix {
        if cell.get("expected").and_then(Value::as_str) != Some("forbidden") {
            continue;
        }
        if cell.get("error_class").and_then(Value::as_str) != Some(INSUFFICIENT_PRIVILEGE) {
            violations.push(format!(
                "the {:?} operation was refused to the {:?} class under {:?} rather than for want \
                 of privilege",
                cell.get("operation").and_then(Value::as_str),
                cell.get("class").and_then(Value::as_str),
                cell.get("error_class"),
            ));
        }
    }

    if observation.get("schema_version").and_then(Value::as_u64) != Some(scope.schema_version) {
        violations.push(format!(
            "the matrix must be checked on schema {} and it recorded {:?}",
            scope.schema_version,
            observation.get("schema_version")
        ));
    }

    violations
}

/// Reports what the redaction sweep required and did not show.
#[allow(
    clippy::too_many_lines,
    reason = "the exact surface set, the exact value-class set, their duplicate checks, and the \
              non-vacuous scan checks all reconcile the same observation against the same scope, \
              and splitting them would scatter checks that have to see the whole observation to \
              be exact"
)]
fn reconcile_redaction(scope: &Scope, runs: &Runs) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(observation) = runs.observations.get(&scope.redaction.report) else {
        return violations;
    };

    match observation
        .get("prohibited_occurrences")
        .and_then(Value::as_u64)
    {
        Some(0) => {}
        other => violations.push(format!(
            "the sweep must find no prohibited value and recorded {other:?} occurrences"
        )),
    }

    let surfaces = observation
        .get("surfaces_scanned")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let required_surfaces = scope
        .redaction
        .surfaces
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let observed_surfaces = surfaces
        .iter()
        .map(|entry| {
            entry
                .get("surface")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    // Exact set, not "every required one is present somewhere": a surface the
    // scope does not declare says nothing about what the scope requires, and
    // letting it stand unremarked is how a sweep that quietly stopped
    // covering a required surface, while adding an unrelated one, could look
    // unchanged in size.
    for missing in required_surfaces.difference(&observed_surfaces) {
        violations.push(format!(
            "the {missing} surface is required and the sweep recorded nothing for it"
        ));
    }
    for extra in observed_surfaces.difference(&required_surfaces) {
        violations.push(format!(
            "the sweep recorded the {extra} surface, which the scope does not declare"
        ));
    }
    for required in &scope.redaction.surfaces {
        let scanned = surfaces
            .iter()
            .find(|entry| entry.get("surface").and_then(Value::as_str) == Some(required.as_str()));
        match scanned
            .and_then(|entry| entry.get("artifacts"))
            .and_then(Value::as_u64)
        {
            Some(count) if count > 0 => {}
            _ => violations.push(format!(
                "the {required} surface is required and the sweep collected nothing from it"
            )),
        }
    }
    // A surface recorded twice could carry the artifact count that hides a
    // surface recorded zero times, if a reader only ever looked for "any"
    // entry with a matching name.
    let mut surface_counts = BTreeMap::new();
    for entry in &surfaces {
        let name = entry
            .get("surface")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        *surface_counts.entry(name).or_insert(0_u32) += 1;
    }
    for (surface, count) in &surface_counts {
        if *count > 1 {
            violations.push(format!("the {surface} surface was recorded {count} times"));
        }
    }

    let classes = observation
        .get("value_classes_scanned")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let required_classes = scope
        .redaction
        .value_classes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let observed_classes = classes
        .iter()
        .map(|entry| {
            entry
                .get("class")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    for missing in required_classes.difference(&observed_classes) {
        violations.push(format!(
            "the {missing} value class is required and the sweep injected no canary for it"
        ));
    }
    for extra in observed_classes.difference(&required_classes) {
        violations.push(format!(
            "the sweep injected the {extra} value class, which the scope does not declare"
        ));
    }
    let mut class_counts = BTreeMap::new();
    for entry in &classes {
        let name = entry
            .get("class")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        *class_counts.entry(name).or_insert(0_u32) += 1;
    }
    for (class, count) in &class_counts {
        if *count > 1 {
            violations.push(format!(
                "the {class} value class was recorded {count} times"
            ));
        }
    }

    // A sweep that collected nothing would report no occurrences too.
    if observation
        .get("artifacts_scanned")
        .and_then(Value::as_u64)
        .is_none_or(|count| count == 0)
    {
        violations.push(
            "the sweep scanned no artifacts, so finding no prohibited value says nothing"
                .to_owned(),
        );
    }
    // Scanning the serialized bytes alone would miss a value that survives
    // only inside a parsed structure; the sweep is required to have scanned
    // strings independently of artifact count for that reason.
    if observation
        .get("strings_scanned")
        .and_then(Value::as_u64)
        .is_none_or(|count| count == 0)
    {
        violations.push(
            "the sweep recorded no scanned strings, so finding no prohibited value says nothing"
                .to_owned(),
        );
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
    manifest: &Value,
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

    let tls = runs.observations.get(&scope.tls.report);
    let roles = runs.observations.get("least-privilege-roles");
    let redaction = runs.observations.get(&scope.redaction.report);

    let document = json!({
        "report": "security",
        "campaign": "M5 PostgreSQL security",
        "scenarios": [
            "verify_full_tls_is_required_in_the_supported_mode",
            "least_privilege_role_cannot_exceed_its_class",
            "redaction_sweep_finds_no_prohibited_value_class",
        ],
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
        "observation": { "execution_manifest": manifest },
        "postgresql_version": tls
            .or(roles)
            .and_then(|observation| observation.get("server_version").cloned()),
        "postgresql_major_version": tls
            .or(roles)
            .and_then(|observation| observation.get("postgres_major_version").cloned()),
        "fixtures": fixtures,
        "policy": scope.policy,
        "tls_mode": scope.tls_mode,
        "tls_certificate_validation_result": tls
            .and_then(|observation| observation.get("certificate_validation_result").cloned()),
        "tls_hostname_validation_result": tls
            .and_then(|observation| observation.get("hostname_validation_result").cloned()),
        "tls_plaintext_fallback_result": tls
            .and_then(|observation| observation.get("plaintext_fallback_result").cloned()),
        "role_matrix_result": roles.map(|observation| json!({
            "schema_version": observation.get("schema_version"),
            "classes": observation.get("classes"),
            "class_attributes": observation.get("class_attributes"),
            "public_grants": observation.get("public_grants"),
            "cells": observation
                .get("matrix")
                .and_then(Value::as_array)
                .map(Vec::len),
        })),
        "redaction_surfaces": redaction
            .and_then(|observation| observation.get("surfaces_scanned").cloned()),
        "redaction_scan_counts": redaction.map(|observation| json!({
            "value_classes_scanned": observation
                .get("value_classes_scanned")
                .and_then(Value::as_array)
                .map(Vec::len),
            "artifacts_scanned": observation.get("artifacts_scanned"),
            "strings_scanned": observation.get("strings_scanned"),
            "prohibited_occurrences": observation.get("prohibited_occurrences"),
        })),
        "reports": reports,
        "privilege_classes": scope.class_document,
        "related": scope.related,
        "violations": violations,
        "passed": violations.is_empty(),
        "result": if violations.is_empty() { "passed" } else { "failed" },
        "notes": [
            "Every report is run on its own so its result is attributable.",
            "A passing report is not sufficient on its own. Each one retains an \
             observation into a directory this runner creates empty, and the \
             runner requires the substance rather than the outcome: that the \
             TLS report refused an untrusted authority, a mismatched name, and \
             a server offering no TLS, and left no unencrypted session behind; \
             that every privilege class was observed on both sides of its \
             boundary and through the path an operator uses, with every refusal \
             carrying 42501; and that the sweep covered every surface and every \
             value class and found nothing.",
            "Each database report is required to name the PostgreSQL major it \
             ran against, because a matrix point is invisible in a connection \
             string and an observation from one supported major would otherwise \
             reconcile perfectly inside a run of another.",
            "No credential, connection string, certificate, or canary is \
             recorded here. The least-privilege classes log in with a password \
             generated for the run, and the sweep records the classes it \
             injected rather than the values.",
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

/// The committed campaign scope document.
struct Scope {
    /// Fixture name to the environment variables it requires.
    fixtures: BTreeMap<String, Vec<String>>,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The transport the support contract names.
    tls_mode: String,
    /// The schema the privilege matrix is checked on.
    schema_version: u64,
    /// What the TLS report must observe.
    tls: Tls,
    /// The privilege classes the campaign must observe.
    classes: Vec<PrivilegeClass>,
    /// The privilege classes as declared, for the retained report.
    class_document: Value,
    /// The exact total cell count the role matrix must record.
    role_matrix_total_cells: u64,
    /// The exact cell identity set (id, class, surface, expected) the role
    /// matrix must equal, from `tests/fixtures/security/role-matrix.json`.
    role_matrix_cells: BTreeSet<(String, String, String, String)>,
    /// What the redaction sweep must cover.
    redaction: Redaction,
    /// The committed policy, as declared.
    policy: Value,
    /// Evidence the campaign keeps and does not run, as declared.
    related: Value,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    #[allow(
        clippy::too_many_lines,
        reason = "the scope document is one denominator, and splitting its reading would scatter \
                  the fields the reconciliation and the retained report both depend on"
    )]
    fn read(root: &Path) -> Result<Self, String> {
        let path = root
            .join("tests")
            .join("fixtures")
            .join("security")
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

        let contract = document
            .get("support_contract")
            .ok_or_else(|| "the scope document declares no support contract".to_owned())?;

        let tls_document = document
            .get("tls")
            .ok_or_else(|| "the scope document declares no TLS obligations".to_owned())?;
        let mut refusals = Vec::new();
        for refusal in array(tls_document, "refusals")? {
            refusals.push(Refusal {
                id: suite::string(refusal, "attempt")?,
                failure_class: suite::string(refusal, "failure_class")?,
            });
        }

        let mut classes = Vec::new();
        for class in array(&document, "privilege_classes")? {
            classes.push(PrivilegeClass {
                name: suite::string(class, "class")?,
                allowed_cells: class
                    .get("allowed_cells")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        format!(
                            "the {:?} class declares no allowed_cells count",
                            class.get("class")
                        )
                    })?,
                forbidden_cells: class
                    .get("forbidden_cells")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        format!(
                            "the {:?} class declares no forbidden_cells count",
                            class.get("class")
                        )
                    })?,
            });
        }

        let role_matrix = document
            .get("role_matrix")
            .ok_or_else(|| "the scope document declares no role matrix denominator".to_owned())?;

        let role_matrix_path = root
            .join("tests")
            .join("fixtures")
            .join("security")
            .join("role-matrix.json");
        let role_matrix_source = fs::read_to_string(&role_matrix_path)
            .map_err(|error| format!("could not read {}: {error}", role_matrix_path.display()))?;
        let role_matrix_document: Value = serde_json::from_str(&role_matrix_source)
            .map_err(|error| format!("could not parse {}: {error}", role_matrix_path.display()))?;
        let mut role_matrix_cells = BTreeSet::new();
        for cell in array(&role_matrix_document, "cells")? {
            role_matrix_cells.insert((
                suite::string(cell, "id")?,
                suite::string(cell, "class")?,
                suite::string(cell, "surface")?,
                suite::string(cell, "expected")?,
            ));
        }
        if role_matrix_cells.len() != array(&role_matrix_document, "cells")?.len() {
            return Err(
                "tests/fixtures/security/role-matrix.json declares a duplicate cell identity"
                    .to_owned(),
            );
        }

        let redaction_document = document
            .get("redaction")
            .ok_or_else(|| "the scope document declares no redaction obligations".to_owned())?;

        Ok(Self {
            fixtures,
            reports,
            tls_mode: suite::string(contract, "tls_mode")?,
            schema_version: contract
                .get("installed_schema_version")
                .and_then(Value::as_u64)
                .ok_or_else(|| "the support contract declares no schema version".to_owned())?,
            tls: Tls {
                report: suite::string(tls_document, "report")?,
                connects: suite::string(tls_document, "connects")?,
                refusals,
            },
            classes,
            class_document: document
                .get("privilege_classes")
                .cloned()
                .unwrap_or(Value::Null),
            role_matrix_total_cells: role_matrix
                .get("total_cells")
                .and_then(Value::as_u64)
                .ok_or_else(|| "the role matrix declares no total_cells".to_owned())?,
            role_matrix_cells,
            redaction: Redaction {
                report: suite::string(redaction_document, "report")?,
                surfaces: text_list(redaction_document, "surfaces"),
                value_classes: text_list(redaction_document, "value_classes"),
            },
            policy: document.get("policy").cloned().unwrap_or(Value::Null),
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

/// Reads a string array field, treating an absent one as empty.
fn text_list(document: &Value, name: &str) -> Vec<String> {
    strings(document, name)
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

/// What the TLS report must observe.
struct Tls {
    /// The report that covers it.
    report: String,
    /// The attempt the supported configuration must complete.
    connects: String,
    /// The refusals the supported configuration must produce.
    refusals: Vec<Refusal>,
}

/// One refusal the supported transport must produce.
struct Refusal {
    /// The attempt the report and the runner share a name for.
    id: String,
    /// The transport reason the refusal must carry.
    failure_class: String,
}

/// One privilege class the campaign must observe on both sides.
struct PrivilegeClass {
    /// The class name the report and the runner share.
    name: String,
    /// The exact number of allowed cells the matrix must record for it.
    allowed_cells: u64,
    /// The exact number of forbidden cells the matrix must record for it.
    forbidden_cells: u64,
}

/// What the redaction sweep must cover.
struct Redaction {
    /// The report that covers it.
    report: String,
    /// The surfaces it must have collected from.
    surfaces: Vec<String>,
    /// The value classes it must have injected.
    value_classes: Vec<String>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::{
        PrivilegeClass, Redaction, Refusal, Report, Runs, Scope, Tls, execution_manifest,
        reconcile_classes, reconcile_redaction, reconcile_tls,
    };
    use serde_json::{Value, json};

    /// A scope shaped like the committed `campaign-scope.json`, without the
    /// filesystem: every reconciliation function under test reads only the
    /// fields built here.
    fn scope() -> Scope {
        Scope {
            fixtures: std::collections::BTreeMap::new(),
            reports: vec![
                report("verify-full-tls", "postgres_verify_full_tls"),
                report("least-privilege-roles", "postgres_least_privilege_roles"),
                report("redaction-sweep", "m5_redaction_sweep"),
            ],
            tls_mode: "verify-full".to_owned(),
            schema_version: 3,
            tls: Tls {
                report: "verify-full-tls".to_owned(),
                connects: "trusted-authority-and-name".to_owned(),
                refusals: vec![
                    refusal("untrusted-authority", "untrusted-authority"),
                    refusal("hostname-mismatch", "hostname-mismatch"),
                    refusal("server-without-tls", "tls-not-offered"),
                ],
            },
            classes: vec![
                class("migration", 2, 4),
                class("runtime", 3, 8),
                class("explorer", 1, 7),
                class("operator", 2, 7),
                class("retention", 3, 7),
            ],
            class_document: Value::Null,
            role_matrix_total_cells: 44,
            role_matrix_cells: role_matrix_cells(),
            redaction: Redaction {
                report: "redaction-sweep".to_owned(),
                surfaces: ["errors", "telemetry", "cli", "bundle"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                value_classes: [
                    "password",
                    "database-url-endpoint",
                    "certificate",
                    "payload",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            },
            policy: Value::Null,
            related: Value::Null,
        }
    }

    fn report(id: &str, target: &str) -> Report {
        Report {
            id: id.to_owned(),
            title: id.to_owned(),
            owes: id.to_owned(),
            package: "oxide-batch".to_owned(),
            target: target.to_owned(),
            name: target.to_owned(),
            fixture: None,
            against_database: true,
        }
    }

    fn refusal(id: &str, failure_class: &str) -> Refusal {
        Refusal {
            id: id.to_owned(),
            failure_class: failure_class.to_owned(),
        }
    }

    fn class(name: &str, allowed_cells: u64, forbidden_cells: u64) -> PrivilegeClass {
        PrivilegeClass {
            name: name.to_owned(),
            allowed_cells,
            forbidden_cells,
        }
    }

    fn runs_with(id: &str, observation: Value) -> Runs {
        let mut runs = Runs::default();
        runs.observations.insert(id.to_owned(), observation);
        runs
    }

    /// A TLS observation that satisfies every requirement `scope()` declares.
    fn valid_tls_observation() -> Value {
        json!({
            "tls_mode": "verify-full",
            "residual_sessions_plaintext": 0,
            "attempts": [
                {
                    "id": "trusted-authority-and-name",
                    "observed": { "result": "connected", "encrypted_sessions": 1, "plaintext_sessions": 0 },
                },
                {
                    "id": "untrusted-authority",
                    "observed": { "result": "refused", "failure_class": "untrusted-authority", "plaintext_sessions": 0 },
                },
                {
                    "id": "hostname-mismatch",
                    "observed": { "result": "refused", "failure_class": "hostname-mismatch", "plaintext_sessions": 0 },
                },
                {
                    "id": "server-without-tls",
                    "observed": { "result": "refused", "failure_class": "tls-not-offered", "plaintext_sessions": 0 },
                },
            ],
        })
    }

    #[test]
    fn a_valid_tls_observation_reconciles_clean() {
        let runs = runs_with("verify-full-tls", valid_tls_observation());
        assert!(reconcile_tls(&scope(), &runs).is_empty());
    }

    #[test]
    fn a_missing_tls_attempt_is_rejected() {
        let mut observation = valid_tls_observation();
        observation["attempts"]
            .as_array_mut()
            .expect("attempts array")
            .retain(|attempt| {
                attempt.get("id").and_then(Value::as_str) != Some("hostname-mismatch")
            });
        let runs = runs_with("verify-full-tls", observation);
        let violations = reconcile_tls(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("hostname-mismatch")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_tls_attempt_masking_a_missing_one_is_rejected() {
        let mut observation = valid_tls_observation();
        let attempts = observation["attempts"]
            .as_array_mut()
            .expect("attempts array");
        let duplicate = attempts[0].clone();
        attempts.retain(|attempt| {
            attempt.get("id").and_then(Value::as_str) != Some("hostname-mismatch")
        });
        attempts.push(duplicate);
        let runs = runs_with("verify-full-tls", observation);
        let violations = reconcile_tls(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("recorded 2 times")),
            "{violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("hostname-mismatch")),
            "{violations:?}"
        );
    }

    #[test]
    fn an_extra_undeclared_tls_attempt_is_rejected() {
        let mut observation = valid_tls_observation();
        observation["attempts"].as_array_mut().expect("attempts array").push(json!({
            "id": "an-attempt-the-scope-does-not-require",
            "observed": { "result": "refused", "failure_class": "untrusted-authority", "plaintext_sessions": 0 },
        }));
        let runs = runs_with("verify-full-tls", observation);
        let violations = reconcile_tls(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("does not require")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_plaintext_fallback_after_a_refusal_is_rejected() {
        let mut observation = valid_tls_observation();
        observation["attempts"][3]["observed"]["plaintext_sessions"] = json!(1);
        let runs = runs_with("verify-full-tls", observation);
        let violations = reconcile_tls(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("unencrypted session")),
            "{violations:?}"
        );
    }

    /// The path to the committed role-matrix denominator.
    fn role_matrix_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("tests")
            .join("fixtures")
            .join("security")
            .join("role-matrix.json")
    }

    /// Reads the committed role-matrix denominator.
    fn role_matrix_document() -> Value {
        let source = std::fs::read_to_string(role_matrix_path()).expect("role-matrix.json");
        serde_json::from_str(&source).expect("role-matrix.json is valid JSON")
    }

    /// The exact cell identity set the committed denominator declares, in the
    /// shape [`Scope`] carries it — read from the real file rather than
    /// hand-copied, so these tests exercise the real committed denominator
    /// and cannot silently drift from it.
    fn role_matrix_cells() -> std::collections::BTreeSet<(String, String, String, String)> {
        role_matrix_document()
            .get("cells")
            .and_then(Value::as_array)
            .expect("cells")
            .iter()
            .map(|cell| {
                (
                    cell["id"].as_str().expect("id").to_owned(),
                    cell["class"].as_str().expect("class").to_owned(),
                    cell["surface"].as_str().expect("surface").to_owned(),
                    cell["expected"].as_str().expect("expected").to_owned(),
                )
            })
            .collect()
    }

    /// A role-matrix observation that satisfies every requirement `scope()`
    /// declares: exactly the committed denominator's cells, each with the
    /// server outcome its declared side requires, no `PUBLIC` grant, schema
    /// 3.
    fn valid_role_matrix_observation() -> Value {
        let matrix = role_matrix_document()
            .get("cells")
            .and_then(Value::as_array)
            .expect("cells")
            .iter()
            .map(|cell| {
                let expected = cell["expected"].as_str().expect("expected");
                let (observed, error_class) = if expected == "allowed" {
                    ("succeeded", Value::Null)
                } else {
                    ("refused", json!("42501"))
                };
                json!({
                    "id": cell["id"],
                    "class": cell["class"],
                    "operation": format!("operation for {}", cell["id"].as_str().unwrap_or("")),
                    "surface": cell["surface"],
                    "expected": cell["expected"],
                    "observed": observed,
                    "error_class": error_class,
                })
            })
            .collect::<Vec<_>>();

        let attributes = ["migration", "runtime", "explorer", "operator", "retention"]
            .into_iter()
            .map(|class| {
                json!({
                    "class": class,
                    "superuser": false,
                    "createdb": false,
                    "createrole": false,
                    "replication": false,
                })
            })
            .collect::<Vec<_>>();

        json!({
            "public_grants": [],
            "schema_version": 3,
            "class_attributes": attributes,
            "matrix": matrix,
        })
    }

    #[test]
    fn a_valid_role_matrix_observation_reconciles_clean() {
        let runs = runs_with("least-privilege-roles", valid_role_matrix_observation());
        assert!(reconcile_classes(&scope(), &runs).is_empty());
    }

    #[test]
    fn a_public_grant_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        observation["public_grants"] = json!(["SELECT"]);
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("PUBLIC")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_forbidden_cell_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let index = matrix
            .iter()
            .position(|cell| {
                cell.get("class").and_then(Value::as_str) == Some("explorer")
                    && cell.get("expected").and_then(Value::as_str) == Some("forbidden")
            })
            .expect("an explorer forbidden cell");
        matrix.remove(index);
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("explorer") && violation.contains("forbidden")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_duplicated_cell_masking_a_missing_one_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let duplicate = matrix
            .iter()
            .find(|cell| {
                cell.get("class").and_then(Value::as_str) == Some("explorer")
                    && cell.get("expected").and_then(Value::as_str) == Some("forbidden")
            })
            .cloned()
            .expect("an explorer forbidden cell");
        // Drop a different explorer-forbidden cell, then duplicate the first
        // one, so the total count is unchanged and only the identity is wrong.
        let index = matrix
            .iter()
            .rposition(|cell| {
                cell.get("class").and_then(Value::as_str) == Some("explorer")
                    && cell.get("expected").and_then(Value::as_str) == Some("forbidden")
            })
            .expect("an explorer forbidden cell");
        matrix[index] = duplicate;
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("recorded 2 times")),
            "{violations:?}"
        );
    }

    #[test]
    fn an_undeclared_class_cell_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        observation["matrix"]
            .as_array_mut()
            .expect("matrix array")
            .push(json!({
                "class": "an-undeclared-class",
                "operation": "anything",
                "surface": "statement",
                "expected": "forbidden",
                "error_class": "42501",
            }));
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("an-undeclared-class")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_forbidden_cell_refused_under_the_wrong_code_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        observation["matrix"][0]["error_class"] = json!("23505");
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("want of privilege")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_schema_version_mismatch_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        observation["schema_version"] = json!(2);
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("schema")),
            "{violations:?}"
        );
    }

    #[test]
    fn an_extra_cell_beyond_the_denominator_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        observation["matrix"]
            .as_array_mut()
            .expect("matrix array")
            .push(json!({
                "id": "runtime.an-extra-cell-nothing-declares",
                "class": "runtime",
                "operation": "an extra operation",
                "surface": "statement",
                "expected": "forbidden",
                "observed": "refused",
                "error_class": "42501",
            }));
        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("runtime.an-extra-cell-nothing-declares")),
            "{violations:?}"
        );
    }

    /// The central regression this corrective pass exists for: a legitimate
    /// cell is removed and a same-class, same-surface, same-expected bogus
    /// cell is substituted for it, so the total cell count, every per-class
    /// allowed/forbidden count, and the duplicate check all stay exactly as
    /// a passing report would report them. Only an exact identity
    /// reconciliation against the committed denominator — not a count of any
    /// kind — can catch this, and this test is the proof that it does: a
    /// verifier that checked shape and counts alone passes this mutation.
    #[test]
    fn a_same_shaped_bogus_cell_substituted_for_a_removed_one_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let removed_index = matrix
            .iter()
            .position(|cell| {
                cell.get("id").and_then(Value::as_str) == Some("explorer.create-job-instance")
            })
            .expect("the explorer.create-job-instance cell");
        matrix.remove(removed_index);
        // Same class, same surface, same expected side as the cell just
        // removed — the total and every per-class count this file's other
        // checks look at are unchanged by this substitution.
        matrix.push(json!({
            "id": "explorer.a-bogus-operation-nothing-declares",
            "class": "explorer",
            "operation": "a bogus operation nothing declares",
            "surface": "statement",
            "expected": "forbidden",
            "observed": "refused",
            "error_class": "42501",
        }));

        let total_before = 44;
        assert_eq!(
            matrix.len(),
            total_before,
            "the mutation must not change the total cell count, or a weaker verifier that only \
             checked the total would also catch it",
        );

        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("explorer.create-job-instance")),
            "the missing legitimate cell must be reported: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("explorer.a-bogus-operation-nothing-declares")),
            "the substituted bogus cell must be reported: {violations:?}"
        );
    }

    #[test]
    fn a_cell_recorded_under_the_wrong_expected_side_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let index = matrix
            .iter()
            .position(|cell| cell.get("id").and_then(Value::as_str) == Some("runtime.service-path"))
            .expect("the runtime.service-path cell");
        // Flipped in place: same id, class, and surface, but the side the
        // committed denominator declares for this identity is "allowed".
        matrix[index]["expected"] = json!("forbidden");
        matrix[index]["observed"] = json!("refused");
        matrix[index]["error_class"] = json!("42501");

        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("runtime.service-path")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_cell_recorded_under_the_wrong_surface_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let index = matrix
            .iter()
            .position(|cell| {
                cell.get("id").and_then(Value::as_str) == Some("retention.place-retention-hold")
            })
            .expect("the retention.place-retention-hold cell");
        // The denominator declares this identity on the statement surface;
        // recording it under service-path instead is a different claim.
        matrix[index]["surface"] = json!("service-path");

        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("retention.place-retention-hold")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_cell_recorded_under_the_wrong_class_is_rejected() {
        let mut observation = valid_role_matrix_observation();
        let matrix = observation["matrix"].as_array_mut().expect("matrix array");
        let index = matrix
            .iter()
            .position(|cell| {
                cell.get("id").and_then(Value::as_str) == Some("operator.ask-execution-to-stop")
            })
            .expect("the operator.ask-execution-to-stop cell");
        // The identity is declared for the operator class; recording the
        // same id under a different (still declared) class is a different
        // claim, not a relabeling.
        matrix[index]["class"] = json!("runtime");

        let runs = runs_with("least-privilege-roles", observation);
        let violations = reconcile_classes(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("operator.ask-execution-to-stop")),
            "{violations:?}"
        );
    }

    /// A redaction observation that satisfies every requirement `scope()`
    /// declares.
    fn valid_redaction_observation() -> Value {
        json!({
            "prohibited_occurrences": 0,
            "artifacts_scanned": 12,
            "strings_scanned": 400,
            "surfaces_scanned": [
                { "surface": "errors", "artifacts": 3 },
                { "surface": "telemetry", "artifacts": 3 },
                { "surface": "cli", "artifacts": 3 },
                { "surface": "bundle", "artifacts": 3 },
            ],
            "value_classes_scanned": [
                { "class": "password", "entered_through": "environment" },
                { "class": "database-url-endpoint", "entered_through": "environment" },
                { "class": "certificate", "entered_through": "configuration" },
                { "class": "payload", "entered_through": "job parameter" },
            ],
        })
    }

    #[test]
    fn a_valid_redaction_observation_reconciles_clean() {
        let runs = runs_with("redaction-sweep", valid_redaction_observation());
        assert!(reconcile_redaction(&scope(), &runs).is_empty());
    }

    #[test]
    fn a_missing_redaction_surface_is_rejected() {
        let mut observation = valid_redaction_observation();
        observation["surfaces_scanned"]
            .as_array_mut()
            .expect("surfaces_scanned array")
            .retain(|entry| entry.get("surface").and_then(Value::as_str) != Some("bundle"));
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("bundle")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_missing_prohibited_value_class_is_rejected() {
        let mut observation = valid_redaction_observation();
        observation["value_classes_scanned"]
            .as_array_mut()
            .expect("value_classes_scanned array")
            .retain(|entry| entry.get("class").and_then(Value::as_str) != Some("certificate"));
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("certificate")),
            "{violations:?}"
        );
    }

    #[test]
    fn an_undeclared_redaction_surface_is_rejected() {
        let mut observation = valid_redaction_observation();
        observation["surfaces_scanned"]
            .as_array_mut()
            .expect("surfaces_scanned array")
            .push(json!({ "surface": "an-undeclared-surface", "artifacts": 3 }));
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("an-undeclared-surface")),
            "{violations:?}"
        );
    }

    #[test]
    fn an_undeclared_redaction_value_class_is_rejected() {
        let mut observation = valid_redaction_observation();
        observation["value_classes_scanned"]
            .as_array_mut()
            .expect("value_classes_scanned array")
            .push(json!({ "class": "an-undeclared-class", "entered_through": "somewhere" }));
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("an-undeclared-class")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_nonzero_prohibited_occurrence_count_is_rejected() {
        let mut observation = valid_redaction_observation();
        observation["prohibited_occurrences"] = json!(1);
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("no prohibited value")),
            "{violations:?}"
        );
    }

    #[test]
    fn zero_artifacts_scanned_is_rejected_even_with_zero_occurrences() {
        let mut observation = valid_redaction_observation();
        observation["artifacts_scanned"] = json!(0);
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("scanned no artifacts")),
            "{violations:?}"
        );
    }

    #[test]
    fn zero_strings_scanned_is_rejected_even_with_zero_occurrences() {
        let mut observation = valid_redaction_observation();
        observation["strings_scanned"] = json!(0);
        let runs = runs_with("redaction-sweep", observation);
        let violations = reconcile_redaction(&scope(), &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("no scanned strings")),
            "{violations:?}"
        );
    }

    fn manifest(commit: &str) -> Value {
        json!({
            "execution_commit": commit,
            "tree_clean": true,
            "objects": { "tests/fixtures/security/campaign-scope.json": "abc123" },
        })
    }

    #[test]
    fn agreeing_execution_manifests_hoist_cleanly() {
        let reports = scope().reports;
        let mut runs = Runs::default();
        for report in &reports {
            runs.observations.insert(
                report.id.clone(),
                json!({ "execution_manifest": manifest("deadbeef") }),
            );
        }
        let (hoisted, violations) = execution_manifest(&reports, &runs);
        assert!(violations.is_empty(), "{violations:?}");
        assert_eq!(hoisted, manifest("deadbeef"));
    }

    #[test]
    fn a_missing_execution_manifest_is_rejected() {
        let reports = scope().reports;
        let mut runs = Runs::default();
        for report in &reports {
            runs.observations.insert(report.id.clone(), json!({}));
        }
        let (_, violations) = execution_manifest(&reports, &runs);
        assert_eq!(violations.len(), reports.len());
    }

    #[test]
    fn disagreeing_execution_manifests_are_rejected() {
        let reports = scope().reports;
        let mut runs = Runs::default();
        for (index, report) in reports.iter().enumerate() {
            let commit = if index == 0 { "deadbeef" } else { "fadedbee" };
            runs.observations.insert(
                report.id.clone(),
                json!({ "execution_manifest": manifest(commit) }),
            );
        }
        let (_, violations) = execution_manifest(&reports, &runs);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("different execution manifests")),
            "{violations:?}"
        );
    }

    #[test]
    fn a_null_execution_manifest_is_rejected() {
        let reports = scope().reports;
        let mut runs = Runs::default();
        for report in &reports {
            runs.observations.insert(
                report.id.clone(),
                json!({ "execution_manifest": Value::Null }),
            );
        }
        let (_, violations) = execution_manifest(&reports, &runs);
        assert_eq!(violations.len(), reports.len());
    }
}
