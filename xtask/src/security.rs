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
        for expected in ["allowed", "forbidden"] {
            if !cells
                .iter()
                .any(|cell| cell.get("expected").and_then(Value::as_str) == Some(expected))
            {
                violations.push(format!(
                    "the matrix records no {expected} operation for the {} class",
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

    let classes = observation
        .get("value_classes_scanned")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for required in &scope.redaction.value_classes {
        if !classes
            .iter()
            .any(|entry| entry.get("class").and_then(Value::as_str) == Some(required.as_str()))
        {
            violations.push(format!(
                "the {required} value class is required and the sweep injected no canary for it"
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
    /// What the redaction sweep must cover.
    redaction: Redaction,
    /// The committed policy, as declared.
    policy: Value,
    /// Evidence the campaign keeps and does not run, as declared.
    related: Value,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
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
            });
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
