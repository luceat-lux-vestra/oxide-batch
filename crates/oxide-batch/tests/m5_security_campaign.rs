//! Scope reconciliation for the M5 `PostgreSQL` security campaign.
//!
//! The campaign has the same two halves the conformance, crash-and-restore, and
//! upgrade campaigns have, split for the same reason:
//!
//! - **what the campaign owes, and which scenario proves each part of it.**
//!   That is a reconciliation between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Two of its three scenarios need a real
//!   database and return green without one, because they skip. That half is
//!   `cargo xtask security`, which requires the fixtures, runs the targets,
//!   requires each declared property to have been observed, and writes the
//!   retained report.
//!
//! The scope document is `tests/fixtures/security/campaign-scope.json` at the
//! workspace root. Both halves read it, so the privilege classes, the TLS
//! refusals, the swept surfaces, and the reports are stated once.
//!
//! Three things this file checks are specific to a security campaign and worth
//! naming. The privilege classes in the scope must be exactly the five the
//! design gate separates, so a class cannot leave the campaign by leaving the
//! document. The TLS obligations must include a refusal that is not about a
//! certificate at all — the server that offers no TLS — because a campaign made
//! only of certificate refusals would pass against a client that fell back to
//! plaintext whenever TLS was unavailable. And the committed least-privilege
//! policy must still be the two SQL files the matrix is checked against, and
//! must still deny every class the cluster-level privileges that would put it
//! outside every grant.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

/// The reports the performance plan's security row requires.
const REQUIRED_REPORTS: &[&str] = &[
    "verify-full-tls",
    "least-privilege-roles",
    "redaction-sweep",
];

/// The scenarios the M5 design gate names for this campaign.
const NAMED_SCENARIOS: &[&str] = &[
    "verify_full_tls_is_required_in_the_supported_mode",
    "least_privilege_role_cannot_exceed_its_class",
    "redaction_sweep_finds_no_prohibited_value_class",
];

/// The privilege classes the M5 preview separates, as the gate names them.
///
/// The list is here rather than only in the scope document on purpose. The
/// document says what the runner requires; this says what review accepted, and
/// a class can only leave the campaign by changing both.
const REQUIRED_CLASSES: &[&str] = &["migration", "runtime", "explorer", "operator", "retention"];

/// The diagnostic surfaces the gate requires the sweep to cover.
const REQUIRED_SURFACES: &[&str] = &["errors", "telemetry", "cli", "bundle"];

/// The transport refusals the supported mode must produce.
///
/// The third is the one that carries the claim. A campaign made only of the
/// first two would pass against a client that refused a bad certificate and
/// then continued unencrypted whenever the server offered no TLS at all.
const REQUIRED_REFUSALS: &[&str] = &[
    "untrusted-authority",
    "hostname-mismatch",
    "tls-not-offered",
];

/// The cluster-level privileges the committed policy must deny every class.
const DENIED_ATTRIBUTES: &[&str] = &["NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION"];

/// The schema the privilege matrix is checked on.
///
/// The M5 preview installed schema 3; M6 `#144` added
/// `0005_item_stream_component_state.sql`, which carries this crate's
/// installed schema to 4 without changing anything schema 3 declared.
const SCHEMA_VERSION: u64 = 4;

/// The transport the M5 preview supports in production.
const TLS_MODE: &str = "verify-full";

/// The regression tests the campaign keeps and does not stand in for.
const KEPT_REGRESSIONS: &[&str] = &[
    "configuration_bounds_and_diagnostics_are_safe",
    "diagnostic_bundle_excludes_every_prohibited_value_class",
];

#[test]
fn campaign_scope_matches_the_accepted_security_obligations() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_REPORTS.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign delivers exactly the reports the performance plan's security row requires",
    );
    assert_eq!(
        scope.classes, REQUIRED_CLASSES,
        "the campaign separates every privilege class the M5 preview promises, in order",
    );
    assert_eq!(
        scope.tls_mode, TLS_MODE,
        "the M5 preview supports one production transport and the scope names another",
    );
    assert_eq!(
        scope.schema_version, SCHEMA_VERSION,
        "the privilege matrix must be checked on the schema the preview installs",
    );
    assert_eq!(
        scope.refusals, REQUIRED_REFUSALS,
        "the TLS report must produce every refusal the supported mode is defined by, including \
         the server that offers no TLS at all",
    );
    assert_eq!(
        scope.surfaces, REQUIRED_SURFACES,
        "the sweep must cover every diagnostic surface the design gate names",
    );
    assert!(
        !scope.value_classes.is_empty(),
        "the sweep must inject a canary for at least one prohibited value class",
    );

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    for scenario in NAMED_SCENARIOS {
        assert!(
            gate.contains(scenario),
            "the design gate must still name {scenario} for the evidence campaigns",
        );
        assert!(
            scope.reports.iter().any(|report| report.name == *scenario),
            "no report in the campaign produces {scenario}",
        );
    }

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Security |"))
        .ok_or_else(|| Failure("the performance plan has no security campaign row".to_owned()))?;
    for obligation in ["verify-full", "least-privilege", "redaction sweep"] {
        assert!(
            row.contains(obligation),
            "the performance plan's security row no longer requires {obligation}",
        );
    }
    for class in REQUIRED_CLASSES {
        assert!(
            row.contains(class),
            "the performance plan's security row no longer separates the {class} class",
        );
    }

    Ok(())
}

/// The role-matrix cell-count denominator xtask's independent verifier
/// requires exact agreement with, counted from the `PERMITTED` and
/// `BOUNDARIES` tables `postgres_least_privilege_roles.rs` declares (see that
/// file's module documentation): every class's allowed and forbidden cell
/// counts and the total they sum to. A class removed from the document, a
/// count edited without the source it counts, or a total that stops matching
/// the sum of the per-class counts would otherwise let the independent
/// verifier check an exact count against a denominator nothing keeps honest.
#[test]
fn role_matrix_denominator_is_internally_consistent() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .class_cells
            .iter()
            .map(|(class, ..)| class.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_CLASSES.iter().copied().collect::<BTreeSet<_>>(),
        "the role matrix denominator must declare exactly the five required classes",
    );
    for (class, allowed, forbidden) in &scope.class_cells {
        assert!(
            *allowed > 0,
            "the {class} class declares zero allowed cells, so the matrix could never prove it \
             does its own work",
        );
        assert!(
            *forbidden > 0,
            "the {class} class declares zero forbidden cells, so the matrix could never prove a \
             boundary",
        );
    }
    let sum = scope
        .class_cells
        .iter()
        .map(|(_, allowed, forbidden)| allowed + forbidden)
        .sum::<u64>();
    assert_eq!(
        sum, scope.role_matrix_total_cells,
        "the role matrix's declared total must equal the sum of every class's declared cells",
    );

    Ok(())
}

/// The committed policy files the least-privilege matrix is checked against
/// must still be the two SQL files the scope names, so an edit that pointed
/// the policy at a different path could not silently substitute what the
/// matrix reconciles evidence against.
#[test]
fn the_scope_names_the_committed_policy_files() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(scope.roles_policy, "tests/fixtures/security/roles.sql");
    assert_eq!(scope.grants_policy, "tests/fixtures/security/grants.sql");

    Ok(())
}

#[test]
fn every_report_declares_the_fixture_it_needs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    for report in &scope.reports {
        let Some(fixture) = &report.fixture else {
            // A report that needs no fixture must be one the campaign can run
            // anywhere, and only the sweep is. Letting any report declare no
            // fixture would be the way to opt out of the runner's fixture check.
            assert_eq!(
                report.id, "redaction-sweep",
                "{} declares no fixture, and only the redaction sweep runs without one",
                report.id,
            );
            assert!(
                !report.against_database,
                "{} declares no fixture and claims to have run against a database",
                report.id,
            );
            continue;
        };
        assert!(
            scope.fixtures.contains_key(fixture),
            "{} needs the {fixture} fixture, which the scope does not declare",
            report.id,
        );
        assert!(
            report.against_database,
            "{} needs a database fixture and is not required to name the major it ran against, \
             so an observation from another matrix point would reconcile",
            report.id,
        );
    }

    for (fixture, variables) in &scope.fixtures {
        assert!(
            !variables.is_empty(),
            "the {fixture} fixture declares no environment, so the runner cannot tell whether it \
             is present",
        );
    }

    Ok(())
}

#[test]
fn the_committed_policy_denies_every_class_a_way_out() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let roles = read_document(&scope.roles_policy)?;
    let grants = read_document(&scope.grants_policy)?;

    // Every class must be created, and created without a cluster-level
    // privilege. A class that could create a role or a database would not be
    // restrained by anything the grants file says.
    for class in REQUIRED_CLASSES {
        let role = format!("oxide_batch_m5_{class}");
        assert!(
            roles.contains(&role),
            "the committed policy does not create a role for the {class} class",
        );
    }
    let created = roles.matches("CREATE ROLE").count();
    assert_eq!(
        created,
        REQUIRED_CLASSES.len(),
        "the committed policy creates {created} roles for {} classes",
        REQUIRED_CLASSES.len(),
    );
    for attribute in DENIED_ATTRIBUTES {
        assert_eq!(
            roles.matches(attribute).count(),
            REQUIRED_CLASSES.len(),
            "the committed policy does not deny {attribute} to every class",
        );
    }

    // Nothing may reach a class through PUBLIC, which would reach every future
    // class at once.
    assert!(
        roles.contains("REVOKE ALL ON DATABASE %I FROM PUBLIC"),
        "the committed policy does not withdraw the database privileges PostgreSQL grants to \
         PUBLIC by default",
    );
    assert!(
        roles.contains("REVOKE ALL ON SCHEMA public FROM PUBLIC"),
        "the committed policy does not withdraw the public-schema privileges PostgreSQL grants \
         to PUBLIC by default",
    );

    // The migration bookkeeping is the one table no class may reach: a class
    // that could rewrite it could tell a runtime it was reading a different
    // schema than it is.
    assert!(
        grants.contains("REVOKE ALL ON oxide_batch._sqlx_migrations FROM"),
        "the committed policy leaves the migration bookkeeping reachable",
    );
    for class in REQUIRED_CLASSES
        .iter()
        .filter(|class| **class != "migration")
    {
        assert!(
            grants.contains(&format!("oxide_batch_m5_{class}")),
            "the committed policy grants the {class} class nothing at all, so the matrix cannot \
             show it doing its own work",
        );
    }

    Ok(())
}

#[test]
fn the_campaign_keeps_the_regressions_it_does_not_replace() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    for kept in KEPT_REGRESSIONS {
        assert!(
            scope.related.iter().any(|entry| entry.contains(kept)),
            "the scope no longer records {kept} as evidence this campaign keeps and does not \
             stand in for",
        );
    }

    // The M2 design gate is kept on its own axis. This campaign must not be
    // recorded as replacing it: it covers a schema that is no longer installed
    // and a role set that is no longer the one the preview separates.
    assert!(
        scope
            .related
            .iter()
            .any(|entry| entry.contains("run-design-gate.sh")),
        "the scope no longer records that the M2 design-gate fixture is kept",
    );

    Ok(())
}

/// Reads one repository document relative to the workspace root.
fn read_document(path: &str) -> Result<String, Box<dyn Error>> {
    let full = workspace_root().join(path);
    fs::read_to_string(&full)
        .map_err(|error| Failure(format!("could not read {}: {error}", full.display())).into())
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The parts of the committed scope document this reconciliation reads.
struct Scope {
    reports: Vec<Report>,
    fixtures: std::collections::BTreeMap<String, Vec<String>>,
    classes: Vec<String>,
    refusals: Vec<String>,
    surfaces: Vec<String>,
    value_classes: Vec<String>,
    tls_mode: String,
    schema_version: u64,
    roles_policy: String,
    grants_policy: String,
    related: Vec<String>,
    role_matrix_total_cells: u64,
    class_cells: Vec<(String, u64, u64)>,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    #[allow(
        clippy::too_many_lines,
        reason = "the scope document is one denominator, and splitting its reading would scatter \
                  the fields this reconciliation and xtask's runner both depend on"
    )]
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("security")
            .join("campaign-scope.json");
        let source = fs::read_to_string(&path)
            .map_err(|error| Failure(format!("could not read {}: {error}", path.display())))?;
        let document: Value = serde_json::from_str(&source)?;

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: text(report, "id")?,
                name: text(report, "name")?,
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

        let mut fixtures = std::collections::BTreeMap::new();
        if let Some(declared) = document.get("fixtures").and_then(Value::as_object) {
            for (fixture, variables) in declared {
                fixtures.insert(
                    fixture.clone(),
                    variables
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                );
            }
        }

        let contract = document
            .get("support_contract")
            .ok_or_else(|| Failure("the scope declares no support contract".to_owned()))?;
        let tls = document
            .get("tls")
            .ok_or_else(|| Failure("the scope declares no TLS obligations".to_owned()))?;
        let redaction = document
            .get("redaction")
            .ok_or_else(|| Failure("the scope declares no redaction obligations".to_owned()))?;
        let policy = document
            .get("policy")
            .ok_or_else(|| Failure("the scope declares no committed policy".to_owned()))?;
        let role_matrix = document
            .get("role_matrix")
            .ok_or_else(|| Failure("the scope declares no role matrix denominator".to_owned()))?;
        let class_cells = array(&document, "privilege_classes")?
            .iter()
            .map(|class| {
                Ok::<_, Box<dyn Error>>((
                    text(class, "class")?,
                    class
                        .get("allowed_cells")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Failure("a class declares no allowed_cells".to_owned()))?,
                    class
                        .get("forbidden_cells")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Failure("a class declares no forbidden_cells".to_owned()))?,
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

        Ok(Self {
            reports,
            fixtures,
            classes: array(&document, "privilege_classes")?
                .iter()
                .filter_map(|class| class.get("class").and_then(Value::as_str))
                .map(str::to_owned)
                .collect(),
            refusals: array(tls, "refusals")?
                .iter()
                .filter_map(|refusal| refusal.get("failure_class").and_then(Value::as_str))
                .map(str::to_owned)
                .collect(),
            surfaces: list(redaction, "surfaces"),
            value_classes: list(redaction, "value_classes"),
            tls_mode: text(contract, "tls_mode")?,
            schema_version: contract
                .get("installed_schema_version")
                .and_then(Value::as_u64)
                .ok_or_else(|| Failure("the support contract declares no schema".to_owned()))?,
            roles_policy: text(policy, "roles")?,
            grants_policy: text(policy, "grants")?,
            related: document
                .get("related")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(ToString::to_string)
                .collect(),
            role_matrix_total_cells: role_matrix
                .get("total_cells")
                .and_then(Value::as_u64)
                .ok_or_else(|| Failure("the role matrix declares no total_cells".to_owned()))?,
            class_cells,
        })
    }
}

/// One report the campaign delivers, as the scope declares it.
struct Report {
    id: String,
    name: String,
    fixture: Option<String>,
    against_database: bool,
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| Failure(format!("the scope document has no {name}")).into())
}

/// Reads one required string field.
fn text(document: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Failure(format!("a scope entry has no {name}")).into())
}

/// Reads a string array field, treating an absent one as empty.
fn list(document: &Value, name: &str) -> Vec<String> {
    document
        .get(name)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

/// A reconciliation failure that is not a parse failure.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
