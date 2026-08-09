//! Scope reconciliation for the M5 `PostgreSQL` resource-bound campaign.
//!
//! The campaign has the two halves the conformance, crash-and-restore, upgrade,
//! and security campaigns have, split for the same reason:
//!
//! - **what the campaign owes, and which report proves each part of it.** That
//!   is a reconciliation between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [capacity budgets](../../../docs/operations/capacity-and-resource-budgets.md),
//!   the [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Three of its four reports need a real
//!   database and return green without one, because they skip. That half is
//!   `cargo xtask resource-bounds`.
//!
//! A resource campaign needs a stronger denominator than the others, and this
//! is where that difference lives. The other campaigns enumerate obligations
//! that are written down somewhere — ledger rows, commit phases, schema paths,
//! privilege classes — so a document can list them and review can check the
//! list. The obligations here are *every bounded resource the framework owns*,
//! and that set is defined by the code rather than by a document. A campaign
//! that proved nine ceilings out of an unstated number of them would look
//! exactly like a complete one.
//!
//! So the reconciliation runs in both directions.
//!
//! From the code outward, [`every_declared_bound_is_classified`] scans every
//! library crate for the bounds it declares and requires each one to appear in
//! the scope document — as a resource with a proving report, or in the
//! out-of-scope list with a reason. A bounded resource added later cannot reach
//! a release without either entering the campaign or being argued out of it in
//! writing, and neither can happen silently.
//!
//! From the operator's document inward,
//! [`every_declared_budget_has_a_proving_report`] requires the capacity budget
//! table and the scope to say the same thing about the same resources. That
//! table is what an operator sizes a deployment from; a number there that the
//! code does not hold is worse than no number, and it is exactly the drift this
//! direction catches.
//!
//! The scope document is `tests/fixtures/resource-bounds/campaign-scope.json`
//! at the workspace root. Both halves read it, so the resources, the policies,
//! the ceilings, and the reports are stated once.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// The reports the performance plan's resource-bounds row requires.
const REQUIRED_REPORTS: &[&str] = &[
    "worker-assignment",
    "bounded-query-paths",
    "bounded-payloads",
    "bounded-shedding",
];

/// The scenario the M5 design gate names for this campaign.
const NAMED_SCENARIO: &str = "declared_ceilings_hold_under_stress_with_backpressure";

/// The resource classes the performance plan requires a finite bound for.
///
/// The list is here rather than only in the scope document on purpose. The
/// document says what the runner requires; this says what the accepted plan
/// obliges, and a class can only leave the campaign by changing both.
const REQUIRED_CLASSES: &[&str] = &[
    "queue",
    "retry-cache",
    "page",
    "buffer",
    "worker-assignment",
    "result-set",
];

/// The overload policies the campaign distinguishes.
///
/// Every one of these is used by some resource in the accepted scope. A
/// campaign that collapsed them would have to make telemetry apply
/// backpressure, which is the opposite of the contract telemetry has.
const REQUIRED_POLICIES: &[&str] = &[
    "fail-closed",
    "bounded-concurrency",
    "bounded-shedding",
    "bounded-truncation",
];

/// The regression tests the campaign keeps and does not stand in for.
const KEPT_REGRESSIONS: &[&str] = &[
    "p010_local_partition_scaling",
    "p012_explorer_pagination_bounds",
    "worker_concurrency_never_exceeds_manifest_bound",
];

/// The library crates whose declared bounds the campaign is answerable for.
///
/// `xtask` and the spikes are excluded because neither ships: a bound declared
/// in a development task is not a resource a deployment holds.
const LIBRARY_CRATES: &[&str] = &[
    "oxide-batch",
    "oxide-batch-core",
    "oxide-batch-plan",
    "oxide-batch-repository",
    "oxide-batch-cli",
];

#[test]
fn campaign_scope_matches_the_accepted_resource_obligations() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_REPORTS.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign delivers exactly the reports the performance plan's resource-bounds row \
         requires",
    );
    assert!(
        scope
            .reports
            .iter()
            .any(|report| report.name == NAMED_SCENARIO),
        "no report in the campaign produces {NAMED_SCENARIO}, which the design gate names",
    );

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    assert!(
        gate.contains(NAMED_SCENARIO),
        "the design gate must still name {NAMED_SCENARIO} for the evidence campaigns",
    );

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Resource bounds |"))
        .ok_or_else(|| Failure("the performance plan has no resource-bounds row".to_owned()))?;
    for obligation in [
        "queue",
        "retry cache",
        "page",
        "buffer",
        "worker assignment",
        "result set",
        "backpressure propagation under stress",
    ] {
        assert!(
            row.contains(obligation),
            "the performance plan's resource-bounds row no longer requires {obligation}",
        );
    }

    // Every class the plan names must actually have something proving it. A
    // class with no resource is the shape a silently shrinking campaign takes.
    let classes = scope
        .resources
        .iter()
        .map(|resource| resource.class.as_str())
        .collect::<BTreeSet<_>>();
    for class in REQUIRED_CLASSES {
        assert!(
            classes.contains(class),
            "the campaign proves no bound of the {class} class, which the performance plan \
             requires a finite bound for",
        );
    }
    assert_eq!(
        classes,
        REQUIRED_CLASSES.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign classifies a resource under a class the performance plan does not name",
    );

    let policies = scope
        .resources
        .iter()
        .map(|resource| resource.policy.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        policies,
        REQUIRED_POLICIES.iter().copied().collect::<BTreeSet<_>>(),
        "each overload policy must be carried by some resource, and no resource may declare a \
         policy the campaign does not distinguish",
    );

    let reports = scope
        .reports
        .iter()
        .map(|report| report.id.as_str())
        .collect::<BTreeSet<_>>();
    for resource in &scope.resources {
        assert!(
            reports.contains(resource.report.as_str()),
            "{} is assigned to the {} report, which the campaign does not deliver",
            resource.name,
            resource.report,
        );
    }

    Ok(())
}

/// Requires every bound the workspace declares to be classified by the scope.
///
/// This is the direction that makes the denominator a denominator. Without it
/// the campaign proves a list, and nothing relates that list to the resources
/// the framework actually owns.
#[test]
fn every_declared_bound_is_classified() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let declared = declared_bounds()?;

    assert!(
        !declared.is_empty(),
        "the scan found no declared bound at all, so it is reading the wrong tree and would \
         accept an empty campaign",
    );

    let in_scope = scope
        .resources
        .iter()
        .flat_map(|resource| resource.symbols.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let excluded = scope
        .excluded
        .iter()
        .flat_map(|entry| entry.symbols.iter())
        .cloned()
        .collect::<BTreeSet<_>>();

    let both = in_scope.intersection(&excluded).collect::<Vec<_>>();
    assert!(
        both.is_empty(),
        "these bounds are claimed by the campaign and excluded from it at the same time: {both:?}",
    );

    for symbol in declared.keys() {
        assert!(
            in_scope.contains(symbol) || excluded.contains(symbol),
            "{symbol} is a bound this workspace declares and the resource campaign neither proves \
             nor excludes. Add it to the resources of \
             tests/fixtures/resource-bounds/campaign-scope.json with the report that proves it, \
             or to out_of_scope with the reason it is not a framework-owned resource.",
        );
    }

    // The other way round: a symbol in the document that the code no longer
    // declares is a resource the campaign believes it is still proving.
    for symbol in in_scope.iter().chain(excluded.iter()) {
        assert!(
            declared.contains_key(symbol),
            "the campaign classifies {symbol}, which this workspace no longer declares",
        );
    }

    // A declared ceiling must be the number the code holds. This is what makes
    // the retained report's `configured_ceiling` a fact rather than a copy of
    // an intention.
    for resource in &scope.resources {
        let Some(ceiling) = resource.ceiling else {
            continue;
        };
        let symbol = resource.symbols.first().ok_or_else(|| {
            Failure(format!(
                "{} declares a ceiling of {ceiling} and names no bound it comes from",
                resource.name,
            ))
        })?;
        let values = declared.get(symbol).map(Vec::as_slice).unwrap_or_default();
        assert!(
            values.contains(&Some(ceiling)),
            "{} declares a ceiling of {ceiling} and {symbol} is {values:?} in the source",
            resource.name,
        );
    }

    Ok(())
}

/// Requires the operator's capacity table and the campaign to agree.
///
/// The table is what a deployment is sized from. A row it carries that no
/// report proves is an unbacked number in front of an operator, and a number
/// that disagrees with the code is worse than an absent one.
#[test]
fn every_declared_budget_has_a_proving_report() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let budgets = declared_budget_rows()?;

    assert!(
        budgets.len() >= 15,
        "the capacity budget table has {} declared bounds, which is fewer than the M4 boundary \
         had; the campaign is reading the wrong table or the table lost rows",
        budgets.len(),
    );

    let claimed = scope
        .resources
        .iter()
        .filter_map(|resource| {
            resource
                .budget_row
                .as_ref()
                .map(|row| (row.clone(), resource))
        })
        .collect::<BTreeMap<_, _>>();

    for (row, bound) in &budgets {
        let resource = claimed.get(row).ok_or_else(|| {
            Failure(format!(
                "the capacity budget declares a bound for {row:?} and no resource in the campaign \
                 claims that row, so an operator is given a number no report proves"
            ))
        })?;
        assert_eq!(
            resource.budget_bound.as_deref(),
            Some(bound.as_str()),
            "the capacity budget declares {row:?} as {bound} and the campaign records {:?}",
            resource.budget_bound,
        );
    }

    for row in claimed.keys() {
        assert!(
            budgets.contains_key(row),
            "the campaign claims the {row:?} budget row, which the capacity document no longer \
             declares",
        );
    }

    Ok(())
}

/// Requires every report to declare the fixture it needs.
#[test]
fn every_report_declares_the_fixture_it_needs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    for report in &scope.reports {
        let Some(fixture) = &report.fixture else {
            // A report that needs no fixture must be one the campaign can run
            // anywhere, and only the shedding report is. Letting any report
            // declare no fixture would be the way to opt out of the runner's
            // fixture check.
            assert_eq!(
                report.id, "bounded-shedding",
                "{} declares no fixture, and only the shedding report runs without one",
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

/// Requires the stress obligations to reach the ceilings they are about.
///
/// A ceiling is proved by a run that filled it. Every resource whose policy is
/// bounded concurrency must therefore carry a stress requirement, because that
/// is the only class of resource in the campaign whose occupancy is a live
/// quantity and can be observed below its bound while looking correct.
#[test]
fn every_live_ceiling_is_required_to_be_reached() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    let reports = scope
        .reports
        .iter()
        .map(|report| report.id.as_str())
        .collect::<BTreeSet<_>>();
    let resources = scope
        .resources
        .iter()
        .map(|resource| resource.name.as_str())
        .collect::<BTreeSet<_>>();

    for requirement in &scope.stress {
        assert!(
            reports.contains(requirement.report.as_str()),
            "a stress requirement names the {} report, which the campaign does not deliver",
            requirement.report,
        );
        assert!(
            resources.contains(requirement.resource.as_str()),
            "a stress requirement names {}, which the campaign does not list as a resource",
            requirement.resource,
        );
        assert!(
            !requirement.requires.is_empty(),
            "the stress requirement for {} says nothing about what reaching it means",
            requirement.resource,
        );
    }

    for resource in &scope.resources {
        if resource.policy != "bounded-concurrency" {
            continue;
        }
        let requirement = scope
            .stress
            .iter()
            .find(|requirement| requirement.resource == resource.name);
        let requirement = requirement.ok_or_else(|| {
            Failure(format!(
                "{} holds a live occupancy against a ceiling and no stress requirement says the \
                 campaign must reach it, so a run whose peak was one worker would pass",
                resource.name,
            ))
        })?;
        assert_eq!(
            requirement.requires, "peak-equals-ceiling",
            "{} is bounded by concurrency, so the campaign must require its observed peak to \
             equal its ceiling rather than merely stay under it",
            resource.name,
        );
    }

    // Shedding is the other policy that can pass without being exercised: a
    // queue that was never filled drops nothing and reports no violation.
    for resource in &scope.resources {
        if resource.policy != "bounded-shedding" {
            continue;
        }
        assert!(
            scope
                .stress
                .iter()
                .any(|requirement| requirement.resource == resource.name),
            "{} sheds under overload and no stress requirement says the campaign must offer it \
             one, so a report that never filled it would pass",
            resource.name,
        );
        assert!(
            !resource.shedding_rule.is_empty(),
            "{} sheds under overload and the campaign does not record which rule it contracts \
             for, so dropping the wrong record would reconcile",
            resource.name,
        );
    }

    Ok(())
}

/// Requires the durable comparison the performance plan makes non-optional.
#[test]
fn the_stressed_run_is_compared_against_a_sequential_baseline() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert!(
        !scope.equivalence.is_empty(),
        "the campaign compares no stressed run against a sequential baseline, and the performance \
         plan holds that a concurrency result which changes a durable observation is invalid \
         regardless of its throughput",
    );

    for comparison in &scope.equivalence {
        assert!(
            scope
                .reports
                .iter()
                .any(|report| report.id == comparison.report),
            "a durable comparison names the {} report, which the campaign does not deliver",
            comparison.report,
        );
        for required in [
            "job-execution-status",
            "step-execution-status",
            "partition-key-set",
            "partition-status-per-key",
            "aggregate-execution-counts",
            "partition-context-per-key",
        ] {
            assert!(
                comparison.must_agree_on.iter().any(|item| item == required),
                "the {} comparison no longer requires {required} to agree between the sequential \
                 baseline and the stressed run",
                comparison.report,
            );
        }
        for required in [
            "duplicate-partition-execution",
            "missing-partition",
            "unfinished-child",
            "leaked-durable-execution",
            "forged-execution-status",
            "partial-launch-after-rejection",
        ] {
            assert!(
                comparison
                    .must_not_observe
                    .iter()
                    .any(|item| item == required),
                "the {} comparison no longer rules out {required}, which is one of the \
                 regressions resource pressure produces",
                comparison.report,
            );
        }
    }

    Ok(())
}

/// Requires the campaign to keep the evidence it does not replace.
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

    // The application-owned side must stay explicitly excluded rather than
    // quietly absent, because a reader cannot tell an unexamined resource from
    // an out-of-boundary one.
    assert!(
        scope
            .excluded
            .iter()
            .any(|entry| entry.name.contains("item buffers")),
        "the scope no longer states that application readers, writers, and item buffers are \
         outside the framework boundary, so their absence reads as an omission",
    );

    Ok(())
}

/// Every bound the workspace declares, by symbol, with the values it holds.
///
/// A symbol maps to more than one value when two crates declare the same bound
/// under the same name, which the campaign classifies once and requires to
/// agree.
type DeclaredBounds = BTreeMap<String, Vec<Option<i128>>>;

/// Reads every bound the shipping crates declare, by symbol.
///
/// The value is the integer the declaration evaluates to when it is a product
/// of integer literals, which covers every byte and count ceiling in the
/// workspace, and `None` for a duration or a reference to another constant.
fn declared_bounds() -> Result<DeclaredBounds, Box<dyn Error>> {
    let mut declared = DeclaredBounds::new();

    for crate_name in LIBRARY_CRATES {
        let source = workspace_root().join("crates").join(crate_name).join("src");
        for file in rust_files(&source)? {
            let text = fs::read_to_string(&file)
                .map_err(|error| Failure(format!("could not read {}: {error}", file.display())))?;
            for (symbol, value) in bounds_in(&text) {
                declared.entry(symbol).or_default().push(value);
            }
        }
    }

    Ok(declared)
}

/// Returns every bound declaration in one source file.
///
/// A declaration is a `const` whose name is bound-shaped and whose type is not
/// a string or a slice. Matching on the name rather than on a marker attribute
/// is deliberate: an attribute is something an author has to remember, and a
/// bound that nobody remembered to mark is exactly the one this scan is for.
fn bounds_in(text: &str) -> Vec<(String, Option<i128>)> {
    let mut found = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line
            .strip_prefix("pub const ")
            .or_else(|| line.strip_prefix("pub(crate) const "))
            .or_else(|| line.strip_prefix("const "))
        else {
            continue;
        };
        let Some((symbol, tail)) = rest.split_once(':') else {
            continue;
        };
        let symbol = symbol.trim();
        if !is_bound_symbol(symbol) {
            continue;
        }
        let Some((_, value)) = tail.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_end_matches(';').trim();
        found.push((symbol.to_owned(), evaluate(value)));
    }

    found
}

/// Reports whether a constant's name makes it a resource bound.
fn is_bound_symbol(symbol: &str) -> bool {
    if !symbol.chars().all(|character| {
        character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
    }) {
        return false;
    }
    symbol.starts_with("MAX_")
        || symbol.starts_with("MIN_")
        || symbol.contains("MAXIMUM")
        || symbol.contains("MINIMUM")
        || symbol.contains("_BUDGET")
        || symbol.contains("_BOUND")
        || symbol.contains("_CAPACITY")
}

/// Evaluates a constant expression that is a product of integer literals.
fn evaluate(expression: &str) -> Option<i128> {
    let mut product: i128 = 1;
    for factor in expression.split('*') {
        let factor = factor.trim().replace('_', "");
        let factor = factor
            .trim_end_matches("usize")
            .trim_end_matches("u128")
            .trim_end_matches("u64")
            .trim_end_matches("u32")
            .trim_end_matches("u16")
            .trim_end_matches("u8");
        product = product.checked_mul(factor.parse::<i128>().ok()?)?;
    }
    Some(product)
}

/// Returns every Rust source file under one directory.
fn rust_files(directory: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut files = Vec::new();
    let entries = fs::read_dir(directory)
        .map_err(|error| Failure(format!("could not read {}: {error}", directory.display())))?;

    for entry in entries {
        let path = entry
            .map_err(|error| Failure(format!("could not read {}: {error}", directory.display())))?
            .path();
        if path.is_dir() {
            files.extend(rust_files(&path)?);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

/// Reads the declared-bounds table of the capacity budget document.
///
/// Returns the resource name and the bound cell exactly as the table writes
/// them, because the campaign records the operator's text rather than a
/// re-rendering of it: two numbers that mean the same thing and read
/// differently are still a drift an operator would trip on.
fn declared_budget_rows() -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let document = read_document("docs/operations/capacity-and-resource-budgets.md")?;
    let mut rows = BTreeMap::new();
    let mut inside = false;

    for line in document.lines() {
        let line = line.trim();
        if line.starts_with("## ") {
            inside = line == "## Declared bounds";
            continue;
        }
        if !inside || !line.starts_with('|') {
            continue;
        }
        let cells = line
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect::<Vec<_>>();
        if cells.len() != 3 || cells[0] == "Resource" || cells[0].starts_with("---") {
            continue;
        }
        rows.insert(cells[0].to_owned(), cells[1].to_owned());
    }

    Ok(rows)
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
    fixtures: BTreeMap<String, Vec<String>>,
    resources: Vec<Resource>,
    excluded: Vec<Excluded>,
    stress: Vec<StressRequirement>,
    equivalence: Vec<Comparison>,
    related: Vec<String>,
}

impl Scope {
    /// Reads the campaign scope document from the workspace.
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("resource-bounds")
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

        let mut fixtures = BTreeMap::new();
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

        Ok(Self {
            reports,
            fixtures,
            resources: Self::read_resources(&document)?,
            excluded: Self::read_exclusions(&document)?,
            stress: Self::read_stress(&document)?,
            equivalence: Self::read_equivalence(&document)?,
            related: document
                .get("related")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(ToString::to_string)
                .collect(),
        })
    }

    /// Reads the bounded resources the campaign is answerable for.
    fn read_resources(document: &Value) -> Result<Vec<Resource>, Box<dyn Error>> {
        let mut resources = Vec::new();
        for resource in array(document, "resources")? {
            resources.push(Resource {
                name: text(resource, "resource")?,
                class: text(resource, "class")?,
                symbols: list(resource, "symbols"),
                ceiling: resource
                    .get("ceiling")
                    .and_then(Value::as_i64)
                    .map(i128::from),
                policy: text(resource, "policy")?,
                report: text(resource, "report")?,
                budget_row: resource
                    .get("budget_row")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                budget_bound: resource
                    .get("budget_bound")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                shedding_rule: resource
                    .get("shedding_rule")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
        Ok(resources)
    }

    /// Reads the bounds the campaign argues are not framework-owned resources.
    ///
    /// An exclusion must carry a reason long enough to be one. A campaign can
    /// always be made complete by moving what it cannot prove into a list, and
    /// the only thing that distinguishes a boundary from that is an argument a
    /// reviewer can disagree with.
    fn read_exclusions(document: &Value) -> Result<Vec<Excluded>, Box<dyn Error>> {
        let mut excluded = Vec::new();
        for entry in array(document, "out_of_scope")? {
            let reason = text(entry, "reason")?;
            assert!(
                reason.len() > 40,
                "an out-of-scope entry must say why in a sentence, and {reason:?} does not",
            );
            excluded.push(Excluded {
                name: text(entry, "resource")?,
                symbols: list(entry, "symbols"),
            });
        }
        Ok(excluded)
    }

    /// Reads the obligations to reach a ceiling rather than stay under it.
    fn read_stress(document: &Value) -> Result<Vec<StressRequirement>, Box<dyn Error>> {
        let stress = document
            .get("stress")
            .ok_or_else(|| Failure("the scope declares no stress obligations".to_owned()))?;
        let mut requirements = Vec::new();
        for requirement in array(stress, "requirements")? {
            requirements.push(StressRequirement {
                report: text(requirement, "report")?,
                resource: text(requirement, "resource")?,
                requires: text(requirement, "requires")?,
            });
        }
        Ok(requirements)
    }

    /// Reads the durable comparisons between baseline and stressed runs.
    fn read_equivalence(document: &Value) -> Result<Vec<Comparison>, Box<dyn Error>> {
        let equivalence = document.get("durable_equivalence").ok_or_else(|| {
            Failure("the scope declares no durable equivalence obligations".to_owned())
        })?;
        let mut comparisons = Vec::new();
        for comparison in array(equivalence, "comparisons")? {
            comparisons.push(Comparison {
                report: text(comparison, "report")?,
                must_agree_on: list(comparison, "must_agree_on"),
                must_not_observe: list(comparison, "must_not_observe"),
            });
        }
        Ok(comparisons)
    }
}

/// One report the campaign delivers, as the scope declares it.
struct Report {
    id: String,
    name: String,
    fixture: Option<String>,
    against_database: bool,
}

/// One bounded resource the campaign is answerable for.
struct Resource {
    name: String,
    class: String,
    symbols: Vec<String>,
    ceiling: Option<i128>,
    policy: String,
    report: String,
    budget_row: Option<String>,
    budget_bound: Option<String>,
    shedding_rule: String,
}

/// One bound the campaign argues is not a framework-owned resource.
struct Excluded {
    name: String,
    symbols: Vec<String>,
}

/// One obligation to actually reach a ceiling rather than stay under it.
struct StressRequirement {
    report: String,
    resource: String,
    requires: String,
}

/// One durable comparison between a sequential baseline and a stressed run.
struct Comparison {
    report: String,
    must_agree_on: Vec<String>,
    must_not_observe: Vec<String>,
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

/// Tests for the scan this reconciliation trusts.
///
/// The scan is the only part of the campaign that can fail silently in the
/// direction that matters: a reader that found nothing would classify nothing
/// and report no gap. These run beside it so what review checks is what the
/// reconciliation uses.
mod scan {
    use super::{bounds_in, evaluate, is_bound_symbol};

    #[test]
    fn a_bound_is_read_with_the_value_it_evaluates_to() {
        assert_eq!(
            bounds_in("pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;"),
            vec![("MAX_RESPONSE_BYTES".to_owned(), Some(262_144))],
        );
        assert_eq!(
            bounds_in("    pub(crate) const MAX_MANIFEST_BYTES: usize = 64 * 1024;"),
            vec![("MAX_MANIFEST_BYTES".to_owned(), Some(65_536))],
        );
        assert_eq!(
            bounds_in("const MAX_NODES: usize = 1_024;"),
            vec![("MAX_NODES".to_owned(), Some(1_024))],
        );
    }

    #[test]
    fn a_bound_whose_value_is_not_a_number_is_still_found() {
        // The value is unreadable and the symbol is not. Losing the symbol
        // would let a bound leave the campaign by being defined in terms of
        // another one.
        assert_eq!(
            bounds_in("    pub const MAX_LISTENERS: usize = MAX_LISTENERS;"),
            vec![("MAX_LISTENERS".to_owned(), None)],
        );
        assert_eq!(
            bounds_in("pub const MAX_SHUTDOWN_DEADLINE: Duration = Duration::from_hours(1);"),
            vec![("MAX_SHUTDOWN_DEADLINE".to_owned(), None)],
        );
    }

    #[test]
    fn a_constant_that_is_not_a_bound_is_not_read_as_one() {
        assert!(bounds_in("pub const VERSION: &str = env!(\"CARGO_PKG_VERSION\");").is_empty());
        assert!(bounds_in("pub const CONFIG_VERSION: u64 = 1;").is_empty());
        assert!(bounds_in("    pub const fn category(&self) -> ExitCategory {").is_empty());
        assert!(bounds_in("pub const ZERO: Self = Self(0);").is_empty());
    }

    #[test]
    fn budget_and_bound_suffixes_are_read_as_bounds() {
        assert!(is_bound_symbol("METRIC_CARDINALITY_BUDGET"));
        assert!(is_bound_symbol("MAX_PARTITIONS"));
        assert!(is_bound_symbol("MIN_EXPORT_QUEUE_RECORDS"));
        // The scan matches a bound by how it is named rather than by a marker
        // an author has to remember, so every spelling this workspace uses for
        // one has to be a spelling it recognizes.
        assert!(is_bound_symbol("MAXIMUM_BYTES"));
        assert!(is_bound_symbol("DEFAULT_RETAINED_EVENT_CAPACITY"));
        assert!(!is_bound_symbol("TELEMETRY_SCHEMA_VERSION"));
        assert!(!is_bound_symbol("Positions"));
    }

    #[test]
    fn a_product_of_literals_is_evaluated_and_a_call_is_not() {
        assert_eq!(evaluate("4 * 1024 * 1024"), Some(4_194_304));
        assert_eq!(evaluate("65_536"), Some(65_536));
        assert_eq!(evaluate("Duration::from_secs(1)"), None);
        assert_eq!(evaluate("MAX_LISTENERS"), None);
    }
}
