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
//! From the code outward, [`every_declared_bound_is_classified`] parses every
//! library crate and requires each constant declared under the repository's
//! [bound declaration convention](../../../docs/engineering/coding-conventions.md)
//! to appear in the scope document — as a resource with a proving report, or in
//! the out-of-scope list with a reason.
//!
//! It parses rather than reading lines because the failure mode of a textual
//! reader is silent: it recognizes the spellings its author thought of, so a
//! ceiling that becomes `pub(super)` in a refactor, or that a formatter wraps
//! across lines, leaves the denominator without leaving the product. Visibility
//! is therefore never consulted, layout cannot be, and associated constants and
//! constants in inline modules are found.
//!
//! What that guarantees, exactly: a constant declared under the convention
//! cannot enter the product without entering the campaign. A ceiling written as
//! a bare literal where it is enforced, or named outside the convention, or
//! produced only by macro expansion, is invisible here — those are ruled out by
//! the convention being a documented rule that review applies, not by this
//! scan. The distinction is worth keeping sharp: a denominator that claimed to
//! find everything would be making the same unfalsifiable claim this campaign
//! exists to replace.
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
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    // A resource with more than one bound symbol proves more than its
    // top-level ceiling: durable-state-envelope's depth beside its bytes, a
    // range's own minimum beside its maximum, a subject-boundary resource's
    // per-subject ceiling. The single-ceiling loop above only ever compares a
    // resource's first symbol, so every other declared symbol is checked
    // here against its own real source value independently — that gap is
    // exactly what let three dimensions across four resources sit
    // unclassified against their own numbers before this closed it.
    for resource in &scope.resources {
        for dimension in &resource.dimensions {
            let values = declared
                .get(&dimension.symbol)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if values.iter().all(Option::is_none) {
                // A duration constant, a call, or a cross-reference: the same
                // limitation this module's own `declared_bounds` doc comment
                // states for every bound the scan cannot evaluate. The symbol
                // is still required above to be declared and classified;
                // only its number is not mechanically checkable here.
                continue;
            }
            assert!(
                values.contains(&Some(dimension.value)),
                "{}'s {} dimension declares {} and {} is {values:?} in the source",
                resource.name,
                dimension.symbol,
                dimension.value,
                dimension.symbol,
            );
        }

        // Every symbol a resource claims must be wired to something: its own
        // ceiling, one of its dimensions, or an explicit informational
        // classification — never left declared and silently unchecked by
        // either loop above.
        for symbol in &resource.symbols {
            let is_the_ceiling_symbol =
                resource.ceiling.is_some() && resource.symbols.first() == Some(symbol);
            let is_a_dimension = resource
                .dimensions
                .iter()
                .any(|dimension| &dimension.symbol == symbol);
            let is_informational = resource.informational.contains(symbol);
            assert!(
                is_the_ceiling_symbol || is_a_dimension || is_informational,
                "{} declares {symbol} among its symbols and neither its ceiling, its dimensions, \
                 nor informational_symbols accounts for it, so nothing checks it against the \
                 source or explains why nothing does",
                resource.name,
            );
        }
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
    }

    let worker_assignment = scope
        .equivalence
        .iter()
        .find(|comparison| comparison.report == "worker-assignment")
        .ok_or_else(|| {
            Failure(
                "the worker-assignment report compares no stressed run against a baseline"
                    .to_owned(),
            )
        })?;
    for required in [
        "outcome",
        "job-execution-status",
        "job-exit-status",
        "step-execution-status",
        "step-exit-status",
        "aggregate-execution-counts",
        "read-write-commit-rollback-counters",
        "partition-execution-count",
        "step-execution-count",
        "partition-key-set",
        "partition-status-per-key",
        "partition-counts-per-key",
        "partition-context-per-key",
    ] {
        assert!(
            worker_assignment
                .must_agree_on
                .iter()
                .any(|item| item == required),
            "the worker-assignment comparison no longer requires {required} to agree between the \
             sequential baseline and the stressed run",
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
            worker_assignment
                .must_not_observe
                .iter()
                .any(|item| item == required),
            "the worker-assignment comparison no longer rules out {required}, which is one of \
             the regressions resource pressure produces",
        );
    }

    let bounded_shedding = scope
        .equivalence
        .iter()
        .find(|comparison| comparison.report == "bounded-shedding")
        .ok_or_else(|| {
            Failure(
                "the bounded-shedding report compares no saturated launch against a baseline, \
                 and shedding is only acceptable because batch work is unaffected by it"
                    .to_owned(),
            )
        })?;
    assert!(
        !bounded_shedding.must_agree_on.is_empty(),
        "the bounded-shedding comparison requires no field to agree, so a shed telemetry record \
         changing a durable observation would pass silently",
    );

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
/// The value is the integer the declaration evaluates to when the expression is
/// a product of integer literals, which covers every byte and count ceiling in
/// the workspace, and `None` for a duration, a call, or a reference to another
/// constant. A symbol whose value cannot be evaluated is still *discovered*:
/// losing it would let a bound leave the campaign by being written differently.
fn declared_bounds() -> Result<DeclaredBounds, Box<dyn Error>> {
    let mut declared = DeclaredBounds::new();

    for crate_name in LIBRARY_CRATES {
        let source = workspace_root().join("crates").join(crate_name).join("src");
        for file in rust_files(&source)? {
            let text = fs::read_to_string(&file)
                .map_err(|error| Failure(format!("could not read {}: {error}", file.display())))?;
            let found = bounds_in(&text)
                .map_err(|error| Failure(format!("could not parse {}: {error}", file.display())))?;
            for (symbol, value) in found {
                declared.entry(symbol).or_default().push(value);
            }
        }
    }

    Ok(declared)
}

/// Returns every bound declaration in one source file.
///
/// The file is parsed rather than read line by line, and that is the whole
/// point of this function. A textual reader recognizes the spellings its author
/// happened to think of: it sees `pub const` and misses `pub(super) const`, and
/// it sees a declaration that fits on one line and misses the same declaration
/// after a formatter wraps it. Both failures are silent, and both would remove a
/// resource from the campaign's denominator without removing it from the
/// product. Visibility is therefore not consulted at all, and layout cannot be
/// consulted, because the input is a syntax tree.
///
/// What is consulted is the constant's *name*, against the repository's bound
/// declaration convention. That is a deliberate choice over a marker attribute:
/// an attribute is something an author has to remember, and a bound nobody
/// remembered to mark is exactly the one this scan is for. It is also the limit
/// of what this scan can promise, and the convention is written down in
/// [the coding conventions](../../../docs/engineering/coding-conventions.md) so
/// the promise has something to be measured against.
///
/// # Errors
///
/// Returns the parse failure when the file is not valid Rust, which is a broken
/// scan rather than an empty one.
fn bounds_in(text: &str) -> Result<Vec<(String, Option<i128>)>, syn::Error> {
    let file = syn::parse_file(text)?;
    let mut found = Vec::new();
    collect(&file.items, &mut found);
    Ok(found)
}

/// Collects bound declarations from a list of items, descending into modules.
///
/// Inline modules and `impl` blocks are descended into because a ceiling
/// declared inside either is still a ceiling the framework enforces. An
/// associated `const` is the shape `FaultStateEnvelope::MAX_ENTRIES` already
/// uses, so missing them would drop the retry cache.
fn collect(items: &[syn::Item], found: &mut Vec<(String, Option<i128>)>) {
    for item in items {
        match item {
            syn::Item::Const(declaration) => {
                record(
                    &declaration.ident,
                    &declaration.ty,
                    &declaration.expr,
                    found,
                );
            }
            syn::Item::Mod(module) => {
                if let Some((_, inner)) = &module.content {
                    collect(inner, found);
                }
            }
            syn::Item::Impl(block) => {
                for member in &block.items {
                    if let syn::ImplItem::Const(declaration) = member {
                        record(
                            &declaration.ident,
                            &declaration.ty,
                            &declaration.expr,
                            found,
                        );
                    }
                }
            }
            syn::Item::Trait(declaration) => {
                for member in &declaration.items {
                    if let syn::TraitItem::Const(constant) = member
                        && let Some((_, value)) = &constant.default
                    {
                        record(&constant.ident, &constant.ty, value, found);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Records one constant when its name and type make it a resource bound.
fn record(
    ident: &syn::Ident,
    ty: &syn::Type,
    expr: &syn::Expr,
    found: &mut Vec<(String, Option<i128>)>,
) {
    let symbol = ident.to_string();
    if !is_bound_symbol(&symbol) || !is_bound_type(ty) {
        return;
    }
    found.push((symbol, evaluate(expr)));
}

/// Reports whether a constant's name makes it a resource bound.
///
/// This is the repository's bound declaration convention, and it is the exact
/// extent of what the scan can find. A ceiling named outside it is invisible
/// here, which is why the convention is a documented rule rather than a habit.
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

/// Reports whether a constant's type can be a resource ceiling.
///
/// A reference or a slice is a catalogue or a message, not a quantity. Every
/// other type is admitted, including `Duration`, because two of the ceilings the
/// capacity budget declares are deadlines.
fn is_bound_type(ty: &syn::Type) -> bool {
    !matches!(ty, syn::Type::Reference(_) | syn::Type::Slice(_))
}

/// Evaluates a constant expression that is a product of integer literals.
///
/// Suffixes and `_` separators are handled by the literal parser rather than by
/// stripping text, so `64 * 1_024usize` and `65536` are read the same way.
/// Anything else — a call, a path to another constant, an unsupported operator —
/// evaluates to `None`, and the symbol is still discovered.
fn evaluate(expression: &syn::Expr) -> Option<i128> {
    match expression {
        syn::Expr::Lit(literal) => match &literal.lit {
            syn::Lit::Int(value) => value.base10_parse::<i128>().ok(),
            _ => None,
        },
        syn::Expr::Paren(inner) => evaluate(&inner.expr),
        syn::Expr::Group(inner) => evaluate(&inner.expr),
        syn::Expr::Binary(binary) if matches!(binary.op, syn::BinOp::Mul(_)) => {
            let left = evaluate(&binary.left)?;
            let right = evaluate(&binary.right)?;
            left.checked_mul(right)
        }
        _ => None,
    }
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

// ---------------------------------------------------------------------
// Contract-check exactness. `verify-ci-contract.sh` binds the dedicated
// workflow and `run-ci-campaign.sh` by exact git blob identity, not by the
// literal presence checks alone. These tests drive the real script against
// an isolated sandbox copy of those files, so a mutation proves the
// checker's actual behaviour rather than one helper's return value, and
// never touches the repository working tree.
// ---------------------------------------------------------------------

#[test]
fn contract_check_passes_on_the_canonical_workflow_and_script() -> Result<(), Box<dyn Error>> {
    assert!(run_resource_bounds_contract_check(|_sandbox| Ok(()))?);
    Ok(())
}

#[test]
fn contract_check_fails_on_an_added_trigger() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-resource-bounds.yml"),
            "  workflow_dispatch:\n",
            "  schedule:\n    - cron: '0 0 * * *'\n",
        )
    })?;
    assert!(
        !passed,
        "an added trigger must fail the contract check even though every expected trigger is \
         still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_widened_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-resource-bounds.yml"),
            "postgres: [\"15\", \"18\"]\n",
            "        include:\n          - postgres: \"16\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional matrix execution point must fail even though the literal \
         postgres: [\"15\", \"18\"] declaration is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_timeout() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-resource-bounds.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace("timeout-minutes: 45", "timeout-minutes: 5");
        assert_ne!(
            source, mutated,
            "the timeout literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed timeout must fail the contract check even though every other literal is \
         unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_report_or_artifact_path() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-resource-bounds.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace(
            "path: target/m5-campaigns/resource-bounds-campaign.json",
            "path: target/m5-campaigns/resource-bounds-campaign-renamed.json",
        );
        assert_ne!(
            source, mutated,
            "the report path literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed retained-report path must fail even though the producer command is unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_appended_script_command() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        append_line(
            &sandbox.join("tests/fixtures/resource-bounds/run-ci-campaign.sh"),
            "echo \"extra command\"",
        )
    })?;
    assert!(
        !passed,
        "an appended command must fail even though the expected cargo command is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_harmless_comment_byte() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        append_line(
            &sandbox.join(".github/workflows/m5-resource-bounds.yml"),
            "# harmless comment",
        )
    })?;
    assert!(
        !passed,
        "exact git blob identity, not heuristic literal parsing, is the retained-evidence \
         boundary: even a harmless trailing comment must fail",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_max_connections_override() -> Result<(), Box<dyn Error>> {
    let passed = run_resource_bounds_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-resource-bounds.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace(
            "--health-cmd \"pg_isready --username postgres --dbname oxide_batch_resource\"",
            "-c max_connections=200\n          --health-cmd \"pg_isready --username postgres \
             --dbname oxide_batch_resource\"",
        );
        assert_ne!(
            source, mutated,
            "the health-cmd literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a max_connections override must fail even though it does not remove any literal the \
         checker requires to be present: the service configuration is itself execution \
         semantics",
    );
    Ok(())
}

/// Copies the real workflow, script, and contract into an isolated sandbox,
/// applies `mutate` to that sandbox, then runs the real `verify-ci-contract.sh`
/// against the (possibly mutated) copy and reports whether it exited zero.
fn run_resource_bounds_contract_check(
    mutate: impl FnOnce(&Path) -> Result<(), Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    let root = workspace_root();
    let sandbox = Sandbox::new("resource-bounds-contract-check")?;

    let workflow_dir = sandbox.path().join(".github/workflows");
    fs::create_dir_all(&workflow_dir)?;
    fs::copy(
        root.join(".github/workflows/m5-resource-bounds.yml"),
        workflow_dir.join("m5-resource-bounds.yml"),
    )?;

    let fixture_dir = sandbox.path().join("tests/fixtures/resource-bounds");
    fs::create_dir_all(&fixture_dir)?;
    for name in [
        "execution-contract.json",
        "run-ci-campaign.sh",
        "verify-ci-contract.sh",
    ] {
        fs::copy(
            root.join("tests/fixtures/resource-bounds").join(name),
            fixture_dir.join(name),
        )?;
    }
    let checker = fixture_dir.join("verify-ci-contract.sh");
    let mut permissions = fs::metadata(&checker)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&checker, permissions)?;

    mutate(sandbox.path())?;

    exec_contract_checker(
        &checker,
        ".github/workflows/m5-resource-bounds.yml",
        sandbox.path(),
    )
}

/// Runs a freshly copied and `chmod`-ed contract-check script, retrying on a
/// transient `ExecutableFileBusy`.
///
/// Immediately exec'ing a file this test process just wrote and `chmod`ed
/// can race the kernel's release of the write mapping under heavy parallel
/// `cargo test` fork/exec load, surfacing as `ETXTBSY` even though the file
/// is complete and correctly permissioned. Each sandbox path is unique per
/// test, so this is never a real conflict — retry briefly before treating it
/// as a genuine failure.
fn exec_contract_checker(checker: &Path, arg: &str, cwd: &Path) -> Result<bool, Box<dyn Error>> {
    let mut attempt = 0u32;
    loop {
        match Command::new(checker).arg(arg).current_dir(cwd).status() {
            Ok(status) => return Ok(status.success()),
            Err(err) if err.kind() == io::ErrorKind::ExecutableFileBusy && attempt < 5 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(20 * u64::from(attempt)));
            }
            Err(err) => return Err(Box::new(err)),
        }
    }
}

/// Inserts `insertion` immediately after the first occurrence of `anchor`.
fn insert_after(path: &Path, anchor: &str, insertion: &str) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    let position = contents.find(anchor).ok_or_else(|| {
        Box::new(Failure(format!(
            "no {anchor:?} anchor found in {}",
            path.display()
        ))) as Box<dyn Error>
    })?;
    let insert_at = position + anchor.len();
    let mut mutated = String::with_capacity(contents.len() + insertion.len());
    mutated.push_str(&contents[..insert_at]);
    mutated.push_str(insertion);
    mutated.push_str(&contents[insert_at..]);
    fs::write(path, mutated)?;
    Ok(())
}

/// Appends one line to a file.
fn append_line(path: &Path, line: &str) -> Result<(), Box<dyn Error>> {
    let mut contents = fs::read_to_string(path)?;
    contents.push('\n');
    contents.push_str(line);
    contents.push('\n');
    fs::write(path, contents)?;
    Ok(())
}

/// A uniquely named temporary directory, removed when it goes out of scope
/// regardless of how the test exits.
struct Sandbox(PathBuf);

impl Sandbox {
    fn new(label: &str) -> Result<Self, Box<dyn Error>> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "oxide-batch-{label}-{}-{unique}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
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
            let mut dimensions = Vec::new();
            for subject in resource
                .get("subjects")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                dimensions.push(Dimension {
                    symbol: text(subject, "symbol")?,
                    value: subject
                        .get("ceiling")
                        .and_then(Value::as_i64)
                        .map(i128::from)
                        .ok_or_else(|| {
                            Failure(format!(
                                "{} declares a subject with no numeric ceiling",
                                text(resource, "resource").unwrap_or_default(),
                            ))
                        })?,
                });
            }
            if let Some(bounds) = resource.get("bounds") {
                for side in ["minimum", "maximum"] {
                    let Some(side_value) = bounds.get(side) else {
                        continue;
                    };
                    let Some(symbol) = side_value.get("symbol").and_then(Value::as_str) else {
                        continue;
                    };
                    let value = side_value
                        .get("value")
                        .and_then(Value::as_i64)
                        .map(i128::from)
                        .ok_or_else(|| {
                            Failure(format!(
                                "{} declares a {side} bound with no value",
                                text(resource, "resource").unwrap_or_default(),
                            ))
                        })?;
                    dimensions.push(Dimension {
                        symbol: symbol.to_owned(),
                        value,
                    });
                }
            }
            let informational = resource
                .get("informational_symbols")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|entry| entry.get("symbol").and_then(Value::as_str))
                .map(str::to_owned)
                .collect::<Vec<_>>();

            resources.push(Resource {
                name: text(resource, "resource")?,
                class: text(resource, "class")?,
                symbols: list(resource, "symbols"),
                ceiling: resource
                    .get("ceiling")
                    .and_then(Value::as_i64)
                    .map(i128::from),
                dimensions,
                informational,
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
    /// Every named dimension this resource declares beyond its single
    /// `ceiling`: one per subject-boundary subject, and one per range-boundary
    /// side that names its own symbol. Each is checked against the real
    /// source value that same symbol evaluates to, independently of every
    /// other dimension this resource has.
    dimensions: Vec<Dimension>,
    /// Symbols classified as a default or capacity hint rather than a
    /// ceiling: still required to be declared in source, but with no
    /// dimension value to cross-check.
    informational: Vec<String>,
    policy: String,
    report: String,
    budget_row: Option<String>,
    budget_bound: Option<String>,
    shedding_rule: String,
}

/// One symbol a resource's evidence independently proves, and the numeric
/// value the campaign declares for it.
struct Dimension {
    symbol: String,
    value: i128,
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
/// and report no gap. Everything here is therefore about *not missing* a
/// declaration, and most of the cases are the same bound written differently —
/// a different visibility, a wrapped line, an arithmetic expression split
/// across lines. A resource must not be able to leave the campaign's
/// denominator by being reformatted, and these are what say so.
#[allow(
    clippy::expect_used,
    reason = "every fixture here is a source fragment written in this file, so a parse failure is \
              a broken test rather than a condition the scan has to handle"
)]
mod scan {
    use super::{bounds_in, evaluate, is_bound_symbol};

    /// What a scan found for one symbol.
    ///
    /// The three cases are the ones that matter and they are not the same:
    /// `Missing` is the silent false negative this whole scan exists to rule
    /// out, `Unevaluated` is a bound that was found and whose value is not a
    /// number, and only the third is a value to check.
    #[derive(Debug, Eq, PartialEq)]
    enum Found {
        /// The scan did not discover the symbol at all.
        Missing,
        /// The scan discovered it and could not evaluate its expression.
        Unevaluated,
        /// The scan discovered it with this value.
        Value(i128),
    }

    /// Reads one source fragment the way the reconciliation does.
    fn scan(source: &str) -> Vec<(String, Option<i128>)> {
        bounds_in(source).expect("the fixture is valid Rust")
    }

    /// Reports what a scan of `source` found for `symbol`.
    fn found(source: &str, symbol: &str) -> Found {
        scan(source)
            .into_iter()
            .find(|(name, _)| name == symbol)
            .map_or(Found::Missing, |(_, value)| {
                value.map_or(Found::Unevaluated, Found::Value)
            })
    }

    #[test]
    fn a_bound_is_read_with_the_value_it_evaluates_to() {
        assert_eq!(
            scan("pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;"),
            vec![("MAX_RESPONSE_BYTES".to_owned(), Some(262_144))],
        );
        assert_eq!(
            scan("pub(crate) const MAX_MANIFEST_BYTES: usize = 64 * 1024;"),
            vec![("MAX_MANIFEST_BYTES".to_owned(), Some(65_536))],
        );
        assert_eq!(
            scan("const MAX_NODES: usize = 1_024;"),
            vec![("MAX_NODES".to_owned(), Some(1_024))],
        );
    }

    #[test]
    fn every_visibility_declares_the_same_bound() {
        // The scan does not consult visibility, and this is why: a ceiling that
        // became `pub(super)` in a refactor is the same ceiling, and a reader
        // that recognized only the spellings its author thought of would drop
        // it from the denominator without dropping it from the product.
        for visibility in [
            "",
            "pub ",
            "pub(crate) ",
            "pub(super) ",
            "pub(self) ",
            "pub(in crate::telemetry) ",
        ] {
            let source = format!("{visibility}const MAX_QUEUE_RECORDS: usize = 64;");
            assert_eq!(
                scan(&source),
                vec![("MAX_QUEUE_RECORDS".to_owned(), Some(64))],
                "a {visibility:?} declaration was not read as the bound it is",
            );
        }
    }

    #[test]
    fn layout_does_not_change_what_is_found() {
        // The same declaration as one line and as a formatter would wrap it.
        // A line-based reader sees the first and misses the second, which is a
        // silent false negative and the reason this scan parses instead.
        let inline = "pub const MAX_WRAPPED_BYTES: usize = 4 * 1024 * 1024;";
        let wrapped = "pub const MAX_WRAPPED_BYTES:
                usize =
                4 * 1024
                    * 1024;";
        assert_eq!(scan(inline), scan(wrapped));
        assert_eq!(
            scan(wrapped),
            vec![("MAX_WRAPPED_BYTES".to_owned(), Some(4_194_304))],
        );

        // Attributes and doc comments sit between the reader and the
        // declaration in real source.
        let annotated = "/// The ceiling.
            #[allow(dead_code)]
            pub(super) const MAX_ANNOTATED: usize = (2 * 8);";
        assert_eq!(
            scan(annotated),
            vec![("MAX_ANNOTATED".to_owned(), Some(16))],
        );
    }

    #[test]
    fn a_bound_declared_inside_a_module_or_an_impl_is_found() {
        // `FaultStateEnvelope::MAX_ENTRIES` is an associated const, so missing
        // these would drop the retry cache — one of the six classes the
        // performance plan names by name.
        assert_eq!(
            found(
                "impl FaultStateEnvelope { pub const MAX_ENTRIES: usize = 256; }",
                "MAX_ENTRIES",
            ),
            Found::Value(256),
        );
        assert_eq!(
            found(
                "mod inner { pub(crate) const MAX_INNER_BYTES: usize = 8 * 8; }",
                "MAX_INNER_BYTES",
            ),
            Found::Value(64),
        );
        assert_eq!(
            found(
                "trait Bounded { const MAX_TRAIT_ITEMS: usize = 12; }",
                "MAX_TRAIT_ITEMS",
            ),
            Found::Value(12),
        );
    }

    #[test]
    fn a_bound_whose_value_is_not_a_number_is_still_found() {
        // The value is unreadable and the symbol is not. Losing the symbol
        // would let a bound leave the campaign by being defined in terms of
        // another one.
        assert_eq!(
            scan("impl S { pub const MAX_LISTENERS: usize = MAX_LISTENERS; }"),
            vec![("MAX_LISTENERS".to_owned(), None)],
        );
        assert_eq!(
            scan("pub const MAX_SHUTDOWN_DEADLINE: Duration = Duration::from_hours(1);"),
            vec![("MAX_SHUTDOWN_DEADLINE".to_owned(), None)],
        );
        assert_eq!(
            scan("const MAX_DERIVED: usize = OTHER + 1;"),
            vec![("MAX_DERIVED".to_owned(), None)],
        );
    }

    #[test]
    fn a_constant_that_is_not_a_bound_is_not_read_as_one() {
        assert!(scan("pub const VERSION: &str = env!(\"CARGO_PKG_VERSION\");").is_empty());
        assert!(scan("pub const CONFIG_VERSION: u64 = 1;").is_empty());
        assert!(scan("impl S { pub const fn category(&self) -> ExitCategory { X } }").is_empty());
        assert!(scan("impl S { pub const ZERO: Self = Self(0); }").is_empty());
        // A bound-shaped name on a catalogue is a list, not a quantity.
        assert!(scan("pub const MAX_NAMES: &[&str] = &[\"a\"];").is_empty());
        assert!(scan("pub const MAX_LABEL: &str = \"other\";").is_empty());
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
        assert_eq!(
            found("const MAX_A: usize = 4 * 1024 * 1024;", "MAX_A"),
            Found::Value(4_194_304),
        );
        assert_eq!(
            found("const MAX_B: usize = 65_536;", "MAX_B"),
            Found::Value(65_536),
        );
        assert_eq!(
            found("const MAX_C: usize = 64 * 1_024usize;", "MAX_C"),
            Found::Value(65_536),
        );
        assert_eq!(
            found("const MAX_D: usize = (2 * (3 * 4));", "MAX_D"),
            Found::Value(24),
        );
        assert_eq!(
            found("const MAX_E: Duration = Duration::from_secs(1);", "MAX_E"),
            Found::Unevaluated,
        );
        assert_eq!(
            found("const MAX_F: usize = OTHER;", "MAX_F"),
            Found::Unevaluated,
        );
        // A symbol the scan never saw is a different answer from one it saw and
        // could not evaluate, and the tests must not be able to confuse them.
        assert_eq!(
            found("const MAX_G: usize = 1;", "MAX_ABSENT"),
            Found::Missing,
        );
    }

    #[test]
    fn an_unparseable_file_is_a_broken_scan_rather_than_an_empty_one() {
        // A scan that silently returned nothing would classify nothing and
        // report no gap, which is the one failure this whole reconciliation
        // cannot survive.
        assert!(bounds_in("pub const MAX_BROKEN: usize =").is_err());
    }

    #[test]
    fn the_evaluator_reads_expressions_rather_than_text() {
        let expression: syn::Expr =
            syn::parse_str("256 * 1024").expect("the fixture is a valid expression");
        assert_eq!(evaluate(&expression), Some(262_144));
    }
}
