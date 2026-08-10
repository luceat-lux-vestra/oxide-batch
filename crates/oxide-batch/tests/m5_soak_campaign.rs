//! Scope reconciliation for the M5 `PostgreSQL` soak campaign.
//!
//! The campaign has the two halves the conformance, crash-and-restore, upgrade,
//! security, and resource-bound campaigns have, split for the same reason:
//!
//! - **what the campaign owes, and what proves it.** That is a reconciliation
//!   between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Its report needs a real database and
//!   returns green without one, because it skips. That half is
//!   `cargo xtask soak`.
//!
//! A soak needs a stronger denominator than most, and the difference is worth
//! stating because it is what these tests are mostly about. The other campaigns
//! enumerate obligations that exist independently of the run — ledger rows,
//! commit phases, schema paths, privilege classes, declared ceilings — so a
//! report either covered them or did not. A soak's obligation is *a period*.
//! Nothing outside the campaign says how long it should be, which means a soak
//! that ran three cycles and a soak that ran three hundred produce reports of
//! exactly the same shape, both green, and only the denominator distinguishes
//! them. So [`the_declared_window_is_a_window`] holds the period, and
//! [`every_growth_rule_is_decided_from_a_declared_observation`] holds the
//! rules, because a rule decided from a metric nothing observes is a rule that
//! passes by default.
//!
//! [`the_m4_soak_measurement_is_retained_rather_than_replaced`] exists for the
//! other direction. This campaign was built on the M4 in-memory measurement and
//! keeps its semantics, and the cheapest way to appear to deliver an M5 soak
//! would be to move that measurement under a `PostgreSQL` fixture and call the
//! result production-preview evidence. The M4 report stays where it is, on the
//! repository it measures, and the campaign scope records it as related
//! evidence it does not run.
//!
//! The scope document is `tests/fixtures/soak/campaign-scope.json` at the
//! workspace root. All three consumers read it — these tests, the runner, and
//! the report itself — so the window, the workload, and the rules are stated
//! once.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

/// The scenario the M5 design gate names for this campaign.
const NAMED_SCENARIO: &str = "soak_reports_no_task_connection_handle_or_memory_growth";

/// The report the performance plan's soak row requires.
const REQUIRED_REPORT: &str = "soak";

/// The resource classes the plan's soak row names, in its own words.
///
/// A campaign that observed three of the four would look complete in its own
/// report, so the fourth is required here rather than inferred from what the
/// report happens to record.
const REQUIRED_CLASSES: &[&str] = &["task", "connection", "handle", "memory"];

/// The M4 measurement this campaign builds on and must not consume.
const M4_MEASUREMENT: &str = "p015_shutdown_restart_soak";

#[test]
fn campaign_scope_matches_the_accepted_soak_obligations() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<Vec<_>>(),
        vec![REQUIRED_REPORT],
        "the campaign delivers exactly the report the performance plan requires",
    );
    let report = &scope.reports[0];
    assert_eq!(
        report.name, NAMED_SCENARIO,
        "the campaign's report must be the scenario the design gate names",
    );
    assert!(
        report.against_database,
        "a soak that is not run against a database is not this campaign",
    );

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    assert!(
        gate.contains(NAMED_SCENARIO),
        "the design gate must still name {NAMED_SCENARIO} for the evidence campaigns",
    );

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Soak |"))
        .ok_or_else(|| Failure("the performance plan has no soak row".to_owned()))?;
    for owed in [
        "P-015",
        "launch",
        "shutdown",
        "restart",
        "recovery",
        "declared duration",
    ] {
        assert!(
            row.contains(owed),
            "the performance plan's soak row no longer requires {owed}, so the campaign's \
             denominator and the accepted plan disagree",
        );
    }
    assert!(
        row.contains(&report.owes),
        "{} claims to discharge an obligation the plan's soak row does not state",
        report.id,
    );

    // The four resource classes are the plan's own list. A campaign that
    // recorded three of them would still produce a green report.
    for class in REQUIRED_CLASSES {
        assert!(
            row.contains(class),
            "the plan's soak row no longer names {class} growth",
        );
        assert!(
            scope.classes.contains(*class),
            "the campaign declares no {class} observation, so it would report on three of the \
             four resource classes the plan names and look complete doing it",
        );
    }

    Ok(())
}

#[test]
fn the_declared_window_is_a_window() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert!(
        scope.warmup_cycles > 0,
        "a soak with no warmup counts one-time startup growth as accumulation",
    );
    assert!(
        scope.measured_cycles > 0,
        "a soak with no measured window decides every growth rule from an empty series",
    );
    // The declared minimum is what the runner and the report both enforce, and
    // a minimum below the declared window would let a short run pass.
    assert_eq!(
        scope.minimum_measured_samples, scope.measured_cycles,
        "the campaign must require as many measured samples as it declares measured cycles",
    );
    assert!(
        scope.measured_cycles >= scope.warmup_cycles,
        "a campaign whose warmup is longer than its measurement is mostly warmup, and warmup is \
         the part the growth rules do not look at",
    );
    // A rule decided on convergence is only a rule while it actually requires
    // the rate to fall. A decay of 100% permits a straight line, which is the
    // one shape it exists to reject.
    for rule in &scope.rules {
        let Some(decay) = rule.decay_percent else {
            continue;
        };
        assert!(
            (1..100).contains(&decay),
            "the {} rule requires the late growth rate to be at most {decay}% of the early one, \
             which either forbids every series or permits a straight one",
            rule.id,
        );
        // The rate is estimated from a third of the window at each end, and an
        // estimate from a handful of page-quantised samples decides nothing:
        // the first CI run put the two supported majors at 0.57 and 0.86 of
        // the same quantity from sixty samples apiece.
        assert!(
            scope.measured_cycles / 3 >= 100,
            "the {} rule estimates a growth rate from {} samples at each end of the measured \
             window, which is too few for the estimate to separate a decaying series from a \
             straight one",
            rule.id,
            scope.measured_cycles / 3,
        );
    }
    assert_eq!(
        scope.termination.len(),
        1,
        "a soak with more than one allowed termination condition can stop early and still pass",
    );

    Ok(())
}

#[test]
fn every_growth_rule_is_decided_from_a_declared_observation() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    assert!(
        !scope.rules.is_empty(),
        "a soak that declares no growth rule cannot fail on growth",
    );
    for rule in &scope.rules {
        assert!(
            scope.observations.contains(&rule.metric),
            "the {} rule is decided from {}, which this campaign does not declare as an \
             observation, so the rule would pass by never being decided",
            rule.id,
            rule.metric,
        );
    }

    // Each resource class the plan names has to be under a rule, not merely
    // recorded. A metric that is sampled and never judged is context.
    for class in REQUIRED_CLASSES {
        assert!(
            scope.rules.iter().any(|rule| scope
                .class_of
                .iter()
                .any(|(metric, declared)| metric == &rule.metric && declared == class)),
            "the campaign observes {class} growth and no declared rule decides anything from it",
        );
    }

    // The history the database is supposed to accumulate must stay out of the
    // rules, or the campaign fails for the framework doing its job.
    for rule in &scope.rules {
        assert!(
            !scope.history_metrics.contains(&rule.metric),
            "{} is durable history, which grows on purpose, and the {} rule would fail the \
             campaign for it",
            rule.metric,
            rule.id,
        );
    }
    assert!(
        !scope.history_metrics.is_empty(),
        "the campaign records no durable history, so a run whose workload stopped doing durable \
         work would show the same flat resource series as a healthy one",
    );

    Ok(())
}

#[test]
fn the_declared_scenario_resolves_to_a_test_this_workspace_declares() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    for report in &scope.reports {
        assert!(
            scope.fixtures.contains(&report.fixture),
            "{} needs an undeclared fixture: {}",
            report.name,
            report.fixture,
        );
        let source = workspace_root()
            .join("crates")
            .join(&report.package)
            .join("tests")
            .join(format!("{}.rs", report.target));
        let declared = fs::read_to_string(&source)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.display())))?;
        assert!(
            declared.contains(&format!("fn {}(", report.name)),
            "{} declares no test named {}",
            source.display(),
            report.name,
        );
    }

    Ok(())
}

#[test]
fn the_m4_soak_measurement_is_retained_rather_than_replaced() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    let measurement = workspace_root()
        .join("crates")
        .join("oxide-batch")
        .join("tests")
        .join("m4_exit_measurements.rs");
    let declared = fs::read_to_string(&measurement)?;
    assert!(
        declared.contains(&format!("fn {M4_MEASUREMENT}(")),
        "the M4 in-memory soak measurement is the baseline this campaign builds on and it is no \
         longer declared; an M5 PostgreSQL campaign does not discharge an M4 in-memory one",
    );
    assert!(
        declared.contains("InMemoryJobRepository"),
        "the M4 measurement no longer runs on the in-memory repository, so it has been converted \
         rather than retained",
    );

    let related = scope
        .related
        .iter()
        .find(|entry| entry.name == M4_MEASUREMENT)
        .ok_or_else(|| {
            Failure(
                "the campaign scope does not record the M4 soak measurement as related evidence"
                    .to_owned(),
            )
        })?;
    assert!(
        !related.run_by_this_campaign,
        "the campaign runs the M4 in-memory measurement as part of its own evidence, which would \
         make an in-memory result part of a PostgreSQL campaign",
    );

    Ok(())
}

#[test]
fn the_campaign_does_not_claim_the_neighbouring_campaigns() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;

    // #102 stays open for these, and a soak that quietly absorbed one of them
    // would make the issue look closable.
    for excluded in [
        "p014-cancellation",
        "p001-p003-p010-performance",
        "published-reference-workload",
        "unbounded-duration",
    ] {
        assert!(
            scope.excluded.contains(excluded),
            "the campaign does not argue {excluded} out of scope, so its boundary is unstated",
        );
    }

    Ok(())
}

/// The committed campaign scope document, as reconciliation reads it.
struct Scope {
    fixtures: BTreeSet<String>,
    reports: Vec<Report>,
    warmup_cycles: u64,
    measured_cycles: u64,
    minimum_measured_samples: u64,
    termination: Vec<String>,
    observations: BTreeSet<String>,
    classes: BTreeSet<String>,
    class_of: Vec<(String, String)>,
    history_metrics: BTreeSet<String>,
    rules: Vec<RuleEntry>,
    excluded: BTreeSet<String>,
    related: Vec<Related>,
}

/// One report the campaign delivers.
struct Report {
    id: String,
    owes: String,
    package: String,
    target: String,
    name: String,
    fixture: String,
    against_database: bool,
}

/// One declared growth rule.
struct RuleEntry {
    id: String,
    metric: String,
    decay_percent: Option<i64>,
}

/// One piece of evidence the campaign records and does not run.
struct Related {
    name: String,
    run_by_this_campaign: bool,
}

impl Scope {
    /// Reads and parses the committed scope document.
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("soak")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let fixtures = document
            .get("fixtures")
            .and_then(Value::as_object)
            .map(|object| object.keys().cloned().collect())
            .ok_or_else(|| Failure("the scope document declares no fixtures".to_owned()))?;

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: field(report, "id")?,
                owes: field(report, "owes")?,
                package: field(report, "package")?,
                target: field(report, "target")?,
                name: field(report, "name")?,
                fixture: field(report, "fixture")?,
                against_database: report
                    .get("database_report")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }

        let window = document
            .get("window")
            .ok_or_else(|| Failure("the scope document declares no window".to_owned()))?;
        let sampling = window
            .get("sampling")
            .ok_or_else(|| Failure("the window declares no sampling".to_owned()))?;

        let mut observations = BTreeSet::new();
        let mut classes = BTreeSet::new();
        let mut class_of = Vec::new();
        let mut history_metrics = BTreeSet::new();
        for observation in array(&document, "observations")? {
            let id = field(observation, "id")?;
            let class = field(observation, "class")?;
            if class == "durable-history" {
                history_metrics.insert(id.clone());
            }
            class_of.push((id.clone(), class.clone()));
            observations.insert(id);
            classes.insert(class);
        }

        let growth = document
            .get("growth_rules")
            .ok_or_else(|| Failure("the scope document declares no growth rules".to_owned()))?;
        let mut rules = Vec::new();
        for rule in array(growth, "rules")? {
            rules.push(RuleEntry {
                id: field(rule, "id")?,
                metric: field(rule, "metric")?,
                decay_percent: rule.get("decay_percent").and_then(Value::as_i64),
            });
        }

        let mut related = Vec::new();
        for entry in array(&document, "related")? {
            related.push(Related {
                name: field(entry, "name")?,
                run_by_this_campaign: entry
                    .get("run_by_this_campaign")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }

        Ok(Self {
            fixtures,
            reports,
            warmup_cycles: number(window, "warmup_cycles")?,
            measured_cycles: number(window, "measured_cycles")?,
            minimum_measured_samples: number(sampling, "minimum_measured_samples")?,
            termination: window
                .pointer("/termination/allowed")
                .and_then(Value::as_array)
                .map(|allowed| {
                    allowed
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .ok_or_else(|| Failure("the window declares no allowed termination".to_owned()))?,
            observations,
            classes,
            class_of,
            history_metrics,
            rules,
            excluded: array(&document, "out_of_scope")?
                .iter()
                .map(|entry| field(entry, "id"))
                .collect::<Result<_, _>>()?,
            related,
        })
    }
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Reads one canonical document from the workspace.
fn read_document(relative: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(relative))?)
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document.get(name).and_then(Value::as_array).ok_or_else(|| {
        Box::new(Failure(format!("the scope document has no {name}"))) as Box<dyn Error>
    })
}

/// Reads one required string field.
fn field(value: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// Reads one required count.
fn number(value: &Value, name: &str) -> Result<u64, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// A reconciliation input the campaign could not read.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
