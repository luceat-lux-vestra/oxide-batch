//! Denominator reconciliation for the M5 performance and reference workload.
//!
//! The accepted performance plan is the only normative document that states
//! what these two campaigns owe. The M5 design gate names no scenario for
//! either one, so the committed scope fixes the report and scenario names and
//! this test reconciles the scope against the plan in both directions.
//!
//! This target does not run the measurements. It makes the denominator
//! reviewable before a database runner exists, so a producer cannot quietly
//! omit P-001, P-003, P-010, a matrix point, or a required observation and
//! still call the campaign complete.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use oxide_batch::MAX_PARTITION_WORKERS;
use serde_json::Value;

const REQUIRED_CAMPAIGNS: &[&str] = &["performance", "reference-workload"];
const REQUIRED_REPORTS: &[&str] = &[
    "p001-fixed-overhead",
    "p003-reference-workload",
    "p010-local-partition-scaling",
];
const REQUIRED_WORKLOADS: &[&str] = &["P-001", "P-003", "P-010"];
const REQUIRED_MATRIX: &[&str] = &["postgres-15", "postgres-18"];
const REQUIRED_TARGET: &str = "postgres_performance";

#[test]
fn scope_declares_exactly_the_two_remaining_campaigns() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let campaigns = array(&scope, "campaigns")?;
    let ids = strings(campaigns, "id");
    assert_eq!(ids, REQUIRED_CAMPAIGNS);

    let performance = object_with_id(campaigns, "performance")?;
    assert_eq!(string_array(performance, "reports")?, REQUIRED_REPORTS);

    let reference = object_with_id(campaigns, "reference-workload")?;
    assert_eq!(
        string_array(reference, "reports")?,
        ["p003-reference-workload"],
        "the published reference workload is the same fixed P-003 run the performance campaign consumes",
    );
    Ok(())
}

#[test]
fn the_plan_still_requires_every_declared_workload() -> Result<(), Box<dyn Error>> {
    let plan = read_document("docs/engineering/performance-plan.md")?;
    let performance = plan_row(&plan, "Performance")?;
    for workload in REQUIRED_WORKLOADS {
        assert!(
            performance.contains(workload),
            "the campaign declares {workload}, but the accepted performance row no longer requires it",
        );
    }

    let reference = plan_row(&plan, "Reference workload")?;
    for owed in [
        "P-003",
        "fixed dataset size",
        "PostgreSQL 15 and 18",
        "throughput",
        "per-item",
        "per-chunk",
        "metadata write count",
        "peak memory",
        "connection count",
    ] {
        assert!(
            reference.contains(owed),
            "the accepted reference-workload row no longer requires {owed}",
        );
    }
    Ok(())
}

#[test]
fn every_report_belongs_to_a_required_workload_and_fixture() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let reports = array(&scope, "reports")?;
    assert_eq!(strings(reports, "id"), REQUIRED_REPORTS);

    let workloads = strings(reports, "workload");
    assert_eq!(workloads, REQUIRED_WORKLOADS);
    for report in reports {
        assert_eq!(
            report.get("target").and_then(Value::as_str),
            Some(REQUIRED_TARGET)
        );
        assert_eq!(
            report.get("fixture").and_then(Value::as_str),
            Some("postgres-performance"),
        );
        assert_eq!(
            report.get("against_database").and_then(Value::as_bool),
            Some(true),
            "all M5 performance reports run against the supported PostgreSQL matrix",
        );
    }
    Ok(())
}

#[test]
fn every_required_measurement_has_exactly_one_report_owner() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let reports = array(&scope, "reports")?;

    let uniquely_owned = [
        ("p001-fixed-overhead", "job-overhead"),
        ("p001-fixed-overhead", "step-overhead"),
        ("p003-reference-workload", "items-per-second"),
        ("p003-reference-workload", "chunks-per-second"),
        ("p003-reference-workload", "per-item-overhead"),
        ("p003-reference-workload", "per-chunk-overhead"),
        ("p003-reference-workload", "business-batch-size"),
        ("p010-local-partition-scaling", "scaling-efficiency"),
        ("p010-local-partition-scaling", "worker-skew"),
        ("p010-local-partition-scaling", "aggregation-duration"),
        ("p010-local-partition-scaling", "peak-owned-tasks"),
    ];

    for (report_id, measurement) in uniquely_owned {
        let owner = object_with_id(reports, report_id)?;
        assert!(
            string_array(owner, "measurements")?.contains(&measurement),
            "{report_id} does not declare the required {measurement} observation",
        );
        let owners = reports
            .iter()
            .filter(|report| {
                string_array(report, "measurements")
                    .is_ok_and(|values| values.contains(&measurement))
            })
            .count();
        assert_eq!(owners, 1, "{measurement} has no unique report owner");
    }

    for common in [
        "end-to-end-duration",
        "repository-round-trips",
        "metadata-writes",
        "peak-resident-memory",
        "peak-connections",
    ] {
        assert!(
            reports.iter().all(|report| {
                string_array(report, "measurements").is_ok_and(|values| values.contains(&common))
            }),
            "every database report must record {common}",
        );
    }
    Ok(())
}

#[test]
fn the_reference_workload_is_fixed_and_restart_relevant() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let p003 = scope
        .pointer("/workloads/p003")
        .ok_or_else(|| Failure("the scope declares no P-003 workload".to_owned()))?;
    let rows = number(p003, "dataset_rows")?;
    let chunk = number(p003, "chunk_size")?;
    assert!(rows > 0 && chunk > 0 && rows % chunk == 0);
    assert_eq!(number(p003, "source_seed")?, 102);
    assert!(
        p003.get("writer")
            .and_then(Value::as_str)
            .is_some_and(|writer| writer.contains("AtomicSameResource")),
        "P-003 must exercise the accepted enlisted same-resource path",
    );

    let report = object_with_id(array(&scope, "reports")?, "p003-reference-workload")?;
    let correctness = string_array(report, "correctness")?;
    for required in [
        "source-row-count-equals-written-row-count",
        "source-digest-equals-written-digest",
        "checkpoint-covers-the-fixed-dataset",
        "business-writes-and-checkpoints-use-atomic-same-resource",
    ] {
        assert!(correctness.contains(&required));
    }
    Ok(())
}

#[test]
fn p010_includes_one_ten_and_the_accepted_maximum() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let p010 = scope
        .pointer("/workloads/p010")
        .ok_or_else(|| Failure("the scope declares no P-010 workload".to_owned()))?;
    let points = p010
        .get("worker_points")
        .and_then(Value::as_array)
        .ok_or_else(|| Failure("P-010 declares no worker points".to_owned()))?
        .iter()
        .filter_map(Value::as_u64)
        .collect::<Vec<_>>();
    assert_eq!(
        points,
        [1, 10, u64::from(MAX_PARTITION_WORKERS)],
        "the scale points must include the sequential fallback, ten workers, and the largest accepted worker budget",
    );
    assert!(
        number(p010, "partitions")? >= u64::from(MAX_PARTITION_WORKERS),
        "the largest worker point needs enough partitions to occupy every worker",
    );
    Ok(())
}

#[test]
fn supported_matrix_matches_the_preview_boundary() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    assert_eq!(string_array(&scope, "supported_matrix")?, REQUIRED_MATRIX);
    let support = read_document("docs/release/support-matrix.md")?;
    for bound in [
        "PostgreSQL 15 | Supported oldest major, release-blocking",
        "PostgreSQL 18 | Supported newest major, release-blocking",
    ] {
        assert!(
            support.contains(bound),
            "the preview support document no longer promises {bound}",
        );
    }
    Ok(())
}

#[test]
fn numeric_results_remain_observational_until_a_budget_is_accepted() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    assert_eq!(
        scope
            .pointer("/execution/cargo_profile")
            .and_then(Value::as_str),
        Some("release"),
    );
    assert_eq!(
        scope
            .pointer("/execution/numeric_status")
            .and_then(Value::as_str),
        Some("observational"),
        "the accepted plan names no binding M5 throughput or latency number",
    );

    let plan = read_document("docs/engineering/performance-plan.md")?;
    assert!(plan.contains("provisional budgets, not release commitments"));
    assert!(plan.contains("PR benchmarks remain informational"));
    Ok(())
}

#[test]
fn related_m4_measurements_are_retained_not_relabelled() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let related = array(&scope, "related")?;
    let paths = strings(related, "path")
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        paths,
        BTreeSet::from([
            "docs/engineering/measurements/m4/p-010.json",
            "docs/engineering/measurements/m4/telemetry-overhead.json",
        ]),
    );
    assert!(related.iter().all(|entry| {
        entry.get("run_by_this_campaign").and_then(Value::as_bool) == Some(false)
    }));
    for path in paths {
        assert!(workspace_root().join(path).is_file());
    }
    Ok(())
}

#[test]
fn the_design_gate_gap_and_later_scope_are_explicit() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    assert!(scope.get("design_gate_scenario_gap").is_some());
    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    for scenario in strings(array(&scope, "reports")?, "scenario") {
        assert!(
            !gate.contains(scenario),
            "the gate now names {scenario}; reconcile against it instead of the recorded gap",
        );
    }

    let out_of_scope = string_array(&scope, "out_of_scope")?.join("\n");
    for required in ["IO-FLAT-001", "P-002", "remote worker", "ledger row"] {
        assert!(out_of_scope.contains(required));
    }
    Ok(())
}

fn read_scope() -> Result<Value, Box<dyn Error>> {
    let source = read_document("tests/fixtures/performance/campaign-scope.json")?;
    Ok(serde_json::from_str(&source)?)
}

fn read_document(path: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(path))?)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn array<'a>(value: &'a Value, name: &str) -> Result<&'a [Value], Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| Failure(format!("the scope declares no {name} array")).into())
}

fn strings<'a>(values: &'a [Value], field: &str) -> Vec<&'a str> {
    values
        .iter()
        .filter_map(|value| value.get(field).and_then(Value::as_str))
        .collect()
}

fn string_array<'a>(value: &'a Value, field: &str) -> Result<Vec<&'a str>, Box<dyn Error>> {
    Ok(value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| Failure(format!("the scope declares no {field} array")))?
        .iter()
        .filter_map(Value::as_str)
        .collect())
}

fn object_with_id<'a>(values: &'a [Value], id: &str) -> Result<&'a Value, Box<dyn Error>> {
    values
        .iter()
        .find(|value| value.get("id").and_then(Value::as_str) == Some(id))
        .ok_or_else(|| Failure(format!("the scope declares no {id} object")).into())
}

fn number(value: &Value, field: &str) -> Result<u64, Box<dyn Error>> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure(format!("the scope declares no numeric {field}")).into())
}

fn plan_row<'a>(plan: &'a str, name: &str) -> Result<&'a str, Box<dyn Error>> {
    plan.lines()
        .find(|line| line.starts_with(&format!("| {name} |")))
        .ok_or_else(|| Failure(format!("the performance plan has no {name} row")).into())
}

#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
