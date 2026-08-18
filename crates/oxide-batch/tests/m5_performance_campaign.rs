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
//! still call the campaign complete. Every check here is fail-closed: a
//! missing, non-string, non-numeric, duplicated, or undeclared entry is a
//! violation, never something a `filter_map` quietly drops from the
//! comparison. [`validate_scope`] and the plan-row parsers are plain
//! `Value -> Result` functions precisely so the negative-mutation tests below
//! can drive them with a mutated in-memory clone of the real fixture, without
//! touching disk.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::MAX_PARTITION_WORKERS;
use serde_json::Value;

const REQUIRED_CAMPAIGNS: &[&str] = &["performance", "reference-workload"];
const REQUIRED_REPORTS: &[&str] = &[
    "p001-fixed-overhead",
    "p003-reference-workload",
    "p010-local-partition-scaling",
];
const REQUIRED_MATRIX: &[&str] = &["postgres-15", "postgres-18"];
const REQUIRED_TARGET: &str = "postgres_performance";
const REQUIRED_FIXTURE: &str = "postgres-performance";

/// The exact contract one declared report owes.
///
/// A report's `measurements` and `correctness` arrays are compared to these
/// sets exactly, in both directions: a missing entry, an extra undeclared
/// one, and a duplicate are each their own violation. `against_database`
/// resolves the P-001 workload-semantics conflict described on
/// [`p001_workload_semantics_are_reconciled_between_scope_and_plan`]: the
/// accepted plan defines P-001 as an in-memory measurement, so it alone
/// carries `false` here.
struct ReportSpec {
    id: &'static str,
    workload: &'static str,
    scenario: &'static str,
    against_database: bool,
    measurements: &'static [&'static str],
    correctness: &'static [&'static str],
}

const REPORT_SPECS: &[ReportSpec] = &[
    ReportSpec {
        id: "p001-fixed-overhead",
        workload: "P-001",
        scenario: "p001_fixed_tasklet_lifecycle_overhead",
        against_database: false,
        measurements: &[
            "end-to-end-duration",
            "job-overhead",
            "step-overhead",
            "repository-round-trips",
            "metadata-writes",
            "peak-resident-memory",
            "peak-connections",
        ],
        correctness: &[
            "every-attempt-completes",
            "durable-job-and-step-statuses-are-completed",
            "no-attempt-is-reused",
        ],
    },
    ReportSpec {
        id: "p003-reference-workload",
        workload: "P-003",
        scenario: "p003_csv_to_postgres_reference_workload",
        against_database: true,
        measurements: &[
            "items-per-second",
            "chunks-per-second",
            "end-to-end-duration",
            "per-item-overhead",
            "per-chunk-overhead",
            "repository-round-trips",
            "metadata-writes",
            "business-batch-size",
            "peak-resident-memory",
            "peak-connections",
        ],
        correctness: &[
            "source-row-count-equals-written-row-count",
            "source-digest-equals-written-digest",
            "checkpoint-covers-the-fixed-dataset",
            "business-writes-and-checkpoints-use-atomic-same-resource",
        ],
    },
    ReportSpec {
        id: "p010-local-partition-scaling",
        workload: "P-010",
        scenario: "p010_postgres_local_partition_scaling",
        against_database: true,
        measurements: &[
            "partitions-per-second",
            "end-to-end-duration",
            "scaling-efficiency",
            "worker-skew",
            "aggregation-duration",
            "repository-round-trips",
            "metadata-writes",
            "peak-resident-memory",
            "peak-connections",
            "peak-owned-tasks",
        ],
        correctness: &[
            "every-scale-point-has-identical-durable-observations",
            "peak-workers-do-not-exceed-the-configured-budget",
            "peak-connections-do-not-exceed-the-derived-pool-budget",
            "no-worker-outlives-its-parent",
            "observed-concurrency-matches-configured-worker-point",
            "pool-below-derived-budget-is-rejected-before-workers-start",
            "business-row-set-matches-fixed-partition-set-at-every-scale-point",
        ],
    },
];

const EXPECTED_PERFORMANCE_ROW: &str = "P-001 fixed overhead and P-003 enlisted-writer throughput against the M4 provisional budgets, plus P-010 at `1`, `10`, and the largest configured worker count";
const EXPECTED_REFERENCE_WORKLOAD_ROW: &str = "One published end-to-end workload derived from P-003, run at a fixed dataset size on PostgreSQL 15 and 18, reporting throughput, per-item and per-chunk overhead, metadata write count, peak memory, and connection count";
const EXPECTED_P001_WORKLOAD_DESCRIPTION: &str = "In-memory no-op tasklet lifecycle";
const EXPECTED_P003_WORKLOAD_DESCRIPTION: &str = "CSV to PostgreSQL with enlisted writer";
const EXPECTED_P003_SOURCE: &str =
    "deterministically-generated RFC 4180 CSV with a header and three scalar columns";
const EXPECTED_MATRIX_RULE: &str = "Every report runs exactly once at each supported matrix point.";

// ---------------------------------------------------------------------
// The real fixture, exercised end to end against the file on disk.
// ---------------------------------------------------------------------

#[test]
fn the_fixture_scope_satisfies_the_full_denominator_contract() -> Result<(), Box<dyn Error>> {
    Ok(validate_scope(&read_scope()?)?)
}

#[test]
fn scope_declares_exactly_the_two_remaining_campaigns() -> Result<(), Box<dyn Error>> {
    Ok(validate_campaigns(&read_scope()?)?)
}

#[test]
fn every_report_matches_its_exact_workload_scenario_and_database_contract()
-> Result<(), Box<dyn Error>> {
    Ok(validate_reports(&read_scope()?)?)
}

#[test]
fn supported_matrix_is_exactly_postgres_15_and_18() -> Result<(), Box<dyn Error>> {
    Ok(validate_matrix(&read_scope()?)?)
}

#[test]
fn execution_stays_release_profile_and_observational() -> Result<(), Box<dyn Error>> {
    Ok(validate_execution(&read_scope()?)?)
}

#[test]
fn p001_workload_values_are_fixed_exactly() -> Result<(), Box<dyn Error>> {
    Ok(validate_workload_p001(&read_scope()?)?)
}

#[test]
fn p003_workload_values_are_fixed_exactly() -> Result<(), Box<dyn Error>> {
    Ok(validate_workload_p003(&read_scope()?)?)
}

#[test]
fn p010_workload_values_are_fixed_exactly() -> Result<(), Box<dyn Error>> {
    Ok(validate_workload_p010(&read_scope()?)?)
}

#[test]
fn the_performance_row_states_exactly_the_accepted_contract() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let campaigns = get_array(&scope, "campaigns", "the scope")?;
    let performance = object_with_id(campaigns, "campaigns", "performance")?;
    let plan_row_name = get_str(performance, "plan_row", "the performance campaign")?;

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let cells = plan_row_cells(&plan, plan_row_name, 2)?;
    let obligation = cells
        .get(1)
        .ok_or_else(|| Failure("the Performance row has no obligation cell".to_owned()))?;
    assert_eq!(
        obligation, EXPECTED_PERFORMANCE_ROW,
        "the accepted Performance row's contract changed; reconcile tests/fixtures/performance/campaign-scope.json against the new text and update EXPECTED_PERFORMANCE_ROW deliberately, not by loosening this check",
    );
    assert_eq!(
        workload_ids_mentioned(obligation),
        REQUIRED_WORKLOADS.iter().copied().collect::<BTreeSet<_>>(),
        "the Performance row no longer names exactly P-001, P-003, and P-010",
    );
    Ok(())
}

#[test]
fn the_reference_workload_row_states_exactly_the_accepted_contract() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let campaigns = get_array(&scope, "campaigns", "the scope")?;
    let reference = object_with_id(campaigns, "campaigns", "reference-workload")?;
    let plan_row_name = get_str(reference, "plan_row", "the reference-workload campaign")?;

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let cells = plan_row_cells(&plan, plan_row_name, 2)?;
    let obligation = cells
        .get(1)
        .ok_or_else(|| Failure("the Reference workload row has no obligation cell".to_owned()))?;
    assert_eq!(
        obligation, EXPECTED_REFERENCE_WORKLOAD_ROW,
        "the accepted Reference workload row's contract changed; reconcile the campaign scope against the new text before updating this constant",
    );
    Ok(())
}

#[test]
fn p001_workload_semantics_are_reconciled_between_scope_and_plan() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let reports = get_array(&scope, "reports", "the scope")?;
    let p001 = object_with_id(reports, "reports", "p001-fixed-overhead")?;
    let against_database = get_bool(p001, "against_database", "p001-fixed-overhead")?;

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let cells = plan_row_cells(&plan, "P-001", 3)?;
    let description = cells.get(1).ok_or_else(|| {
        Failure("the workload table's P-001 row has no description cell".to_owned())
    })?;

    assert_eq!(
        description, EXPECTED_P001_WORKLOAD_DESCRIPTION,
        "the accepted workload table's P-001 description changed; the scope's against_database decision assumes it names an in-memory measurement, so any change here needs an explicit reconciliation, not a silent pass",
    );
    assert!(
        !against_database,
        "the accepted workload table defines P-001 as {description:?}, an in-memory measurement independent of the PostgreSQL major; the scope must not declare against_database=true for it",
    );
    Ok(())
}

#[test]
fn p003_workload_semantics_are_reconciled_between_scope_and_plan() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let p003 = scope
        .pointer("/workloads/p003")
        .ok_or_else(|| Failure("the scope declares no P-003 workload".to_owned()))?;
    let source = get_str(p003, "source", "workloads.p003")?;
    let writer = get_str(p003, "writer", "workloads.p003")?;

    let plan = read_document("docs/engineering/performance-plan.md")?;
    let cells = plan_row_cells(&plan, "P-003", 3)?;
    let description = cells.get(1).ok_or_else(|| {
        Failure("the workload table's P-003 row has no description cell".to_owned())
    })?;

    assert_eq!(
        description, EXPECTED_P003_WORKLOAD_DESCRIPTION,
        "the accepted workload table's P-003 description changed; the scope's source and writer fields assume it still names a CSV-to-PostgreSQL workload, so any change here needs an explicit reconciliation, not a silent pass",
    );
    assert!(
        source.contains("CSV"),
        "the scope's P-003 source no longer names CSV, contradicting the plan's {description:?} workload",
    );
    assert!(
        writer.contains("PostgreSQL"),
        "the scope's P-003 writer no longer names PostgreSQL, contradicting the plan's {description:?} workload",
    );
    Ok(())
}

#[test]
fn supported_matrix_matches_the_preview_boundary() -> Result<(), Box<dyn Error>> {
    validate_matrix(&read_scope()?)?;
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
    validate_execution(&read_scope()?)?;
    let plan = read_document("docs/engineering/performance-plan.md")?;
    assert!(plan.contains("provisional budgets, not release commitments"));
    assert!(plan.contains("PR benchmarks remain informational"));
    Ok(())
}

#[test]
fn related_m4_measurements_are_retained_not_relabelled() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let related = get_array(&scope, "related", "the scope")?;
    let paths = ordered_strings(
        &related
            .iter()
            .map(|entry| entry.get("path").cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>(),
        "related[].path",
    )?
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert_eq!(
        paths,
        BTreeSet::from([
            "docs/engineering/measurements/m4/p-010.json".to_owned(),
            "docs/engineering/measurements/m4/telemetry-overhead.json".to_owned(),
        ]),
    );
    assert!(related.iter().all(|entry| {
        entry.get("run_by_this_campaign").and_then(Value::as_bool) == Some(false)
    }));
    for path in paths {
        assert!(workspace_root().join(&path).is_file());
    }
    Ok(())
}

#[test]
fn the_design_gate_gap_and_later_scope_are_explicit() -> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    assert!(scope.get("design_gate_scenario_gap").is_some());
    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    let reports = get_array(&scope, "reports", "the scope")?;
    for scenario in ordered_strings(
        &reports
            .iter()
            .map(|report| report.get("scenario").cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>(),
        "reports[].scenario",
    )? {
        assert!(
            !gate.contains(scenario),
            "the gate now names {scenario}; reconcile against it instead of the recorded gap",
        );
    }

    let out_of_scope = get_array(&scope, "out_of_scope", "the scope")?;
    let out_of_scope = ordered_strings(out_of_scope, "out_of_scope")?.join("\n");
    for required in ["IO-FLAT-001", "P-002", "remote worker", "ledger row"] {
        assert!(out_of_scope.contains(required));
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Parser-level regression: a plan row that gains or loses an obligation
// must not parse as equal to the accepted contract.
// ---------------------------------------------------------------------

#[test]
fn performance_row_parser_detects_an_added_workload_obligation() -> Result<(), Box<dyn Error>> {
    let augmented = "| Performance | P-001 fixed overhead and P-003 enlisted-writer throughput against the M4 provisional budgets, plus P-010 at `1`, `10`, and the largest configured worker count, plus P-099 something new |";
    let cells = plan_row_cells(augmented, "Performance", 2)?;
    assert_ne!(cells[1], EXPECTED_PERFORMANCE_ROW);
    assert_ne!(
        workload_ids_mentioned(&cells[1]),
        REQUIRED_WORKLOADS.iter().copied().collect::<BTreeSet<_>>(),
        "a row with an added P-ID must not parse as the same obligation set",
    );
    Ok(())
}

#[test]
fn performance_row_parser_detects_a_removed_workload_obligation() -> Result<(), Box<dyn Error>> {
    let reduced = "| Performance | P-001 fixed overhead against the M4 provisional budgets, plus P-010 at `1`, `10`, and the largest configured worker count |";
    let cells = plan_row_cells(reduced, "Performance", 2)?;
    assert_ne!(cells[1], EXPECTED_PERFORMANCE_ROW);
    assert!(!workload_ids_mentioned(&cells[1]).contains("P-003"));
    Ok(())
}

#[test]
fn plan_row_parser_rejects_a_table_with_no_matching_row() {
    let plan = "| Other | something else |";
    assert!(plan_row_cells(plan, "Performance", 2).is_err());
}

#[test]
fn plan_row_parser_rejects_a_duplicated_performance_row() {
    let plan = "| Performance | first obligation |\n| Performance | second obligation |";
    assert!(plan_row_cells(plan, "Performance", 2).is_err());
}

#[test]
fn plan_row_parser_rejects_a_duplicated_reference_workload_row() {
    let plan =
        "| Reference workload | first obligation |\n| Reference workload | second obligation |";
    assert!(plan_row_cells(plan, "Reference workload", 2).is_err());
}

#[test]
fn plan_row_parser_rejects_a_performance_row_with_an_extra_cell() {
    let plan = "| Performance | obligation | unexpected extra cell |";
    assert!(plan_row_cells(plan, "Performance", 2).is_err());
}

#[test]
fn plan_row_parser_rejects_a_reference_workload_row_with_an_extra_cell() {
    let plan = "| Reference workload | obligation | unexpected extra cell |";
    assert!(plan_row_cells(plan, "Reference workload", 2).is_err());
}

#[test]
fn plan_row_parser_rejects_a_workload_row_with_a_missing_cell() {
    let plan = "| P-001 | In-memory no-op tasklet lifecycle |";
    assert!(plan_row_cells(plan, "P-001", 3).is_err());
}

#[test]
fn plan_row_parser_rejects_a_workload_row_with_an_extra_cell() {
    let plan =
        "| P-001 | In-memory no-op tasklet lifecycle | fixed framework overhead | extra cell |";
    assert!(plan_row_cells(plan, "P-001", 3).is_err());
}

#[test]
fn p003_workload_table_row_change_is_detected_by_the_parser() -> Result<(), Box<dyn Error>> {
    let mutated = "| P-003 | CSV to Parquet with enlisted writer | parser, batch write, transaction, metadata |";
    let cells = plan_row_cells(mutated, "P-003", 3)?;
    assert_ne!(cells[1], EXPECTED_P003_WORKLOAD_DESCRIPTION);
    Ok(())
}

// ---------------------------------------------------------------------
// Negative mutation regressions. Each starts from the real, passing
// fixture and applies exactly one malformation, then asserts the shared
// `validate_scope` reconciliation rejects it. This is the property the
// campaign actually depends on: a producer-side fixture drift, not just a
// hand-written unit test of one helper, has to fail.
// ---------------------------------------------------------------------

const REQUIRED_WORKLOADS: &[&str] = &["P-001", "P-003", "P-010"];

#[test]
fn the_healthy_fixture_passes_reconciliation() -> Result<(), Box<dyn Error>> {
    assert!(validate_scope(&read_scope()?).is_ok());
    Ok(())
}

#[test]
fn unnamed_campaign_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["campaigns"][0]["id"] = Value::String(String::new());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_campaign_id_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["campaigns"][0]["id"] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn unnamed_report_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["reports"][0]["id"] = Value::String(String::new());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_report_id_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["reports"][0]["id"] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn missing_workload_field_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = report_position(&scope, "p003-reference-workload")?;
    scope["reports"][index]
        .as_object_mut()
        .ok_or_else(|| Failure("report is not an object".to_owned()))?
        .remove("workload");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_workload_field_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = report_position(&scope, "p003-reference-workload")?;
    scope["reports"][index]["workload"] = Value::from(3);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_matrix_entry_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["supported_matrix"][0] = Value::from(15);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn malformed_worker_point_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p010"]["worker_points"][2] = Value::String("64".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_dataset_rows_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["dataset_rows"] = Value::from(9_999);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_chunk_size_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["chunk_size"] = Value::from(10);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_source_seed_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["source_seed"] = Value::from(103);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_writer_missing_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]
        .as_object_mut()
        .ok_or_else(|| Failure("workloads.p003 is not an object".to_owned()))?
        .remove("writer");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_writer_weakened_away_from_enlisted_atomic_same_resource_fails_reconciliation()
-> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["writer"] =
        Value::String("test-local direct PostgreSQL writer".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p001_measured_attempts_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p001"]["measured_attempts"] = Value::from(128);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_partition_count_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p010"]["partitions"] = Value::from(64);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_reintroducing_enlisted_wording_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    // The accepted performance plan assigns enlisted-writer/AtomicSameResource
    // semantics to P-003 only; P-010's own workload-table row and the
    // Performance plan row's P-010 clause never say "enlisted"
    // (docs/engineering/performance-plan.md). #125's campaign-local P-010
    // wording accidentally claimed enlistment anyway, which the accepted
    // architecture does not require and the actual producer cannot deliver
    // (TaskletContext exposes no transaction handle to any Tasklet). Any
    // wording drift back toward "enlisted" must fail reconciliation exactly
    // like every other fixed P-010 value below, so the campaign cannot drift
    // back into the same overconstraint.
    let mut scope = read_scope()?;
    scope["workloads"]["p010"]["work_per_partition"] = Value::String(
        "one deterministic enlisted business write and one durable partition result".to_owned(),
    );
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_missing_postgresql_business_write_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p010"]["work_per_partition"] =
        Value::String("one durable partition result".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_missing_durable_partition_result_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p010"]["work_per_partition"] =
        Value::String("one deterministic PostgreSQL business write".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_missing_partitions_per_second_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p010-local-partition-scaling", "measurements")?;
    remove_string(&mut scope, &pointer, "partitions-per-second")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p001_missing_correctness_entry_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p001-fixed-overhead", "correctness")?;
    remove_string(&mut scope, &pointer, "no-attempt-is-reused")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p010_missing_correctness_entry_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p010-local-partition-scaling", "correctness")?;
    remove_string(&mut scope, &pointer, "no-worker-outlives-its-parent")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn duplicate_measurement_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p003-reference-workload", "measurements")?;
    push_string(&mut scope, &pointer, "items-per-second")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn unknown_measurement_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p003-reference-workload", "measurements")?;
    push_string(&mut scope, &pointer, "unspecified-measurement")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn duplicate_correctness_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p001-fixed-overhead", "correctness")?;
    push_string(&mut scope, &pointer, "every-attempt-completes")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn unknown_correctness_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p001-fixed-overhead", "correctness")?;
    push_string(&mut scope, &pointer, "unspecified-correctness-obligation")?;
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_measurement_entry_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p003-reference-workload", "measurements")?;
    let target = scope
        .pointer_mut(&pointer)
        .ok_or_else(|| Failure(format!("{pointer} does not resolve")))?;
    target[0] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn non_string_correctness_entry_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let pointer = report_pointer(&scope, "p001-fixed-overhead", "correctness")?;
    let target = scope
        .pointer_mut(&pointer)
        .ok_or_else(|| Failure(format!("{pointer} does not resolve")))?;
    target[0] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p001_declared_against_a_live_database_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = report_position(&scope, "p001-fixed-overhead")?;
    scope["reports"][index]["against_database"] = Value::from(true);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_source_missing_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]
        .as_object_mut()
        .ok_or_else(|| Failure("workloads.p003 is not an object".to_owned()))?
        .remove("source");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_source_non_string_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["source"] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn p003_source_drift_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["workloads"]["p003"]["source"] =
        Value::String("a differently generated dataset".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn performance_plan_row_missing_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = campaign_position(&scope, "performance")?;
    scope["campaigns"][index]
        .as_object_mut()
        .ok_or_else(|| Failure("campaign is not an object".to_owned()))?
        .remove("plan_row");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn performance_plan_row_wrong_value_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = campaign_position(&scope, "performance")?;
    scope["campaigns"][index]["plan_row"] = Value::String("Reference workload".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn reference_workload_plan_row_missing_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = campaign_position(&scope, "reference-workload")?;
    scope["campaigns"][index]
        .as_object_mut()
        .ok_or_else(|| Failure("campaign is not an object".to_owned()))?
        .remove("plan_row");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn reference_workload_plan_row_non_string_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    let index = campaign_position(&scope, "reference-workload")?;
    scope["campaigns"][index]["plan_row"] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn matrix_rule_missing_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["execution"]
        .as_object_mut()
        .ok_or_else(|| Failure("execution is not an object".to_owned()))?
        .remove("matrix_rule");
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn matrix_rule_non_string_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["execution"]["matrix_rule"] = Value::from(1);
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

#[test]
fn matrix_rule_wrong_value_fails_reconciliation() -> Result<(), Box<dyn Error>> {
    let mut scope = read_scope()?;
    scope["execution"]["matrix_rule"] =
        Value::String("Some reports may run at fewer matrix points.".to_owned());
    assert!(validate_scope(&scope).is_err());
    Ok(())
}

// ---------------------------------------------------------------------
// The pure denominator contract. `Value -> Result<(), Failure>` throughout,
// so both the on-disk fixture and a mutated in-memory clone can drive it.
// ---------------------------------------------------------------------

fn validate_scope(scope: &Value) -> Result<(), Failure> {
    validate_campaigns(scope)?;
    validate_reports(scope)?;
    validate_matrix(scope)?;
    validate_execution(scope)?;
    validate_workload_p001(scope)?;
    validate_workload_p003(scope)?;
    validate_workload_p010(scope)?;
    Ok(())
}

fn validate_campaigns(scope: &Value) -> Result<(), Failure> {
    let campaigns = get_array(scope, "campaigns", "the scope")?;
    let ids = ids_of(campaigns, "campaigns")?;
    if ids.as_slice() != REQUIRED_CAMPAIGNS {
        return Err(Failure(format!(
            "the scope declares campaigns {ids:?}, not exactly {REQUIRED_CAMPAIGNS:?}",
        )));
    }

    let performance = object_with_id(campaigns, "campaigns", "performance")?;
    let performance_reports = get_array(performance, "reports", "the performance campaign")?;
    let performance_reports =
        ordered_strings(performance_reports, "the performance campaign's reports")?;
    if performance_reports.as_slice() != REQUIRED_REPORTS {
        return Err(Failure(
            "the performance campaign must declare exactly p001-fixed-overhead, \
             p003-reference-workload, and p010-local-partition-scaling, in that order"
                .to_owned(),
        ));
    }
    if get_str(performance, "plan_row", "the performance campaign")? != "Performance" {
        return Err(Failure(
            "the performance campaign must bind plan_row exactly to the accepted plan's \
             Performance row"
                .to_owned(),
        ));
    }

    let reference = object_with_id(campaigns, "campaigns", "reference-workload")?;
    let reference_reports = get_array(reference, "reports", "the reference-workload campaign")?;
    let reference_reports = ordered_strings(
        reference_reports,
        "the reference-workload campaign's reports",
    )?;
    if reference_reports.as_slice() != ["p003-reference-workload"] {
        return Err(Failure(
            "the reference-workload campaign must declare exactly the shared \
             p003-reference-workload report: running the same fixed workload twice would \
             produce two samples, not two different obligations"
                .to_owned(),
        ));
    }
    if get_str(reference, "plan_row", "the reference-workload campaign")? != "Reference workload" {
        return Err(Failure(
            "the reference-workload campaign must bind plan_row exactly to the accepted plan's \
             Reference workload row"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_reports(scope: &Value) -> Result<(), Failure> {
    let reports = get_array(scope, "reports", "the scope")?;
    let ids = ids_of(reports, "reports")?;
    if ids.as_slice() != REQUIRED_REPORTS {
        return Err(Failure(format!(
            "the scope declares reports {ids:?}, not exactly {REQUIRED_REPORTS:?} in order",
        )));
    }

    for spec in REPORT_SPECS {
        let report = object_with_id(reports, "reports", spec.id)?;

        if get_str(report, "workload", spec.id)? != spec.workload {
            return Err(Failure(format!(
                "{} does not declare workload {}",
                spec.id, spec.workload
            )));
        }
        if get_str(report, "target", spec.id)? != REQUIRED_TARGET {
            return Err(Failure(format!(
                "{} does not declare target {REQUIRED_TARGET}",
                spec.id
            )));
        }
        if get_str(report, "scenario", spec.id)? != spec.scenario {
            return Err(Failure(format!(
                "{} does not declare scenario {}",
                spec.id, spec.scenario
            )));
        }
        if get_str(report, "fixture", spec.id)? != REQUIRED_FIXTURE {
            return Err(Failure(format!(
                "{} does not declare fixture {REQUIRED_FIXTURE}",
                spec.id
            )));
        }
        if get_bool(report, "against_database", spec.id)? != spec.against_database {
            return Err(Failure(format!(
                "{} declares against_database={}, and the accepted plan requires {}",
                spec.id, !spec.against_database, spec.against_database
            )));
        }

        let measurements = get_array(report, "measurements", spec.id)?;
        let measurements = exact_string_set(measurements, &format!("{}.measurements", spec.id))?;
        let expected_measurements: BTreeSet<&str> = spec.measurements.iter().copied().collect();
        if measurements != expected_measurements {
            return Err(Failure(format!(
                "{}.measurements is {measurements:?}, not exactly the required {expected_measurements:?}",
                spec.id
            )));
        }

        let correctness = get_array(report, "correctness", spec.id)?;
        let correctness = exact_string_set(correctness, &format!("{}.correctness", spec.id))?;
        let expected_correctness: BTreeSet<&str> = spec.correctness.iter().copied().collect();
        if correctness != expected_correctness {
            return Err(Failure(format!(
                "{}.correctness is {correctness:?}, not exactly the required {expected_correctness:?}",
                spec.id
            )));
        }
    }
    Ok(())
}

fn validate_matrix(scope: &Value) -> Result<(), Failure> {
    let matrix = get_array(scope, "supported_matrix", "the scope")?;
    let matrix = ordered_strings(matrix, "supported_matrix")?;
    if matrix.as_slice() != REQUIRED_MATRIX {
        return Err(Failure(format!(
            "supported_matrix is {matrix:?}, not exactly {REQUIRED_MATRIX:?}",
        )));
    }
    Ok(())
}

fn validate_execution(scope: &Value) -> Result<(), Failure> {
    let execution = scope
        .get("execution")
        .ok_or_else(|| Failure("the scope declares no execution block".to_owned()))?;
    if get_str(execution, "cargo_profile", "execution")? != "release" {
        return Err(Failure(
            "execution.cargo_profile must be release".to_owned(),
        ));
    }
    if get_str(execution, "numeric_status", "execution")? != "observational" {
        return Err(Failure(
            "execution.numeric_status must remain observational until an accepted budget \
             exists; this campaign invents none"
                .to_owned(),
        ));
    }
    if get_str(execution, "matrix_rule", "execution")? != EXPECTED_MATRIX_RULE {
        return Err(Failure(format!(
            "execution.matrix_rule must state the accepted execution cardinality exactly: \
             {EXPECTED_MATRIX_RULE:?}",
        )));
    }
    Ok(())
}

fn validate_workload_p001(scope: &Value) -> Result<(), Failure> {
    let p001 = scope
        .pointer("/workloads/p001")
        .ok_or_else(|| Failure("the scope declares no P-001 workload".to_owned()))?;
    if get_str(p001, "tasklet", "workloads.p001")? != "no-op" {
        return Err(Failure("workloads.p001.tasklet must be no-op".to_owned()));
    }
    if get_u64(p001, "warmup_attempts", "workloads.p001")? != 16 {
        return Err(Failure(
            "workloads.p001.warmup_attempts must be 16".to_owned(),
        ));
    }
    if get_u64(p001, "measured_attempts", "workloads.p001")? != 256 {
        return Err(Failure(
            "workloads.p001.measured_attempts must be 256".to_owned(),
        ));
    }
    if !get_bool(p001, "fresh_job_parameters_per_attempt", "workloads.p001")? {
        return Err(Failure(
            "workloads.p001.fresh_job_parameters_per_attempt must be true".to_owned(),
        ));
    }
    Ok(())
}

fn validate_workload_p003(scope: &Value) -> Result<(), Failure> {
    let p003 = scope
        .pointer("/workloads/p003")
        .ok_or_else(|| Failure("the scope declares no P-003 workload".to_owned()))?;
    if get_u64(p003, "dataset_rows", "workloads.p003")? != 10_000 {
        return Err(Failure(
            "workloads.p003.dataset_rows must be exactly 10,000".to_owned(),
        ));
    }
    if get_u64(p003, "chunk_size", "workloads.p003")? != 100 {
        return Err(Failure(
            "workloads.p003.chunk_size must be exactly 100".to_owned(),
        ));
    }
    if get_u64(p003, "source_seed", "workloads.p003")? != 102 {
        return Err(Failure(
            "workloads.p003.source_seed must be exactly 102".to_owned(),
        ));
    }
    if get_str(p003, "writer", "workloads.p003")?
        != "test-local enlisted PostgreSQL writer using AtomicSameResource"
    {
        return Err(Failure(
            "workloads.p003.writer must name the accepted enlisted AtomicSameResource writer \
             path exactly"
                .to_owned(),
        ));
    }
    if get_str(p003, "source", "workloads.p003")? != EXPECTED_P003_SOURCE {
        return Err(Failure(format!(
            "workloads.p003.source must name the accepted deterministic dataset exactly: {EXPECTED_P003_SOURCE:?}",
        )));
    }
    Ok(())
}

fn validate_workload_p010(scope: &Value) -> Result<(), Failure> {
    let p010 = scope
        .pointer("/workloads/p010")
        .ok_or_else(|| Failure("the scope declares no P-010 workload".to_owned()))?;
    if get_u64(p010, "partitions", "workloads.p010")? != 100 {
        return Err(Failure(
            "workloads.p010.partitions must be exactly 100".to_owned(),
        ));
    }
    let worker_points = get_array(p010, "worker_points", "workloads.p010")?;
    let worker_points = exact_u64_list(worker_points, "workloads.p010.worker_points")?;
    let expected = [1, 10, u64::from(MAX_PARTITION_WORKERS)];
    if worker_points != expected {
        return Err(Failure(format!(
            "workloads.p010.worker_points is {worker_points:?}, not exactly the sequential \
             fallback, ten workers, and the accepted largest worker budget {expected:?}",
        )));
    }
    if get_u64(p010, "partitions", "workloads.p010")? < u64::from(MAX_PARTITION_WORKERS) {
        return Err(Failure(
            "workloads.p010.partitions must be enough to occupy the largest worker point"
                .to_owned(),
        ));
    }
    if get_str(p010, "largest_worker_point_source", "workloads.p010")?
        != "oxide_batch::MAX_PARTITION_WORKERS"
    {
        return Err(Failure(
            "workloads.p010.largest_worker_point_source must name oxide_batch::MAX_PARTITION_WORKERS"
                .to_owned(),
        ));
    }
    if get_str(p010, "work_per_partition", "workloads.p010")?
        != "one deterministic PostgreSQL business write and one durable partition result"
    {
        return Err(Failure(
            "workloads.p010.work_per_partition must name the accepted fixed unit of work; the \
             accepted performance plan assigns enlisted-writer/AtomicSameResource semantics to \
             P-003 only (docs/engineering/performance-plan.md's workload table names P-010 as \
             \"1/10/100 local partitions/chunks\" with no enlistment claim), so this field must \
             not reintroduce \"enlisted\" for P-010"
                .to_owned(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Fail-closed JSON helpers. Every one of these returns `Err` on a missing,
// non-string, non-numeric, empty, or duplicated element rather than
// silently excluding it, per the reconciliation rule above.
// ---------------------------------------------------------------------

fn get_str<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a str, Failure> {
    match value.get(field) {
        None => Err(Failure(format!("{context} has no {field} field"))),
        Some(Value::String(text)) if !text.is_empty() => Ok(text.as_str()),
        Some(Value::String(_)) => Err(Failure(format!("{context} has an empty {field}"))),
        Some(_) => Err(Failure(format!("{context} has a non-string {field}"))),
    }
}

fn get_bool(value: &Value, field: &str, context: &str) -> Result<bool, Failure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| Failure(format!("{context} has no boolean {field}")))
}

fn get_u64(value: &Value, field: &str, context: &str) -> Result<u64, Failure> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure(format!("{context} has no non-negative integer {field}")))
}

fn get_array<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a [Value], Failure> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| Failure(format!("{context} declares no {field} array")))
}

/// Every element must be a non-empty string, in the order given. A missing,
/// non-string, or empty entry is an error rather than something skipped.
fn ordered_strings<'a>(values: &'a [Value], context: &str) -> Result<Vec<&'a str>, Failure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| match value {
            Value::String(text) if !text.is_empty() => Ok(text.as_str()),
            Value::String(_) => Err(Failure(format!("{context}[{index}] is an empty string"))),
            _ => Err(Failure(format!("{context}[{index}] is not a string"))),
        })
        .collect()
}

/// The exact set an array claims to hold. Rejects a duplicate entry rather
/// than silently deduplicating it: `exact_string_set` and `ordered_strings`
/// disagreeing on length is precisely how a duplicate declares itself.
fn exact_string_set<'a>(values: &'a [Value], context: &str) -> Result<BTreeSet<&'a str>, Failure> {
    let ordered = ordered_strings(values, context)?;
    let set: BTreeSet<&str> = ordered.iter().copied().collect();
    if set.len() != ordered.len() {
        return Err(Failure(format!("{context} contains a duplicate entry")));
    }
    Ok(set)
}

/// Every element must be a non-negative integer, in order.
fn exact_u64_list(values: &[Value], context: &str) -> Result<Vec<u64>, Failure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_u64()
                .ok_or_else(|| Failure(format!("{context}[{index}] is not a non-negative integer")))
        })
        .collect()
}

/// The `id` field of every element, in order. Fails closed: an entry
/// anywhere in `values` with a missing, empty, or non-string `id` stops the
/// whole read, so a malformed record cannot hide behind an unrelated lookup.
fn ids_of<'a>(values: &'a [Value], context: &str) -> Result<Vec<&'a str>, Failure> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| get_str(value, "id", &format!("{context}[{index}]")))
        .collect()
}

fn object_with_id<'a>(values: &'a [Value], context: &str, id: &str) -> Result<&'a Value, Failure> {
    let ids = ids_of(values, context)?;
    let position = ids
        .iter()
        .position(|candidate| *candidate == id)
        .ok_or_else(|| Failure(format!("{context} declares no {id} object")))?;
    Ok(&values[position])
}

// ---------------------------------------------------------------------
// Structured, exact plan-row parsing. No substring probing: a row cell is
// compared to the accepted text exactly, so an obligation added to the row
// fails the comparison the same way one removed from it does.
// ---------------------------------------------------------------------

/// Splits the `| Name | ... |` table row whose first cell is exactly `name`
/// into its trimmed cells, requiring the row to appear exactly once and to
/// have exactly `expected_cells` cells.
///
/// Both requirements are fail-closed on purpose: a `.find()` over duplicated
/// rows would silently ignore obligation drift in every row after the first,
/// and accepting whatever cell count a row happens to have would let an
/// obligation move into (or disappear from) an unread trailing cell. The
/// two-column M5 campaign table (`| Campaign | Required reports |`) passes
/// `expected_cells: 2`; the three-column workload table
/// (`| ID | Workload | Primary pressure |`) passes `expected_cells: 3`.
fn plan_row_cells(plan: &str, name: &str, expected_cells: usize) -> Result<Vec<String>, Failure> {
    let prefix = format!("| {name} |");
    let matches: Vec<&str> = plan
        .lines()
        .filter(|line| line.trim_start().starts_with(&prefix))
        .collect();
    let line = match matches.as_slice() {
        [] => return Err(Failure(format!("the plan has no {name} row"))),
        [line] => *line,
        _ => {
            return Err(Failure(format!(
                "the plan names {name} in {} rows, not exactly one",
                matches.len()
            )));
        }
    };
    let cells: Vec<String> = line
        .trim()
        .trim_start_matches('|')
        .trim_end_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_owned())
        .collect();
    if cells.len() != expected_cells {
        return Err(Failure(format!(
            "the {name} row has {} cell(s), not exactly the expected {expected_cells}",
            cells.len()
        )));
    }
    Ok(cells)
}

/// Every `P-###` token mentioned in `text`, matched on a word boundary so
/// `P-0011` does not register as `P-001`.
fn workload_ids_mentioned(text: &str) -> BTreeSet<&str> {
    let bytes = text.as_bytes();
    let mut ids = BTreeSet::new();
    let mut search_from = 0;
    while let Some(offset) = text[search_from..].find("P-") {
        let start = search_from + offset;
        let digits_start = start + 2;
        let has_three_digits = bytes
            .get(digits_start..digits_start + 3)
            .is_some_and(|digits| digits.iter().all(u8::is_ascii_digit));
        if has_three_digits {
            let end = digits_start + 3;
            let left_boundary_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
            let right_boundary_ok = end >= bytes.len() || !bytes[end].is_ascii_alphanumeric();
            if left_boundary_ok && right_boundary_ok {
                ids.insert(&text[start..end]);
            }
        }
        search_from = start + 2;
    }
    ids
}

// ---------------------------------------------------------------------
// Mutation helpers: locate and edit one report's field inside a cloned
// in-memory scope, for the negative tests above.
// ---------------------------------------------------------------------

fn report_position(scope: &Value, id: &str) -> Result<usize, Failure> {
    scope["reports"]
        .as_array()
        .ok_or_else(|| Failure("reports is not an array".to_owned()))?
        .iter()
        .position(|report| report["id"] == id)
        .ok_or_else(|| Failure(format!("no report named {id} in the fixture")))
}

fn campaign_position(scope: &Value, id: &str) -> Result<usize, Failure> {
    scope["campaigns"]
        .as_array()
        .ok_or_else(|| Failure("campaigns is not an array".to_owned()))?
        .iter()
        .position(|campaign| campaign["id"] == id)
        .ok_or_else(|| Failure(format!("no campaign named {id} in the fixture")))
}

fn report_pointer(scope: &Value, id: &str, field: &str) -> Result<String, Failure> {
    Ok(format!("/reports/{}/{field}", report_position(scope, id)?))
}

fn remove_string(scope: &mut Value, pointer: &str, value: &str) -> Result<(), Failure> {
    scope
        .pointer_mut(pointer)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| Failure(format!("{pointer} does not resolve to an array")))?
        .retain(|entry| entry.as_str() != Some(value));
    Ok(())
}

fn push_string(scope: &mut Value, pointer: &str, value: &str) -> Result<(), Failure> {
    scope
        .pointer_mut(pointer)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| Failure(format!("{pointer} does not resolve to an array")))?
        .push(Value::String(value.to_owned()));
    Ok(())
}

// ---------------------------------------------------------------------
// Disk access.
// ---------------------------------------------------------------------

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

#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}

// ---------------------------------------------------------------------
// Report cardinality and the shared P-003 identity. The scope's flat
// `reports` array is what makes this mechanical: `cargo xtask performance`
// iterates it once per matrix job, not once per campaign row, so the
// performance and reference-workload campaigns naming the same report id
// is what makes them share one execution instead of doubling it.
// ---------------------------------------------------------------------

#[test]
fn p003_reference_workload_is_declared_exactly_once_and_shared_by_both_campaigns()
-> Result<(), Box<dyn Error>> {
    let scope = read_scope()?;
    let reports = get_array(&scope, "reports", "the scope")?;
    let occurrences = reports
        .iter()
        .filter(|report| {
            report.get("id").and_then(Value::as_str) == Some("p003-reference-workload")
        })
        .count();
    assert_eq!(
        occurrences, 1,
        "the scope's flat reports array must declare p003-reference-workload exactly once; \
         cargo xtask performance runs this list once per matrix job, so a second declaration \
         is the one thing that would make the shared report execute twice",
    );

    let campaigns = get_array(&scope, "campaigns", "the scope")?;
    let performance = object_with_id(campaigns, "campaigns", "performance")?;
    let performance_reports =
        ordered_strings(get_array(performance, "reports", "performance")?, "reports")?;
    assert!(
        performance_reports.contains(&"p003-reference-workload"),
        "the performance campaign must declare the shared p003-reference-workload report",
    );
    let reference = object_with_id(campaigns, "campaigns", "reference-workload")?;
    let reference_reports = ordered_strings(
        get_array(reference, "reports", "reference-workload")?,
        "reports",
    )?;
    assert_eq!(
        reference_reports,
        vec!["p003-reference-workload"],
        "the reference-workload campaign must declare exactly the same shared report and \
         nothing else, so it names the one execution rather than a second one",
    );

    // Both rows name the *same* entry in the reports array (there is exactly
    // one, checked above), and that array runs once per matrix point: across
    // the two supported PostgreSQL majors this yields exactly 2 p001, 2 p003,
    // and 2 p010 retained reports in total. P-003 being claimed by two
    // campaign rows must not become 4 executions.
    let matrix = get_array(&scope, "supported_matrix", "the scope")?;
    assert_eq!(
        ordered_strings(matrix, "supported_matrix")?.len(),
        2,
        "the matrix must have exactly two points",
    );
    Ok(())
}

#[test]
fn the_semantic_closure_covers_what_the_campaign_runs() -> Result<(), Box<dyn Error>> {
    let closure: Value = serde_json::from_str(&read_document(
        "tests/fixtures/performance/campaign-semantics.json",
    )?)?;
    let paths = closure
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the closure declares no categories".to_owned()))?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for required in [
        // The code under test.
        "crates/oxide-batch/src",
        "crates/oxide-batch-repository/src",
        // The report and its mechanics.
        "crates/oxide-batch/tests/postgres_performance.rs",
        "crates/oxide-batch/tests/performance",
        // The denominator.
        "tests/fixtures/performance/campaign-scope.json",
        // The verifier, whose verdicts are part of the result.
        "xtask/src/performance.rs",
        // The resolved dependency graph: this campaign measures durations.
        "Cargo.lock",
        // How the dedicated workflow runs it, in release profile.
        ".github/workflows/m5-performance.yml",
        "tests/fixtures/performance/execution-contract.json",
        "tests/fixtures/performance/run-ci-campaign.sh",
        "tests/fixtures/performance/verify-ci-contract.sh",
    ] {
        assert!(
            paths.iter().any(|path| path == required),
            "{required} is not in the campaign's semantic closure, so a change to it would leave \
             retained evidence looking valid when it is evidence of something else",
        );
    }

    assert!(
        !paths.iter().any(|path| path == ".github/workflows/ci.yml"),
        "ci.yml is unrelated to the dedicated performance campaign and must not invalidate its \
         evidence",
    );

    for path in &paths {
        assert!(
            workspace_root().join(path).exists(),
            "{path} is declared as campaign semantics and does not exist, so the producer cannot \
             record its object identity",
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Contract-check exactness. `verify-ci-contract.sh` binds two files —
// `.github/workflows/m5-performance.yml` and `run-ci-campaign.sh` — by exact
// git blob identity, not by the literal presence checks alone. These tests
// drive the real script against an isolated sandbox copy of those files, so a
// mutation proves the checker's actual behaviour rather than one helper's
// return value, and never touches the repository working tree.
// ---------------------------------------------------------------------

#[test]
fn contract_check_passes_on_the_canonical_workflow_and_script() -> Result<(), Box<dyn Error>> {
    assert!(run_performance_contract_check(|_sandbox| Ok(()))?);
    Ok(())
}

#[test]
fn contract_check_fails_on_an_added_trigger() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-performance.yml"),
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
fn contract_check_fails_on_a_job_level_write_permission() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-performance.yml"),
            "runs-on: ubuntu-24.04\n",
            "    permissions:\n      contents: write\n",
        )
    })?;
    assert!(
        !passed,
        "a job-level permission override must fail even though the workflow-level contents: \
         read line is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_widened_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-performance.yml"),
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
fn contract_check_fails_on_a_changed_release_profile() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        let contract = sandbox.join("tests/fixtures/performance/execution-contract.json");
        let source = fs::read_to_string(&contract)?;
        let mutated = source.replace(
            "\"cargo_profile\": \"release\"",
            "\"cargo_profile\": \"debug\"",
        );
        assert_ne!(
            source, mutated,
            "the cargo_profile literal was not found to mutate"
        );
        fs::write(&contract, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a contract that no longer declares release profile must fail even though the workflow \
         and script are unchanged: a debug-build figure is not comparable to anything release \
         planning could use",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_additional_producer_command() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-performance.yml"),
            "run: ./tests/fixtures/performance/run-ci-campaign.sh ${{ matrix.postgres }}\n",
            "      - name: Run something else\n        run: echo \"an extra producer step\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional step after the campaign producer must fail even though the declared \
         producer command is still present unmodified",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_appended_script_command() -> Result<(), Box<dyn Error>> {
    let passed = run_performance_contract_check(|sandbox| {
        append_line(
            &sandbox.join("tests/fixtures/performance/run-ci-campaign.sh"),
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
    let passed = run_performance_contract_check(|sandbox| {
        append_line(
            &sandbox.join(".github/workflows/m5-performance.yml"),
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

/// Copies the real workflow, script, and contract into an isolated sandbox,
/// applies `mutate` to that sandbox, then runs the real `verify-ci-contract.sh`
/// against the (possibly mutated) copy and reports whether it exited zero.
///
/// The sandbox mirrors the relative layout the script assumes
/// (`tests/fixtures/performance/execution-contract.json` beside its working
/// directory), so the check runs exactly as CI runs it, and `mutate` can never
/// affect the real repository files.
fn run_performance_contract_check(
    mutate: impl FnOnce(&Path) -> Result<(), Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    let root = workspace_root();
    let sandbox = Sandbox::new("performance-contract-check")?;

    let workflow_dir = sandbox.path().join(".github/workflows");
    fs::create_dir_all(&workflow_dir)?;
    fs::copy(
        root.join(".github/workflows/m5-performance.yml"),
        workflow_dir.join("m5-performance.yml"),
    )?;

    let fixture_dir = sandbox.path().join("tests/fixtures/performance");
    fs::create_dir_all(&fixture_dir)?;
    for name in [
        "execution-contract.json",
        "run-ci-campaign.sh",
        "verify-ci-contract.sh",
    ] {
        fs::copy(
            root.join("tests/fixtures/performance").join(name),
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
        ".github/workflows/m5-performance.yml",
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
