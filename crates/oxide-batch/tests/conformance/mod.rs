//! Stable first-vertical-slice identifiers and executable scenario names.

use crate::support::{ScenarioId, ScenarioIdError};

/// The acceptance criteria of the first vertical slice, with the scenario that
/// observes each one.
///
/// These identifiers come from
/// [the first vertical slice](../../../../docs/product/first-vertical-slice.md)
/// and are **not** feature-ledger rows. The ledger's own coverage is
/// reconciled by the M5 conformance campaign, which is keyed by ledger row
/// identifier and lives in
/// `tests/fixtures/conformance/accepted-scope.json`.
///
/// Keeping identifiers beside executable names makes missing or renamed
/// scenarios visible in ordinary review.
pub const MATRIX_SCENARIOS: &[(&str, &str)] = &[
    ("VS-LAUNCH-001", "first_launch_creates_execution_graph"),
    (
        "JOB-INSTANCE-001",
        "job_instance_same_identifying_parameters",
    ),
    ("JOB-EXEC-001", "restart_creates_new_execution"),
    ("JOB-COMPLETE-001", "completed_instance_rejects_launch"),
    (
        "TASKLET-001",
        "successful_launch_borrows_context_and_persists_final_graph",
    ),
    (
        "TASKLET-STOP-001",
        "cooperative_stop_during_async_work_is_persisted",
    ),
    (
        "TASKLET-PANIC-001",
        "tasklet_panic_is_classified_and_runtime_remains_usable",
    ),
    (
        "LISTENER-ORDER-001",
        "listeners_nest_and_reverse_after_order",
    ),
    (
        "LISTENER-FAIL-001",
        "after_listener_failure_retains_original_outcome_and_work",
    ),
    ("STEP-STATUS-001", "exit_status_does_not_forge_batch_status"),
    ("CHUNK-COMMIT-001", "committed_chunk_advances_checkpoint"),
    ("CHUNK-ROLLBACK-001", "crash_before_commit_replays_chunk"),
    ("RESTART-001", "crash_after_commit_does_not_replay_chunk"),
    (
        "JOB-CONCURRENCY-001",
        "concurrent_launch_creates_single_instance",
    ),
    ("RECOVERY-001", "orphan_requires_operator_decision"),
    ("OBS-INSPECT-001", "inspection_redacts_record_contents"),
    ("RETRY-001", "retry_limit_persists_across_restart"),
    ("SKIP-001", "skip_count_commits_with_chunk"),
    ("FLOW-001", "exit_status_selects_transition"),
    ("OBS-001", "telemetry_correlates_execution"),
];

/// Validates a matrix row identifier for use in a report.
///
/// # Errors
///
/// Returns [`ScenarioIdError`] for a malformed identifier.
pub fn scenario_id(value: &str) -> Result<ScenarioId, ScenarioIdError> {
    ScenarioId::new(value)
}
