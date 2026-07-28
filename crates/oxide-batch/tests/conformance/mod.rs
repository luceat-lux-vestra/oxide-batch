//! Stable compatibility-matrix identifiers and executable scenario names.

use crate::support::{ScenarioId, ScenarioIdError};

/// Matrix rows known to the harness.
///
/// Keeping row IDs beside executable names makes missing or renamed scenarios
/// visible in ordinary review. Status remains authoritative in the matrix.
pub const MATRIX_SCENARIOS: &[(&str, &str)] = &[
    ("VS-LAUNCH-001", "first_launch_creates_execution_graph"),
    (
        "JOB-INSTANCE-001",
        "job_instance_same_identifying_parameters",
    ),
    ("JOB-EXEC-001", "restart_creates_new_execution"),
    ("JOB-COMPLETE-001", "completed_instance_rejects_launch"),
    ("STEP-STATUS-001", "exit_status_does_not_forge_batch_status"),
    ("CHUNK-COMMIT-001", "committed_chunk_advances_checkpoint"),
    ("CHUNK-ROLLBACK-001", "rolled_back_chunk_replays"),
    ("RESTART-001", "restart_resumes_latest_committed_checkpoint"),
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
