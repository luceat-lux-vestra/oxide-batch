# Compatibility and Conformance Matrix

**State:** Template

No row is marked supported until an executable scenario passes against a
released OxideBatch version.

| ID | Capability | Spring reference | Target level | OxideBatch version | Scenario | Status | Known difference |
| --- | --- | --- | --- | --- | --- | --- | --- |
| JOB-INSTANCE-001 | Identifying parameters select job instance | Job domain/configuration | Behavioral | — | `job_instance_same_identifying_parameters` | Planned M1 | Canonical Rust types |
| JOB-EXEC-001 | Restart creates a new execution for same instance | Job restartability | Behavioral | — | `restart_creates_new_execution` | Planned M1 | API differs |
| JOB-COMPLETE-001 | Completed instance rejects repeat launch | Job restartability | Behavioral | — | `completed_instance_rejects_launch` | Planned M1 | Error type differs |
| STEP-STATUS-001 | Batch status and exit status are distinct | Step domain | Semantic | — | `exit_status_does_not_forge_batch_status` | Planned M1 | — |
| CHUNK-COMMIT-001 | Chunk commit advances checkpoint | Chunk processing | Behavioral | — | `committed_chunk_advances_checkpoint` | Planned M2 | Own schema |
| CHUNK-ROLLBACK-001 | Failed chunk does not advance checkpoint | Chunk processing | Behavioral | — | `rolled_back_chunk_replays` | Planned M2 | Delivery scope explicit |
| RECOVERY-001 | Orphaned running execution needs recovery | Advanced metadata | Operational | — | `orphan_requires_operator_decision` | Planned M2/M4 | Operator API differs |
| RETRY-001 | Retry is bounded and counted | Retry | Behavioral | — | `retry_limit_persists_across_restart` | Planned M3 | Error classification differs |
| SKIP-001 | Skipped items have durable counts | Step fault tolerance | Behavioral | — | `skip_count_commits_with_chunk` | Planned M3 | — |
| FLOW-001 | Exit outcome selects next step | Step flow | Behavioral | — | `exit_status_selects_transition` | Planned M3 | Rust-native definition |
| OBS-001 | Job and step execution are observable | Observability | Operational | — | `telemetry_correlates_execution` | Planned M4 | OpenTelemetry mapping |

## Status values

- Planned
- Implemented
- Verified
- Partial
- Unsupported
- Deferred

## Row rules

- Link an exact official reference section and the local executable scenario.
- Record the Spring Batch version actually observed when behavior is compared.
- A known difference must state whether it changes semantic, behavioral,
  operational, schema, or API compatibility.
- Regression of a Verified row is a compatibility defect and potential release
  blocker.
