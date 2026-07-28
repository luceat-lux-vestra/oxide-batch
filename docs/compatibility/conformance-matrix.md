# Compatibility and Conformance Matrix

**State:** Template

No row is marked supported until an executable scenario passes against a
released OxideBatch version.

| ID | Capability | Spring reference | Target level | OxideBatch version | Scenario | Status | Known difference |
| --- | --- | --- | --- | --- | --- | --- | --- |
| VS-LAUNCH-001 | First launch creates the execution graph | Job domain | Behavioral | Unreleased | `first_launch_creates_execution_graph` | Implemented | Canonical Rust types |
| JOB-INSTANCE-001 | Identifying parameters select job instance | Job domain/configuration | Behavioral | Unreleased | `job_instance_same_identifying_parameters` | Implemented | Canonical Rust types |
| JOB-EXEC-001 | Restart creates a new execution for same instance | Job restartability | Behavioral | Unreleased | `restart_creates_new_execution` | Implemented | API differs |
| JOB-COMPLETE-001 | Completed instance rejects repeat launch | Job restartability | Behavioral | Unreleased | `completed_instance_rejects_launch` | Implemented | Error type differs |
| TASKLET-001 | A single tasklet step drives job and step lifecycle | Step/tasklet execution | Behavioral | Unreleased | `successful_launch_borrows_context_and_persists_final_graph` | Implemented | Async-first Rust contract |
| TASKLET-STOP-001 | Tasklet stop is cooperative and persisted | Step/tasklet execution | Behavioral | Unreleased | `cooperative_stop_during_async_work_is_persisted` | Implemented | Explicit stop token |
| TASKLET-PANIC-001 | User panic is classified as execution failure | Step/tasklet execution | Semantic | Unreleased | `tasklet_panic_is_classified_and_runtime_remains_usable` | Implemented | Panic payload is redacted |
| STEP-STATUS-001 | Batch status and exit status are distinct | Step domain | Semantic | Unreleased | `exit_status_does_not_forge_batch_status` | Implemented | — |
| CHUNK-COMMIT-001 | Chunk commit advances checkpoint | Chunk processing | Behavioral | — | `committed_chunk_advances_checkpoint` | Planned M2 | Own schema |
| CHUNK-ROLLBACK-001 | Failed chunk does not advance checkpoint | Chunk processing | Behavioral | — | `rolled_back_chunk_replays` | Planned M2 | Delivery scope explicit |
| RESTART-001 | Restart resumes at the latest committed checkpoint | Job restartability | Behavioral | — | `restart_resumes_latest_committed_checkpoint` | Planned M2 | Own context format |
| JOB-CONCURRENCY-001 | Concurrent launches create one job instance | Job repository | Behavioral | Unreleased | `concurrent_launch_creates_single_instance` | Partial | In-memory optimistic commit implemented; PostgreSQL locking remains M2 |
| RECOVERY-001 | Orphaned running execution needs recovery | Advanced metadata | Operational | — | `orphan_requires_operator_decision` | Planned M2/M4 | Operator API differs |
| OBS-INSPECT-001 | Execution inspection redacts record contents | Metadata/operations | Operational | Unreleased | `inspection_redacts_record_contents` | Partial | M1 in-memory contract implemented; durable schema remains M2 |
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
