# First Vertical Slice

**State:** Accepted

**Target milestone:** M1 design, M2 durable implementation

## Scenario

Import a deterministic list of input records, transform each record, and write
results to PostgreSQL in chunks. Deliberately terminate the worker during the
second chunk, inspect execution metadata, restart with the same identifying
parameters, and complete without replaying the first committed chunk.

## Why this slice

It crosses the framework's highest-risk boundaries: domain identity, lifecycle
state, user component contracts, transaction enlistment, execution context,
checkpointing, crash recovery, operator visibility, and telemetry.

## Acceptance criteria

| ID | Criterion | Executable scenario | Gate |
| --- | --- | --- | --- |
| VS-LAUNCH-001 | The first launch creates one job instance, job execution, and step execution. | `first_launch_creates_execution_graph` | M1 |
| JOB-INSTANCE-001 | Reusing the same identifying parameters selects the same job instance. | `job_instance_same_identifying_parameters` | M1 |
| JOB-EXEC-001 | Each retry or restart attempt has a distinct execution identity. | `restart_creates_new_execution` | M1 contract, M2 durable verification |
| CHUNK-COMMIT-001 | A committed chunk advances its checkpoint and counters atomically. | `committed_chunk_advances_checkpoint` | M2 |
| CHUNK-ROLLBACK-001 | A crash before commit replays only work allowed by the documented delivery guarantee. | `rolled_back_chunk_replays` | M2 |
| RESTART-001 | A restart resumes from the latest committed checkpoint. | `restart_resumes_latest_committed_checkpoint` | M2 |
| JOB-COMPLETE-001 | Launching an already completed instance is rejected. | `completed_instance_rejects_launch` | M1 |
| JOB-CONCURRENCY-001 | Concurrent launch attempts cannot create duplicate job instances. | `concurrent_launch_creates_single_instance` | M2 |
| OBS-INSPECT-001 | Status, exit status, counts, timestamps, and failure summaries are inspectable without exposing record contents. | `inspection_redacts_record_contents` | M1 contract, M2 durable verification |
| OBS-001 | Structured events identify the job, instance, execution, step, and attempt. | `telemetry_correlates_execution` | M1 event contract, M4 exporter verification |

## Required failure injection

Durable counters below are the values visible after the transaction ends. Work
performed only in memory during a rolled-back chunk is not reflected in them.

| Injection and scenario | Expected metadata | Replay and counters | Outcome and operator action |
| --- | --- | --- | --- |
| Reader fails before returning an item: `reader_failure_preserves_checkpoint` | Latest committed checkpoint and context are unchanged. | The current position is eligible on restart; durable item/write counts do not advance. | Execution becomes `FAILED`; restart is allowed after the input fault is corrected. |
| Processor fails for a selected item: `processor_failure_rolls_back_chunk` | The open chunk has no durable metadata changes. | The whole uncommitted chunk is replayed; durable chunk counters do not advance. | Execution becomes `FAILED`; diagnose the typed processor error and restart. |
| Writer fails before or during database work: `writer_failure_rolls_back_business_and_checkpoint` | Business writes, checkpoint, context, counters, and optimistic version roll back together. | The whole uncommitted chunk is replayed; no durable write count advances. | Execution becomes `FAILED`; correct the writer/database fault and restart. |
| Worker exits immediately before commit: `crash_before_commit_replays_chunk` | Only earlier commits remain; the interrupted execution can appear `STARTED` until recovery. | The current chunk is replayed from the previous checkpoint; its counters did not commit. | Recovery classifies the orphan before creating a restart execution. |
| Worker exits immediately after commit: `crash_after_commit_does_not_replay_chunk` | The new checkpoint, context, counters, and business writes are durable even if acknowledgement was not observed. | The committed chunk is not replayed. | Recovery uses durable metadata, records the orphan decision, and resumes after the committed checkpoint. |
| Optimistic-lock conflict: `optimistic_conflict_has_one_winner` | Exactly one version update commits; the losing transaction leaves no metadata or business change. | Losing work is replayable from its prior checkpoint; its counters do not advance. | The loser fails with a typed conflict; retry/restart uses the winning version. |
| Before/after listener fails: `listener_failure_preserves_committed_work` | A before-listener prevents user work; an after-listener cannot undo earlier committed chunks. Original and listener failures remain in diagnostic context. | No uncommitted work or counters survive; earlier committed chunks are not replayed. | The enclosing execution becomes `FAILED`; restart follows the retained checkpoint. |
| Stop arrives during a chunk: `stop_during_chunk_uses_commit_boundary` | The last completed checkpoint remains authoritative; no partial metadata is exposed. | An uncommitted chunk is replayable and contributes no durable counters. | Execution reaches `STOPPED` cooperatively; an explicit restart creates a new execution. |
| Database disconnect during commit: `disconnect_during_commit_never_guesses_outcome` | A proven rollback leaves both resources unchanged; an ambiguous outcome is recorded as `UNKNOWN` rather than inferred. | Replay is allowed only after durable metadata establishes the commit outcome. | A proven rollback fails normally; ambiguity requires an audited operator recovery decision. |

## M1/M2 boundary

M1 owns the domain, identity, lifecycle, in-memory repository, event, and
redaction contracts used by these scenarios. M2 supplies PostgreSQL metadata,
chunk transactions, durable checkpoints, crash recovery, and the remaining
integration evidence. M1 APIs must keep the repository and unit-of-work ports
needed by the accepted M2 atomicity contract.

## Measurements

The slice records per-job and per-step duration, committed chunks, processed and
written item counts, rollback count, checkpoint serialization size, repository
operation latency, and restart recovery time. Identifiers use bounded internal
IDs; job parameters, execution context, record contents, and error payloads are
never metric labels.

## Non-goals

Retry/skip policy breadth, conditional flow, distributed execution, and maximum
throughput are not required in the first slice. The slice establishes
correctness boundaries before those features are layered on.
