# M7 Start-Control Completion Evidence

**Issue:** #611  
**Parent:** #199  
**Ledger row:** `STEP-STARTLIMIT-001`  
**Scope:** M7 completion of the accepted M3 `StartLimit` and `allow_start_if_complete` semantics.

## Authority preserved

This slice does not introduce a second start-control model. The M3 contract in
`docs/architecture/basic-flow.md` remains authoritative:

- `StartLimit` is a nonzero `u32`, defaulting to `u32::MAX`;
- accounting is per job instance and stable logical `NodeId`;
- creating a step execution in `STARTING` consumes one start;
- the repository checks the limit and creates the step in one transaction;
- a completed step with `allow_start_if_complete = false` is reused on restart;
- a completed step with `allow_start_if_complete = true` requires real re-entry and therefore a new start;
- the controls do not turn a completed or abandoned job instance into a restartable one.

Gate E additionally requires M7 composition not to reset or shadow those counters.

## M7 execution-path audit

| Logical-step path | Start-control authority | Evidence |
| --- | --- | --- |
| ordinary top-level step | `run_scope` passes the compiled `StartControls` to atomic `create_flow_step_execution`; completed/allow=false is reused before creation | existing `flow.rs` M3 tests and PostgreSQL tests |
| nested flow | compiled nested scope keeps the inner stable `NodeId`; the same `run_scope` path is used | `nested_flow_completed_step_reuse_does_not_consume_start`, `nested_flow_required_reentry_uses_same_start_counter` |
| M7 split subflow | each branch uses a compiled flow scope and therefore the same logical-step path | `split_subflow_reentry_obeys_embedded_start_limit` |
| legacy linear split branch | branch step uses its declared `StartControls` and stable step `NodeId` | existing split runtime plus the common repository start-limit authority |
| custom leaf | custom declaration carries `StartControls`; runtime reuses completed/allow=false state or performs the same atomic create before invoking the handler | `custom_leaf_required_reentry_obeys_start_limit_before_handler` |
| partition manager | manager `StartControls` are passed to atomic step creation; exhaustion is normalized to `FlowFailure::StartLimitExceeded` | `partition_manager_start_limit_rejects_restart_before_worker_work` |
| partition worker | worker declaration supplies the controls; worker logical identity is deterministically derived from manager `NodeId` + partition key and remains stable across restart | `partition_worker_retry_obeys_stable_partition_start_limit` |
| repeat-wrapped step | repeat is entered only after the owning step execution has passed the same create/start guard; repeat does not own a second start counter | existing repeat/restart suites plus this path audit |
| nested job | a nested-job node owns a child job lifecycle, not a synthetic parent step execution; child steps enforce their own definition controls | reviewed N/A for parent-node start controls |
| split/join structural nodes | structural routing nodes do not enter `STARTING` as logical steps | reviewed N/A |

## Partition semantics

The accepted M4 local-partition contract remains authoritative. A completed
partition is copied forward and is not rerun. `allow_start_if_complete` on the
worker declaration therefore does not override completed-partition reuse.
A failed, stopped, or unfinished partition is retried as a new worker attempt,
and that retry is subject to the worker's ordinary M3 start controls.

Before #611, a partition-manager or partition-worker start-limit rejection
could escape the partition runtime as a raw repository/runtime error. In the
worker case this occurred after the restarted parent had entered `STARTED`.
The #611 runtime correction converts exhaustion to the same bounded
`FlowExecutionOutcome::Failed(FlowFailure::StartLimitExceeded)` used by other
flow-step paths and commits the active partition parent to `FAILED`. No
synthetic worker execution is created when the limit has already been
exhausted.

## Restart-policy interaction

#606 remains authoritative for `RestartPolicy`.

- completed + `allow_start_if_complete = false`: reuse is allowed without a
  false non-restartable rejection or additional start;
- actual re-entry + `RestartPolicy::NotRestartable`: the non-restartable guard
  rejects before step creation/user work;
- otherwise, a permitted re-entry reaches the existing atomic start-limit
  check.

The existing nested non-restartable test now uses a finite start limit as an
explicit precedence fixture.

## PostgreSQL / crash evidence disposition

#611 adds no schema, counter, commit protocol, or newly durable state boundary.
Both in-memory and PostgreSQL repositories already implement the same
`create_flow_step_execution` atomic check/create contract. Existing PostgreSQL
evidence includes:

- `postgres_start_controls_survive_restart`;
- `postgres_failed_start_consumes_limit`.

The M7 PG15/18 process-kill requirement is therefore **reviewed N/A for a new
#611 durability boundary**: this slice composes already-durable M3/M4
operations and corrects runtime outcome normalization after an existing
repository rejection. #200 remains responsible for final M7-wide evidence
aggregation and release disposition.

## Result

On the #611 candidate, every accepted M7 path that actually creates a logical
step either reuses completed state before creation or reaches the same stable
logical-identity, atomic start-limit authority. Structural nodes do not acquire
a parallel start-counter model.
