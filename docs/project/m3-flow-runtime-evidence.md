# M3 Durable Flow Runtime Evidence

**State:** Implementation evidence

**Issue:** [#64](https://github.com/luceat-lux-vestra/oxide-batch/issues/64)

**Date:** 2026-08-01

This record maps the M3 basic-flow implementation to the accepted
[basic-flow contract](../architecture/basic-flow.md). It covers the finite
acyclic M3 slice: sequential and conditional tasklet/chunk steps, typed
deciders, durable transition decisions, restart traversal, start limits, and
`allow_start_if_complete`. Split, nested flow/job, non-restartable policy, and
the complete M7 flow population remain outside this claim.

## Implemented boundary

- `FlowJob` binds every node of a canonical format-2
  `CompiledExecutionPlan` to exactly one tasklet, chunk tasklet, or decider.
- `FlowLauncher` commits a step terminal result before appending its selected
  transition, and commits the transition before starting its target.
- `TaskletOutcome::CompletedWith` persists a bounded custom `ExitStatus`
  without allowing it to forge `BatchStatus`.
- `JobExecutionDecider` receives immutable, sensitivity-aware identity,
  parameters, and preceding durable step state. Synchronous and asynchronous
  error/panic boundaries become typed, redacted job failures and append no
  successful decision.
- In-memory and PostgreSQL units of work append and read ordered
  `FlowDecision` rows, find restart-reusable decisions, and reconstruct the
  latest logical step across attempts.
- Before append, both repositories verify the execution fingerprint and the
  selected source, outcome, and target against the exact persisted format-2
  manifest. They also validate source-step lineage and any reused prior
  decision.
- Start creation atomically counts `(job_instance, step_logical_id)` history.
  Entering `STARTING`, including before-listener failure, consumes the finite
  limit. PostgreSQL serializes this decision by locking the job-instance row.
- Restart skips a completed step by default, records
  `CompletedStepReuse`, reuses a matching decider result, and invokes the
  decider again when an explicitly rerun preceding step changes its durable
  input digest.

Unknown commit remains fail-closed: no outgoing transition is selected and the
job remains `UNKNOWN`. A cooperative stop or `Stop` terminal persists
`STOPPED`; `Fail` persists `FAILED`; `Complete` persists `COMPLETED`.

## Named scenario evidence

| Ledger row | Scenario | Evidence |
| --- | --- | --- |
| `FLOW-SEQUENCE-001` | `exit_status_selects_most_specific_transition` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `FLOW-SEQUENCE-001` | `ambiguous_transition_is_rejected` | `equally_specific_overlapping_patterns_are_rejected` in [`tests/plan.rs`](../../crates/oxide-batch/tests/plan.rs) |
| `FLOW-SEQUENCE-001` | `unmapped_exit_fails_job` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `FLOW-SEQUENCE-001` | `committed_transition_survives_restart` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `FLOW-DECIDER-001` | `decider_result_and_target_commit_together` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) and repository manifest validation |
| `FLOW-DECIDER-001` | `committed_decider_is_not_reinvoked` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `FLOW-DECIDER-001` | `decider_input_change_records_new_path` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `FLOW-DECIDER-001` | `decider_panic_is_redacted_failure` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `STEP-STARTLIMIT-001` | `start_limit_is_atomic_per_instance_and_logical_step` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `STEP-STARTLIMIT-001` | `failed_start_consumes_limit` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `STEP-STARTLIMIT-001` | `completed_step_is_skipped_by_default` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |
| `STEP-STARTLIMIT-001` | `allow_start_if_complete_reruns_on_restart_path` | [`tests/flow.rs`](../../crates/oxide-batch/tests/flow.rs) |

`stop_fail_and_end_terminals_persist_lifecycle_status` covers the three M3
terminals. [`tests/postgres_flow.rs`](../../crates/oxide-batch/tests/postgres_flow.rs)
covers persisted custom-exit selection plus sequential, decider, failed-target,
completed-step reuse, decider reuse, successful end, and a `Stop` terminal
across PostgreSQL restart. It also covers `allow_start_if_complete` rerun and
failed-start limit exhaustion against PostgreSQL.
`flow_launcher_executes_a_bound_chunk_step` in
[`tests/chunk_runtime.rs`](../../crates/oxide-batch/tests/chunk_runtime.rs)
pins chunk execution and committed counters through the same flow launcher.

## Validation and remaining release evidence

The following local baseline passed on 2026-08-01:

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

The PostgreSQL tests are environment-gated by
`OXIDEBATCH_POSTGRES_TEST_URL`. On 2026-08-01, schema-2 migration and all five
`postgres_flow` tests passed serially against Homebrew PostgreSQL 18.4 on
macOS arm64. They also run against migrated PostgreSQL 15 and 18 databases in
the repository CI matrix; without the variable they explicitly self-skip. The
M3 exit workstream retains those axes and adds separate-process step-result and
decision-commit crash evidence in
[`postgres_flow_crash_recovery.rs`](../../crates/oxide-batch/tests/postgres_flow_crash_recovery.rs).

Post-commit flow telemetry is implemented by `FlowEventSink` and pinned by
`flow_event_sink_panic_is_non_authoritative` plus the event-order assertion in
`exit_status_selects_most_specific_transition`. Released-version
verification, M7 advanced flow coverage, and operator-service crash recovery
remain outside this evidence. The joined milestone result is recorded in the
[M3 exit evidence](m3-exit-evidence.md).
