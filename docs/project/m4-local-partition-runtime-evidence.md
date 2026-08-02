# M4 Bounded Local-Partition Runtime Evidence

**State:** Partial implementation (unreleased)

**Issue:** [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)

**Date:** 2026-08-03

This record covers the tasklet-only bounded local execution slice accepted by
the [M4 local-scale contract](../architecture/local-scale.md). It reuses the
format-3 manifest, durable partition repository, and atomic deterministic
aggregation delivered by issues #88, #89, and #90. It adds no remote worker,
scheduler, lease, fencing, nested split, dynamic partitioning, or distributed
protocol.

## Implemented boundary

- `PartitionPlanFactory` is invoked only when no durable plan exists. Its
  bounded output commits before worker creation. A committed plan, including a
  plan whose commit acknowledgement was lost but whose exact rows are visible,
  is reused without invoking the partitioner again.
- `PartitionTaskletFactory` is invoked separately for each assigned child with
  an owned key, ordinal, and bounded context. Every call must construct an
  independently owned `TaskletStep`; shared mutable component state is outside
  this contract.
- Each durable worker logical identity is deterministically derived from the
  manager logical ID and byte-exact partition key. Different partitions do not
  collide with the schema's per-execution step identity constraints, while a
  retry of the same partition retains its ordinary start-control lineage.
- The manager owns one finite `buffer_unordered` scope. It creates no detached
  tasks and drains every completed, stopped, cancelled, failed, panicked, or
  ambiguous child before returning. Factory and tasklet panics are caught at
  framework boundaries and become durable failures.
- The manifest limits active workers to `1..=64` and partitions to
  `1..=1024`. Launch additionally rejects a repository whose actual connection
  capacity is below the manifest pool budget. The pending vector, worker
  futures, and repository use are finite.
- Stop is observed before factory invocation and throughout tasklet execution.
  The default sibling policy requests cooperative stop after the first failed
  or ambiguous child, while retaining and joining every already-owned future.
- Restart creates a distinct parent execution, carries forward completed
  partitions byte-for-byte, and resets only retryable incomplete partitions.
  An unrecovered `UNKNOWN` child blocks reassignment, aggregation, and parent
  completion. Explicit audited recovery of the prior job is required before a
  restart attempt can be created.
- Final status, exit status, counters, and failure are read from
  `RepositoryUnitOfWork::aggregate_step_partitions`; the runtime contains no
  second aggregation policy. An ambiguous aggregate commit is resolved only by
  a fresh idempotent repository call and durable inspection.

The current worker slice accepts tasklet components only. Local chunk-worker
composition, dynamic partitioning, nested partition/split graphs, scheduling,
and remote execution are not inferred from this evidence.

## Named executable evidence

| Scenario | Evidence |
| --- | --- |
| `concurrency=1` and parallel durable equivalence | [`concurrency_one_matches_parallel_durable_observations`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Completion-order independent failure | [`completion_order_does_not_change_failed_aggregate`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Partial success and failure restart | [`completed_partition_is_not_rerun_on_restart`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Panic to durable failure | [`child_panic_is_durable_failure`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Pre-start cancellation skips factory | [`cancellation_before_pending_worker_skips_its_factory`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Parent stop cancels and joins | [`parent_stop_cancels_and_joins_active_workers`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Unknown child blocks aggregation | [`unknown_partition_blocks_parent_aggregation`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Ambiguous aggregate commit inspection | [`aggregate_commit_unknown_is_resolved_by_durable_inspection`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Worker ceiling | [`worker_concurrency_never_exceeds_manifest_bound`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Repository connection ceiling | [`repository_capacity_is_revalidated_before_launch`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| Repeated task-scope drain | [`repeated_partition_runs_leave_no_active_worker`](../../crates/oxide-batch/tests/local_partition_runtime.rs) |
| PostgreSQL process kill and completed-child reuse | [`committed_partition_is_reused_after_process_kill_and_recovery`](../../crates/oxide-batch/tests/postgres_local_partition_crash_recovery.rs) |
| Runnable PostgreSQL operations flow | [`postgres_local_partition.rs`](../../crates/oxide-batch/examples/postgres_local_partition.rs) |

The PostgreSQL repository and process-kill targets run on PostgreSQL 15 and 18
in CI. The checked-in example exposes `migrate`, `launch`, `inspect`,
`interrupt`, `recover`, and `restart` so the same boundary can be exercised by
an operator without treating the generic CLI as a definition loader.

## Claim boundary

`SCALE-LOCALPART-001` remains unreleased `Partial`; it is not `Verified`.
Issue #80 and M4 are not closed by code existence. The M4 exit review still
owns the final process-kill, resource-bound, cancellation, PostgreSQL matrix,
and soak-evidence judgment, including CI results for both supported database
edges.
