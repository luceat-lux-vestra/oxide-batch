# M4 Durable Partition Repository Evidence

**State:** Partial implementation (unreleased)

**Issue:** [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)

**Date:** 2026-08-02

This record covers the repository and aggregation slices of the accepted M4
bounded local-scale contract: validated partition-plan values, atomic schema-3
plan creation, byte-ordered reads, worker assignment, terminal result
publication, optimistic conflict handling, and atomic parent aggregation. It
does not claim that the format-3 runtime launches partition workers.

## Implemented boundary

- `PartitionKey` enforces the `1..=128` UTF-8 byte bound without normalizing
  the byte-exact identity. `PartitionPlanEntry` rejects execution-context
  envelopes above the schema-3 `4 KiB` ceiling before persistence.
- `RepositoryUnitOfWork::create_step_partition_plan` validates `1..=1024`
  entries and duplicate keys, locks the parent in PostgreSQL, and inserts the
  complete `STARTING` plan in one transaction. Assignment in that same unit of
  work is rejected so no worker can start before the plan commits.
- Plan reads return every partition in `partition_key` byte order. A persisted
  plan cannot be replaced on restart, which gives the future runtime the
  fail-closed path for reusing it without invoking the partitioner again.
- Worker assignment is versioned. New `STARTING` rows and restart-eligible
  `FAILED`/`STOPPED` rows may move to `STARTED`; a retry replaces the prior
  worker reference and clears its exit status and counters before work. The
  worker attempt must exist under the same job execution, cannot be the parent,
  and cannot be assigned to another partition.
- Terminal results accept only `COMPLETED`, `FAILED`, `STOPPED`, or `UNKNOWN`
  and persist exit status and counters under compare-and-swap. A completed
  partition cannot be assigned again.
- `aggregate_step_partitions` sorts independently supplied snapshots by their
  byte-exact keys, applies `UNKNOWN > FAILED > STOPPED > COMPLETED`, selects the
  first matching exit status, and checks every counter sum. Active children,
  duplicate keys, empty/oversized plans, and overflow fail without an
  aggregate.
- `RepositoryUnitOfWork::aggregate_step_partitions` locks/reads the complete
  durable plan and publishes status, exit status, counters, failure metadata,
  terminal timestamp, and version with the parent step in the same transaction.
  Rolling back the unit leaves the parent unchanged.
- In-memory and PostgreSQL adapters implement the same portable repository
  contract. PostgreSQL verifies the stored partition-context checksum before
  returning runtime state; the explorer continues to expose only redacted
  context descriptors.
- Retention accounting and deletion now include in-memory partition rows in
  the same child-before-parent order already required by schema 3.

## Named executable evidence

| Scenario | Evidence |
| --- | --- |
| Public key/context/result bounds | [`partition_key_and_context_bounds_fail_before_persistence`](../../crates/oxide-batch/src/partition.rs), [`partition_result_accepts_only_runtime_terminal_outcomes`](../../crates/oxide-batch/src/partition.rs) |
| Plan-before-worker transaction boundary | [`partition_plan_commits_before_any_worker_starts`](../../crates/oxide-batch/tests/contract/mod.rs) |
| Deterministic aggregation order and severity | [`aggregation_is_deterministic_in_partition_key_order`](../../crates/oxide-batch/src/partition.rs) |
| Parent/aggregate transaction boundary | [`partition_aggregation_commits_with_parent_terminal_state`](../../crates/oxide-batch/tests/contract/mod.rs) |
| Stale writer loses CAS | [`partition_plan_commits_before_any_worker_starts`](../../crates/oxide-batch/tests/contract/mod.rs) |
| Persisted plan and completed-result reuse | [`partition_plan_commits_before_any_worker_starts`](../../crates/oxide-batch/tests/contract/mod.rs) |
| Duplicate-key atomic rejection | [`duplicate_partition_key_is_rejected`](../../crates/oxide-batch/tests/contract/mod.rs) |
| In-memory shared adapter run | [`shared_repository_contract_runs_against_a_test_adapter`](../../crates/oxide-batch/tests/harness.rs) |
| PostgreSQL shared adapter run | [`shared_repository_contract_passes_on_postgres`](../../crates/oxide-batch/tests/postgres_repository.rs) |

The PostgreSQL contract is environment-gated. A local run without
`OXIDEBATCH_POSTGRES_TEST_URL` proves compilation but reports the database case
as skipped; required PostgreSQL 15/18 execution remains CI and M4 exit evidence.

## Remaining issue #80 boundary

- partition component factories plus explicit shared-component concurrent-use
  validation (tasklet split factories are recorded in the
  [parallel-split runtime evidence](m4-parallel-split-evidence.md));
- owned bounded partition-worker execution, cancellation, panic isolation, and
  sibling failure policy;
- runtime reuse of the committed plan, including `UNKNOWN` partition blocking;
- PostgreSQL process-kill, sequential-equivalence, cancellation, contention,
  memory/connection/task ceiling, and soak/leak evidence.

`SCALE-LOCALPART-001` remains unreleased `Partial`. `SCALE-PARSTEP-001` is
unchanged by this repository slice. No transport, lease, fencing token,
heartbeat, remote-worker, local-chunking, or work-stealing behavior was added.
