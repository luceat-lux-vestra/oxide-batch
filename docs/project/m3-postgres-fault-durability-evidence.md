# M3 PostgreSQL Fault-Durability Evidence

**State:** Complete on merge

**Issue:** [#62](https://github.com/luceat-lux-vestra/oxide-batch/issues/62)

**Date:** 2026-07-31

This record maps the fourth M3 workstream's exit criteria to durable
`PostgreSQL` behavior. It promotes the accepted schema-2 design into immutable
migration SQL, persists the fault-tolerance counters and retry state the
[fault-tolerance contract](../architecture/fault-tolerance.md) defines, and
reconstructs them across restart.

It does not claim compiled-plan lowering, logical-ID semantics beyond the
format-1 backfill, or flow decisions. `ob_flow_decision` is created and
constrained by this migration because schema 2 is immutable, but no runtime path
writes it; issue
[#64](https://github.com/luceat-lux-vestra/oxide-batch/issues/64) owns its
queries.

## Durable model

Schema 2 extends `ob_step_execution` with `step_logical_id`, per-phase retry and
skip counters, `no_rollback_count`, and a bounded checksummed fault-state
envelope. Format 1 of that envelope is canonical JSON containing the prior
committed checkpoint digest and at most 256 digest-sorted unresolved retry
entries, each holding only phase, stable category, reserved ordinal, classifier
revision, and the opaque retry-key digest. It contains no item value, error
text, parameter, or context value.

Two named queries own the durable transitions:

- `FAULT-RESERVE-001` is one short metadata-only transaction. It reads the step
  row `FOR UPDATE`, validates the retained envelope against the committed
  checkpoint and the configured limits, requires the supplied ordinal to
  directly follow the persisted one, and updates the phase retry count,
  `rollback_count`, and the envelope under
  `WHERE version = expected AND status = 'STARTED'`.
- `FAULT-COMMIT-001` is the existing enlisted chunk commit. It adds this
  chunk's skip and no-rollback deltas to the totals it read when the transaction
  began and writes the empty envelope, because the commit that advances the
  checkpoint supersedes the whole retry generation.

Restart copies the committed counters and the retained envelope to the new step
attempt in the same insert that copies the checkpoint, and the runtime seeds its
bounded limits and retry-key generation from
`ChunkTransactionManager::inherited_progress`.

## Exit criteria

| Exit criterion | Evidence |
| --- | --- |
| Immutable schema-v2 migration upgrades every supported prior schema and rejects newer versions | `crates/oxide-batch/migrations/0002_fault_tolerance_and_flow.sql` runs in one sqlx-owned transaction under the existing advisory lock and refuses to start unless the singleton is `1`. The design-gate fixture applies migration `0001` to a dedicated database, seeds `design-gate/schema1-seed.sql`, applies `0002`, runs `design-gate/verify-schema2-upgrade.sql`, and then requires the reapplication guard. `newer_schema_is_rejected_without_guessing_compatibility` and the fixture's forced version `3` prove a newer schema is never guessed compatible. |
| Retry/skip/rollback counters and required policy state commit or roll back with the chunk and cannot be double-counted after crash | `skips_counters_and_fault_state_commit_with_the_chunk` shows a rolled-back attempt leaving every counter and the reservation untouched, then one commit applying the skip delta, the no-rollback delta, and the envelope clear together. Deltas are applied to the totals read when the transaction began, so replaying an uncommitted chunk cannot double-count. `accepted_skips_are_committed_as_one_delta` shows the runtime hands exactly one delta to exactly one commit. |
| Restart continues from the accepted durable policy boundary and exhausts the same total limit as uninterrupted execution | `retry_reservation_is_a_durable_compare_and_swap` reserves an ordinal, proves a second writer with the same ordinal loses, and shows a fresh store — a new process — resuming the persisted ordinal instead of refilling the budget. `inherited_skip_totals_exhaust_the_shared_limit` shows an inherited skip total spending the shared limit so the next skippable failure fails the step. `create_step_execution` copies the counters and envelope to the restart attempt. |
| PostgreSQL 15/18 integration, disconnect, process-kill, restore, TLS, and least-privilege role evidence covers the changed schema and transaction | The `postgres-15/18-repository` CI axes run the three new durability tests beside the existing chunk, disconnect, optimistic-conflict, and process-kill suites. The `postgres-15/16/17/18-design-gate` axes run the upgrade fixture, the schema-2 runtime DML smoke under the least-privilege runtime role with `verify-full` TLS, and the dump/restore rehearsal that must observe schema version `2`. |
| Setup, migration, transaction, and recovery documentation names upgrade and rollback consequences | [PostgreSQL setup](../operations/postgres-setup.md), [persistence and migrations](../operations/persistence-and-migrations.md), [the schema-2 migration guide](../operations/migrations/0002-fault-tolerance-and-flow.md), [transaction guarantees](../operations/transaction-guarantees.md), and [the crash/restart runbook](../operations/crash-restart-and-recovery.md) name schema version 2, the restore-only rollback, and the reservation boundary. |

## Named scenarios satisfied by this workstream

| Ledger row | Scenario | Evidence |
| --- | --- | --- |
| `FT-RETRY-001` | `retry_reservation_survives_restart` | `retry_reservation_is_a_durable_compare_and_swap` (a new store resumes the persisted ordinal) |
| `FT-RETRY-001` | `stale_retry_reservation_loses_cas` | the same test's rejected duplicate ordinal, plus `reservation_requires_the_next_ordinal_of_one_generation` |
| `FT-SKIP-001` | `skip_count_commits_with_chunk` | `skips_counters_and_fault_state_commit_with_the_chunk` |
| `FT-ROLLBACK-001` | `crash_before_commit_replays_chunk` | the existing process-kill matrix, now covering the schema-2 row |
| `META-UPGRADE-001` | `schema1_upgrades_to_schema2` | `design-gate/verify-schema2-upgrade.sql` |
| `META-UPGRADE-001` | `schema2_corruption_fails_closed` | `corrupt_fault_state_fails_before_component_work`, `tampered_state_fails_closed`, and the fixture's constraint probes |
| `META-UPGRADE-001` | `schema2_backup_restores_schema1` | the design-gate dump/restore rehearsal, which restores the migrated schema and verifies version `2` before the forced newer-version rejection |

`schema1_runtime_rejects_schema2` cannot be executed by this workstream, because
it requires a released schema-1 binary. It is represented by
`newer_schema_is_rejected_without_guessing_compatibility` and by
`design-gate/verify_supported_schema.sql`, which both prove the runtime refuses
a version above the one it supports rather than guessing compatibility.

## Deliberate decisions recorded here

- The enlisted chunk commit writes the *empty* envelope rather than removing
  resolved keys individually. A chunk commits only when every buffered input is
  classified, and the advancing checkpoint supersedes every key derived from the
  previous generation, so clearing the generation is both the contract's
  "clears all resolved keys in the same commit that advances the checkpoint" and
  the only outcome that cannot leak capacity.
- A durable store cannot know its step execution at definition time, so
  `FaultStateStore::bind` is added with a process-local default. The runtime
  binds once before the first attempt; an unbound durable store fails closed
  with `FaultStateError::Unbound` rather than guessing a target row.
- The runtime derives retry keys from the digest of the durable checkpoint
  envelope, including the empty one, and the adapter recomputes the same digest
  from the step row. An identical key therefore resumes its persisted ordinal
  across restart, and state retained against a superseded checkpoint is
  corruption.
- `jsonb` does not preserve stored byte order, so the adapter re-emits the
  document through the framework's canonical member order before validating the
  checksum. `empty_state_matches_the_published_migration_vector` pins the exact
  bytes and checksum that migration `0002` installs, so the SQL default and the
  encoder cannot drift.
- `ChunkExecutionReport` fault counters are now cumulative durable totals rather
  than per-attempt counts, because the shared skip limit is defined across every
  attempt of one job instance. `rollback_count` remains per-attempt; see the
  limitation below.

## Residual limitations

- A terminal known rollback does not increment the durable `rollback_count`.
  Only the acknowledged retry reservation does, so the column is a lower bound
  for a step that failed without reserving a retry. Recording it requires
  step-lifecycle counter plumbing from the chunk report to the terminal step
  update, which is owned by the M3 exit workstream
  ([#65](https://github.com/luceat-lux-vestra/oxide-batch/issues/65)).
- `ob_flow_decision` has no runtime writer or read query yet.
- Measured migration duration is reported per major by the design-gate fixture
  rather than pinned as a threshold, because it scales with the deployment's
  step-execution history.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
./tests/fixtures/postgres/run-design-gate.sh 15
./tests/fixtures/postgres/run-design-gate.sh 18
```

The `PostgreSQL` tests skip with a printed reason when
`OXIDEBATCH_POSTGRES_TEST_URL` is unset. The design-gate fixture requires a
Docker-compatible daemon; the release-blocking runs are the CI matrix axes.
