# Schema 2 Fault-Tolerance and Flow Migration

**State:** Implemented; released with the schema-2 runtime

**Source schema:** `1`

**Target schema:** `2`

**Rolling deployment:** Not supported. All schema-1 runtimes must be quiesced
before migration, and they reject schema 2 on startup.

**Immutable SQL:** `crates/oxide-batch/migrations/0002_fault_tolerance_and_flow.sql`

This is the version-specific migration and rollback contract. The executable
fixture that proves it is
`tests/fixtures/postgres/run-design-gate.sh`, which applies migration `0001`
to a separate database, seeds realistic schema-1 history, applies migration
`0002`, and runs `tests/fixtures/postgres/design-gate/verify-schema2-upgrade.sql`.

## Data-model changes

Schema 2 extends `ob_step_execution` with:

- non-null `step_logical_id`, backfilled byte-for-byte from `step_name`;
- non-negative `read_retry_count`, `process_retry_count`, and
  `write_retry_count`;
- non-negative `read_skip_count`, `process_skip_count`, and
  `write_skip_count`;
- non-negative `no_rollback_count`;
- fault-state format, schema ID, schema version, canonical JSON payload, and
  32-byte checksum.

The existing execution failure-category constraint retains every schema-1
value and adds `OPTIMISTIC_CONFLICT`, `TIMEOUT`,
`UNSUPPORTED_CAPABILITY`, and `UNKNOWN_COMMIT`.

All new counters default to zero. Existing rows receive the empty format-1
fault-state envelope
`{"checkpoint": "<64 zero hex digits>", "entries": []}` and its published
SHA-256 checksum
`a491114819e0d3bd8b7ca004dc0636f95b45e2fcb1a67ddb5726beaea12f9922`. The
runtime asserts the same vector in
`crates/oxide-batch/tests/fault_state.rs::empty_state_matches_the_published_migration_vector`,
so the migration default and the framework encoder cannot drift apart. Non-empty envelopes hold at
most 256 digest-sorted retry entries and remain bounded to 64 KiB. The
migration adds a unique constraint on
`(job_execution_id, step_logical_id)` and the bounded history index required by
`STEP-START-001`.

Schema 2 also creates append-only `ob_flow_decision`:

- positive identity primary key;
- foreign keys to job execution, optional source step execution, and optional
  reused decision;
- positive decision sequence;
- bounded `source_node_id`, observed outcome, and optional target node ID;
- checked transition kind and terminal kind;
- 32-byte plan fingerprint and decision-input digest;
- facade-clock `decided_at`;
- unique `(job_execution_id, sequence)` and
  `(job_execution_id, source_node_id)`.

Foreign keys use `RESTRICT`. The table contains no parameter, context, item,
credential, endpoint, SQL, user error, or decider-private value.

## Atomic migration

The migration runs under the existing OxideBatch advisory lock and one
PostgreSQL transaction:

1. require singleton schema version 1;
2. add nullable columns and the new table;
3. backfill logical IDs, zero counters, and the exact empty fault envelope in
   bounded primary-key batches when fixture size requires it;
4. validate byte limits, checksums, non-negative counters, foreign keys, and
   uniqueness;
5. make required columns non-null and add constraints/indexes;
6. set the singleton version to 2;
7. commit.

If bounded backfill cannot remain in one transaction within the accepted lock
and statement deadlines, implementation must stop and obtain a revised
expand/migrate/contract decision. It may not silently ship a resumable
non-transactional variant.

Any failure before commit leaves schema version 1 and all existing rows
unchanged. The migrator does not rewrite format-1 definition manifests.

## Application and manifest compatibility

A schema-2 runtime reads definition manifest formats 1 and 2. Existing format-1
definitions retain their exact bytes and digest. A format-1 runtime sees schema
2 as `NewerSchema` and performs no work.

Creating schema-2 executions may add format-2 definitions, logical step IDs,
fault state, and flow decisions that a schema-1 runtime cannot preserve.
Downgrade therefore uses restore; there is no reverse SQL.

## Before migration

1. Stop all launchers and wait for bounded shutdown.
2. Resolve or explicitly retain every active, orphaned, and `UNKNOWN`
   execution.
3. Record application, OxideBatch, PostgreSQL, schema, and manifest-reader
   versions.
4. Validate migrator/runtime roles and `verify-full` TLS.
5. Take a logical backup and prove restoration into an isolated clean
   database;
6. record counts and maximum sizes for definitions, job/step executions,
   contexts, and checkpoints.

Backups and diagnostic output must not dump parameter, context, checkpoint, or
business values into CI logs.

## Verification and canary

The release fixture must prove:

- exactly one schema-version row contains `2`;
- every old step row has `step_logical_id = step_name`, zero new counters, and
  the published empty-state checksum;
- all new constraints, foreign keys, and named indexes exist and validate;
- schema-1 repository reads remain semantically represented through the
  schema-2 runtime;
- retry reservation, atomic skip commit, start-limit selection, and flow
  decision append queries use their named indexes;
- runtime DML succeeds while runtime DDL and operator-reader writes fail;
- schema 3 is rejected by the schema-2 runtime;
- corrupted fault state and corrupt decision digests fail closed;
- format-1 one-step canary launch/restart and format-2 conditional-flow canary
  both produce the expected redacted observations.

CI runs upgrade and restore evidence on PostgreSQL 15 and 18. PostgreSQL 16 and
17 retain the configured migration smoke axis.

## Rollback and restore

The only supported rollback is:

1. stop every schema-2 writer;
2. preserve safe migration and canary diagnostics;
3. discard the schema-2 database or schema;
4. restore the verified pre-migration schema-1 backup;
5. verify version 1 constraints and representative reads with the old runtime;
6. resume through a schema-1 canary.

Executions, decisions, counters, and definitions created after the backup are
lost. The operator must reconcile any associated business effects before
resuming. Reverse deletion of the new table/columns is not a supported
downgrade because it would erase restart-relevant state.

## Released evidence

| Requirement | Evidence |
| --- | --- |
| Immutable SQL | `crates/oxide-batch/migrations/0002_fault_tolerance_and_flow.sql`, applied inside one sqlx-owned transaction under the existing advisory lock |
| Empty-state vector | `fault_state.rs::empty_state_matches_the_published_migration_vector` pins the canonical bytes and checksum the migration installs |
| Realistic source fixtures | `design-gate/schema1-seed.sql` seeds completed, failed-with-active-restart, stopped, and unresolved `UNKNOWN` instances |
| Upgrade verification | `design-gate/verify-schema2-upgrade.sql` asserts the singleton version, byte-for-byte logical-ID backfill, zero counters, the published envelope, every new constraint and index, extended category acceptance, and closed-fail bounds |
| Reapplication safety | The design-gate fixture reapplies `0002` and requires the `schema version 1 is required` rejection |
| PostgreSQL matrix | `postgres-15/16/17/18-design-gate` run the upgrade fixture; `postgres-15/18-repository` run the schema-2 runtime, TLS, least-privilege, and process-kill suites |
| Backup and restore | The design-gate fixture dumps the migrated schema, restores it into a clean database, and requires schema version `2` |
| Newer-version rejection | The restored database is forced to version `3` and the runtime must refuse it |

The measured migration duration is reported by the design-gate fixture for each
supported major. The migration adds columns with constant defaults and one
bounded backfill of `step_logical_id`, so its lock window scales with the number
of step-execution rows; deployments with unusually large history must measure it
against their own snapshot before scheduling the maintenance window.

## Residual limitations

- The M3 exit workstream now commits a terminal known rollback with the failed
  step lifecycle and provides process-kill certification for retry reservation,
  skip callback, and flow-decision boundaries. See
  [M3 exit evidence](../../project/m3-exit-evidence.md).
- Released-version verification remains separate from this migration guide.
