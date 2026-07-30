# Schema 2 Fault-Tolerance and Flow Migration

**State:** Accepted design; unreleased until issue #62 promotes immutable SQL

**Source schema:** `1`

**Target schema:** `2`

**Rolling deployment:** Not supported. All schema-1 runtimes must be quiesced
before migration, and they reject schema 2 on startup.

This is the version-specific migration and rollback contract. The final
`0002_<lower_snake_case>.sql`, checksum, measured duration, and exact canary
queries are added by the implementation workstream before schema 2 is
released.

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
fault-state envelope and its published checksum. Non-empty envelopes hold at
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

## Release evidence still required

Issue #62 must replace design placeholders with:

- the immutable SQL filename and released checksum;
- measured migration/lock/WAL/disk requirements;
- realistic active, terminal, failed, stopped, and `UNKNOWN` source fixtures;
- PostgreSQL 15–18 results, query plans, least-privilege grants, and TLS;
- backup artifact checksum and restore transcript;
- canary acceptance IDs and residual limitations.
