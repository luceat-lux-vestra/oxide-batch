# Schema 3 Operations and Local-Scale Migration

**State:** Accepted design; unreleased

**Source schema:** `2`

**Target schema:** `3`

**Rolling deployment:** Not supported. All schema-2 runtimes must be quiesced
before migration, and they reject schema 3 on startup.

**Immutable SQL:** `crates/oxide-batch/migrations/0003_operations_and_local_scale.sql`

**Corrective schema-3 patch:**
`crates/oxide-batch/migrations/0004_schema3_split_aggregate_patch.sql`

This is the version-specific migration and rollback contract for the M4
operator, retention, shutdown/stale, and bounded local-scale slices. The
logical model it installs is owned by the
[PostgreSQL physical metadata model](../../architecture/postgres-physical-metadata-model.md).

## Data-model changes

Schema 3 extends `ob_job_execution` with:

- nullable 16-byte `owner_token` recording the process that owns a
  non-terminal execution;
- nullable `stop_requested_at` facade-clock instant;
- nullable bounded `stop_requested_by` actor reference of at most 128 bytes.

A null `owner_token` means ownership was never recorded, which is the state of
every pre-existing row. It is not a lease and never authorizes takeover.

Schema 3 extends `ob_job_instance` with a single optional hold: nullable
`hold_actor`, `hold_reason`, and `hold_placed_at`. The three columns are all
null or all non-null, enforced by one check constraint.

Schema 3 creates three tables:

- `ob_operator_request`, the append-only audit and idempotency record for
  every mutating operator action, unique on `(action, operation_id)`, whose job
  execution and job instance references are both optional because a rejected
  launch may precede either row;
- `ob_retention_action`, the append-only audit record for holds and applied
  purges;
- `ob_step_partition`, the durable partition plan and per-partition result for
  a partitioned step execution, unique on
  `(step_execution_id, partition_key)`.

The execution failure-category constraint retains every schema-2 value and adds
`SHUTDOWN_INCOMPLETE` and `STALE_RECOVERED`. New reason-code and outcome-class
columns use checked text rather than PostgreSQL enums so later versions can add
values transactionally.

The accepted schema-3 model requires the flow-decision transition-kind
constraint to retain every schema-2 value and accept `SPLIT_AGGREGATE` for a
format-3 structural join. The already-published `0003` checksum is preserved.
The idempotent corrective migration `0004_schema3_split_aggregate_patch.sql`
adds that accepted value after verifying the application schema is exactly 3;
it does not introduce application schema version 4. Existing flow decisions
are unchanged.

Every new table uses `RESTRICT` foreign keys and contains no parameter,
context, item, checkpoint, credential, endpoint, SQL, user error text, or
free-form operator text.

## Atomic migration

The migration runs under the existing OxideBatch advisory lock and one
PostgreSQL transaction:

1. require singleton schema version 2;
2. add nullable columns to `ob_job_execution` and `ob_job_instance`;
3. create the three new tables, their constraints, and their indexes;
4. extend the failure-category constraint;
5. validate constraints, foreign keys, and uniqueness;
6. set the singleton version to 3;
7. commit.

The migrator then applies the corrective schema-3 patch under its migration
lock. The patch requires schema version 3, replaces only the named
flow-decision check constraint, validates it, and commits without changing the
singleton application schema version. This preserves the immutable `0003`
checksum for databases that already applied it and gives fresh databases the
same final schema.

The migration performs no backfill. Every added column is nullable with no
default, and every new table starts empty, so the lock window is independent of
history size. If a future revision of this design requires a backfill, it must
obtain a revised expand/migrate/contract decision before implementation rather
than shipping a resumable non-transactional variant.

Any failure before commit leaves schema version 2 and all existing rows
unchanged. The migrator does not rewrite definition manifests of any format.

## Application and manifest compatibility

A schema-3 runtime reads definition manifest formats 1, 2, and 3. Existing
format-1 and format-2 definitions retain their exact bytes and digests. A
schema-2 runtime sees schema 3 as `NewerSchema` and performs no work.

Executions created under schema 3 may add owner tokens, stop requests, holds,
operator requests, retention actions, partitions, and format-3 definitions that
a schema-2 runtime cannot preserve. Downgrade therefore uses restore; there is
no reverse SQL.

## Before migration

1. Stop all launchers and wait for bounded shutdown.
2. Resolve or explicitly retain every active, orphaned, and `UNKNOWN`
   execution.
3. Record application, OxideBatch, PostgreSQL, schema, and manifest-reader
   versions.
4. Validate migrator, runtime, operator-reader, and operator-writer roles and
   `verify-full` TLS.
5. Take a logical backup and prove restoration into an isolated clean
   database.
6. Record counts and maximum sizes for definitions, job and step executions,
   contexts, and checkpoints.

Backups and diagnostic output must not dump parameter, context, checkpoint, or
business values into CI logs.

## Privilege changes

The migrator owns the new objects. The runtime receives DML on
`ob_step_partition` and the new `ob_job_execution` columns, plus insert on
`ob_operator_request` for actions it performs itself. The operator-reader
receives `SELECT` only. The operator-writer receives `SELECT`, insert on
`ob_operator_request` and `ob_retention_action`, the hold columns of
`ob_job_instance`, and the narrowly granted deletes required by purge. No role
gains DDL, and no role may delete a definition, an upgrade edge, or the schema
version row.

## Verification and canary

The release fixture must prove:

- exactly one schema-version row contains `3`;
- every pre-existing execution has a null owner token, null stop request, and
  unchanged counters, statuses, and payload bytes;
- every pre-existing instance has all three hold columns null;
- all new constraints, foreign keys, and named indexes exist and validate;
- the extended failure-category constraint accepts the new values and still
  accepts every schema-2 value;
- operator idempotency uniqueness rejects a duplicate `(action, operation_id)`;
- partition uniqueness rejects a duplicate `(step_execution_id,
  partition_key)`;
- the hold check constraint rejects a partially populated hold;
- named operator, explorer, retention, and partition queries use their named
  indexes on realistic history;
- runtime DML succeeds while runtime DDL, operator-reader writes, and runtime
  purge deletes fail;
- schema 4 is rejected by the schema-3 runtime;
- corrupt partition contexts and mismatched checksums fail closed;
- a format-2 conditional-flow canary and a format-3 split and partition canary
  both produce the expected redacted observations.

CI runs upgrade and restore evidence on PostgreSQL 15 and 18. PostgreSQL 16 and
17 retain the configured migration smoke axis.

## Rollback and restore

The only supported rollback is:

1. stop every schema-3 writer;
2. preserve safe migration and canary diagnostics;
3. discard the schema-3 database or schema;
4. restore the verified pre-migration schema-2 backup;
5. verify version 2 constraints and representative reads with the old runtime;
6. resume through a schema-2 canary.

Executions, partitions, operator requests, retention actions, and definitions
created after the backup are lost. The operator must reconcile any associated
business effects, including any applied purge, before resuming. Dropping the
new tables and columns is not a supported downgrade because it would erase
restart-relevant partition state and destructive-action audit.

## Required evidence

| Requirement | Evidence |
| --- | --- |
| Immutable SQL | Preserve the `0003` checksum; apply the bounded corrective `0004` constraint patch under the existing migration lock |
| Realistic source fixtures | Schema-2 seed with completed, failed-with-active-restart, stopped, and unresolved `UNKNOWN` history |
| Upgrade verification | A schema-3 verification script asserting every item in the verification list |
| Reapplication safety | Reapplying the migration must fail with a `schema version 2 is required` guard |
| PostgreSQL matrix | Design-gate upgrade fixtures on 15, 16, 17, and 18; runtime, TLS, least-privilege, and process-kill suites on 15 and 18 |
| Backup and restore | Dump the migrated schema, restore into a clean database, and require schema version `3` |
| Newer-version rejection | Force the restored database to version `4` and require refusal |

## Residual limitations

- Purge is destructive and has no reverse operation. Its only recovery path is
  restore from a verified backup.
- `owner_token` records ownership evidence only. It is not a lease and must not
  be presented as distributed ownership while
  [RFC-0009](../../rfcs/0009-transport-neutral-worker-protocol.md) remains
  proposed.
- Released-version verification remains separate from this migration guide.
