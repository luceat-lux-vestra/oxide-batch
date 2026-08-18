# M5 Upgrade and Rollback Guide

**State:** Accepted

**Applies to:** OxideBatch `0.5.0`, the M5 Embedded Core Production Preview

How to move a deployment onto this release's schema and how to get back off
it if that goes wrong. The normative rules live in
[persistence and migration operations](../operations/persistence-and-migrations.md#backup-restore-and-rollback)
and the [schema-3 migration guide](../operations/migrations/0003-operations-and-local-scale.md);
this page sequences them for an operator planning an upgrade window. See
[documentation strategy](../documentation/strategy.md) for that ownership
split.

## Supported source schema versions

This release runs against **metadata schema 3 only**. It upgrades directly
from schema `1` or schema `2` — there is no requirement to pass through every
intermediate version — and refuses to start against a schema newer than `3`.
A schema-2 runtime, in turn, refuses to open a schema-3 database at all
(`NewerSchema`), so schema 3 is **not** a rolling upgrade: every schema-2
writer must be quiesced first. See
[PostgreSQL setup](../operations/postgres-setup.md#initialize-schema-version-3)
for the exact fail-closed table (`SchemaUninitialized`, `MigrationRequired`,
accepted, `NewerSchema`).

Migrations are forward-only and immutable once released; `PostgresMigrator::migrate`
applies only the pending migrations on an existing database and installs `1`,
`2`, and `3` in sequence on an empty one. Existing format-1 and format-2
definition manifests keep their exact bytes and digests under schema 3 — the
schema upgrade does not rewrite or reinterpret them.

## Upgrade procedure

1. **Stop every launcher** for the affected deployment and wait for bounded
   shutdown (see [graceful process shutdown](../operations/crash-restart-and-recovery.md#graceful-process-shutdown)).
2. **Resolve or explicitly retain** every active, orphaned, or `UNKNOWN`
   execution — do not migrate schema underneath ambiguous state. Use
   `execution recover` from the [operator guide](operator-guide.md#recover)
   for anything you cannot cleanly quiesce.
3. **Record versions**: application, OxideBatch, PostgreSQL, schema, and
   manifest-reader.
4. **Validate roles and TLS**: migrator, runtime, operator-reader, and
   operator-writer identities exist with their least-privilege grants, and
   `verify-full` TLS is in place for production traffic. See
   [roles and database ownership](../operations/postgres-setup.md#roles-and-database-ownership).
5. **Take a verified backup** and prove it restores into an isolated, clean
   database before touching production — see
   [backup and rollback](#backup-and-rollback) below.
6. **Record baseline counts and sizes** for definitions, job/step executions,
   contexts, and checkpoints, so post-migration verification has something to
   compare against.
7. **Apply the migration** with the migrator identity:

   ```rust,no_run
   use oxide_batch::{PostgresConfig, PostgresMigrator};

   # async fn migrate() -> Result<(), Box<dyn std::error::Error>> {
   let config = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
   PostgresMigrator::migrate(&config).await?;
   # Ok(())
   # }
   ```

8. **Verify**: exactly one schema-version row reads `3`; every pre-existing
   execution keeps a null owner token and stop request and unchanged counters,
   statuses, and payload bytes; every pre-existing instance keeps all three
   hold columns null; runtime DML succeeds while runtime DDL and
   operator-reader writes fail. The complete verification list migration `3`
   must satisfy is in
   [verification and canary](../operations/migrations/0003-operations-and-local-scale.md#verification-and-canary).
9. **Run canary work** — representative reads and a small live job — before
   resuming full traffic.

Reapplying migration `0003` against an already-migrated database fails
closed with a `schema version 2 is required` guard rather than silently
no-op'ing or corrupting state.

## Backup and rollback

Take a logical backup with the migrator/backup identity before migrating, and
prove it restores:

```console
pg_dump --format=custom --file=pre-upgrade.dump "$MIGRATOR_DATABASE_URL"
pg_restore --clean --if-exists --dbname="$ISOLATED_RESTORE_URL" pre-upgrade.dump
```

Verify the schema version and representative reads against the restored
database before you trust the backup. A deployment-specific physical or
managed backup with its own tested restore objective is still required in
production; the logical dump above is conformance evidence, not a
substitute.

**Downgrade means restoring that backup — there is no reverse migration.**
Reverse SQL is supplied only when it is tested and cannot discard data the
prior version needs, and no released schema version currently ships one.
Rollback from schema 3:

1. stop every schema-3 writer;
2. preserve migration and canary diagnostics for later investigation;
3. discard the schema-3 database or schema;
4. restore the verified pre-migration schema-2 backup;
5. verify schema-2 constraints and representative reads with the *old*
   runtime;
6. resume through a schema-2 canary.

**What you lose on rollback:** every execution, partition, operator request,
retention action, and definition created after the backup was taken —
including the effects of any purge applied after that point. Reconcile any
associated business effects before resuming. Dropping only the new schema-3
tables and columns instead of restoring is **not** a supported downgrade: it
would erase restart-relevant partition state and destructive-action audit
that later reads depend on.

## Recovery after ambiguous commit or process failure

An upgrade window is also an operational window: a crash, `SIGKILL`, or an
ambiguous commit response during or around migration is handled exactly like
any other crash, through explicit, evidence-based, audited recovery — never
by guessing. See
[crash, restart, and recovery](../operations/crash-restart-and-recovery.md#expected-crash-state)
and the [operator guide's recovery walkthrough](operator-guide.md#recover). If
durable fault state cannot be validated after a failure (unsupported version,
checksum mismatch, unknown enumeration value), the affected step fails closed
before any component runs; treat that as metadata corruption, inspect before
acting, and restore from the verified backup rather than editing the envelope
by hand.

## Purge is not a rollback path

Retention purge has no reverse operation. If you need to undo an applied
purge, the only path is restoring a verified backup — see the
[operator guide's retention section](operator-guide.md#retention).

## Support window

Before `1.0`, only the latest preview line receives fixes; see the
[release and support policy](../release/support-policy.md#support-window) for
the complete pre-1.0 support rule and the
[support matrix](../release/support-matrix.md#m5-production-preview-support-bounds)
for this release's exact upgrade/downgrade expectations.
