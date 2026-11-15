# M7 Upgrade and Rollback Guide

**State:** Accepted release-line guidance

**Applies to:** OxideBatch `0.7.0` M7 release line (`v0.7.0`)

This guide sequences the supported PostgreSQL metadata upgrade and rollback
boundary for the M7 release. Normative migration and recovery rules remain in
[persistence and migrations](../operations/persistence-and-migrations.md),
[PostgreSQL setup](../operations/postgres-setup.md), and
[crash, restart, and recovery](../operations/crash-restart-and-recovery.md).

## Release schema boundary

`0.7.0` runs against **metadata schema 12**. The published `0.6.0` release
runs schema 4, so schema 4 is the normal production upgrade source for this
release. The retained M7 migration/conformance campaigns also exercise the
historical direct-upgrade denominator and populated intermediate M7 schema
boundaries; those campaign facts do not turn older pre-1.0 release lines into
parallel maintained support lines.

The M7 sequence advances schema 4 through the accepted nested-job, scope
provenance, repeat, definition-registry/upgrade, fork-lineage, and explicit
operator `FORK` migrations. The final migration
`0013_operator_fork_action.sql` advances schema 11 to schema 12.

Runtime startup never applies migrations. A runtime connected to an older
schema fails closed with `MigrationRequired`; a runtime facing a schema newer
than 12 fails with `NewerSchema`. Use `PostgresMigrator` with the dedicated
migrator identity before starting the `0.7.0` runtime.

## Upgrade from 0.6.0

1. **Quiesce every older writer.** Stop launchers and wait for bounded
   shutdown. `0.7.0` is not a rolling metadata-schema upgrade from `0.6.0`.
2. **Resolve active or ambiguous work.** Re-read durable execution/operator
   state and preserve diagnostics before migration.
3. **Record the current schema.** A normal `0.6.0` deployment must report
   schema 4 before this release upgrade.
4. **Take a verified backup.** Restore it into an isolated database and prove
   representative reads before touching the production schema.
5. **Run the dedicated migrator.** Apply the immutable pending migration
   sequence through schema 12. Do not run application traffic concurrently.
6. **Verify schema 12 and repository access.** Confirm the restricted runtime
   identity can read/write only the intended runtime surface and that the
   migrator identity is no longer used by application traffic.
7. **Run a canary.** Exercise representative launch/restart, scoped state,
   repeat, definition selection, and operator/explorer reads before resuming
   full traffic.

## Definition compatibility after migration

Schema migration and definition compatibility are separate authorities.
`0.7.0` persists definition-registry identity, compatible-upgrade edges, and
fork lineage. A successful schema upgrade therefore does **not** imply that a
changed application definition is restart-compatible. Definition drift and
missing compatibility edges fail closed; use the accepted M7 definition
evolution model rather than forcing a restart.

See the [M7 Gate-D evidence](../project/m7-gate-d-acceptance-evidence.md) and
[M7 exit evidence](../project/m7-exit-evidence.md).

## Backup and rollback

Rollback remains **restore-based**. No released reverse migration is supplied.
Before migration, create and verify a backup, for example:

```console
pg_dump --format=custom --file=pre-0.7.0.dump "$MIGRATOR_DATABASE_URL"
pg_restore --clean --if-exists --dbname="$ISOLATED_RESTORE_URL" pre-0.7.0.dump
```

If rollback is required:

1. stop every schema-12 writer;
2. preserve migration/canary diagnostics;
3. discard the schema-12 database or schema;
4. restore the verified pre-upgrade backup;
5. deploy the matching older runtime;
6. verify the restored schema and run a canary before reopening traffic.

Any execution, definition, fork, scope/repeat state, or operator action created
after the backup is lost when restoring it. Reconcile associated business
effects separately. Dropping only the newer tables/constraints is not a
supported downgrade.

## Release support boundary

The exact `0.7.0` runtime/database support matrix is
[the release support matrix](../release/support-matrix.md#070-m7-release-line).
PostgreSQL is the only durable metadata adapter in this release. MySQL,
SQLite, SQL Server, other database certification, messaging, and remote
execution are later milestones and are not implied by this upgrade path.
