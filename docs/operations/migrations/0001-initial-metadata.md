# Migration 0001 — Initial PostgreSQL Metadata

**State:** Released

**Source version:** uninitialized/empty

**Target version:** 1

**Migration:** `crates/oxide-batch/migrations/0001_initial_metadata.sql`

**SHA-256:** `612ef00037e65095cb43391ee1b30164b95d2c61d2738e0d169a38a77bcc3d96`

## Compatibility and ownership

Schema version 1 is understood by OxideBatch `0.1.0-alpha.1` with the
`postgres` feature. Runtime startup requires exactly version 1 and never
applies this migration automatically.

Use a dedicated migrator role that can create and own the fixed `oxide_batch`
schema. Grant the runtime role schema usage, metadata-table DML, and identity
sequence usage only after migration. The executable least-privilege reference
is `tests/fixtures/postgres/design-gate/roles.sql`; deployment tooling remains
responsible for database login creation, `CONNECT`, and credential rotation.

## Preflight and backup

For an empty database, confirm that no application has created objects in the
`oxide_batch` schema. For a retry after interrupted setup, retain a database
backup or recreate the intended empty database through deployment tooling.

Record the database major, application revision, crate version, target schema
version, and migration checksum. PostgreSQL 15 through 18 are the M2 support
matrix.

## Apply

Construct `PostgresConfig` from the migrator credential and call:

```rust,no_run
use oxide_batch::{PostgresConfig, PostgresMigrator};

# async fn migrate() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
PostgresMigrator::migrate(&config).await?;
# Ok(())
# }
```

The migrator takes a database-scoped advisory lock within the configured lock
deadline, verifies that it is not crossing a newer-version boundary, and lets
SQLx apply the embedded migration transactionally. Reapplying the same released
migration is a verified no-op; modified migration bytes are rejected by the
recorded SQLx checksum.

## Verify

As migrator, verify the singleton:

```sql
SELECT version
FROM oxide_batch.ob_schema_version
WHERE singleton = true;
```

The result must be exactly `1`. Confirm that `_sqlx_migrations` records version
1 successfully, then connect once through `PostgresJobRepository` using the
runtime identity. A runtime role must be able to begin and roll back a
repository unit of work but must receive SQLSTATE `42501` for schema DDL.

The CI rehearsal additionally loads representative metadata, checks constraints
and optimistic updates, takes a logical `pg_dump`, restores it into a clean
database, rereads version 1, and runs the shared repository contract.

## Rollback and recovery

There is no reverse migration that preserves version-1 metadata in an
uninitialized schema. Before production writes, rollback may recreate the
intended empty database through deployment tooling. After any metadata write,
restore the verified pre-migration backup instead of dropping individual
tables.

If migration returns an infrastructure error, do not start runtimes. Inspect
the advisory-lock holder, PostgreSQL transaction outcome, `_sqlx_migrations`,
and `ob_schema_version` with a fresh healthy connection. Reapplying is allowed
only when the database is still uninitialized or reports version 1 with the
released checksum. A version above 1 must not be modified by this runtime.
