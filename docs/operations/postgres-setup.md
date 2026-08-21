# PostgreSQL Setup

**State:** Implemented for M2, the unreleased M4 schema-3 slice, and the
unreleased M6 schema-4 slice (`#144`, additive `ItemStream` component state)

**Supported M2 matrix:** PostgreSQL 15 through 18 on Linux x86_64 GNU

PostgreSQL 15 and 18 are the release-blocking repository, transaction, TLS,
role, and crash/restart axes. PostgreSQL 16 and 17 receive connection,
migration, repository, and vertical-slice smoke coverage. Other versions are
not part of the M2 support promise.

## Roles and database ownership

Use separate deployment identities:

- **migrator:** owns the fixed `oxide_batch` schema and applies immutable
  migrations;
- **runtime:** receives schema usage, metadata-table DML, and identity-sequence
  use, but no DDL;
- **operator reader:** receives bounded read-only metadata access, so it can
  plan a purge but never apply one;
- **operator writer:** receives the recovery, operator-request, hold, stop, and
  retention-audit inserts and updates the audited operator paths need, plus the
  narrowly granted deletes one bounded purge batch requires. The runtime role
  keeps no metadata `DELETE` privilege and therefore cannot purge.

Deployment tooling creates login credentials, grants database `CONNECT`, and
rotates secrets. OxideBatch migrations do not create production passwords.
The executable reference grants are
`tests/fixtures/postgres/design-gate/roles.sql` and
`roles-after-migration.sql`. They cover schema 2; the schema-3 grants a purge
requires are specified by
[the schema-3 migration guide](migrations/0003-operations-and-local-scale.md)
and are not yet executable, so a deployment applies them itself.

## TLS

Production configuration uses `TlsMode::VerifyFull`, certificate validation,
and hostname verification. Supply either system trust roots or one bounded CA
certificate through `CaCertificate`. There is no invalid-certificate mode.

`TlsMode::Plaintext` requires an explicit opt-in and is limited to local or
isolated test environments. Connection strings and certificate contents or
paths are redacted from facade diagnostics.

## Initialize the current schema (4)

Enable the adapter:

```toml
[dependencies]
oxide-batch = { version = "0.5.0", features = ["postgres"] }
```

Apply the released migrations with the migrator identity before starting any
runtime. `PostgresMigrator::migrate` installs schema `1` through `4` on an
empty database and applies only the pending migrations on an existing one.
No installed schema past 1 is a rolling upgrade: quiesce every runtime that
supports an older schema first, because it rejects the newer schema on
startup rather than guessing at compatibility.

```rust,no_run
use oxide_batch::{PostgresConfig, PostgresMigrator};

# async fn migrate() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
PostgresMigrator::migrate(&config).await?;
# Ok(())
# }
```

Then connect with the runtime identity:

```rust,no_run
use std::sync::Arc;

use oxide_batch::{PostgresConfig, PostgresJobRepository, SystemClock};

# async fn connect() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresConfig::new(std::env::var("RUNTIME_DATABASE_URL")?)?;
let repository =
    PostgresJobRepository::connect(config, Arc::new(SystemClock)).await?;
# repository.close().await?;
# Ok(())
# }
```

Runtime startup is fail-closed:

| Durable state | Result |
| --- | --- |
| Missing schema/version row | `SchemaUninitialized` |
| Supported older schema | `MigrationRequired` |
| Exact schema version 4 | connection accepted |
| Version above 4 | `NewerSchema` |

Runtime startup never applies migrations automatically.

## Verify the installation

As migrator, the singleton query must return exactly `3`:

```sql
SELECT version
FROM oxide_batch.ob_schema_version
WHERE singleton = true;
```

Connect once through `PostgresJobRepository` as runtime. Confirm the runtime can
begin and roll back a repository unit of work and receives SQLSTATE `42501`
when it attempts DDL.

To reproduce the complete TLS, least-privilege, migration, repository, and
backup/restore fixture locally with a Docker-compatible daemon:

```console
./tests/fixtures/postgres/run-design-gate.sh 15
./tests/fixtures/postgres/run-design-gate.sh 18
```

Run `16` and `17` in place of the major for the supported intermediate axes.

## Backup and restore

Before a schema change, quiesce launchers, resolve or retain every ambiguous
execution, record application/framework/schema versions, and take a verified
backup. Default rollback is restoration of that compatible backup; no released
schema version has a destructive reverse migration. The schema-1 to schema-2
consequences, including the loss of executions created after the backup, are
recorded in
[the schema-2 migration guide](migrations/0002-fault-tolerance-and-flow.md), and
the schema-2 to schema-3 consequences in
[the schema-3 migration guide](migrations/0003-operations-and-local-scale.md).
Purge has no reverse operation, so an applied purge is recoverable only by
restoring a verified backup.

The design-gate fixture performs a logical `pg_dump`, restores into a clean
database of the same major version, and rereads the schema singleton.
Production deployments additionally need a deployment-specific physical or
managed backup with a tested restore objective.

See [migration 0001](migrations/0001-initial-metadata.md) for checksum,
application, rollback, and interrupted-migration details.
