# PostgreSQL Setup

**State:** Implemented for M2

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
- **operator reader:** receives bounded read-only metadata access;
- **operator writer:** receives only the recovery reads/inserts/updates needed
  by the audited recovery path.

Deployment tooling creates login credentials, grants database `CONNECT`, and
rotates secrets. OxideBatch migrations do not create production passwords.
The executable reference grants are
`tests/fixtures/postgres/design-gate/roles.sql` and
`roles-after-migration.sql`.

## TLS

Production configuration uses `TlsMode::VerifyFull`, certificate validation,
and hostname verification. Supply either system trust roots or one bounded CA
certificate through `CaCertificate`. There is no invalid-certificate mode.

`TlsMode::Plaintext` requires an explicit opt-in and is limited to local or
isolated test environments. Connection strings and certificate contents or
paths are redacted from facade diagnostics.

## Initialize schema version 1

Enable the adapter:

```toml
[dependencies]
oxide-batch = { version = "0.1.0-alpha.1", features = ["postgres"] }
```

Apply the released migration with the migrator identity before starting any
runtime:

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
| Exact schema version 1 | connection accepted |
| Version above 1 | `NewerSchema` |

Runtime startup never applies migrations automatically.

## Verify the installation

As migrator, the singleton query must return exactly `1`:

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
backup. Default rollback is restoration of that compatible backup; schema
version 1 has no destructive reverse migration.

The design-gate fixture performs a logical `pg_dump`, restores into a clean
database of the same major version, and rereads the schema singleton.
Production deployments additionally need a deployment-specific physical or
managed backup with a tested restore objective.

See [migration 0001](migrations/0001-initial-metadata.md) for checksum,
application, rollback, and interrupted-migration details.
