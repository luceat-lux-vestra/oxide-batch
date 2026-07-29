# Persistence and Migration Operations

**State:** Accepted

## Ownership model

OxideBatch owns its metadata schema. Spring Batch and OxideBatch processes do
not concurrently mutate the same metadata tables. Business data remains owned
by the application even when an adapter enlists it in the same transaction.

Recommended PostgreSQL roles:

- **migrator:** schema DDL only during controlled deployment;
- **runtime:** required metadata DML and sequence/function use, no schema DDL;
- **operator-reader:** read-only metadata diagnostics;
- **operator-writer:** narrowly granted recovery/maintenance operations.

Applications may collapse roles for development, but production guidance keeps
them distinct.

M2 uses a fixed `oxide_batch` schema. The migrator owns the schema and all
objects. The runtime receives metadata table DML and identity-sequence use but
no DDL. The operator reader receives `SELECT`; the operator writer receives
`SELECT` plus only the recovery decision insert and execution columns required
for an audited compare-and-swap transition. Neither operator role owns objects,
changes grants, bypasses row security, terminates backends, or assumes another
role.

The executable grants in
`tests/fixtures/postgres/design-gate/roles.sql` are a least-privilege contract,
not a production credential bootstrap mechanism. Deployment tooling creates
login secrets and grants `CONNECT` at the database boundary. OxideBatch
migrations never contain production passwords.

The complete table, key, constraint, index, and query contract is the
[PostgreSQL physical metadata model](../architecture/postgres-physical-metadata-model.md).

## Schema rules

- Every table has a documented invariant and ownership boundary.
- Database constraints protect uniqueness and referential integrity; application
  checks alone are insufficient.
- Optimistic-lock versions protect concurrent updates where row locks do not
  span the operation.
- Timestamps are UTC instants; application-local dates are explicit values.
- Identifiers and parameter keys have canonical encodings and stable collation
  assumptions.
- Serialized context includes format/version metadata and size limits.
- Indexes are justified by named repository queries and measured plans.
- Schema/table prefixes or namespaces are configurable only if migrations and
  queries remain safe and testable.

## Migration rules

- Migrations are immutable after release and use a monotonic schema version.
- Migration files use `NNNN_<lower_snake_case>.sql`, beginning at `0001`, with
  contiguous four-digit versions. One version has one file; a correction gets a
  new version rather than an edited released file.
- Released checksums are recorded in release provenance. A checksum mismatch is
  a deployment error even when the SQL would otherwise run.
- A migration is transactional when PostgreSQL permits it; non-transactional
  steps require an explicit resume/repair procedure.
- Startup never performs an unannounced destructive migration.
- The runtime rejects a schema newer than it understands.
- Compatibility during rolling application deployment is documented per
  release; it is not assumed.
- Each release tests upgrades from every supported source version using realistic
  metadata fixtures.

The migrator bootstraps the fixed schema and singleton
`ob_schema_version` row. It obtains a PostgreSQL advisory lock scoped to the
database and OxideBatch schema before reading or changing the version. A second
migrator waits only for the configured lock timeout and then fails safely.
Runtime startup reads the singleton through its normal role:

- missing schema/version row is `SchemaUninitialized`;
- a lower supported version is `MigrationRequired` and does not auto-migrate;
- the exact supported version is accepted;
- a higher version is `NewerSchema` and is never guessed compatible.

There is no released durable schema before M2 version 1. Therefore the first
upgrade matrix contains the empty/uninitialized fixture and version 1. From the
second durable schema onward, CI restores realistic fixtures for every released
source version still supported, migrates each to the target, and runs repository
plus vertical-slice reads. Fixtures include active, terminal, failed, stopped,
and `UNKNOWN` executions without storing real user records.

Every schema-changing release includes a guide copied from
[the migration-guide template](migration-guide-template.md). The guide names
source/target versions, application compatibility, lock/downtime expectations,
backup and restore commands, invariants, canary queries, rollback, and recovery
from every non-transactional phase.

## Pool, TLS, and timeout contract

The application constructs facade-owned configuration. The PostgreSQL adapter
converts it internally; no SQLx pool, connect-options, TLS, URL, error, row, or
transaction type appears in the facade.

| Facade-owned value | Bounds and M2 default | Behavior |
| --- | --- | --- |
| `PoolSize` | `1..=1024`, default `10` | Maximum connections owned by one repository instance |
| `AcquireTimeout` | `1 ms..=5 min`, default `30 s` | Wait for a pool permit/connection |
| `ConnectTimeout` | `1 ms..=5 min`, default `10 s` | Establish TCP and TLS plus authenticate |
| `StatementTimeout` | `1 ms..=24 h`, default `30 s` | Server-side limit for ordinary repository statements |
| `LockTimeout` | `1 ms..=5 min`, default `5 s` | Server-side wait for row/index/advisory locks |
| `IdleTransactionTimeout` | `1 s..=24 h`, default `60 s` | Server protection against abandoned transactions |
| `ConnectionIdleTimeout` | `1 s..=24 h`, default `10 min` | Retire an idle pooled connection |
| `ConnectionMaxLifetime` | `1 min..=7 d`, default `30 min` | Bound connection age and credential/certificate staleness |
| `PoolCloseTimeout` | `1 ms..=5 min`, default `30 s` | Cooperative repository shutdown before reporting incomplete close |

Zero, overflow, contradictory bounds, an acquire timeout longer than pool close,
or a lock timeout longer than its statement timeout is invalid configuration.
Chunk transactions may use a separately typed statement deadline, but it must
be finite and at least the lock timeout. Effective diagnostics expose duration
classes and numeric limits, never endpoints or secrets.

One PostgreSQL repository owns one pool and is created, used, closed, and
dropped on the application-owned Tokio runtime that created it. There is no
process-global pool and no implicit runtime. Clones share the repository-owned
pool; constructing a second repository constructs a distinct pool budget.

Connection acquisition and pre-transaction statements are cancellation-safe.
After a transaction begins, timeout, future cancellation, protocol error,
connection loss, or commit error makes that physical connection suspect. The
adapter attempts protocol cancellation only within its own bounded deadline,
then detaches and closes the connection instead of returning it to the pool.
A commit error is always `UNKNOWN` until a new healthy connection reads durable
metadata. Pool capacity can temporarily shrink while replacement connections
are established.

Supported production TLS is Rustls-backed certificate validation with full
hostname verification. `TlsMode::VerifyFull` uses system roots or an explicitly
configured bounded CA bundle; client identity material uses redacting secret
types. `TlsMode::Plaintext` requires an explicit opt-in and is accepted only for
local or isolated test environments. There is no “accept invalid certificate”
mode.

Safe repository diagnostics may contain the operation/query ID from the
physical model, timeout class, elapsed duration, retry/attempt number, pool
size/idle counts, an allowlisted SQLSTATE, schema version, and opaque execution
identifiers. They exclude connection strings, hostnames, usernames, passwords,
certificate contents/paths, SQL text, bound values, parameters, contexts,
checkpoints, and database-driver debug output.

## Backup, restore, and rollback

Before a schema upgrade:

1. stop or quiesce launchers according to the release runbook;
2. confirm no ambiguous running execution remains;
3. record application, framework, and schema versions;
4. take and verify a restorable backup;
5. apply migration with the dedicated role;
6. validate invariants and representative reads;
7. start canary work before full resumption.

Default rollback is restore from a compatible backup. Reverse SQL is supplied
only when it is tested and cannot discard data required by the previous
version.

At least once per schema-changing release, CI or the release rehearsal takes a
logical `pg_dump` with the migrator/backup identity, restores it into a clean
database with the same PostgreSQL major and required roles, verifies the schema
version and constraints, and runs representative repository reads. Production
guidance additionally requires a deployment-specific physical or managed
backup whose restore objective is tested; the small logical fixture is
conformance evidence, not a substitute for that backup.

## Retention and purge

Deletion is an operator action, not an automatic side effect of execution.
Retention policy must define:

- terminal statuses and minimum age eligible for purge;
- whether job/step contexts or failure summaries have separate retention;
- legal/audit holds;
- batching, locks, and impact on running launches;
- referential deletion order and verification;
- emitted audit evidence.

No purge operation may target a running, stopping, or ambiguous execution.

## Recovery

Stale detection provides evidence; it does not automatically rewrite status.
Recovery records operator, time, prior state, reason, and resulting state.
Ambiguous external side effects require application-specific confirmation
before an execution is made restartable.
