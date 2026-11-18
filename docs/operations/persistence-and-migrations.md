# Persistence and Migration Operations

**State:** Accepted

This document is the canonical operational policy for repository capabilities,
schema lifecycle, adapter certification, export/import, retention, and
upgrade/downgrade. The
[repository and transaction model](../architecture/repository-and-transaction-model.md)
owns accepted target port boundaries and delivery capabilities.

## Current and target scope

PostgreSQL and the OxideBatch-owned schema remain the reference semantic and
release-backed supported durable profile under
[ADR-0006](../architecture/decisions/0006-repository-capability-model.md).
MySQL/MariaDB and SQLite now have first-party bootstrap-core implementations,
but they are not yet runnable-core certified or support-promoted. SQL Server
remains a planned M8 target. MongoDB now has an `Implemented` support-level-core
candidate on main for versions 8.0.38 and 8.2.12 with schema 3; its optional
capabilities remain explicit and fail closed. This does not add MongoDB to a
released support promise: release-backed support still requires a named
OxideBatch release and support-matrix promotion.

M8 uses four deliberately different terms:

- **implemented bootstrap adapter:** code exists and passes the primitive
  repository contract;
- **runnable-core certified adapter:** an ordinary tasklet `FlowLauncher`,
  manifest-digest strict restart, revision-label compatibility, logical
  flow-step/start-limit authority, and durable decision reuse pass the portable
  runnable-core contract;
- **capability-certified adapter:** one or more additional named capability
  suites pass;
- **release-backed supported adapter:** named database/client/configuration
  versions appear in the support matrix with all required operational evidence.

The PostgreSQL implementation remains the reference semantic profile. Another
adapter may use different SQL, keys, locking, or migration machinery, but it
must preserve every capability it claims and disclose every limitation. A
lowest-common-denominator abstraction must not remove a certified PostgreSQL
fast path.

## Repository capability negotiation

The target versioned capability descriptor includes:

- schema and migration versions plus supported upgrade sources;
- uniqueness, compare-and-swap, isolation, locking, server-time, lease, and
  fencing behavior;
- transaction and delivery modes, including same-resource enlistment and
  unknown-commit classification;
- maximum identifier, parameter, context, checkpoint, page, and batch sizes;
- pagination, streaming, retention, archive, export/import, backup, restore,
  and rolling-upgrade features.

Static requirements are checked when a plan is compiled. Actual
database/server features are negotiated at connection or launch. Missing
capabilities fail explicitly; they never silently weaken restart or delivery
semantics.

## Adapter support and certification

The accepted relational program evaluates PostgreSQL, MySQL/MariaDB, SQLite,
SQL Server, Oracle, DB2, and HANA. The additive runnable-core contract is a
pre-support execution gate; it does not replace checkpoint/context restart or
the broader operational matrix below. Each promoted adapter must pass:

- the shared repository, explorer, operator, definition, retention, and
  migration contract suites;
- duplicate-launch, concurrent restart/stop, optimistic-conflict, stale-lease,
  and fencing tests relevant to its capabilities;
- crash/disconnect/unknown-commit tests;
- every supported schema upgrade plus backup/restore;
- query-plan/index, pagination, time precision, TLS/authentication, privilege,
  and resource-bound evidence;
- its feature-ledger and support-matrix rows.

Certification names exact product and adapter versions and a support tier.
External certification may supply evidence where CI licensing prevents a
first-party matrix, but it cannot omit schema, migration, locking, or
concurrency semantics.

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

The MongoDB metadata adapter has a separate unreleased schema lineage.
Schema version 2 existed as an implementation-stage runnable-core layout, and
schema version 3 added the durable checkpoint/context state required by the
current restart contract. No MongoDB schema version 1 layout was ever defined,
and version 2 is not a release-backed migration source. The current support
candidate therefore bootstraps an uninitialized database directly to schema
version 3; runtime connection and migration accept exactly version 3, reject an
installed marker below 3 with `MigrationRequired`, and reject a marker above 3
with `NewerSchema`. The first MongoDB schema change after a release-backed
version 3 profile must define and certify every older source version it claims
to support, including pre-upgrade backup and restore-only rollback evidence. A
numeric marker alone never creates a compatibility claim. This policy does not
promote MongoDB support.

There is no released durable schema before M2 version 1. Therefore the first
upgrade matrix contains the empty/uninitialized fixture and version 1. From the
second durable schema onward, CI restores realistic fixtures for every released
source version still supported, migrates each to the target, and runs repository
plus vertical-slice reads. Fixtures include active, terminal, failed, stopped,
and `UNKNOWN` executions without storing real user records.

The released source versions are `1` and `2`. The design-gate fixture applies
migration `0001` to a dedicated database, seeds
`design-gate/schema1-seed.sql` with completed, failed-with-active-restart,
stopped, and unresolved `UNKNOWN` history, applies migration `0002`, and runs
`design-gate/verify-schema2-upgrade.sql`. Reapplying `0002` must fail with the
`schema version 1 is required` guard rather than modify an upgraded database.

Every schema-changing release includes a guide copied from
[the migration-guide template](migration-guide-template.md). The guide names
source/target versions, application compatibility, lock/downtime expectations,
backup and restore commands, invariants, canary queries, rollback, and recovery
from every non-transactional phase.

The accepted, unreleased schema-2 design is documented in
[fault-tolerance and flow migration](migrations/0002-fault-tolerance-and-flow.md).
It requires a quiesced transactional schema-1-to-2 upgrade, supports no mixed
schema-1/schema-2 writers, leaves format-1 manifests byte-identical, and uses
verified backup restore rather than destructive reverse SQL.

The accepted, unreleased schema-3 design is documented in
[operations and local-scale migration](migrations/0003-operations-and-local-scale.md).
It adds ownership and stop evidence, one instance hold, operator and retention
audit, and durable local partitions. It requires a quiesced transactional
schema-2-to-3 upgrade, performs no backfill, supports no mixed
schema-2/schema-3 writers, leaves formats 1 and 2 byte-identical, and uses
verified backup restore rather than destructive reverse SQL.

### Schema-version lifecycle and rolling operation

Each adapter documents:

- versions the runtime can read/write and versions the migrator can upgrade;
- whether N and N-1 application binaries may run concurrently during a rolling
  deployment;
- the expand/migrate/contract phases, quiescence requirements, and feature
  flags that protect mixed-version operation;
- downgrade capability, if any, and data introduced by the new version that an
  old runtime cannot preserve.

Compatibility is fail-closed. “Rolling upgrade supported” is a release-specific
claim with mixed-version tests. Default downgrade remains restore from a
verified compatible backup; reverse migration is provided only when it is
non-destructive and tested.

### Definition and context codec migrations

Definition manifests, compiled-plan manifests, parameters, execution contexts,
checkpoints, external blob references, and protocol payloads have independent
schema/codec versions. A database schema migration does not imply their
compatibility.

Readers reject unknown newer versions. A directed definition/context upgrade
is bounded, deterministic, checksum-verified, and commits atomically with the
new execution or import record. Failed upgrades leave prior durable bytes and
execution lineage unchanged.

### MongoDB authentication and runtime privilege baseline

The M8 MongoDB profile treats authentication and database privilege as adapter
certification inputs, not as application-owned implementation details. The
connection URI remains private adapter configuration and may carry credentials
and `authSource`; `MongoConfig` redacts the URI from `Debug` output. Production
credentials must still come from deployment secret management rather than
repository source, migration files, or retained evidence.

The executable authenticated fixture uses three disposable principals:

- a fixture bootstrap administrator exists only to create the test roles and
  users and is never passed to `MongoMigrator` or `MongoJobRepository`;
- the **migrator** role is scoped to the OxideBatch metadata database and may
  read/write repository collections plus create collections and indexes needed
  by the controlled schema bootstrap; it has no cluster or user-administration
  role;
- the **runtime** role may `find` within the metadata database and may
  `insert`, `update`, and `remove` only from the runtime-owned collections
  `ob_counter`, `ob_core_job_definition`, `ob_job_instance`,
  `ob_job_execution`, `ob_step_execution`, and `ob_flow_decision`. It
  receives no write authority over `ob_schema`, no index/collection DDL, and
  no user or cluster administration.

The pinned MongoDB certification lane proves the migrator can initialize the
schema with its scoped credential, the runtime can open a repository and commit
ordinary metadata DML with its narrower credential, schema-marker mutation and
index creation are denied to the runtime, user-administration inspection is
denied, and an invalid password fails closed as `RepositoryError::Unavailable`.

This is an executable least-privilege baseline, not a production credential
bootstrap mechanism. Deployments may use different usernames and secret
providers, but broad `root`, `clusterAdmin`, `userAdminAnyDatabase`, or
cross-database runtime grants are outside this profile.

### MongoDB TLS transport baseline

The MongoDB support-level profile requires certificate-validated TLS with
server-identity verification for production-like transport. The connection URI
may carry `tls=true` and a deployment-managed `tlsCAFile`; the URI remains
redacted by `MongoConfig`. Disabling certificate validation, disabling
hostname verification, or using broad insecure TLS modes is outside this
profile.

The executable TLS fixture creates a private CA and a server certificate whose
only subject alternative name is `127.0.0.1`, starts MongoDB with
`requireTLS`, and uses the same pinned 8.0.38 and 8.2.12 images as the
repository certification matrix. It requires a valid CA plus matching server
identity to migrate the schema, open `MongoJobRepository`, and commit ordinary
metadata DML. It separately requires wrong-CA trust, hostname mismatch, and a
plaintext connection to the TLS-only server to fail closed as
`RepositoryError::Unavailable` within bounded connection/server-selection
deadlines.

Production deployments own CA rotation, certificate issuance, filesystem
permissions, and secret delivery. OxideBatch does not mint production
certificates and does not weaken driver verification to recover from expired,
untrusted, or identity-mismatched certificates. Plaintext remains appropriate
only for explicit isolated development/test fixtures and is not part of this
MongoDB support-level transport profile.

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

For the first MongoDB support candidate, the portable Gate-C backup/restore
floor is exercised as a same-schema version-3 rehearsal on each pinned MongoDB
8.0.38 and 8.2.12 lane. The fixture creates bounded deterministic metadata
through the ordinary repository API, closes the application writers, takes a
logical `mongodump`, records a SHA-256 digest and bounded archive size, and
restores with `mongorestore` into a separate clean replica set running the
same named MongoDB version. The restore is verified **without running the
migrator first**, so missing collections or indexes cannot be silently repaired
before certification.

Post-restore verification reopens the ordinary `MongoJobRepository` and
reconciles schema version, durable job/step identity and lifecycle state,
checkpoint/context restart authority, durable flow decisions, and the required
unique-index set against the source fixture. It then performs a real restart
mutation plus a fresh completed flow and requires all restored ID allocators to
advance beyond pre-backup identities. A successful dump/restore process exit is
therefore insufficient by itself. The logical fixture is disposable
conformance evidence; production deployments still own authenticated backup
identities, encryption, retention, physical or managed backup policy, and
tested restore objectives. This evidence does not by itself promote MongoDB to
a release-backed support tier.

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

Archive and purge primitives are bounded, paginated, resumable, and
idempotent where specified. They preserve legal/audit holds, definition and
migration lineage required to interpret retained executions, and a
verification summary. Export is not a successful archive until checksums,
record counts, and restore/read evidence pass.

The M4 slice implements only holds and a guarded two-phase purge of terminal
history on the PostgreSQL reference adapter, as specified by the
[operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md).
A purge plan returns bounded candidates and a plan digest; application
re-validates eligibility and observed versions and rejects a stale plan
without deleting anything. Purge requires the operator-writer role and its
narrowly granted deletes; the runtime role cannot purge and the operator-reader
role can plan but not apply. Archive packages, export/import, checksum
verification of exported data, retention policy storage, scheduled purge, and
cross-adapter portability remain M8 scope.

## Metadata export and import

The accepted neutral package contains a versioned manifest, source framework
and schema versions, counts, checksums, lineage, bounded records, and redaction
classification. It may represent definitions, parameters, instances,
executions, step executions, statuses, counters, contexts with approved codecs,
and retention metadata.

Export/import MUST:

- use a dedicated role and a quiesced or explicitly snapshot-consistent source;
- validate size, depth, count, checksum, identity, and referential invariants;
- support dry-run and isolated-schema rehearsal;
- be idempotent by package/source identity;
- record mapping/tool versions and every omitted or transformed field;
- fail closed on unknown context, definition, or status semantics;
- reconcile counts, fingerprints, statuses, and representative explorer reads;
- preserve a rollback path through backup/restore.

The [Spring Batch migration contract](../compatibility/spring-batch-migration.md)
owns Spring-specific extraction and mapping. Spring Batch and OxideBatch never
mutate one live metadata schema. Import tooling does not translate arbitrary
Java code.

## Recovery

Stale detection provides evidence; it does not automatically rewrite status.
Recovery records operator, time, prior state, reason, and resulting state.
Ambiguous external side effects require application-specific confirmation
before an execution is made restartable.

The M4 evidence, clock, digest, and permitted-result rules are owned by the
[M4 shutdown and stale-recovery contract](../architecture/shutdown-and-recovery.md).
Ownership tokens recorded from schema 3 are evidence only; they are not leases
and never authorize takeover.
