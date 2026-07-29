# M2 PostgreSQL Repository and Migration Evidence

**State:** Complete on merge

**Issue:** [#41](https://github.com/luceat-lux-vestra/oxide-batch/issues/41)

**Date:** 2026-07-29

This record maps the third M2 workstream's exit criteria to the released
schema, adapter implementation, and executable PostgreSQL evidence. It does not
claim that chunk orchestration, enlisted business writes, or durable restart
selection exists; issues #42–#44 own those capabilities.

| Exit criterion | Evidence |
| --- | --- |
| Immutable approved schema | `crates/oxide-batch/migrations/0001_initial_metadata.sql` creates schema version 1 with bounded names and JSON objects, positive IDs, foreign keys, canonical instance uniqueness, one-unresolved-execution enforcement, optimistic versions, checkpoint/context columns, recovery audit rows, and named indexes. SQLx records and rechecks the released migration checksum. |
| Schema startup and migration policy | Runtime `connect` reads only the singleton version and returns `SchemaUninitialized`, `MigrationRequired`, or `NewerSchema` without auto-migrating. `PostgresMigrator` uses a bounded advisory lock, applies the embedded contiguous migration set, rejects newer schemas before mutation, and is idempotent. |
| Shared repository contract | `tests/postgres_repository.rs` runs the reusable `run_repository_contract` cases against PostgreSQL. The same cases cover the in-memory reference repository. PostgreSQL 15 and 18 run released migrations plus the full contract; the 15–18 design matrix also runs the full contract against least-privilege fixtures. |
| Database-authoritative launch serialization | PostgreSQL identity columns allocate durable IDs. Instance insertion uses the unique `(job_name, instance_key)` constraint with `ON CONFLICT DO NOTHING` and reads the authoritative row. Execution creation locks the instance, classifies the latest status, allocates the next attempt, and relies on both attempt uniqueness and the partial unresolved index. |
| Stable instance identity | The adapter implements the accepted version-1 length-prefixed, typed, byte-ordered encoding and SHA-256 digest. A golden vector includes UTF-8, boolean, signed, and high unsigned values; persisted parameter JSON preserves value type and remains redacted from diagnostics. |
| Typed optimistic conflicts | Job and step mutations validate the facade snapshot, update with `WHERE version = expected`, increment exactly once, and reread zero-row outcomes as `LifecycleError::StaleVersion` or a typed missing record. |
| Least privilege and validated TLS | `run-design-gate.sh` provisions separate migrator, runtime, operator-reader, and operator-writer roles on PostgreSQL 15–18. It proves runtime DML, denied runtime DDL, denied operator-reader writes, Rustls `verify-full` adapter connection, and logical backup/restore. |
| Cancellation and ambiguous commit | A unit of work owns one checked-out connection after explicit `BEGIN`. Drop/cancellation, setup failure, protocol failure, rollback failure, and commit failure mark it close-on-drop so it cannot re-enter the pool. Commit failure is always `CommitOutcomeUnknown`; integration evidence terminates the backend, observes that classification, and verifies pool replacement. |
| Bounded safe configuration | `PostgresConfig` enforces pool size and all accepted timeout relationships. Production defaults use `TlsMode::VerifyFull`; plaintext is explicit. `Debug`, `Display`, and mapped repository errors exclude connection strings, endpoints, identities, passwords, certificate paths/contents, SQL, bound values, and payloads. |
| Facade isolation | The `postgres` feature is optional. Public APIs contain facade and standard-library types only; SQLx pool, connection, row, migration, TLS implementation, and error types remain private. Existing compile-fail fixtures continue to reject SQLx leakage. |

## Released migration provenance

| Version | File | SHA-256 |
| --- | --- | --- |
| 1 | `0001_initial_metadata.sql` | `612ef00037e65095cb43391ee1b30164b95d2c61d2738e0d169a38a77bcc3d96` |

Released migration bytes are immutable. Any correction is a new contiguous
migration and must add its checksum to release provenance.

## Reproduction

Fast repository gates:

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

One real PostgreSQL/TLS/role/contract axis:

```console
./tests/fixtures/postgres/run-design-gate.sh 18
```

CI runs that design fixture for PostgreSQL 15, 16, 17, and 18. Separate
released-schema jobs start empty PostgreSQL 15 and 18 databases, apply and
reapply migration 1, run the shared contract, terminate an active transaction
backend to prove unknown-commit handling, verify pool recovery, and reject
schema version 2.

## Boundary handed to implementation

Issue #42 may orchestrate reader, processor, writer, stop, listener, and
checked-counter outcomes while using either repository implementation for
lifecycle metadata. Issue #43 may extend the adapter-owned connection boundary
to lend `BusinessTransaction` and atomically update the already released
checkpoint, context, counter, and step-version columns. Issue #44 may use the
definition, execution-attempt, context, and recovery tables for durable
restart selection and explicit recovery decisions.
