# M2 Durable Metadata Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#39](https://github.com/luceat-lux-vestra/oxide-batch/issues/39)

**Date:** 2026-07-29

This record maps the first M2 workstream's exit criteria to reviewable
contracts and executable evidence. It authorizes implementation work; it does
not claim that the PostgreSQL adapter or a released schema exists.

| Exit criterion | Evidence |
| --- | --- |
| Persisted definition identity and restart rules | [ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md) defines revision drift, canonical manifests, exact comparison, fail-closed rejection, directed upgrades, and atomic upgrade failure. |
| Complete physical metadata model | [Physical model](../architecture/postgres-physical-metadata-model.md) and `tests/fixtures/postgres/design-gate/0001_draft_metadata.sql` name every table, key, constraint, index, owned query, context/checkpoint column, and optimistic invariant. |
| Stable identifiers and instance keys | The physical model fixes byte limits, `C` collation, raw 32-byte digests, positive `bigint` IDs, and version-1 length-prefixed typed instance-key encoding. |
| PostgreSQL/TLS/roles/CI fixtures | `.github/workflows/ci.yml` runs explicit PostgreSQL 15–18 images. `run-design-gate.sh` provisions validated Rustls `verify-full` TLS, migrator/runtime/operator roles, DDL/DML separation, and restore evidence. |
| Pool/timeouts/cancellation/diagnostics | [Persistence operations](../operations/persistence-and-migrations.md) fixes facade-owned bounded value types, defaults, runtime/pool ownership, server timeouts, suspect-connection disposal, `UNKNOWN` commit handling, and safe diagnostic fields. |
| Migration operations | Persistence operations and the [migration-guide template](../operations/migration-guide-template.md) define immutable names/checksums, singleton bootstrap, empty-to-v1 and future all-version fixtures, newer-schema rejection, and backup/restore rehearsal. |
| Facade isolation | The facade crate has no SQLx or Rustls dependency, existing compile-fail coverage rejects a PostgreSQL driver re-export, and the accepted contracts prohibit URL, credential, TLS implementation, pool, row, error, and transaction types. |

## Reproduction

Fast repository gates:

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

One real PostgreSQL axis:

```console
./tests/fixtures/postgres/run-design-gate.sh 18
```

The CI matrix supplies `15`, `16`, `17`, and `18`. The fixture applies schema
DDL as the migrator, writes as runtime, verifies runtime DDL and operator-reader
writes are denied, checks TLS session state through SQLx/Rustls, restores a
logical backup into a clean database, then proves schema version `2` is rejected
by the version-`1` contract.

## Boundary handed to implementation

Issue #40 can implement facade component/context contracts against ADR-0004.
Issue #41 owns the internal PostgreSQL crate, production configuration types,
immutable migration promotion, golden instance-key vectors, repository query
implementations/plans, and the shared repository contract suite. The draft DDL
may change during that implementation only through a reviewed update to this
accepted model; after its first durable release, changes are forward migrations
only.
