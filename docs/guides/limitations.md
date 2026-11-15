# M7 Release Limitations

**State:** Accepted release-line guidance

**Applies to:** OxideBatch `0.7.0` M7 release line (`v0.7.0`)

This is the readable limitations record for the M7 release line. The
[feature ledger](../compatibility/conformance-matrix.md) remains authoritative
for row status and evidence. Publishing a version never promotes an
`Implemented` or `Partial` row to release-backed `Verified` by itself.

## Supported boundary

- Rust MSRV is 1.95; the pinned development/CI/release toolchain is 1.97.1.
- Linux x86_64 GNU remains the supported runtime target.
- PostgreSQL 15 and 18 are the release-blocking durable-adapter axes;
  PostgreSQL 16 and 17 receive smoke coverage.
- Metadata schema is 12. Upgrade from the published `0.6.0` schema-4 line
  requires quiesced migration and a verified backup; rollback is restore-based.
- Certificate-validated Rustls `verify-full` is the production PostgreSQL
  transport mode. Plaintext remains for isolated/local test environments.
- Deployment remains single-host and embedded in the application process.

## M7 capability included in this release line

The accepted M7 boundary includes:

- advanced compiled sequential/conditional flow, split/nested flow and
  nested-job composition, custom leaf nodes, and durable decision reuse;
- live job/step scope, component factories, typed late binding, bounded
  same-scope dependencies, cleanup, and durable scope provenance;
- repeat context/interceptors and durable repeat identity/state;
- definition registry identity, explicit compatible-upgrade edges, drift
  rejection, fork lineage, and compatible restart selection;
- parameter incrementing and launch identity, start/non-restart controls,
  operator restart/fork semantics, and bounded explorer projections.

These capabilities are backed by the accepted M7 evidence set, including
[M7 exit evidence](../project/m7-exit-evidence.md). Ledger promotion remains
a separate governance action and is not silently performed by the release.

## Explicitly not included

`0.7.0` does **not** claim:

- MySQL, MariaDB, SQLite, SQL Server, Oracle, or another durable metadata
  adapter; database portability/certification begins in M8;
- generic cross-resource exactly-once semantics or universal distributed
  transactions;
- Kafka/AMQP or other broker/stream adapters;
- remote step/partition/chunk execution or a multi-host worker protocol;
- project-wide `1.0`, GA, enterprise readiness, production-readiness across
  all roadmap categories, or complete Spring Batch parity;
- Java/Spring source compatibility.

## Operational caveats

- Runtime startup verifies schema 12 but never migrates automatically.
- The `0.6.0` -> `0.7.0` metadata change is not a rolling upgrade; quiesce
  older writers before migration.
- Definition compatibility is independent of schema compatibility. Changed
  definitions need the accepted registry/upgrade contract and fail closed
  when drift or a missing edge is detected.
- Forking creates explicit new lineage; it is never disguised as restart.
- The shipped CLI remains a guarded repository operator, not a standalone
  job-definition loader. Application work still requires a host application
  that supplies the compiled definition catalog.
- M7 operator fork is a service-level capability; the accepted evidence
  explicitly does not claim new CLI/API-server/hosted control-plane work.

See the [support matrix](../release/support-matrix.md#070-m7-release-line),
[upgrade and rollback guide](upgrade-and-rollback.md), and
[M7 operator fork evidence](../project/m7-operator-fork-evidence.md) for the
bounded release contract.
