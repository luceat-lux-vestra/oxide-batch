# M2 Durable Chunk and Restart Kickoff Gate

**State:** Active (2026-07-29)

**Umbrella:** GitHub issue
[#13](https://github.com/luceat-lux-vestra/oxide-batch/issues/13)

**Kickoff tracking:** GitHub issue
[#38](https://github.com/luceat-lux-vestra/oxide-batch/issues/38)

This record turns the accepted M2 roadmap outcome into definition-ready work.
M2 is active, but implementation may cross a named decision boundary only
after that boundary's gate below is closed.

## Satisfied prerequisites

- [x] M1 is complete and its repository/unit-of-work ports preserve the
      borrowed transaction shape required by M2.
- [x] ADR-0003 accepts PostgreSQL, SQLx, database-authoritative instance
      uniqueness, compare-and-swap execution updates, and outcome-unknown
      handling after an ambiguous commit.
- [x] Architecture spike 0002 proves atomic business/checkpoint commits,
      duplicate-launch serialization, optimistic conflicts, crash boundaries,
      connection discard, and migration version rejection.
- [x] Architecture spike 0003 proves bounded versioned JSON execution-context
      upgrades.
- [x] M1 test support supplies deterministic clocks, identifiers, events,
      timeouts, fixtures, and a reusable repository contract harness.

The original blockers on issue #13 are therefore resolved.

## Decisions required before dependent implementation

| Gate | Owner | Required decision and evidence | Blocks |
| --- | --- | --- | --- |
| Definition compatibility | Runtime/API owner | Closed by [ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md): persisted identity, fail-closed comparison, rejection categories, and directed upgrade rules | Durable restart |
| Physical metadata model | Repository owner | Closed by the [PostgreSQL physical metadata model](../architecture/postgres-physical-metadata-model.md) and executable draft DDL | PostgreSQL adapter and migrations |
| PostgreSQL support | Repository owner | Closed for implementation by the 15–18 matrix, validated Rustls fixture, least-privilege roles, bytewise encodings, and explicit CI images | Supported adapter claim |
| Pool and timeout policy | Repository owner | Closed by the typed bounds, ownership, cancellation, disposal, and safe-diagnostic contract in [persistence operations](../operations/persistence-and-migrations.md) | Production adapter configuration |
| Migration operations | Repository and documentation owners | Closed by immutable naming/checksum rules, version bootstrap/rejection, upgrade matrix, backup/restore fixture, and [guide template](../operations/migration-guide-template.md) | First durable schema release |

These decisions refine accepted ADR-0003 without reopening its choice of
PostgreSQL or SQLx. A change to the public compatibility or transaction
guarantee still requires an ADR or RFC.

## PostgreSQL M2 matrix

M2 targets PostgreSQL majors 15 through 18. PostgreSQL 15 is the oldest
release-blocking integration target and PostgreSQL 18 is the newest. Full
repository, transaction, migration, TLS, and restart suites run against both;
intermediate supported majors receive at least connection, migration, and
vertical-slice smoke coverage.

PostgreSQL 14 is not promoted into the M2 support promise because its upstream
final release is scheduled for November 2026, before a durable OxideBatch
release is expected. The matrix is reviewed at M2 exit against the
[PostgreSQL versioning policy](https://www.postgresql.org/support/versioning/).
CI uses explicit major tags rather than `latest`.

Supported production connectivity requires certificate-validated TLS through
the Rustls-backed SQLx path. Plaintext connections remain available for local
and isolated test environments and are never presented as the production
default.

## Delivery workstreams and order

1. [#39](https://github.com/luceat-lux-vestra/oxide-batch/issues/39) closes
   the definition, physical-schema, support, pool, and migration decision gates
   with reviewable contracts and fixtures.
2. [#40](https://github.com/luceat-lux-vestra/oxide-batch/issues/40) adds
   facade-owned reader, processor, writer, chunk, checkpoint, and bounded
   execution-context contracts plus reusable component tests.
3. [#41](https://github.com/luceat-lux-vestra/oxide-batch/issues/41) adds
   the PostgreSQL adapter, versioned migrations, least-privilege role fixtures,
   and the shared repository contract suite.
4. [#42](https://github.com/luceat-lux-vestra/oxide-batch/issues/42)
   implements chunk orchestration with deterministic counters, listener
   boundaries, cooperative stop, and rollback behavior.
5. [#43](https://github.com/luceat-lux-vestra/oxide-batch/issues/43) enlists
   PostgreSQL business writes with checkpoint, context, counters, and optimistic
   version in one adapter-owned transaction.
6. [#44](https://github.com/luceat-lux-vestra/oxide-batch/issues/44)
   implements durable launch/restart selection and explicit orphan/unknown
   recovery classification.
7. [#45](https://github.com/luceat-lux-vestra/oxide-batch/issues/45) runs the
   crash matrix and M2 conformance slice, publishes setup/recovery
   documentation, and records exit evidence.

Core contract and deterministic fixture work may proceed while PostgreSQL
operational decisions are reviewed. Durable schema, adapter, and restart work
must follow the gates that govern them.

All five design gates were closed on 2026-07-29. Issue #41 still owns the
adapter, released migrations, and full repository evidence; closing design does
not promote the draft fixture into a supported schema. The criterion-by-
criterion mapping is retained in the
[M2 design-gate evidence](m2-design-gate-evidence.md).

Issue #40's facade-owned component, transaction-enlistment, checked-count, and
bounded durable-state contracts are complete on merge. Their criterion mapping
and implementation handoff are retained in the
[M2 component-contract evidence](m2-component-contract-evidence.md).

Issue #41's PostgreSQL adapter, released schema-v1 migration, redacting
configuration, shared repository contract, TLS/role matrix, disconnect
classification, and migration evidence are complete on merge. The mapping and
handoff to chunk orchestration are retained in the
[M2 PostgreSQL repository evidence](m2-postgres-repository-evidence.md).

Issue #42's deterministic chunk orchestrator, checked committed-only counters,
listener nesting, cooperative stop/rollback behavior, lifecycle integration,
chunk events, late acknowledgement, and explicit unknown-commit path are
complete on merge. Their criterion mapping and handoff to PostgreSQL atomic
enlistment are retained in the
[M2 chunk runtime evidence](m2-chunk-runtime-evidence.md).

Issue #43's adapter-owned PostgreSQL chunk transaction, borrowed business-write
port, checkpoint/context/counter/version CAS, healthy-connection inspection,
known rollback, ambiguous commit classification, and PostgreSQL 15/18
integration matrix are complete on merge. Their criterion mapping and handoff
to durable restart selection are retained in the
[M2 PostgreSQL atomic chunk transaction evidence](m2-postgres-chunk-transaction-evidence.md).

## Definition of done

M2 closes only when:

- `CHUNK-COMMIT-001`, `CHUNK-ROLLBACK-001`, `RESTART-001`, durable
  `JOB-EXEC-001`, `JOB-CONCURRENCY-001`, and durable `OBS-INSPECT-001` pass;
- committed chunks are not replayed and uncommitted chunks follow the
  documented at-least-once boundary;
- business writes, checkpoint, context, counters, and execution version commit
  or roll back together for enlisted PostgreSQL writers;
- commit ambiguity is represented as `UNKNOWN` and requires durable inspection
  plus an explicit recovery decision;
- repository migrations pass from every supported schema version and reject a
  newer schema;
- PostgreSQL 15 and 18 integration gates pass in CI, including validated TLS
  and least-privilege runtime/migrator roles;
- the crash worker covers every injection point in the first vertical slice;
- public APIs and diagnostics expose no SQLx types, credentials, record
  contents, or execution-context payloads;
- PostgreSQL setup, migration, transaction guarantee, crash/restart, and
  recovery documentation is executable and reviewed.

## Scope controls

M2 does not include retry/skip policies, conditional multi-step flow, automatic
orphan takeover, a recovery CLI, distributed execution, or exactly-once effects
for external resources. Those remain assigned to later roadmap gates.
