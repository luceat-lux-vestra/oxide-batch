# RFC-0010: Metadata Export/Import and Spring Batch Migration

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** compatibility and repository maintainers
- **Target milestone:** M12
- **Related ADRs:** [ADR-0003](../architecture/decisions/0003-postgres-metadata.md),
  [ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md)

## Summary

Define a neutral versioned job IR and metadata package, a Java-side Spring Batch
extractor, one-way OxideBatch import, mapping reports, dry-run, validation,
lineage, and reconciliation—without arbitrary Java translation or shared live
schemas.

## Context and current accepted rule

ADR-0003 requires an OxideBatch-owned schema and identifies a future import
tool instead of live coexistence. ADR-0004 requires explicit definition
identity and context upgrades. Metadata import is outside the original 1.0
promise.

## Problem

Feature parity alone does not make existing jobs or history migratable.
Spring definitions may include container wiring, Java code, expressions,
custom serializers, and resource transactions that cannot be inferred safely.

## Proposal

- Define the neutral bounded IR and mapping workflow in the Spring migration
  contract.
- Build a pinned Java-side extractor for executable Spring definitions.
- Classify each construct as exact/native, partial, manual port, unsupported,
  deferred, or not applicable.
- Generate Rust definition/component stubs only where semantics are explicit.
- Define a versioned checksummed metadata export/import package.
- Import one way into a quiesced OxideBatch-owned schema with lineage,
  idempotency, dry-run, rehearsal, reconciliation, and backup rollback.
- Map only reviewed context codecs and definition fingerprints.
- Never claim automatic arbitrary Java source/bytecode translation or live
  bidirectional/shared-schema operation.

## Alternatives

1. Read Spring tables directly at runtime. Rejected due to schema/behavior
   coupling and concurrent ownership risk.
2. Translate Java source automatically. Rejected because arbitrary behavior,
   transactions, and dependencies cannot be proven.
3. Provide only a written guide. Insufficient for repeatable metadata
   reconciliation.
4. Import history without definitions/context. Useful only for archive and
   cannot support restart; must be a separately labeled profile if offered.

## Consequences

Migration tools need Java and Rust release ownership, security review, fixture
maintenance, and version matrices. Many custom components require manual
porting. Users receive an auditable assessment instead of false automation.

## Compatibility impact

Migration compatibility is named by exact Spring source, extractor, IR,
OxideBatch, and target schema versions. It does not imply Java API, live schema,
or behavioral parity beyond verified ledger rows.

## Metadata, restart, and transaction impact

Imported identities, statuses, counters, context, and lineage must satisfy
OxideBatch invariants. Running/ambiguous source executions are not made
restartable automatically. Context/definition mappings are explicit. Import
transactions are bounded and resumable; a partial package cannot appear
complete.

## Migration and rollout

Start with dry-run definition mapping, then isolated archive-only metadata,
then restart-capable profiles for reviewed contexts. Rehearse backup, import,
reconciliation, canary, and restore. Each source version gets immutable
fixtures and a migration guide.

## Validation and evidence plan

- IR/package parser fuzzing, bounds, checksums, and provenance;
- standard graph/policy/component mapping fixtures;
- unsupported Java/custom context reports;
- all supported source schema/status/parameter/context fixtures;
- repeated/partial/corrupt import and restore;
- fingerprint/lineage/count/explorer reconciliation;
- differential traces and at least five reference workloads;
- security/redaction and least-privilege role tests.

## Unresolved questions

- Initial IR and package encoding.
- First supported Spring schema/application versions beyond 6.0.4.
- Archive-only versus restart-capable import profiles.
- Distribution and support model for the Java extractor.

## Decision

**Accepted by the project owner on 2026-07-30.**

The one-way neutral IR/package, mapping report, dry-run, lineage, and
reconciliation boundary is accepted. Production tooling still requires
reviewed schemas, a mapping prototype, security/license review, and migration
fixtures. Shared live schemas and arbitrary Java-code translation remain
permanent non-goals.
