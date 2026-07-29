# Spring Batch Migration Contract

**State:** Accepted

**Governing decision:**
[RFC-0010](../rfcs/0010-metadata-and-spring-migration.md)

This document is the canonical target contract for moving Spring Batch job
definitions and metadata to OxideBatch.

## Goals

- analyze a pinned Spring Batch 6.x job definition through a Java-side
  extractor;
- emit a neutral, versioned job manifest/IR;
- map standard graph, policy, parameter, context, status, and component
  constructs to explicit OxideBatch equivalents;
- report unsupported or divergent constructs without guessing;
- export/import selected historical metadata one way into an
  OxideBatch-owned schema;
- support dry-run, validation, reconciliation, and auditable cutover.

## Non-goals

The tooling does not translate arbitrary Java source, bytecode, closures,
custom tasklets, processors, listeners, Spring Beans, SpEL, application
transactions, or third-party integrations into correct Rust automatically.
Spring Batch and OxideBatch do not concurrently mutate one metadata schema.
No live bidirectional replication or Java API/source compatibility is implied.

## Neutral manifest/IR

The IR contains a schema version, Spring source version, extracted job and step
graph, stable source identifiers, parameter definitions, flow transitions,
restart and fault-tolerance policies, scoped/late-bound references, standard
component descriptors, context schemas, and source evidence. It excludes
credentials, item data, arbitrary serialized Java objects, and executable code.

The OxideBatch analyzer produces a mapping report with:

- exact/native equivalent, partial equivalent, manual port, unsupported,
  deferred, or not-applicable disposition;
- affected feature-ledger IDs and source references;
- required Rust component stubs and capability declarations;
- semantic, operational, transaction, and data differences;
- blockers, warnings, and evidence required before cutover.

## Definition workflow

1. Pin the Spring Batch, application, extractor, and IR versions.
2. Extract a definition from a controlled Java environment.
3. Validate and bound the IR before processing.
4. Generate the mapping report and Rust-native definition skeleton.
5. Implement and review custom components manually.
6. Compile the OxideBatch plan and compare normalized traces.
7. Run synthetic and reference workloads through failure/restart fixtures.
8. Approve cutover only when every relevant ledger row has a disposition.

Spring definition identity maps to the OxideBatch manifest/fingerprint through
a versioned mapping record; it is never inferred from a name alone.

## Metadata export/import

Export uses a neutral, bounded, versioned package with manifest, checksums,
source schema/application versions, counts, and redaction classification.
Supported records may include job parameters, instances, executions, step
executions, statuses, exit descriptions, counters, and explicitly supported
execution-context values.

Arbitrary Java-serialized context is not trusted or automatically decoded.
Each context key needs a reviewed codec/mapping. Unknown keys are reported and
block a restart-capable import unless an explicit omission policy proves they
are irrelevant.

Import writes only through a dedicated migrator into a quiesced
OxideBatch-owned schema. It is idempotent by source identity, validates
referential and count invariants, records lineage and mapping versions, and
never silently marks ambiguous/running source work restartable.

## Dry-run, cutover, and rollback

Dry-run validates the package, reports mappings and capacity, and performs no
mutation. A rehearsal imports into an isolated schema, runs explorer queries,
compares counts and normalized states, and executes reference restarts where
supported.

Production cutover requires source quiescence, verified backup, final export,
checksum verification, import, reconciliation, application deployment, and a
canary. Rollback restores the pre-import OxideBatch backup or abandons the
isolated target; it does not resume dual writes.

## Evidence

Required fixtures cover each source schema/version, identifying parameters,
statuses, counters, context codecs, definition mappings, corrupted/oversized
packages, repeated import, partial failure, backup/restore, and at least five
representative workloads. Differential traces demonstrate observable behavior;
documentation alone cannot certify migration compatibility.
