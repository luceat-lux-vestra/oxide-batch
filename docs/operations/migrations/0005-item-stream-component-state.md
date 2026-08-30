# Schema 4 Item-Stream Component-State Migration

**State:** Candidate release guidance for `oxide-batch` `0.6.0`; not yet
published

**Source schema:** `3`

**Target schema:** `4`

**Rolling deployment:** Not supported. Every schema-3 writer must be quiesced
before migration; a schema-3 runtime rejects schema 4 on startup.

**Immutable SQL:**
`crates/oxide-batch/migrations/0005_item_stream_component_state.sql`

This migration adds the durable component-state envelope required by the M6
`ItemStream` lifecycle. It is additive and does not rewrite existing
definitions, checkpoints, execution counters, or context payloads.

## Application

`PostgresMigrator::migrate` applies migration `0005` after the accepted schema-3
sequence and advances the singleton version to `4` in the same migration
transaction. It requires schema version `3` before making any change. A fresh
database receives the complete migration sequence; an existing schema-3
database receives only the pending `0004` corrective patch, if applicable, and
`0005`.

The new `ob_component_state` table stores one bounded, versioned state envelope
per `(step_execution_id, namespace)`. Inline payload bytes remain exact bytes
for checksum verification; external payload references retain their content
identity and encoded length. Existing rows have no component state and remain
unchanged.

## Upgrade procedure

1. Stop all schema-3 launchers and wait for bounded shutdown.
2. Resolve or explicitly retain active, orphaned, and `UNKNOWN` executions.
3. Record application, OxideBatch, PostgreSQL, and schema versions.
4. Verify migrator/runtime/operator roles and production `verify-full` TLS.
5. Take a logical backup and prove restoration into an isolated database.
6. Run `PostgresMigrator::migrate` with the migrator identity.
7. Verify exactly one singleton row reports version `4`, the new table's
   constraints and index exist, and representative pre-existing metadata is
   unchanged.
8. Run a schema-4 canary that opens, checkpoints, reloads, and restarts an
   `ItemStream`, including checksum and version-rejection cases, before
   resuming normal traffic.

The migration does not provide a rolling upgrade. A schema-3 runtime refuses
schema 4 as `NewerSchema`; no runtime applies migrations automatically.

## Rollback

There is no reverse SQL. Rollback means stopping every schema-4 writer,
preserving diagnostics, discarding the migrated database or schema, restoring
the verified schema-3 backup, and resuming through a schema-3 canary with the
old runtime. Work and component state created after the backup are lost and
must be reconciled with application-owned business effects.

## Release evidence

The M6 Gate B and full component campaigns exercise component-state atomicity,
restart selection, corruption rejection, and PostgreSQL 15/18 behavior. The
release checklist additionally requires package, clean-consumer, checksum,
provenance, and PostgreSQL release-smoke evidence before any candidate is
promoted to released compatibility status.
