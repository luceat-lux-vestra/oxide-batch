# Persistence and Migration Operations

**State:** Proposed

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
- A migration is transactional when PostgreSQL permits it; non-transactional
  steps require an explicit resume/repair procedure.
- Startup never performs an unannounced destructive migration.
- The runtime rejects a schema newer than it understands.
- Compatibility during rolling application deployment is documented per
  release; it is not assumed.
- Each release tests upgrades from every supported source version using realistic
  metadata fixtures.

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
