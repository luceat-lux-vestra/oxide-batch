# ADR-0003: PostgreSQL Metadata Repository

- **State:** Accepted
- **Date:** 2026-07-29
- **Owners:** maintainers
- **Deciders:** project owner

## Context

Restart, duplicate-launch prevention, optimistic locking, and checkpoint
integrity depend on concrete transactional behavior. Supporting multiple
databases before those guarantees are proven would hide important differences.

## Decision

PostgreSQL is the only durable repository targeted for 1.0. Use SQLx as the
initial adapter and migration mechanism. OxideBatch owns a versioned metadata
schema; it does not share Spring Batch tables. Repository contracts remain
independent of SQLx.

Store schema version explicitly. Every released forward migration must be
tested from all supported source versions. Downgrade support is documented per
release rather than assumed.

Database uniqueness is authoritative for job-instance identity. Contending
launches insert under a unique `(job_name, instance_key)` constraint and handle
the conflict result; application-side read-then-insert checks are not
sufficient. Mutable execution rows use compare-and-swap updates whose expected
version is part of the `WHERE` clause, with zero affected rows classified as an
optimistic-lock conflict.

Transactional PostgreSQL writers receive a borrowed OxideBatch-owned business
transaction port. The PostgreSQL adapter owns the SQLx transaction and commits
business rows, counters, context/checkpoint, and optimistic version together.
Writers targeting another resource cannot claim this atomic guarantee.

Commit acknowledgement is binary only on success. Query cancellation,
connection loss, or a commit error makes the affected connection ineligible
for pool reuse. The general commit outcome is `UNKNOWN` until recovery reads
durable metadata through a healthy connection; OxideBatch never infers
external-resource effects.

## Consequences

- locking and transaction semantics can be tested precisely;
- users need PostgreSQL for durable 1.0 operation;
- business writes can share a transaction only through an explicit adapter
  contract;
- Spring Batch data requires a future import tool rather than live coexistence;
- additional databases need separate ADRs and conformance suites.
- unique constraints and row versions are correctness mechanisms, not optional
  performance optimizations;
- cancellation and connection failure paths may reduce pool capacity until the
  suspect connection is discarded and replaced;
- SQL pools are owned and dropped within the Tokio runtime that created them.

## Alternatives considered

- Reusing Spring Batch tables suggests interoperability that Rust and Java
  processes cannot safely guarantee.
- A generic SQL abstraction would force guarantees toward the weakest backend.
- SQLite is useful for examples but does not validate production concurrency
  behavior.

## Validation

[Spike 0002](../spikes/0002-postgres-transactions-and-recovery.md) passed
against PostgreSQL 18.4 with SQLx 0.9.0 and demonstrated:

- atomic commit and rollback of business rows plus checkpoint metadata;
- unique-index lock contention (`55P03`) and one instance from twelve
  concurrent launches;
- one winner and one conflict in an optimistic update race;
- rollback at three pre-commit process-exit phases and durability after commit;
- rollback and a commit error when the backend was terminated during a
  deliberately delayed commit;
- connection discard after cancellation/failure and recovery through a healthy
  pool;
- idempotent migration application and newer-schema rejection.

[Spike 0003](../spikes/0003-execution-context-evolution.md) demonstrated the
versioned JSON context stored by the metadata repository.

M2 must extend this evidence across the accepted PostgreSQL major-version
matrix, TLS configurations, runtime roles, and every released migration source
version.

## Revisit triggers

Revisit SQLx if transaction lifetimes leak into domain APIs, a suspect
connection cannot be excluded from reuse through supported APIs, or required
PostgreSQL capabilities cannot be expressed safely.
