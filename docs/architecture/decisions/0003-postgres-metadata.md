# ADR-0003: PostgreSQL Metadata Repository

- **State:** Proposed
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

## Consequences

- locking and transaction semantics can be tested precisely;
- users need PostgreSQL for durable 1.0 operation;
- business writes can share a transaction only through an explicit adapter
  contract;
- Spring Batch data requires a future import tool rather than live coexistence;
- additional databases need separate ADRs and conformance suites.

## Alternatives considered

- Reusing Spring Batch tables suggests interoperability that Rust and Java
  processes cannot safely guarantee.
- A generic SQL abstraction would force guarantees toward the weakest backend.
- SQLite is useful for examples but does not validate production concurrency
  behavior.

## Validation

The M0/M2 spikes must prove duplicate instance serialization, optimistic-lock
conflicts, atomic business-write/checkpoint commits, migrations, and crash
recovery against supported PostgreSQL versions.

## Revisit triggers

Revisit SQLx if transaction lifetimes leak into domain APIs or required
PostgreSQL capabilities cannot be expressed safely.
