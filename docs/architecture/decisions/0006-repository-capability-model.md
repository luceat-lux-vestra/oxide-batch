# ADR-0006: Repository Services and Capability Model

- **State:** Accepted
- **Date:** 2026-07-30
- **Owners:** repository and runtime maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0007](../../rfcs/0007-repository-services-and-capabilities.md)
- **Supersedes:** [ADR-0003](0003-postgres-metadata.md)

## Context

ADR-0003 correctly established PostgreSQL/SQLx, an OxideBatch-owned schema,
database-authoritative identity, optimistic versions, adapter-owned
transactions, and explicit unknown commit outcomes. Its assumption that
PostgreSQL is the only durable repository targeted for project-wide 1.0 no
longer fits the accepted M8 portability and M14 support program.

A single growing repository trait would also combine commands, queries,
operator behavior, retention, transactions, leases, and dialect differences.

## Decision

Retain every proven PostgreSQL correctness rule from ADR-0003 and make
PostgreSQL the current reference and fast-path adapter.

Adopt separate target boundaries for:

- `JobRepository`;
- `JobExplorer`;
- `JobOperator`;
- `DefinitionRegistry`;
- `ExecutionTransaction`;
- `ChunkTransaction`;
- `RepositoryCapabilities`;
- `RetentionRepository`.

All list/query operations are bounded, paginated, or streamed. Plan and runtime
validation use versioned capability descriptors and reject unsupported
requirements rather than weakening guarantees.

Delivery modes distinguish atomic same-resource, transactional messaging,
outbox, inbox/deduplication, idempotent external effects, at-least-once, and
best effort. No universal distributed transaction or blanket exactly-once
abstraction is provided.

Additional database adapters may enter the M8 support matrix only after
independent contract, migration, crash, concurrency, backup/restore, and
resource evidence. Their existence cannot reduce the PostgreSQL fast path.

## Consequences

- the target public/internal surface has more focused ports;
- capabilities and selected delivery mode affect plan validation and may affect
  definition identity;
- every adapter carries a significant certification and support obligation;
- PostgreSQL remains the only currently implemented durable adapter;
- future schemas may add effect, retention, lease, or capability metadata
  through forward migrations.

## Alternatives considered

- A single repository port would become unbounded and difficult to optimize.
- Generic SQL semantics would hide meaningful backend differences.
- Permanent PostgreSQL exclusivity would conflict with accepted portability and
  feature-ledger scope.
- A generic distributed transaction manager would promise guarantees resources
  cannot supply.

## Validation

Before the service split affects production code:

- compatibility adapters preserve the current facade and PostgreSQL behavior;
- shared contracts and normalized repository writes remain equivalent;
- capability mismatch fails at plan/launch boundaries;
- PostgreSQL duplicate-launch, CAS, transaction, disconnect, migration, and
  restore evidence continues to pass;
- every new adapter passes its independent certification matrix.

## Revisit triggers

Revisit if service separation prevents an atomic adapter-owned transaction,
capability descriptors cannot express a required backend semantic, or adapter
certification cost makes a proposed support tier unsustainable.
