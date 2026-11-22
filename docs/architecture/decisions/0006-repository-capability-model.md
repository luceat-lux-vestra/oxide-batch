# ADR-0006: Repository Services and Capability Model

- **State:** Accepted; partially superseded by [ADR-0016](0016-durable-effect-definition-identity-and-runtime-boundary.md) for M8 durable-effect delivery/profile scope
- **Date:** 2026-07-30
- **Owners:** repository and runtime maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0007](../../rfcs/0007-repository-services-and-capabilities.md)
- **Supersedes:** [ADR-0003](0003-postgres-metadata.md)
- **Partial supersession scope:** ADR-0016 narrows mandatory M8 durable-effect
  definition semantics to local EffectJournal/Outbox/InboxDedup correctness.
  This ADR remains authoritative for repository-service separation,
  repository-owned transactions, and capability negotiation.

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

Delivery requirements/profiles distinguish atomic same-resource,
transactional messaging, outbox, inbox/deduplication, idempotent external
effects, at-least-once, and best effort. M8 Gate-B later froze these names as
composable semantic requirements rather than a requirement for one flat public
enum. No universal distributed transaction or blanket exactly-once abstraction
is provided.

Additional database adapters may enter the M8 support matrix only after
independent contract, migration, crash, concurrency, backup/restore, and
resource evidence. Their existence cannot reduce the PostgreSQL fast path.

## Consequences

- the target public/internal surface has more focused ports;
- capabilities and normalized delivery requirement/profile selections affect
  plan validation and may affect definition identity;
- every adapter carries a significant certification and support obligation;
- at ADR acceptance time, PostgreSQL was the only implemented durable adapter;
- future schemas may add effect, retention, lease, or capability metadata
  through forward migrations.

### Current implementation status

PostgreSQL remains the reference, capability-rich, release-backed supported
durable adapter. MySQL/MariaDB and SQLite have first-party bootstrap-core
implementations but have not yet passed the additive runnable-core profile or
support-promotion evidence. SQL Server remains planned. MongoDB has completed
the M8 support-level core profile on main for versions 8.0.38 and 8.2.12 with
schema 3 and is therefore an `Implemented` candidate; it is not release-backed
`Verified` until a named OxideBatch release carries the claim.

The current M8 sequencing also defers structural splitting of
`RepositoryUnitOfWork` while adapters are brought to the runnable-core
boundary. It remains the adapter-owned atomic transaction aggregate; database
portability may not use that deferral to add a generic SQL abstraction or
silently expand optional capability families. Explorer remains a separate
bounded read port.

The canonical M8 durable-effect specialization is the
[Gate-B record](../../project/m8-gate-b-durable-effect-delivery.md). It owns
EffectId/effect-attempt semantics, composition, Outbox/Inbox/effect-journal
atomicity, idempotency, unknown external outcomes, plan-identity classification,
and the M9 handoff without changing this ADR's repository-service split.

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
