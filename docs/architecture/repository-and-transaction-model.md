# Repository and Transaction Model

**State:** Accepted

**Governing decisions:**
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md) and
[ADR-0006](decisions/0006-repository-capability-model.md)

This document is the canonical target architecture for repository services,
adapter capabilities, metadata transactions, and business-effect delivery.
PostgreSQL remains the current reference and only implemented durable adapter.

## Service boundaries

| Port/service | Responsibility |
| --- | --- |
| `JobRepository` | durable execution commands, identity creation, lifecycle compare-and-swap, and checkpoint writes |
| `JobExplorer` | read-only, bounded, paginated or streaming queries |
| `JobOperator` | launch, stop, restart, abandon, recover, and idempotent application services |
| `DefinitionRegistry` | revision, manifest, fingerprint, artifact, and upgrade resolution |
| `ExecutionTransaction` | atomic metadata changes for one bounded operation |
| `ChunkTransaction` | enlisted business writes plus checkpoint, context, counters, and version |
| `RepositoryCapabilities` | adapter dialect, isolation, locking, schema, transaction, query, and migration features |
| `RetentionRepository` | bounded archive, purge, hold, and verification primitives |

Command and query interfaces are separate. Unbounded list APIs and
implementation-driver types are prohibited at the facade boundary.

## Lifecycle authority and concurrency

The repository is authoritative for instance uniqueness, execution state,
definition identity, checkpoints, ownership, and recovery audit. Database
constraints protect identity and referential invariants. Mutable rows use
compare-and-swap versions. Distributed ownership additionally uses a durable
lease and monotonically changing fencing token.

A stale version or fencing token cannot update checkpoint, context, counters,
assignment, or terminal status. Telemetry, process memory, and message
delivery never override repository state.

## Capability negotiation

Adapters publish a versioned descriptor covering:

- schema and migration version;
- uniqueness, compare-and-swap, locking, isolation, and transaction support;
- maximum parameter, context, checkpoint, page, batch, and identifier sizes;
- same-resource enlistment and unknown-commit classification;
- pagination and streaming forms;
- lease/fencing and server-time behavior;
- backup, restore, retention, and rolling-upgrade support.

Plan compilation evaluates static capabilities. Connection/launch time
negotiation validates actual deployed capabilities. A missing requirement
fails explicitly; it does not degrade the delivery guarantee.

## Transaction and delivery modes

| Mode | Required behavior |
| --- | --- |
| `AtomicSameResource` | business writes and progress commit in one resource transaction |
| `TransactionalMessage` | broker transaction/ack/offset behavior is explicit and adapter-specific |
| `Outbox` | business state and an outbox record commit together; delivery is separate and deduplicated |
| `InboxDedup` | durable message/effect identity suppresses replayed input or commands |
| `IdempotentExternalEffect` | application idempotency key and effect journal support safe retry/reconciliation |
| `AtLeastOnce` | duplicates are possible and documented |
| `BestEffort` | automatic reconciliation is unavailable; operator action may be required |

There is no universal transaction-manager abstraction and no generic
cross-resource exactly-once claim. A component cannot advertise a stronger
mode than its resource supplies.

## Same-resource path

For PostgreSQL metadata and PostgreSQL business writes, the adapter owns one
connection and transaction. Business writes, checkpoint, execution context,
counters, and optimistic version commit or roll back together. The facade
lends a bounded OxideBatch-owned transaction port; SQLx types remain private.

The launched runtime supplies job/step execution identity when beginning this
transaction. A state provider prepares bounded checkpoint/context values at
the commit boundary from the prior durable counters and the open chunk; a
missing execution identity or state-preparation failure is a known
not-committed outcome. Standalone/non-enlisted execution cannot infer or claim
the PostgreSQL atomic mode.

After an ambiguous commit response, the physical connection is discarded and
the outcome is `UNKNOWN` until a healthy connection reads durable state.

## M3 fault and flow transactions

The [M3 fault-tolerance contract](fault-tolerance.md) adds one metadata-only
retry-reservation transaction after a known rollback and before backoff. Its
step compare-and-swap increments exactly one phase retry counter plus the
durably acknowledged rollback count and replaces the bounded checksummed
fault-state envelope. A stale reservation cannot spend the same retry ordinal
twice.

An accepted skip commits its phase count, retry-key removal, checkpoint,
context, business writes, listener work, ordinary counters, and optimistic
version in the existing chunk transaction. Known rollback changes none of
those values except `rollback_count` in the subsequent authoritative metadata
update. An unknown commit changes no inferred counter and enters `UNKNOWN`.

The [M3 basic-flow contract](basic-flow.md) appends a selected transition after
the source step result is durable and before the target starts. Decider result
and target selection share one transaction. Step start-limit comparison and
step-execution creation also share one transaction across the job instance and
logical step ID.

## M4 operator, explorer, retention, and partition transactions

The [M4 operator, explorer, and retention contract](operator-and-explorer-services.md)
fixes the observable behavior of the `JobExplorer`, `JobOperator`, and
`RetentionRepository` ports for this milestone.

Each mutating operator action commits one append-only operator request row in
the same transaction as its lifecycle compare-and-swap, so idempotency, audit,
and effect cannot diverge. A durable stop request is a compare-and-swap on the
execution row; it does not transition an execution whose owner is gone.
Recovery keeps its accepted M2 shape of one appended decision plus one
compare-and-swap, and additionally requires a matching evidence digest and
observed version. Each purge batch is one transaction that contains its own
retention audit row and deletes in instance-owned order.

The [M4 local-scale contract](local-scale.md) adds three metadata boundaries:
the complete partition plan for one parent step commits before any worker
starts; each partition result is one compare-and-swap on its own row; and
parent aggregation shares one transaction with the parent step's terminal
lifecycle update. A stale partition writer loses its compare-and-swap rather
than publishing a result twice.

Explorer reads are single-statement, keyset paginated, ordered by immutable
keys, and bounded by page size, response size, and the configured statement
timeout. They take no lock and never participate in a chunk transaction.

## Adapter certification

Every adapter runs the same logical contract suite plus adapter-specific
evidence for:

- duplicate launch and concurrent execution creation;
- optimistic conflicts, transaction isolation, and stale fencing;
- crash and disconnect at each commit phase;
- schema initialization, upgrade, newer-version rejection, and restore;
- pagination consistency and bounded resource use;
- retention safety and audit;
- same-resource and cross-resource guarantees it claims.

Certification records product versions, configuration, evidence links,
limitations, and support tier. A lowest-common-denominator interface must not
disable a documented PostgreSQL fast path.

## Security and privacy

Credentials, endpoints, SQL, bound values, parameters, contexts, checkpoint
payloads, and driver diagnostics are excluded from public errors and telemetry.
Migration, runtime, explorer, and operator privileges are distinct. Recovery
and destructive retention operations are authenticated by the deployment,
guarded, bounded, and audited.
