# RFC-0007: Repository Services, Capabilities, and Delivery Modes

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** repository and runtime maintainers
- **Target milestone:** M5-M9
- **Related ADR:** [ADR-0003](../architecture/decisions/0003-postgres-metadata.md)

## Summary

Separate repository commands, explorer queries, operator services, definition
registry, retention, and transaction scopes; add capability negotiation and
explicit delivery modes; permit certified relational adapters beyond
PostgreSQL.

## Context and current accepted rule

ADR-0003 selects PostgreSQL/SQLx as the only durable 1.0 repository, preserves
facade-owned ports, and defines same-resource atomicity plus unknown commit.
The current unit-of-work contract is intentionally small.

## Problem

Adding every query, operator action, chunk transaction, retention primitive,
lease, and dialect to one object-safe port would produce an unbounded,
lowest-common-denominator abstraction. New databases and brokers need explicit
capabilities rather than implied equivalence.

## Proposal

- Separate `JobRepository`, `JobExplorer`, `JobOperator`,
  `DefinitionRegistry`, `ExecutionTransaction`, `ChunkTransaction`,
  `RepositoryCapabilities`, and `RetentionRepository`.
- Require pagination/streaming; prohibit unbounded lists.
- Negotiate schema, isolation, lock/CAS, transaction, size, migration, query,
  lease/fencing, and retention capabilities.
- Define `AtomicSameResource`, `TransactionalMessage`, `Outbox`,
  `InboxDedup`, `IdempotentExternalEffect`, `AtLeastOnce`, and `BestEffort`.
- Reject unsatisfied plan requirements; never silently weaken guarantees.
- Keep PostgreSQL the reference/fast path while certifying additional
  relational adapters at M8.

## Alternatives

1. Grow one repository trait. Rejected for object-safety and responsibility
   overload.
2. Use generic SQL semantics. Rejected because correctness differs by backend.
3. Claim distributed transactions. Rejected as misleading and impractical.
4. Keep PostgreSQL permanently exclusive. Safe but conflicts with proposed
   portability and full ledger coverage.

## Consequences

More ports and capability descriptors increase API surface and adapter work.
Plans fail earlier and guarantees become explicit. Adapter certification and
support tiers are mandatory.

## Compatibility impact

Existing PostgreSQL behavior and facade adapters are preserved during the
split. Additional adapters do not reduce PostgreSQL guarantees. Public port
changes follow pre-1.0 or stable deprecation policy.

## Metadata, restart, and transaction impact

Capabilities and selected delivery mode become definition/plan-relevant.
Repository schema may add lease/fencing, effect journal, outbox/inbox, retention,
and adapter metadata through forward migrations. Same-resource atomicity and
unknown-commit handling remain unchanged.

## Migration and rollout

First split service responsibilities behind compatibility adapters. Add the
descriptor and reject-only validation. Certify PostgreSQL. Add each new adapter
with independent migration/support evidence. Schema changes are forward-only
and retain restore rollback.

## Validation and evidence plan

- shared service and adapter contract suites;
- PostgreSQL behavior/plan equivalence;
- duplicate launch, CAS, stale fencing, crash/disconnect, unknown commit;
- all-version migration and backup/restore per adapter;
- delivery-mode crash/redelivery/idempotency fixtures;
- pagination/resource bounds and query plans;
- capability mismatch plan/launch rejection.

## Unresolved questions

- Exact Tier-1 database set and external certification format.
- Whether MongoDB metadata warrants a separate RFC.
- Effect-journal schema and retention relation.
- Which capabilities affect the definition fingerprint.

## Decision

**Accepted by the project owner on 2026-07-30.**

The separated services, capability negotiation, and explicit delivery modes
are the accepted target. PostgreSQL remains the current reference and only
implemented durable adapter. Port migration requires a compatibility prototype
and PostgreSQL behavior evidence; every additional adapter requires independent
certification before a support claim.
