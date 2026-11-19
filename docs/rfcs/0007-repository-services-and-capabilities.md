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
  `InboxDedup`, `IdempotentExternalEffect`, `AtLeastOnce`, and `BestEffort`
  as semantic delivery requirements/profiles. Gate-B permits explicit
  composition; the vocabulary is not a requirement for one flat public enum.
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

Capabilities and normalized delivery semantics that select or reinterpret
durable state become definition/plan-relevant. Concrete endpoints, credentials,
provider accounts, capacity knobs, and compatible deployment implementation
choices remain runtime/deployment values and are revalidated by negotiation.
Repository schema may add lease/fencing, effect journal, outbox/inbox, retention,
and adapter metadata through forward migrations. Same-resource atomicity and
unknown-commit handling remain unchanged. The exact Gate-B fingerprint member
set cannot change until the ADR-0009 follow-up required by the
[Gate-B record](../project/m8-gate-b-durable-effect-delivery.md) is accepted.

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

## M8 design reconciliation

The M8 design gates resolved the questions that were intentionally left open
when this RFC was accepted:

- [Gate-A](../project/m8-gate-a-repository-portability.md) owns database target
  dispositions, repository capability negotiation, and adapter certification;
  MongoDB is a distinct non-relational metadata-adapter profile rather than a
  reason to introduce a second repository abstraction.
- [Gate-B](../project/m8-gate-b-durable-effect-delivery.md) owns durable effect
  identity, delivery requirements, idempotency, effect-journal/outbox/inbox
  persistence semantics, unknown external outcomes, plan identity boundaries,
  and the M9 handoff.
- [Gate-C](../project/m8-gate-c-retention-portability.md) owns generic
  retention/archive/export portability and restore evidence. Gate-B effect
  evidence has an independent minimum survival floor and cannot be erased by an
  ordinary history purge.

Exact public API names, physical effect-journal schema, SQL, manifest member
encoding, and adapter implementation remain implementation work rather than
unresolved portable semantics.

## Decision

**Accepted by the project owner on 2026-07-30.**

The separated services, capability negotiation, and explicit delivery
semantics are the accepted target. PostgreSQL remains the reference fast path;
additional adapters may implement the same ports only with independent
capability-specific evidence and explicit limitations. A support claim remains
release- and version-specific. Gate-A, Gate-B, and Gate-C are the M8 design
records that specialize this RFC without creating a lowest-common-denominator
database or delivery abstraction.
