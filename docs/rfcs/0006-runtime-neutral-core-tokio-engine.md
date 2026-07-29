# RFC-0006: Runtime-Neutral Core and Explicit Tokio Engine

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** runtime maintainers
- **Target milestone:** M5-M6
- **Related ADR:** [ADR-0002](../architecture/decisions/0002-execution-model.md)

## Summary

Keep Tokio as the explicit default execution engine while making core, item,
plan, and repository contracts independent of Tokio types and dependencies.

## Context and current accepted rule

ADR-0002 accepts Tokio 1.x, no hidden global runtime, facade-owned futures, and
adapter-isolated infrastructure types. The current runtime legitimately uses
Tokio synchronization primitives.

## Problem

Calling the whole architecture runtime-neutral while orchestration and core
boundaries are not clearly separated is misleading. Supporting every executor
now would multiply tests; exposing Tokio throughout the core would make future
embedding unnecessarily difficult.

## Proposal

- `core`, `plan`, `item`, and repository contract boundaries do not depend on
  Tokio.
- The default engine explicitly owns Tokio tasks, synchronization, timers,
  cancellation integration, and blocking pools.
- Applications own the runtime lifecycle; there is no hidden global executor.
- OxideBatch defines only narrow semantic boundaries for spawn/join,
  cancellation, monotonic deadlines, and blocking isolation.
- No general async-runtime compatibility layer is created.
- Another engine requires user demand, an adapter design, and complete
  conformance/cancellation evidence.

## Alternatives

1. Expose Tokio everywhere. Simpler but makes stable contracts executor-bound.
2. Support multiple runtimes immediately. Rejected due to test and API cost.
3. Implement a comprehensive runtime abstraction. Rejected as duplicative and
   leaky.
4. Use a synchronous core. Rejected by current I/O, transaction, and
   cancellation evidence.

## Consequences

Tokio remains an intentional supported dependency of the engine. More internal
boundaries are required. Runtime-neutral claims become precise rather than
marketing language.

## Compatibility impact

No intended facade behavior change. Public Tokio types remain prohibited unless
a later accepted decision exposes one. Engine-specific feature/configuration
names are versioned.

## Metadata, restart, and transaction impact

No persisted format change. Dropping/cancelling engine futures must preserve
the accepted unit-of-work, suspect-connection, and unknown-commit rules.
Monotonic deadlines are not persisted as absolute restart state.

## Migration and rollout

Identify Tokio dependencies, move domain/contracts behind neutral boundaries,
retain one Tokio engine, and compare facade traces. Do not add a second runtime
as proof. Roll back by restoring internal module placement without changing
public types or metadata.

## Validation and evidence plan

- dependency checks forbidding Tokio in neutral crates;
- current async/blocking cancellation and panic spike cases;
- transaction borrowing and connection-lifecycle tests;
- structured task leak and shutdown tests;
- facade/API and normalized trace equivalence;
- benchmark overhead of any semantic adapter boundary.

## Unresolved questions

- Whether the Tokio engine is a separate crate or module at first.
- Which cancellation primitive is facade-owned versus engine-internal.
- Minimum semantic interface needed by distributed worker mode.

## Decision

**Accepted by the project owner on 2026-07-30.**

Tokio remains the explicit initial engine, while core, plan, item, and
repository contract boundaries are runtime-neutral. ADR-0002 remains binding
for current execution, cancellation, panic, blocking, and transaction
borrowing. Extraction cannot begin until the dependency map and unchanged
behavior evidence pass.
