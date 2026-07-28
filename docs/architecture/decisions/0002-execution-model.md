# ADR-0002: Async Execution Model

- **State:** Proposed
- **Date:** 2026-07-29
- **Owners:** maintainers
- **Deciders:** project owner

## Context

Batch workloads combine asynchronous database/network I/O, CPU-bound item
processing, blocking libraries, cooperative stopping, and user-defined
components. Choosing an execution model changes every public extension trait.

## Decision

Adopt async-first execution on Tokio 1.x for the initial runtime while keeping
Tokio types out of core domain values and persistence contracts wherever
practical. Define explicit adapters for blocking and CPU-bound user work.
OxideBatch does not create a hidden global runtime.

The final trait representation is accepted only after the M0 object-safety and
cancellation spike.

## Consequences

- PostgreSQL and remote I/O can avoid blocking worker threads;
- cancellation and bounded concurrency have one initial runtime model;
- users embedding another async ecosystem need an adapter;
- careless blocking item code can starve the runtime and must be isolated;
- runtime semantics, not Tokio implementation details, form the public promise.

## Alternatives considered

- A synchronous core is simpler but makes concurrent I/O and cancellation
  composition harder.
- Runtime-neutral boxed futures reduce coupling but still require an executor
  and may complicate ergonomics.
- Supporting multiple runtimes from the start multiplies the test matrix before
  core semantics are stable.

## Validation

The M0 spike must demonstrate object-safe user components, borrowed transaction
contexts, cancellation, panic isolation, and blocking adapters.

## Revisit triggers

Revisit if the spike cannot express transaction-scoped writers safely or if a
runtime-neutral contract has comparable ergonomics and testability.
