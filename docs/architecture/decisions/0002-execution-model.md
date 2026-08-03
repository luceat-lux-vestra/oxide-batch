# ADR-0002: Async Execution Model

- **State:** Accepted
- **Partially superseded by:**
  [ADR-0008](0008-item-component-contract.md) replaces the boxed representation
  for the three item component traits only. Its revisit trigger — boxed-future
  allocation against a performance budget — fired with
  [spike 0004](../spikes/0004-static-and-erased-item-path.md). This record
  stays `Accepted` and continues to govern the execution model and the other
  twenty-one extension points that use the boxed form.
- **Date:** 2026-07-29
- **Owners:** maintainers
- **Deciders:** project owner

## Context

Batch workloads combine asynchronous database/network I/O, CPU-bound item
processing, blocking libraries, cooperative stopping, and user-defined
components. Choosing an execution model changes every public extension trait.

## Decision

Adopt async-first execution on Tokio 1.x for the initial runtime while keeping
Tokio types out of core domain values and persistence contracts. OxideBatch
does not create a hidden global runtime.

Dynamically dispatched public extension methods return an OxideBatch-owned
alias for:

```rust
Pin<Box<dyn Future<Output = T> + Send + 'a>>
```

This explicit representation is dyn-compatible and permits futures to borrow
the component and call-scoped arguments. Native `async fn` traits may be used
internally for static dispatch, but they are not the sole public extension form
while they remain non-dyn-compatible. The public contract does not require the
`async-trait` macro.

Blocking and CPU-bound user work uses an explicit adapter with a configured
nonzero concurrency bound. A stop is honored before a blocking call starts.
Once synchronous work starts, OxideBatch awaits it to completion, reports a
late stop, and stops before the next unit; it does not detach an uninterruptible
side effect.

Panics at async and blocking component boundaries become framework-owned typed
failures. Panic payloads are not stable error or telemetry data.

## Consequences

- PostgreSQL and remote I/O can avoid blocking worker threads;
- cancellation and bounded concurrency have one initial runtime model;
- users embedding another async ecosystem need an adapter;
- careless blocking item code can starve the runtime and must be isolated;
- runtime semantics, not Tokio implementation details, form the public promise.
- dynamic extension calls allocate a boxed future;
- blocking cancellation latency is at least the remaining duration of the
  current synchronous call;
- applications own the Tokio runtime lifecycle, and runtime-scoped resources
  such as SQL pools must not outlive or move between runtimes.

## Alternatives considered

- A synchronous core is simpler but makes concurrent I/O and cancellation
  composition harder.
- Runtime-neutral boxed futures reduce coupling but still require an executor
  and may complicate ergonomics.
- Supporting multiple runtimes from the start multiplies the test matrix before
  core semantics are stable.

## Validation

[Spike 0001](../spikes/0001-async-public-traits.md) demonstrated:

- compiler rejection of a native async trait object;
- a boxed-future trait object borrowing call-scoped input;
- cooperative async cancellation;
- async and blocking panic classification;
- bounded blocking concurrency without timer starvation;
- the documented late-stop behavior for running blocking work.

[Spike 0002](../spikes/0002-postgres-transactions-and-recovery.md)
demonstrated a dynamically dispatched writer borrowing a transaction port whose
SQLx transaction remained adapter-internal.

## Revisit triggers

Revisit if dyn-compatible native async traits are stable across the MSRV, boxed
future allocation violates an accepted performance budget, transaction-scoped
writers can no longer be expressed safely, or a runtime-neutral contract has
comparable ergonomics and testability.
