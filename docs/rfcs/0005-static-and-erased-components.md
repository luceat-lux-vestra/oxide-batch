# RFC-0005: Dual Static and Erased Component Architecture

- **State:** Proposed
- **Created:** 2026-07-30
- **Owner:** API and performance maintainers
- **Target milestone:** M5-M6
- **Related ADR:** [ADR-0002](../architecture/decisions/0002-execution-model.md)

## Summary

Provide a generic/native-async item hot path and explicit erased adapters for
heterogeneous composition, avoiding per-item boxed futures while preserving an
ergonomic dynamic facade.

## Context and current accepted rule

ADR-0002 uses an OxideBatch-owned boxed future for dynamically dispatched
public extension traits and permits native async traits internally. Current M2
item contracts follow that safe, object-compatible shape.

## Problem

Applying boxing and dynamic dispatch to every item would allocate and indirect
the highest-volume path. Removing erasure entirely would prevent heterogeneous
plans, registries, and a simple facade.

## Proposal

- Define generic reader/processor/writer/stream traits with associated item and
  future types or native async where MSRV permits.
- Monomorphize the item path and prohibit an item-per-item heap allocation.
- Provide `Erased*` adapters at registry, plan, step/chunk, facade, and
  out-of-process boundaries.
- Require semantic parity between native and erased paths.
- Preserve explicit bounded blocking adapters and panic/cancellation rules.
- Offer typed performance-oriented and ergonomic erased builders when evidence
  justifies both.

## Alternatives

1. Keep boxed futures everywhere. Simple but likely violates the proposed hot
   path budget.
2. Static generics only. Fast but incompatible with heterogeneous composition.
3. Stable Rust dylib ABI. Rejected as an unsound long-term compatibility
   promise.
4. Adopt `async-trait` publicly. It still boxes and exposes a macro choice.

## Consequences

API surface, compile time, monomorphized code size, and documentation grow.
The performance-critical boundary becomes testable. Adapter authors must
declare thread-safety, state, and capability behavior once for both paths.

## Compatibility impact

Current boxed traits remain supported through adapters during pre-1.0 rollout.
Any public removal follows deprecation policy. Type inference, error types, and
builder ergonomics require compile/API review.

## Metadata, restart, and transaction impact

Both paths use identical logical component IDs, state schema, checkpoints,
counters, and transaction ports. Switching representation cannot change a
definition fingerprint unless the component's declared restart semantics
change.

## Migration and rollout

Prototype tasklet and item pipelines outside the stable facade; measure;
implement adapters; run trace/state equivalence; add opt-in typed builders;
lower erased facade components at chunk/step boundaries; deprecate redundant
forms only after M6 evidence.

## Validation and evidence plan

- allocations per item/chunk, throughput, latency, binary size, and compile time;
- object-safety and borrowing prototypes;
- compile-fail item-type and `Send`/`Sync` tests;
- native/erased lifecycle, stop, panic, transaction, and restart equivalence;
- no SQLx/Tokio/serializer leakage;
- representative standard components and composite wrappers.

## Unresolved questions

- The exact stable-Rust trait form at MSRV 1.95.
- Whether the facade defaults to erased or infers a typed plan.
- Code-size and compile-time budgets for monomorphization.

## Approval gate

Acceptance requires a reproducible spike and measurements for both paths,
reviewed public ergonomics, and a superseding ADR that updates ADR-0002's
allocation consequence without weakening cancellation or transaction borrowing.

## Current implementation constraint

While this RFC remains `Proposed`, existing M2 boxed component contracts may be
used to complete the smallest durable vertical slice and retained later as
erased compatibility adapters. Do not expand the public item-component catalog,
stabilize per-item boxed execution as the long-term hot path, or begin M6 item
API work before the spike and approval gate complete.
