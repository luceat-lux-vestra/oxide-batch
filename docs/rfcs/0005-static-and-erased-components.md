# RFC-0005: Dual Static and Erased Component Architecture

- **State:** Accepted
- **Created:** 2026-07-30
- **Accepted:** 2026-08-03, on the evidence of
  [spike 0004](../architecture/spikes/0004-static-and-erased-item-path.md)
- **Recorded as:** [ADR-0008](../architecture/decisions/0008-item-component-contract.md)
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

All three closed by [spike 0004](../architecture/spikes/0004-static-and-erased-item-path.md).

- *The exact stable-Rust trait form at MSRV 1.95.* Return-position
  `impl Future<Output = ..> + Send + 'a` with an explicit call lifetime,
  implemented as a plain `async fn`.
- *Whether the facade defaults to erased or infers a typed plan.* Neither: the
  trait form is not dyn compatible, so the facade cannot default to it, and
  erasure is an explicit `Boxed*` handle that is itself an instance of the
  contract. The typed plan is what a caller gets by not constructing a handle.
- *Code-size and compile-time budgets for monomorphization.* No budget
  exception is needed at the proposed scale: 1,100 bytes per additional typed
  pipeline against 4,761 for a boxed one, with compile time a wash. The
  crossover for large component bodies is an M6 measurement.

## Approval gate

Acceptance requires a reproducible spike and measurements for both paths,
reviewed public ergonomics, and a superseding ADR that updates ADR-0002's
allocation consequence without weakening cancellation or transaction borrowing.

## M5 gate outcome

*Historical record. This section states the position at the M5 design gate,
before the spike ran. It is superseded by the spike evidence and approval
below, and is kept because the M5 milestone's posture still follows from it.*

**Continued deferral, recorded on 2026-08-03 by the
[M5 design gate](../project/m5-design-gate-evidence.md).**

The approval gate above requires a reproducible spike and measurements for both
paths. That spike has not run, so approval is unavailable, and M5 is a
stabilization milestone whose extraction and fingerprint work must not change
the item hot path underneath itself. This RFC therefore stays `Proposed`
through M5.

The consequences are:

- M5 retains the accepted [ADR-0002](../architecture/decisions/0002-execution-model.md)
  boxed component boundary and introduces no native static hot path;
- the roadmap's M5 dependency on this RFC is satisfied by this recorded
  decision rather than by an approval, as the
  [M5 kickoff gate](../project/m5-kickoff-gate.md) provides;
- the P-002 static-versus-erased measurement is an M6 obligation, not an M5
  campaign;
- M5 exits on the boxed boundary, and no preview claim depends on this RFC.

The decision is revisited at M6 kickoff, where the spike, measurements, and the
superseding ADR are prerequisites for the item-model work.

## Spike evidence

The reproducible spike the approval gate requires ran on 2026-08-03 and is
recorded as
[spike 0004](../architecture/spikes/0004-static-and-erased-item-path.md).

The spike settles the shape as well as the numbers. There is one public trait
per role, declared with an explicit call lifetime and an `impl Future<Output =
..> + Send + 'a` return, and implemented with a plain `async fn`. Erasure is a
concrete handle over a sealed, private, dyn-compatible mirror rather than a
second public trait, so a registry stores a named type and the chunk loop
exists once. The typed and dynamically dispatched pipelines are that one
function with different type arguments.

- The concrete path allocates nothing per item, for a pipeline with no item
  listeners. The boxed handle allocates exactly `2N + 1 + chunks` futures — two
  allocations and about 61 ns per item on the measured host.
- Item listeners stay boxed and remain a per-item allocation: a listener set is
  heterogeneous by design. The scope limit is recorded rather than claimed
  away.
- The two are observationally identical across completion, filtering, stop at
  each component, failure at each component, and panic at each component. Since
  they are the same function, this is regression cover rather than the argument.
- Return type notation and dyn-compatible `async fn` in traits were checked on
  nightly 1.99.0 and are unavailable, so erasure has to be built explicitly at
  MSRV 1.95.
- A writer still borrows an enlisted transaction for the duration of its call,
  concretely and through the handle, with identical statement counts.
- Monomorphizing sixteen distinct pipelines costs 1,100 bytes each against
  4,761 for the boxed handle, and compile time is a wash, so this direction
  needs no budget exception at the proposed scale. The crossover for large
  component bodies is not located.

**Correction.** An earlier reading of this spike reported that erasure forces
`I: 'static` on the item type. That was an artifact of eliding the call
lifetime, not a property of the design, and the bound is absent from the
recorded contract.

The public-ergonomics review is also complete and recorded in the same spike.
It was carried out by building M6's decorator and composite shapes against the
contract rather than by judging the declaration on paper: leaf components write
a plain `async fn`, delegating components tie their lifetimes, a generic
composite states one extra bound per referenced or returned type, and a fully
decorated pipeline still measures zero allocations per item. Each contract
trait carries `#[diagnostic::on_unimplemented]`, and the implementer-facing
error wording is pinned by compiler fixtures. No change to the declaration was
required.

This RFC was accepted on this same spike evidence on 2026-08-03 (see the
header above). [ADR-0008](../architecture/decisions/0008-item-component-contract.md)
records the contract and supersedes ADR-0002 **in part** — the three item
component traits only, with the execution model and the other twenty-one
boxed extension points staying under ADR-0002. Transaction and restart
equivalence and the P-002 measurement against real components remain M6
obligations, tracked by the
[M6 kickoff gate](../project/m6-kickoff-gate.md).

## Current implementation constraint

Approval unblocks M6 item-model work. It does not change M5.

M5 remains a stabilization milestone and still exits on the accepted ADR-0002
boxed boundary: the contract lands in M6, not underneath M5's fingerprint and
crate-extraction work. Until that M6 change, existing boxed component contracts
stay in use and the public item-component catalog does not expand.

Two obligations survive approval and belong to M6: transaction and restart
equivalence over the `PostgreSQL` fixtures, and the P-002 measurement against
the reference workload with real components. A third is newly named rather than
inherited — per-item item-listener allocation, which
[ADR-0008](../architecture/decisions/0008-item-component-contract.md) puts
outside its scope.
