# ADR-0008: Item Component Contract and Erasure Boundary

- **State:** Accepted
- **Date:** 2026-08-03
- **Owners:** API and performance maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0005](../../rfcs/0005-static-and-erased-components.md)
- **Supersedes in part:** [ADR-0002](0002-execution-model.md) — the public
  item component representation only. Every other extension point ADR-0002
  governs keeps its boxed form, and ADR-0002 stays `Accepted`.
- **Scope note:** item listeners are outside this decision and keep the boxed
  form. Reducing their per-item allocation is separate work needing its own
  evidence.

## Context

ADR-0002 chose an OxideBatch-owned `Pin<Box<dyn Future + Send + 'a>>` alias for
dynamically dispatched public extension methods, and recorded as a consequence
that "dynamic extension calls allocate a boxed future". It also listed a
revisit trigger: boxed-future allocation violating an accepted performance
budget. That trigger has fired.

[Spike 0004](../spikes/0004-static-and-erased-item-path.md) measured one chunk
workload under both dispatch forms. The boxed form allocates exactly
`2N + 1 + chunks` futures for `N` items — two allocations per item on the
framework's highest-volume path — and costs about 61 ns per item more than a
monomorphized equivalent. The item hot path is the one place in a
chunk-oriented framework where a per-call allocation is paid once per business
record rather than once per step.

ADR-0002's remaining decisions are not in question. Async-first execution on
Tokio, Tokio types kept out of core contracts, no hidden global runtime, the
explicitly bounded blocking adapter with its late-stop rule, and panic
conversion to framework-owned typed failures all stand.

Nor is the boxed representation in question everywhere. Twenty-four public
traits currently use it. Three of them are item components; the other
twenty-one — `BusinessTransaction`, `ChunkTransaction`,
`ChunkTransactionManager`, `JobRepository`, `RepositoryUnitOfWork`, the
explorer and recovery ports, the job/step/chunk/item listeners, `Tasklet`,
`JobExecutionDecider`, `BackoffSleeper`, `FaultStateStore`, and
`TelemetryExportSink` — are either invoked once per step or per job, or are
structurally heterogeneous, or are handed across a boundary as `&mut dyn`. The
per-item argument does not reach them, and replacing their representation would
buy nothing.

This ADR therefore supersedes ADR-0002 **in part**: the public item component
representation, and nothing else. ADR-0002 remains the accepted record for the
execution model and for every other extension point.

The public item component traits have never been released. The only published
version, `v0.1.0-alpha.1`, contains governance, facade metadata, and pre-alpha
documentation. There is no compatibility promise to break.

## Decision

### One public contract per component role

Public component traits are generic and return an opaque future, with the call
lifetime stated explicitly:

```rust
pub trait ItemReader<I>: Send {
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a;
}
```

The receiver and the call scope share one lifetime, mirroring the signature
ADR-0002 published. `ItemProcessor` and `ItemWriter` follow the same shape.
There is no second public trait for the same role.

Implementors write `async fn`. The `Send` bound remains enforced against the
implementation body. The contract does not require `async-trait`, and no
executor, database driver, or serializer type appears in it.

### Erasure is a type, not a trait

Each role has one concrete erased handle — `BoxedReader<I>`,
`BoxedProcessor<I, O>`, `BoxedWriter<I>` — that implements the same public
trait. Each wraps a dyn-compatible mirror trait that is private to the crate:
not exported, not nameable, not implementable outside it, and therefore free to
change shape without a public break. Constructing a handle is the single point
at which a pipeline stops being monomorphized.

The item type is not constrained to `'static`, by the contract or by the
handle. A *component* must be `'static` to be placed in a handle, which is the
ordinary requirement for storing it in a registry. A generic reader decorator
does need `I: 'static`, for the reason given under Consequences.

### Scope

The contract covers the item reader, processor, and writer. It does not cover
item listeners.

Item listeners are also invoked per item — `ItemListenerSet::before_read` and
its siblings run around every read, process, and write — and each registered
listener costs one boxed future per item per phase. They keep the ADR-0002
representation, because a listener set is a heterogeneous, registration-ordered
collection whose whole purpose is dynamic composition; monomorphizing it would
mean type-level lists and would trade a real allocation for a worse public API.

The consequence has to be stated plainly rather than buried: **the
zero-allocation result applies to a pipeline with no item listeners.** An empty
set costs nothing, because the dispatch loop runs zero times, so the default
configuration does get it. Every registered item listener adds one boxed future
per item per phase it participates in. Reducing that is separate work with its
own evidence, and this ADR does not claim it.

### One chunk loop

The chunk driver is generic over the contract. The monomorphized pipeline and
the dynamically dispatched pipeline are the same code with different type
arguments. Erasure is a decision made where components are assembled, not a
second execution path.

### Definition identity is unaffected

Representation is not restart-relevant. A component that moves from the boxed
form to the contract, or from a concrete type to a handle, keeps its logical
component identity, revision, state schema, checkpoint semantics, and
transaction ports, so the
[ADR-0004](0004-job-definition-restart-compatibility.md) definition fingerprint
stabilized by the M5 design gate does not change. A fingerprint change remains
required only when a component's *declared restart semantics* change, which
this decision does not touch.

### Preserved from ADR-0002

- async-first execution on Tokio 1.x, with Tokio types out of core domain and
  persistence contracts, and no hidden global runtime;
- cooperative stop: the stop token is passed through the call scope, honoured
  before a blocking call starts, and a stop arriving during a running
  synchronous call is reported after that call completes;
- the explicit blocking adapter with a configured nonzero concurrency bound;
- panic conversion at component boundaries into framework-owned typed
  failures, with payloads outside the error and telemetry contract;
- the borrowed enlisted transaction: a writer receives
  `&mut dyn BusinessTransaction` through `WriteContext<'a>` for exactly the
  duration of its call, and the adapter's concrete transaction type does not
  cross the boundary.

## Migration

The ADR-0002 item component traits are unreleased, so they are replaced rather
than deprecated. The order is fixed by governance — this ADR is accepted before
the dependent work, not alongside it:

1. publish the contract, the sealed mirror, and the `Boxed*` handles;
2. make `chunk_runtime::ChunkStep` generic over the contract, with the handles
   as the instantiation used by name-resolved plan components;
3. port the existing components and their tests;
4. remove `oxide_batch::{ItemReader, ItemProcessor, ItemWriter}` in the same
   change that removes their last use.

An adapter from the contract onto the ADR-0002 traits exists and is exercised
(`spikes/m6-item-hot-path/src/erased.rs`). It is migration insurance for
out-of-tree code during step 3, not a published compatibility surface, and it
does not survive step 4.

## Consequences

- the item hot path allocates nothing per item, and per-item boxing survives
  only where a handle is explicitly constructed;
- there is one trait to implement per role, and implementors write `async fn`
  with no lifetime, future type, or `Box::pin` in sight;
- a component that delegates to another component must tie its lifetimes
  explicitly (`async fn write<'a>(&'a self, items: &'a [O], context:
  WriteContext<'a>)`), because the inner call unifies receiver, item, and
  context at one lifetime. Composites, classifiers, delegates, validators, and
  wrappers are all delegating components;
- a *generic* composite additionally states bounds a leaf never has to: a type
  that appears in the returned value needs `'static`, and a type passed by
  reference needs `Sync`. In practice that is `I: 'static` for a reader
  decorator, `I: Sync, O: 'static` for a processor decorator, and `I: Sync` for
  a writer composite;
- decoration costs no dispatch: a pipeline of observer reader, filtering
  processor, and fan-out writer measures zero allocations per item, so the hot
  path result survives real composition;
- `WriteContext` is not `Copy`, so a composite writer reborrows the enlisted
  transaction per delegate and hands it to them one at a time;
- each contract trait carries `#[diagnostic::on_unimplemented]`, so a missing
  or wrong impl reports the component, the item type, and the signature to
  write. Lifetime errors remain outside what that attribute can annotate;
- each additional monomorphized pipeline costs roughly 1,100 bytes of code
  against 4,761 for an erased one at the measured component size, and compile
  time is unchanged. Large component bodies will move this and are not
  measured here;
- registries, plans, facades, and out-of-process boundaries name a handle type
  rather than a trait object;
- the crate carries one private dyn-compatible mirror trait per role, whose
  only implementors are blanket impls over the public trait.

## Alternatives considered

- **Keep ADR-0002 unchanged.** Rejected: it leaves two allocations and ~61 ns
  per item on the highest-volume path with no way to opt out.
- **Publish generic traits alongside the ADR-0002 boxed traits.** Rejected: two
  contracts per role, two step types, and duplicated surrounding wiring, with
  the allocation win available only to users who know to ask for it.
- **Blanket-implement the generic trait for `Box<dyn ItemReader<I>>`.**
  Verified to work. Rejected because it makes the boxed traits a permanent
  second public contract, needs an irrevocable blanket impl per container
  shape, and defers the "you are boxing here" signal to the registry boundary.
- **Associated future types via GATs.** Implementors would have to name a type
  that `async` blocks do not have.
- **`async-trait` in the public contract.** Still boxes, and forces a macro
  choice on implementors. Already rejected by ADR-0002.
- **Wait for the language.** Return type notation and dyn-compatible `async fn`
  in traits were both checked on nightly 1.99.0 on 2026-08-03. Neither is
  usable, and neither is available at MSRV 1.95.

## Validation

[Spike 0004](../spikes/0004-static-and-erased-item-path.md), reproducible with
`./spikes/m6-item-hot-path/run-item-hot-path-spike.sh`, demonstrated:

- zero heap allocations per item on the monomorphized path against exactly
  `2N + 1 + chunks` on the erased one, asserted as an identity rather than a
  threshold;
- 4.82 ns per item against 66.10 ns on the measured host;
- identical trace, counters, terminal outcome, and durable fold across
  thirteen completion, filtering, stop, and failure scenarios, and identical
  panic payloads for each component role;
- a writer borrowing an enlisted transaction for its whole call, concretely and
  through the handle, with matching statement counts;
- compiler rejection of the contract trait as a trait object, which is why the
  sealed mirror exists, and restored object safety, `Send`, and heterogeneous
  registries through the handle;
- a contract component still producing a `Box<dyn oxide_batch::ItemReader<I>>`,
  which is what makes retiring the ADR-0002 traits safe;
- marginal code size and compile time for one against sixteen pipelines on each
  form;
- an ergonomics review carried out by building M6's decorator and composite
  shapes against the contract, measuring the bounds a generic composite needs,
  and pinning the implementer-facing diagnostics with compiler fixtures.

Not yet demonstrated, and required before M6 exit rather than before this
decision: transaction and restart equivalence over the `PostgreSQL` fixtures,
and the P-002 measurement against the M6 reference workload with real
components.

## Revisit triggers

Revisit if dyn-compatible `async fn` in traits or return type notation
stabilizes across the MSRV, if real components move the code-size crossover far
enough that monomorphization needs a budget exception, if the delegating-
component lifetime cost proves unworkable in the M6 component catalog, or if
transaction-scoped writers can no longer be expressed under this contract.
