# Item-Processing Model

**State:** Active

**Governing decision:** [ADR-0008](decisions/0008-item-component-contract.md)
(Accepted 2026-08-03) fixes the item reader/processor/writer public contract
shape recorded below. The remaining sections of this document — chunk
lifecycle, state and checkpointing, composition taxonomy,
completion/retry/skip/rollback, standard-component requirements, and evidence
— stay open pending the M6 design-gate closure tracked by the
[M6 kickoff gate](../project/m6-kickoff-gate.md).

This document is the canonical specification for item components, chunk
lifecycle, stream state, composition, and the accepted native/erased
boundary. [RFC-0005](../rfcs/0005-static-and-erased-components.md) was
accepted on 2026-08-03 on the evidence of
[spike 0004](spikes/0004-static-and-erased-item-path.md); its decision is
recorded as [ADR-0008](decisions/0008-item-component-contract.md), which
supersedes [ADR-0002](decisions/0002-execution-model.md) **in part** — the
item reader/processor/writer public representation only. ADR-0002 remains the
accepted record for the execution model and every other extension point,
including item listeners (see below).

## Component contracts

- `ItemReader<I>` produces an item, end-of-input, cooperative stop, or a typed
  read failure.
- `ItemProcessor<I, O>` produces an output item, filters the input, stops, or
  returns a typed process failure.
- `ItemWriter<O>` accepts a bounded chunk and reports committed participation,
  stop, or a typed write failure.
- `ItemStream` opens state before work, contributes a versioned update at a
  commit boundary, and closes in a documented order.
- `Checkpoint` identifies the last durably committed input position and state
  needed to resume without reinterpreting earlier effects.

Components declare whether they are stateful, restartable, thread-safe,
order-sensitive, reentrant, remotely serializable/resolvable, and capable of
participating in each transaction or delivery mode.

The `ItemReader<I>`, `ItemProcessor<I, O>`, and `ItemWriter<O>` shape above is
fixed by ADR-0008: one public trait per role, an explicit call lifetime, and
an opaque `impl Future` return, with erasure provided by a concrete
`BoxedReader`/`BoxedProcessor`/`BoxedWriter` handle rather than a second
public trait. `ItemStream` and `Checkpoint` state/version mechanics remain
open — closed by M6 Gate C (item-stream and component state), not by
ADR-0008.

**Item listeners are out of scope for ADR-0008** and keep the ADR-0002 boxed
`Pin<Box<dyn Future<Output = T> + Send + 'a>>` form: a listener set is a
heterogeneous, registration-ordered collection, and each registered listener
still costs one boxed future per item per phase. Reducing that allocation is
a separate, evidence-gated decision (M6 Gate F), not assumed by this
document.

## Accepted contract shape and erasure boundary

ADR-0008 fixes this section. Public component traits are generic, with an
explicit call lifetime and an opaque future return
(`impl Future<Output = ..> + Send + 'a`); implementors write a plain
`async fn`. The monomorphized pipeline MUST NOT allocate or box one future
per item. Borrowing, zero-copy inputs, and reusable buffers are allowed when
lifetimes cannot escape a chunk or transaction.

Erasure is a concrete type, not a second trait: `BoxedReader<I>`,
`BoxedProcessor<I, O>`, and `BoxedWriter<I>` each implement the same public
trait over a sealed, private, dyn-compatible mirror. Constructing a handle is
the single point at which a pipeline stops being monomorphized; it MAY
allocate at that step, chunk, registry, or process boundary, but not silently
at every item boundary. The native and erased forms are the same chunk driver
with different type arguments and share identical lifecycle, failure, state,
and transaction semantics — not two parallel implementations.

Synchronous CPU or blocking components use an explicit bounded adapter.
Starting, cancellation, completion, and late-stop behavior are observable.

## Chunk lifecycle

A canonical chunk attempt:

1. opens required streams and loads their last committed state;
2. reads up to a bounded completion policy;
3. processes each item and records filtered or failed outcomes;
4. invokes the writer with the bounded output chunk;
5. prepares stream state, counters, and checkpoint;
6. commits enlisted business effects and durable progress as one declared
   transaction, or follows the selected cross-resource delivery mode;
7. publishes the commit result and continues, stops, or closes.

No state or count becomes authoritative before its commit. Failure or
cancellation before commit leaves the previous checkpoint authoritative.
Unknown commit outcomes enter explicit recovery rather than automatic replay.

Open, update, close, and listener/interceptor ordering are part of the
conformance contract. Close errors do not erase earlier committed chunks.

## State and checkpointing

Component state uses a namespace, schema ID and version, codec ID and version,
bounded encoded size and depth, checksum, and sensitivity class. Migration is
explicit and deterministic. Unknown newer versions fail closed. Large state
uses a bounded external blob capability with content identity; it is not
silently inlined into metadata.

Stateful components that disable persistence MUST declare the resulting
restart limitation. A plan cannot mark the step restartable unless every
required state transition can be reconstructed.

## Composition taxonomy

Standard composition includes:

- composite and delegating readers, processors, and writers;
- classifier-selected delegates;
- validator and filter processors;
- peek and aggregate readers;
- multi-resource readers and writers;
- synchronization/thread-safety wrappers;
- line, resource, database, messaging, object-store, and HTTP adapters.

Composition preserves ordering, transaction participation, checkpoint
ownership, error classification, and close behavior. A wrapper MUST NOT claim a
stronger capability than its least-capable delegate.

## Completion, retry, skip, and rollback

Completion policies may use bounded item count, time, composite conditions, or
an adaptive policy whose decision is persisted. Retry and skip counters are
durable at their defined commit boundary. Backoff is cancellable. Rollback
classification is typed, and no-rollback behavior must state which effects and
state can remain visible.

## Standard-component requirements

Every first-party component documents:

- input/output types and format/version;
- state schema and checkpoint ownership;
- ordering, restart, and thread-safety properties;
- transaction and delivery capabilities;
- resource bounds, backpressure, cancellation, and close behavior;
- sensitive data and diagnostic fields;
- contract, crash, conformance, and performance evidence;
- support tier and maintained versions.

## Evidence

Required evidence includes native/erased semantic-equivalence tests,
allocation measurements, compile-fail type checks, reusable component contract
suites, state migration fixtures, stop/failure coverage at every lifecycle
boundary, chunk crash tests, and restart tests for composites and decorators.
