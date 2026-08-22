# Item-Processing Model

**State:** Active

**Governing decision:** [ADR-0008](decisions/0008-item-component-contract.md)
(Accepted 2026-08-03) fixes the item reader/processor/writer public contract
shape recorded below. The remaining sections of this document — state and
checkpointing, composition taxonomy, and standard-component requirements —
are closed as design by the
[M6 design-gate evidence](../project/m6-design-gate-evidence.md) (Gates C, D,
E, and F). Design closure fixes the contract those sections state; it does
not itself implement state persistence, the component catalog, or the
listener decision — see each closed section below for its implementation
owner. The chunk-lifecycle and completion/retry/skip/rollback sections below
describe accepted M2/M3 behavior and are not part of the M6 gate closure.

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
public trait. `ItemStream` and `Checkpoint` state/version mechanics are
closed by [M6 Gate C](../project/m6-design-gate-evidence.md#gate-c--itemstream--component-state)
(see [State and checkpointing](#state-and-checkpointing) below); Gate C
closes the contract, not its implementation, which remains
[#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144)'s to
build.

**Item listeners are out of scope for ADR-0008** and keep the ADR-0002 boxed
`Pin<Box<dyn Future<Output = T> + Send + 'a>>` form: a listener set is a
heterogeneous, registration-ordered collection, and each registered listener
still costs one boxed future per item per phase. [M6 Gate F](../project/m6-design-gate-evidence.md#gate-f--item-listener-allocation)
closes this as an explicit KEEP decision for M6: no allocation-reducing
listener type system is introduced, and listener allocation cost is measured
and reported separately from the listener-free typed-path guarantee rather
than folded into it.

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

Closed by [M6 Gate C](../project/m6-design-gate-evidence.md#gate-c--itemstream--component-state).
Component state identity is a stable namespace, scoped under the owning
component's logical identity — never a display name, runtime object
identity, or process-local pointer/object address — plus a schema ID and
version and a codec ID and version. Delegate/composite state namespaces MUST
NOT collide (see [Composition taxonomy](#composition-taxonomy)).

Bounds reuse the M5 context envelope unchanged: default encoded size
`64 KiB` and default depth `16`, hard ceilings of `1 MiB` and depth `64`, and
a `128`-byte schema-identifier bound. No new bound is introduced for
component state.

A checksum is verified before any decode or migration step runs. A checksum
mismatch is a typed corruption failure: corrupt state is never replaced with
empty or default state, never advances a checkpoint, and is never exposed as
a raw value in diagnostics. If the durable checksum encoding is implemented
before an algorithm change is anticipated, the format carries an algorithm
identity and version so a future change is a versioned migration rather than
a silent reinterpretation.

Migration is explicit and deterministic: an equal version decodes directly,
an older version applies one bounded deterministic directed migration chain,
and a newer version, an unknown schema, or an unknown codec all fail closed.
A migration failure is a known, not-committed outcome, and migration never
changes component or definition identity.

Component durable state carries an explicit sensitivity/disclosure
classification, declared once as part of the owning component's schema/state
contract under [standard-component requirements](#standard-component-requirements)
— not as a second, separately maintained envelope field, so identity and
disclosure policy never diverge into two sources of truth. Absent an explicit
non-sensitive declaration, durable component state is treated as sensitive by
default (fail-safe).

A sensitive state's raw payload MUST NOT appear in an error, a `Debug` or
`Display` implementation, a log, a tracing/telemetry event, an operator
diagnostic, or a diagnostic/support bundle — including when the state is
corrupt, fails checksum verification, fails to decode, or fails migration.
Diagnostics may disclose only safe metadata: the logical state namespace,
schema identity/version, codec identity/version, a framework-owned failure
category, size/bound metadata, and the checksum verification result. This
extends the same disclosure discipline standard-component requirements already
require of every first-party component's sensitive-data classification to
durable component state's raw payload specifically, rather than defining a
second taxonomy. A migration MUST NOT weaken a state's sensitivity/disclosure
policy; unknown or malformed sensitivity metadata is treated as sensitive
(fail-safe), never as a signal to relax disclosure.

Stateful components that disable persistence MUST declare the resulting
restart limitation. A plan cannot mark the step restartable unless every
required state transition can be reconstructed. Large state uses only a
bounded external blob capability addressed by content identity; it is never
silently inlined into metadata.

This contract is the named evidence owner for `META-CONTEXT-001`'s remaining
gap (an architecture spike rather than codec migration tests); the row
promotes only when [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144)
lands state-migration and rejection-fixture evidence against a named release.

### Implementation status (#144)

This contract is implemented. The `ItemStream` open/update/close trait, its
sealed erasure boundary, and `BoxedStream` live in
`crates/oxide-batch/src/item_stream.rs`, in the same ADR-0008 shape as
`ItemReader`/`ItemProcessor`/`ItemWriter`. The component-state envelope --
namespace, schema id/version, codec id/version, checksum algorithm id/version
and value, bounded inline/external payload, and the codec-version migration
axis alongside the reused M5 schema-version axis -- lives in
`crates/oxide-batch-core/src/component_state.rs`, reusing `StateLimits`,
`StateSchemaId`, `StateSchemaVersion`, `StateSchemaUpgrade`, and
`VersionedStateCodec` unchanged. `ComponentStreamIdentity` is a restart-relevant
token registered through `ChunkComponentRevisions::with_stream_revision`,
exactly like the existing reader/processor/writer revisions; a stream-free
chunk definition's manifest and fingerprint are unaffected. Open runs once per
step attempt before any component call; update runs once per committing chunk
attempt, after the writer succeeds and before the durable commit; close runs
once per step attempt, in reverse successful-open order, for every stream that
opened. `PostgreSQL` persistence adds a side table
(`crates/oxide-batch/migrations/0005_item_stream_component_state.sql`) keyed by
`(step_execution_id, namespace)`, bound into the same transaction and commit
statement as the existing checkpoint/context update
(`PostgresChunkTransaction::commit_with_component_state`) -- never a second
connection or transaction.

Executable evidence: `crates/oxide-batch/tests/item_stream.rs` (lifecycle and
ordering), `crates/oxide-batch/tests/item_stream_state.rs` (envelope, codec
and schema migration, checksum, bounds, sensitivity, restartability, external
reference), and `crates/oxide-batch/tests/postgres_item_stream_crash_recovery.rs`
(process-kill evidence before and after a proven commit). See
[the M6 `ItemStream` evidence record](../project/m6-item-stream-evidence.md)
for the full scenario inventory.

## Composition taxonomy

Closed by [M6 Gate E](../project/m6-design-gate-evidence.md#gate-e--composition-semantics).
Standard composition includes:

- composite and delegating readers, processors, and writers;
- classifier-selected delegates;
- validator and filter processors;
- peek and aggregate readers;
- multi-resource readers and writers;
- synchronization/thread-safety wrappers;
- line, resource, database, messaging, object-store, and HTTP adapters.

A wrapper's advertised capability is the intersection (meet) of its
delegates' capabilities, never their union and never a capability none of
them has:

- **ordering** — if any required delegate is order-sensitive, the composite
  stays order-sensitive;
- **transaction participation** — a composite must not claim a stronger
  transaction/delivery mode than every required delegate supports;
  `WriteContext`'s enlisted transaction reborrows sequentially into each
  delegate, and two delegates never hold the same `&mut BusinessTransaction`
  simultaneously;
- **restartability** — if any required delegate cannot reconstruct its
  state, the composite cannot claim restartability;
- **checkpoint/state** — a wrapper documents its own checkpoint ownership
  and its delegates' component-state namespaces, and must not hide a
  delegate's state or let two delegates' namespaces collide;
- **thread safety** — a wrapper must not advertise a capability every
  required delegate does not itself satisfy, unless the wrapper's own
  synchronization genuinely provides the narrower capability it separately
  advertises;
- **error classification** — a wrapper must not arbitrarily strengthen,
  weaken, or hide a delegate failure's classification; a filter/validator
  may convert an outcome only as its explicit, documented semantic purpose;
- **close ordering** — the default rule is close in the reverse of
  successful open order; a close failure on one delegate does not skip the
  close attempt on any other already-opened delegate, and never erases an
  earlier primary runtime failure;
- **classifier-selected delegates** — a wrapper that selects among several
  delegates at runtime must not infer a stronger static capability from the
  one delegate a given run selected; the static declaration holds for every
  delegate it could select.

A wrapper MUST NOT claim a stronger capability than its least-capable
delegate for any property above.

### Implementation status (#146)

The standard composition catalog required above is implemented under
`oxide_batch::item_components` (a dedicated public module, not flattened
into the facade root): basic iterator/list readers and minimal delegates
(`IterReader`, `IdentityProcessor`, `NoopWriter`); composite/delegating
readers, processors, and writers (`CompositeReader`, `ChainProcessor`,
`FanOutWriter`); classifier-selected delegates (`ClassifyingProcessor`,
`ClassifyingWriter`); a validator processor (`ValidatingProcessor`) whose
failure is a typed `ProcessorError`, never a panic or silent filter; a
filter processor (`FilterProcessor`) using `ProcessOutcome::Filtered`; a
peek reader (`PeekReader`); an aggregate reader (`AggregatingReader`) whose
bound reuses `ChunkSize`; and synchronization/thread-safety wrappers
(`SynchronizedProcessor`, `SynchronizedWriter`) that establish real mutual
exclusion via an async-aware lock -- the one case this taxonomy permits a
wrapper to strengthen rather than only meet. No catalog type owns
`ItemStream` state itself; a delegate that implements `ItemStream` is
registered independently at assembly, per the "must not hide a delegate's
state" rule above. Multi-resource readers/writers, format-specific
adapters, and item-listener changes remain out of scope, per issue #146.

Executable evidence: `crates/oxide-batch-test/tests/item_components_basic.rs`,
`item_components_composite.rs`, `item_components_classify.rs`,
`item_components_decorators.rs`, `item_components_stream_composition.rs`,
and `postgres_item_components_restart.rs`; and
`crates/oxide-batch/tests/item_components_equivalence.rs` and
`item_components_allocation.rs` (no `oxide-batch-test` dependency, so they
stay in `oxide-batch`'s own suite). See
[the M6 #146 evidence record](../project/m6-146-composition-catalog-evidence.md)
for the full scenario inventory.

## Completion, retry, skip, and rollback

Completion policies may use bounded item count, time, composite conditions, or
an adaptive policy whose decision is persisted. Retry and skip counters are
durable at their defined commit boundary. Backoff is cancellable. Rollback
classification is typed, and no-rollback behavior must state which effects and
state can remain visible.

## Standard-component requirements

Closed by [M6 Gate D](../project/m6-design-gate-evidence.md#gate-d--standard-component-semantics).
Every first-party component documents, at minimum:

- input type and output type;
- format and format version;
- state schema and checkpoint ownership;
- ordering semantics, restartability, and thread-safety;
- reentrancy, where relevant;
- transaction capability and delivery capability;
- maximum/bounded resource behavior, buffering behavior, and backpressure
  behavior;
- cancellation behavior and close behavior;
- sensitive-data classification and diagnostic/redaction behavior;
- malformed-input behavior and failure classification;
- support tier;
- required contract evidence, crash/restart evidence where stateful, and
  performance/resource evidence where applicable.

A prose claim of "supported" is not completion. A component pull request
ships its declared contract and its executable evidence together; a
contract without evidence, or evidence without a declared contract, is
incomplete.

## Evidence

Required evidence includes native/erased semantic-equivalence tests,
allocation measurements, compile-fail type checks, reusable component contract
suites, state migration fixtures, stop/failure coverage at every lifecycle
boundary, chunk crash tests, and restart tests for composites and decorators.
