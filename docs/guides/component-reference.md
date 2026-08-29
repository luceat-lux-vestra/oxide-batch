# Component Reference

**State:** Accepted

This is the user-facing reference for every first-party M6 item component:
what it does, what state it holds, what it guarantees, and where the
evidence for each guarantee lives. It documents behavior a caller can rely
on — for the underlying contracts these components implement, see the
[Item-Processing Model](../architecture/item-processing-model.md); for the
actual test evidence behind every claim below, see the
[component conformance matrix](../engineering/campaigns/m6/component-conformance-matrix.md),
which this document does not duplicate row-for-row.

## Support tier

This repository has one support-tier system, defined in the
[Integration Model](../architecture/integration-model.md#support-tiers):
**First-party** (maintained in the OxideBatch organization, covered by the
release support matrix), **Certified third-party**, and **Experimental**.
Every component in this reference is **First-party**. This reference does
not introduce a second, finer-grained tier scheme layered on top of that
one; where components differ meaningfully in evidence maturity, that
distinction is the conformance matrix's "Covered" vs. "N/A — `<reason>`"
per row, not a new tier label. `InMemoryObjectStore` is the one component
worth flagging explicitly here even though it is still First-party: it is
the object-store *capability contract* fixture, not a certified cloud
adapter — see its own entry below.

## How to read an entry

Every entry states: input/output, state and checkpoint ownership,
ordering, restartability, transaction/delivery capability (where
applicable), resource bounds, cancellation, close behavior, thread safety,
malformed-input behavior, failure classification, and sensitive-data
handling. A field reading "inherits the delegate's" means the component
adds nothing of its own in that dimension — see
[Restart and State](restart-and-state.md#composition-and-restartability-inheritance)
for why that is a real, checked guarantee rather than an assumption.

## Typed traits and erasure

`ItemReader<I>` / `ItemProcessor<I,O>` / `ItemWriter<I>` are the traits
every component below implements. `BoxedReader<I>` / `BoxedProcessor<I,O>` /
`BoxedWriter<I>` (`crates/oxide-batch/src/chunk.rs`) erase any concrete
implementation behind `Box<dyn ...>` at the caller's choice, with no
behavioral difference from the typed form in any durable/observable
respect — that equivalence is Gate B's entire subject; see
[Restart and State § Typed vs Boxed representation irrelevance](restart-and-state.md#typed-vs-boxed-representation-irrelevance).
The two representations differ only in per-item dispatch/allocation cost,
measured (not asserted as a threshold) by Gate H — see the
[performance plan](../engineering/performance-plan.md).

## Standard processors, delegates, classifiers, composites (#146)

- **`IterReader`, `IdentityProcessor`, `NoopWriter`** (`item_components::basic`):
  minimal, stateless fixture-grade components. Input/output: whatever `I`
  the caller instantiates them over. State: none. Restartability: N/A —
  hold no durable state of their own. Malformed input/failure: not
  applicable (no parsing or I/O). Thread safety: `Send + Sync` unconditionally.

- **`FilterProcessor`**: wraps a delegate `ItemProcessor`, discarding items a
  predicate rejects. State/restart: none of its own — inherits the
  delegate's. Ordering: preserves delegate order among retained items.
  Failure: a delegate error propagates unchanged.

- **`PeekReader`**: one-item lookahead over a delegate reader. State: an
  in-memory buffer of at most one item, never reported as read progress
  until consumed — restart resumes the delegate from its own last committed
  position, re-peeking from there (restartability: exactly the delegate's).
  Failure: a delegate failure during peek is not cached — the next call
  retries the delegate rather than replaying a stale error. Cancellation:
  preserves a delegate `Stopped` outcome.

- **`AggregatingReader`**: combines up to a bounded number (`ChunkSize`)
  of delegate items into one logical output item via a caller-supplied
  aggregation function. State: an in-memory buffer, bounded, never reported
  as read progress until a full or final partial group is returned.
  Restartability: exactly the delegate's — see its own doc comment
  (`crates/oxide-batch/src/item_components/aggregate.rs`) for the full
  reasoning. Malformed input/failure: a delegate failure preserves the
  in-flight buffer across the call (the framework's fault-retry contract
  re-invokes the same reader instance rather than reconstructing it, so
  discarding already-read input on a retryable failure would lose real
  data) — the buffer is cleared only by cooperative stop, which discards
  the partial group rather than emitting it. Bound: never exceeds the
  configured `ChunkSize`, proved as its own dedicated test, not left as an
  assumed default.

- **`ClassifyingProcessor`, `ClassifyingWriter`**: route to one delegate from
  a bounded, configured set, keyed by a value derived from the item.
  Capability (state, thread-safety, etc.) is uniformly the delegate type
  `D`'s own — because every entry shares one Rust type, a
  `ClassifyingProcessor`/`ClassifyingWriter` can never claim a stronger
  static capability than its least-capable delegate; a heterogeneous
  delegate set must be represented as `Boxed*` to be stored under one `D`
  at all. State/restart: none of its own — each delegate's own evidence
  applies unchanged. Failure: a delegate's error/stop propagates unchanged,
  never reclassified or swallowed.

- **`CompositeReader`, `ChainProcessor`, `FanOutWriter`**: pure,
  monomorphized composition — no `Boxed*` erasure is introduced by these
  types themselves (a pipeline built entirely from them keeps ADR-0008's
  zero-per-item-allocation property). Capability is the *meet* (intersection)
  of the delegates', never their union (Gate E). Ordering: preserves each
  delegate's relative order. State/restart: none added; a delegate that
  implements `ItemStream` is registered independently under its own
  namespace — never proxied or hidden by these wrappers, so no delegate's
  state can be silently lost or collide. Thread safety: `Send + Sync`
  exactly when every delegate is. Cancellation: any delegate's `Stopped`
  outcome stops the composite immediately; later delegates in the same
  call are never invoked.

- **`SynchronizedProcessor`, `SynchronizedWriter`**: wrap a delegate behind
  an async-aware mutex held for the delegate's entire call (including any
  `.await` inside it) — a genuine mutual-exclusion guarantee these wrappers
  add themselves, the one case Gate E permits a wrapper to *strengthen*
  rather than only meet a delegate's capability. Does not implement or
  imply the M10 multi-threaded local execution model; today's chunk runtime
  already drives one component call at a time. State/restart: opaque
  pass-through — inherits the delegate's. Ordering under contention:
  first-acquired, first-served for `tokio::sync::Mutex`, not a hard FIFO
  guarantee.

- **`ValidatingProcessor`**: rejects items failing a caller-supplied
  predicate as a typed processor failure rather than silently dropping or
  passing them through. State/restart: none of its own.

## Restartable flat-file / structured-file components (#147, #148)

- **`DelimitedReader`/`DelimitedWriter`** (CSV and CSV-family dialects):
  input/output a caller-defined record type via `DelimitedRecord`
  conversion. State: byte-offset read/write position, via a paired
  `ItemStream`. Restart: reader resumes strictly after the last fully
  committed record, never mid-multiline; writer truncates any uncommitted
  tail and resumes exactly once — both proved against a real `PostgreSQL`
  fixture. Malformed input: a parse failure is a typed `ReaderError`, not a
  partial/garbage record. Resource bounds: bounded read buffers, reused
  across records (no per-record allocation growth).

- **`FixedWidthReader`/`FixedWidthWriter`**: same shape as delimited, over a
  declared `FixedWidthLayout`/field set instead of a dialect. Malformed
  input: a record that violates a field's byte width, or splits a
  multi-byte UTF-8 boundary, is its own typed rejection — never silently
  truncated or padded. Restart: shares the same file/harness pattern and
  evidence as delimited (byte-offset restart position).

- **`JsonArrayReader`/`JsonArrayWriter`**: reads/writes one JSON array
  document, one element at a time. State: element-offset position. Restart:
  resumes strictly after the last fully committed element, never
  mid-element; writer truncates an uncommitted tail and fails closed if the
  file is shorter than its own committed-length record (a corruption
  signal, not silently re-derived).

- **`JsonLinesReader`/`JsonLinesWriter`**: same restart/fail-closed shape as
  JSON array, over newline-delimited JSON instead of one array document —
  resumes after the last fully committed line.

## Multi-resource and object-store components (#150)

- **`MultiResourceReader`/`MultiResourceWriter`**: traverse an ordered set
  of physical resources (files, objects) as one logical input/output, where
  the resource count is not known until runtime (unlike `CompositeReader`,
  which requires a statically-declared delegate list). State: **one**
  namespace for the whole resource set — resource index plus the current
  resource's own embedded delegate position — never one namespace per
  resource. At most one resource is open at a time; the current resource's
  stream is always closed before the next opens. Restart: resumes across a
  resource-boundary crash with no duplication and no skip, for both reader
  and writer — the writer's own restart/crash evidence
  (`multi_resource_writer_restarts_across_a_resource_boundary_crash`) was a
  confirmed gap this campaign closed; see the conformance matrix. A
  resource reaching its own boundary is a distinct event from the
  enclosing chunk attempt reaching its terminal outcome, and is recorded
  as such (`StreamRuntimeOutcome::ResourceBoundary`, never `Committed`).

- **`InMemoryObjectStore`** (`ObjectStoreCapability`): the provider-neutral
  capability *contract fixture* for object-store adapters — bounded
  get/put/stat/list with a stable version token — not a certified cloud
  adapter. No cloud SDK integration ships in M6; full S3/Azure/GCS
  certification is M9. Buffering: whole-object (get fully parses before
  serving items; put accumulates the whole object and reissues it per
  write call, matching real object-store `PUT` semantics), bounded by
  `max_object_bytes` checked *before* a proportional buffer is allocated,
  not as a post-hoc length check. Restart/crash: **N/A** — this module has
  no durable backing store to restart against; a real crash/restart claim
  is only meaningful once a real provider adapter exists, which sits on
  the same `MultiResourceReaderOpener`/`WriterOpener` restart model already
  evidenced above, per #150's own scope note.

## PostgreSQL components (#149)

- **`PostgresCursorReader`**: server-side `DECLARE CURSOR` paging. State:
  keyset/cursor position via `ItemStream`. Restart/crash: a `PostgreSQL`
  server-side cursor does not survive a crash by construction — "a fresh
  process has no cursor and no transaction," per its own doc comment —
  so restart re-executes `DECLARE CURSOR` from the durable keyset position
  rather than attempting to resume a session. This is the tested contract
  itself, not an evidence gap.

- **`PostgresPagingReader`**: keyset-predicate paging with no server-side
  session to lose — each page is independently re-derivable, so "restart"
  is simply resuming the keyset predicate from durable state.

- **`PostgresBatchWriter`**: an enlisted writer with no connection of its
  own — it requires participation in the chunk's own transaction structurally,
  the same enlistment pattern documented in
  [Restart and State § Checkpoint relationship and transaction atomicity](restart-and-state.md#checkpoint-relationship-and-transaction-atomicity).
  Atomicity with checkpoint/counters is Gate B's subject
  (`gate_b_01`–`08` exercise this same enlistment mechanism directly).

## Completion policies

- **`ItemCountCompletionPolicy`, `TimeCompletionPolicy`**: stateless
  thresholds. No persisted decision, no restart obligation.
- **`CompositeCompletionPolicy`**: pure composition of member policies —
  inherits their restart evidence, adds none of its own.
- **`AdaptiveCompletionPolicy`**: the one first-party completion policy
  with genuinely persisted, restart-relevant decision state, evidenced
  against a real `PostgreSQL` fixture
  (`crates/oxide-batch/tests/postgres_completion_policy_restart.rs`).

## Pipeline/job construction

- **`ChunkPipelineBuilder`** (#152): application-facing constructor over
  `ChunkStep`/`ChunkJob`. Its typed-vs-`Boxed*` fingerprint equivalence is
  proved at the definition level
  (`typed_and_boxed_pipelines_share_one_fingerprint`) and extended to a
  real-database restart-selection guarantee by Gate B's `gate_b_08`.
- **`ChunkJob`/`ChunkStep`**: the chunk runtime every component above runs
  under. See the [Item-Processing Model](../architecture/item-processing-model.md)
  for its lifecycle contract.
- **`FlowJob`**: flow-level orchestration over one or more steps, including
  partition/split execution. Restart/crash evidence:
  `postgres_flow_crash_recovery.rs`,
  `postgres_local_partition_crash_recovery.rs`,
  `postgres_local_split_crash_recovery.rs`.
- **Item listeners** (`ReadListener`/`ProcessListener`/`WriteListener`/
  `RetryListener`/`SkipListener`): kept as a boxed, per-item-per-phase
  representation for M6 — a deliberate decision (Gate F), not an oversight
  — with its allocation cost measured and reported separately from typed
  component cost, never folded into it. See
  [performance plan § Gate H](../engineering/performance-plan.md) and
  `crates/oxide-batch/tests/item_listener_allocation.rs`.
