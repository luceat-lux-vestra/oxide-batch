# Extension Guide

**State:** Accepted

This guide is for implementing a custom `ItemReader`, `ItemProcessor`,
`ItemWriter`, stateful `ItemStream`, or `CompletionPolicy`. It states the
contract each trait imposes and links to the canonical documents that own
the underlying rules, rather than restating them; see
[Restart and State](restart-and-state.md) for what a stateful component
specifically owes the framework across a restart, and the
[fault-tolerance contract](../architecture/fault-tolerance.md) for the full
retry/skip policy `FailureCategory` feeds into.

## `ItemReader<I>`

```rust
pub trait ItemReader<I>: Send {
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a;
}
```

(`crates/oxide-batch/src/chunk.rs`) `&mut self`: a reader is used
exclusively, never called concurrently with itself. Return
`ReadOutcome::Item(item)` for one item, `ReadOutcome::EndOfInput` when
exhausted, or `ReadOutcome::Stopped` when `context.stop_token()` requests
cooperative stop — check it at a reasonable granularity (not necessarily
every call, but never ignore it entirely on a long-running read).
Malformed input is a typed `ReaderError`, not a partial or best-effort
item — see `DelimitedReader`'s handling in the
[component reference](component-reference.md) for a first-party example of
this discipline. A panic inside `read` is converted to a typed failure at
the framework boundary rather than unwinding the whole step (verified by
`crates/oxide-batch-test/tests/gate_g_scenarios.rs::failure_panic_and_stop_injection_are_available_to_application_tests`,
which injects a real panic and asserts the step reports
`ChunkFailure::ReaderPanic` rather than propagating the unwind) — but a
reader must not *rely* on this to avoid its own resource cleanup; a panic
still means the current call's work is lost.

### Example: a minimal stateless reader

Modeled on `crates/oxide-batch/src/item_components/basic.rs`'s
`IterReader` (grep to confirm the real type before assuming the shape below
is copied verbatim — this is simplified for exposition):

```rust
struct RangeReader { next: i64, end: i64 }

impl ItemReader<i64> for RangeReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        if self.next >= self.end {
            return Ok(ReadOutcome::EndOfInput);
        }
        let item = self.next;
        self.next += 1;
        Ok(ReadOutcome::Item(item))
    }
}
```

### Example: a stateful reader with a checkpoint

A reader that must resume from where a previous attempt left off pairs
itself with an `ItemStream` implementation, registered under its own
`ComponentStreamIdentity`. `crates/oxide-batch/src/item_components/delimited.rs`'s
`DelimitedReader`/`DelimitedReaderStream` pair is the first-party pattern to
study: the reader tracks an in-memory cursor during a step attempt: the
paired stream's `open` restores that cursor from the last committed
checkpoint at the start of an attempt, and its `update` produces the
candidate checkpoint envelope for the chunk about to commit. See
[Restart and State § Logical component identity, stream identity, and
revision](restart-and-state.md#logical-component-identity-stream-identity-and-revision)
for exactly what identity/schema/version metadata this requires, and
[§ Schema/codec version, migration, and rejection](restart-and-state.md#schemacodec-version-migration-and-rejection)
for what a version mismatch on restart must do (fail closed — never
silently reinterpret or default).

## `ItemProcessor<I, O>`

```rust
pub trait ItemProcessor<I, O>: Send + Sync {
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> impl Future<Output = Result<ProcessOutcome<O>, ProcessorError>> + Send + 'a;
}
```

`&self`, not `&mut self`: a processor may be called concurrently unless it
adds its own synchronization (see `SynchronizedProcessor` in the
[component reference](component-reference.md#standard-processors-delegates-classifiers-composites-146)
for the first-party pattern if your processor genuinely needs serialized
access to some internal resource). Return `ProcessOutcome::Item(output)`,
`ProcessOutcome::Filtered` to drop the item without writing it, or
`ProcessOutcome::Stopped`. A processor is stateless by convention in this
codebase — every first-party processor either holds no state or delegates
to something that does; if yours needs durable state, the same `ItemStream`
pairing pattern as a stateful reader applies.

## `ItemWriter<I>`

```rust
pub trait ItemWriter<I>: Send + Sync {
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> impl Future<Output = Result<WriteOutcome, WriterError>> + Send + 'a;
}
```

Writes one nonempty batch (a chunk's items, delivered together). **The
enlistment rule is the one most worth getting right**: `context.transaction()`
returns `Option<&mut dyn BusinessTransaction>` — `Some` when this call
participates in the chunk's own atomic transaction
(`context.is_enlisted()`). A writer that needs `AtomicSameResource` delivery
**must** write through that transaction (`business.execute(BusinessStatement::new(sql,
&values))`) rather than its own independent connection — a writer using its
own connection commits immediately regardless of whether the chunk's own
checkpoint/counter commit later succeeds or fails, which silently defeats
the atomicity guarantee. This is not a hypothetical mistake: building this
campaign's own Gate B test harness made exactly this error once, and a
forced-failure test caught it directly — see [Restart and State § Checkpoint
relationship and transaction
atomicity](restart-and-state.md#checkpoint-relationship-and-transaction-atomicity)
for the full account, and `PostgresBatchWriter` in the [component
reference](component-reference.md#postgresql-components-149) for the
first-party writer that has no connection field of its own for exactly this
reason — it requires enlistment structurally, so the mistake is not
representable.

## Stateful `ItemStream`

```rust
pub trait ItemStream: Send + Sync {
    fn open(&self, context: StreamOpenContext<'_>)
        -> impl Future<Output = Result<StreamOpenOutcome, StreamOpenError>> + Send + '_;
    // update(..) -> candidate component-state envelope for the chunk about to commit
    // close(..)
}
```

(`crates/oxide-batch/src/item_stream.rs` — read the full trait there for
`update`/`close`'s exact signatures before implementing). `open` restores
last-committed state or begins initial execution; if it fails, **no**
reader/processor/writer call starts for that step attempt — a broken
stream fails the whole attempt closed rather than starting with corrupt or
absent state. `update` produces the candidate durable envelope for the
chunk about to commit — this is where your component's checkpoint bytes
actually get produced; see [Restart and State § Schema/codec version,
migration, and rejection](restart-and-state.md#schemacodec-version-migration-and-rejection)
for the decode/migration rules that apply to whatever you encode here.

**Proving your own stateful component's restart correctness**: don't copy
Gate B's `crates/oxide-batch/tests/support/gate_b.rs` internals (those are
scoped to this repository's own typed-vs-`Boxed*` equivalence campaign,
against its own PostgreSQL fixture setup). Use the public test-kit's
`oxide_batch_test::restart` module instead — `range_reader`/
`ObservingTransactions` are the public equivalents of the same pattern,
built for exactly this. See the [test-kit tutorial](test-kit-tutorial.md#restart-testing)
and the real worked example in
`crates/oxide-batch-test/tests/postgres_item_components_restart.rs::peek_decorated_reader_restarts_from_the_last_committed_checkpoint`.

## `CompletionPolicy`

```rust
pub trait CompletionPolicy: Send + Sync {
    fn begin_chunk(&self, /* .. */);
    fn end_chunk(&self, /* .. */);
    // ..
}
```

(`crates/oxide-batch/src/completion.rs` — read it in full; the lifecycle
contract in its own doc comment, `REPEAT-POLICY-001`, is precise about
exactly when `begin_chunk`/`end_chunk` fire, including for a replayed
attempt — don't assume). Most custom policies are stateless thresholds like
first-party `ItemCountCompletionPolicy`/`TimeCompletionPolicy`. If yours
persists a decision across restarts the way `AdaptiveCompletionPolicy`
does, it owns that state's namespace and revision the same way a stateful
reader/writer does — see [Restart and State § Policy-owned state/revision
semantics](restart-and-state.md#policy-owned-staterevision-semantics).

## Failure classification

`FailureCategory` (`crates/oxide-batch-core/src/domain/execution.rs`) is
how a component tells the framework what kind of failure occurred:
`InvalidDefinition`, `DuplicateExecution`, `IllegalTransition`,
`TransientInfrastructure`, `PermanentInfrastructure`, `UserComponent`,
`Cancelled`, `Serialization`, `Invariant`, `OptimisticConflict`, `Timeout`,
`UnsupportedCapability`, and the commit-outcome-unknown category described
in [Restart and State](restart-and-state.md#checkpoint-relationship-and-transaction-atomicity).
A custom component returns the category that actually describes the
failure — `UserComponent` for your own component's logic rejecting input,
`TransientInfrastructure`/`PermanentInfrastructure` for a dependency it
calls out to. The category feeds the framework's retry/skip/fault policy;
see the [fault-tolerance contract](../architecture/fault-tolerance.md) for
the full policy this drives, which this guide does not restate.

## Cancellation

Every call context (`ReadContext`, `ProcessContext`, `WriteContext`, stream
contexts) carries a `stop_token()`. Checking it and returning the relevant
`Stopped` outcome is cooperative — nothing forces a component to check
frequently, but a long-running call that never checks makes cooperative
stop ineffective for that component. See
`crates/oxide-batch-test/tests/gate_g_scenarios.rs`'s stop-injection case
(`ComponentAction::Stop`) for a real, working example of asserting this
behavior in a test.
