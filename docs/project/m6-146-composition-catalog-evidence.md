# M6 Standard Composition Catalog Evidence

**State:** Complete on merge

**Issue:** [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146)

This record maps issue #146's required component families and evidence
requirements to production types and deterministic test evidence. It
implements the standard composition catalog under the composition rules
closed by [M6 Gate E](m6-design-gate-evidence.md#gate-e--composition-semantics)
and the standard-component requirements closed by
[Gate D](m6-design-gate-evidence.md#gate-d--standard-component-semantics); it
does not reopen ADR-0008, Gate C/D/E, or the `ItemStream` contract (#144), and
it does not implement multi-resource components, format-specific adapters
(CSV/JSON/PostgreSQL), the item-listener taxonomy, the pipeline configuration
DSL, or M10 multi-threaded local execution.

## Component families delivered

All catalog types live under `oxide_batch::item_components` (a dedicated
public module, not flattened into the facade root, to avoid pollution of the
already-large `oxide_batch::*` re-export surface). Every type documents its
full standard-component contract (input/output, state/checkpoint, ordering,
restartability, thread safety, reentrancy, transaction/delivery, bounded
resource, cancellation, close, sensitive diagnostics, malformed-input
behavior, support tier, and evidence pointer) in its own rustdoc.

| Family | Type(s) | Module |
| --- | --- | --- |
| Basic iterator/list readers and minimal delegates | `IterReader`, `IdentityProcessor`, `NoopWriter` | `item_components::basic` |
| Composite/delegating reader | `CompositeReader` (sequential multi-delegate) | `item_components::composite` |
| Composite/delegating processor | `ChainProcessor` (two-stage chain; nest to compose more) | `item_components::composite` |
| Composite/delegating writer | `FanOutWriter` (sequential enlisted-transaction reborrow) | `item_components::composite` |
| Classifier-selected delegates | `ClassifyingProcessor`, `ClassifyingWriter` | `item_components::classify` |
| Validator processor | `ValidatingProcessor`, `ItemValidator` | `item_components::validate` |
| Filter processor | `FilterProcessor`, `ItemFilter` | `item_components::filter` |
| Peek reader | `PeekReader`, `PeekOutcome` | `item_components::peek` |
| Aggregate reader | `AggregatingReader` (bound reuses `ChunkSize`) | `item_components::aggregate` |
| Synchronization/thread-safety wrappers | `SynchronizedProcessor`, `SynchronizedWriter` (`tokio::sync::Mutex`) | `item_components::sync` |

The decorator catalog maps to fewer generic implementations where reuse is
cleaner, per the issue's own allowance: `ValidatingProcessor` and
`FilterProcessor` both compose with `ChainProcessor` instead of each carrying
its own inner-delegate generic; `ClassifyingProcessor`/`ClassifyingWriter`
share one `Classifier<I, K>` trait; `SynchronizedProcessor`/
`SynchronizedWriter` share one mutex-serialization shape.

## Composition semantics (capability meet, Gate E)

No new capability-declaration framework was introduced. Ordering,
restartability, thread safety, and error classification are proved by tests
against the real production types rather than a separate descriptor object,
per the issue's own guidance to reuse Rust trait bounds and explicit
component APIs where they already express the guarantee:

| Rule | Evidence |
| --- | --- |
| Ordering preserved / order-sensitivity propagates | `composite_reader_concatenates_delegates_in_order`, `classifying_writer_preserves_original_item_order_across_delegates` |
| Restartability: wrapper never hides delegate state | `item_components_stream_composition.rs` (see below); wrapper rustdocs document delegate `ItemStream` pairing is registered independently, never proxied |
| Thread safety: wrapper never claims more than its delegate, except the permitted synchronization refinement | `sync.rs` rustdoc; `synchronized_processor_serializes_concurrent_callers` |
| Error classification unchanged | `composite_reader_failure_propagates_unchanged_without_touching_next_delegate`, `chain_processor_first_stage_failure_propagates_unchanged`, `chain_processor_second_stage_failure_propagates_unchanged`, `classifying_processor_delegate_failure_propagates_unchanged` |
| Close ordering (reverse successful-open, non-blocking, primary-preserving) | `a_close_failure_on_one_stream_does_not_block_another_opened_streams_close` |
| Classifier never infers a stronger capability from the selected delegate | `classifying_processor_heterogeneous_delegates_share_one_erased_type` -- every delegate shares one Rust type `D` (here `BoxedProcessor<I, O>`), so there is no way to construct a classifier whose static declaration is stronger for one key than another |

## `oxide-batch-test` consumption (closes #145's final condition)

Every test file below drives production components through the public
`oxide-batch-test` surface -- `ComponentFixture` for scoped calls, `TestStep`
for real `ChunkStep::execute` runs, `TestJob` for a full restart through
`JobLauncher`, and `inject`/`restart` for distinguishable failure/stop
injection and durable restart evidence -- rather than a hand-rolled parallel
harness. Because `oxide-batch-test` depends on `oxide-batch` (never the
reverse, per Gate G), these files live under
`crates/oxide-batch-test/tests/`, not `crates/oxide-batch/tests/`, to avoid a
workspace dev-dependency cycle:

| Kit facility | Used by |
| --- | --- |
| `ComponentFixture` | `item_components_basic.rs`, `item_components_composite.rs`, `item_components_classify.rs`, `item_components_decorators.rs` |
| `TestStep` | `item_components_stream_composition.rs` |
| `TestJob` + `restart::range_reader` + `restart::ObservingTransactions` | `postgres_item_components_restart.rs` |
| `inject::{InjectedReader, InjectedProcessor, InjectedStream}` | all of the above except `item_components_basic.rs` |

This is #146's own required consumption, and it is also the later M6
component issue #145 needed to close: #145 remains open only until a
component issue actually consumes the kit as its primary harness, and #146's
suite above is that consumer.

## Typed/erased semantic equivalence

`item_components_equivalence.rs` drives one representative decorated
pipeline -- `PeekReader` over `CompositeReader` of two `IterReader`s, a
`ChainProcessor` of `FilterProcessor` and `IdentityProcessor`, and a
`SynchronizedWriter` over a recording writer -- through the same production
`ChunkStep` twice: once fully typed/monomorphized, once through
`BoxedReader`/`BoxedProcessor`/`BoxedWriter`. Both report identical produced
items, `ChunkExecutionOutcome`, committed counts, and (for the stop case)
identical empty writer effects.

## Allocation evidence

`item_components_allocation.rs` reuses `chunk_allocation.rs`'s exact
methodology (a single `#[test]` per file, `stats_alloc`, a delta between a
200-item and a 20,000-item run so ordinary amortized `Vec` growth cannot be
mistaken for a regression) against the same decorated pipeline. Measured on
the development host: the typed path's allocator-call delta across 19,800
additional items is 21 (Vec growth only, `<< delta_items / 100`); the erased
positive control's delta is 79,221 (`>= delta_items`), proving the harness
would catch a regression. Composition/decoration does not reintroduce
per-item boxing.

## Writer transaction reborrow

`fan_out_writer_reborrows_the_same_transaction_sequentially_in_order` and
`classifying_writer_preserves_original_item_order_across_delegates` (which
also enlists a transaction) exercise the real `WriteContext::enlisted`/
`transaction()` reborrow path with a `BusinessTransaction` fixture that
records statement order: both prove delegates write sequentially through the
one reborrowed transaction, in order, never opening a second transaction.
`fan_out_writer_failure_short_circuits_remaining_delegates` and
`fan_out_writer_stop_short_circuits_before_any_delegate` prove a failure or
stop terminates the fan-out without invoking later delegates.

## Stop and failure propagation at every nesting level

| Component | Stop evidence | Failure evidence |
| --- | --- | --- |
| `CompositeReader` | `composite_reader_stop_short_circuits_without_advancing_to_next_delegate` | `composite_reader_failure_propagates_unchanged_without_touching_next_delegate` |
| `ChainProcessor` | `chain_processor_filtered_first_stage_short_circuits_second` (stop and filter share the short-circuit path) | `chain_processor_first_stage_failure_propagates_unchanged`, `chain_processor_second_stage_failure_propagates_unchanged` |
| `FanOutWriter` | `fan_out_writer_stop_short_circuits_before_any_delegate` | `fan_out_writer_failure_short_circuits_remaining_delegates` |
| `ClassifyingProcessor`/`ClassifyingWriter` | `classifying_writer_stop_short_circuits` | `classifying_processor_delegate_failure_propagates_unchanged`, `classifying_processor_missing_key_is_a_typed_failure`, `classifying_writer_missing_key_is_a_typed_failure` |
| `PeekReader` | `peek_preserves_stop` | `peek_failure_is_not_cached_and_retries_the_delegate` |
| `AggregatingReader` | `aggregate_stop_discards_the_partial_group` | `aggregate_failure_discards_the_partial_group_rather_than_truncating_it` |
| Every registered `ItemStream` (close specifically) | -- | `a_close_failure_on_one_stream_does_not_block_another_opened_streams_close` |

A one-shot marker-based injection (`InjectionLog`/`InjectionId`) distinguishes
every injected failure/stop from a genuine framework defect, per #145's own
requirement.

## Peek semantics (5.6)

`item_components_decorators.rs`: `peek_returns_next_item_without_consuming_it`,
`repeated_peek_is_stable_and_calls_the_delegate_once`,
`read_after_peek_consumes_the_buffered_item_exactly_once`,
`peek_end_of_input_is_stable`, `peek_preserves_stop`,
`peek_failure_is_not_cached_and_retries_the_delegate`. Restart/checkpoint
safety is proved against a real durable, restartable reader/stream pair
(`oxide-batch-test`'s `restart::range_reader`), calling `peek` before *every*
real read throughout a full run, by
`postgres_item_components_restart.rs::peek_decorated_reader_restarts_from_the_last_committed_checkpoint`
(requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips, not fails, otherwise, per
this repository's PostgreSQL evidence convention).

## Aggregate bounds (5.7)

`item_components_decorators.rs`: `aggregate_emits_exactly_at_the_bound`,
`aggregate_emits_a_partial_final_group_then_stable_end_of_input`,
`aggregate_empty_input_is_end_of_input_not_an_empty_group`,
`aggregate_failure_discards_the_partial_group_rather_than_truncating_it`,
`aggregate_stop_discards_the_partial_group`,
`aggregate_never_buffers_beyond_its_bound` (an aggregation function that
asserts its input group never exceeds the configured bound, so the bound is
proved rather than merely typical).

## Close ordering where lifecycle composition exists (5.8)

No catalog type owns `ItemStream` state itself: a delegate that implements
`ItemStream` is registered independently at `ChunkStep`/`TestStep`
assembly, never proxied or hidden by a composite/decorator, per Gate E's
"must not hide a delegate's state" rule. Composed multi-stream lifecycle is
therefore a property of the already-implemented (#144) `ChunkStep`
registration, exercised here through catalog reader/processor/writer
components (`CompositeReader`, `ChainProcessor`, `NoopWriter`) driving the
step:
`item_components_stream_composition.rs::a_close_failure_on_one_stream_does_not_block_another_opened_streams_close`
proves reverse-successful-open close order, a close failure on one stream
not blocking another already-opened stream's close, and the close failure
not erasing the earlier committed primary outcome
(`ChunkExecutionReport::original_outcome`/`stream_close_failed`), all through
the existing production contract rather than a reimplementation.

## Known intentional scope exclusions

Per issue #146's own out-of-scope list, section 3 of the driving task, and
Gate E: multi-resource readers/writers and object-store support (#150),
CSV/delimited/fixed-width (#147), JSON/JSONL (#148), PostgreSQL
cursor/paging/SQL batch components (#149), item-listener taxonomy changes
(#151), pipeline configuration DSL/builder work (#152), M7 scope/late
binding, and M10 multi-threaded local execution. A classifier-selected
*reader* was deliberately not added: nothing meaningful to classify exists
before a read produces an item, and Spring Batch has no such component
either.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo test -p oxide-batch-test --features postgres --test postgres_item_components_restart
```

The `postgres_item_components_restart` target requires
`OXIDEBATCH_POSTGRES_TEST_URL` set to an isolated migrated database and is
skipped otherwise.

## Ledger disposition

`ITEM-COMPOSITE-001` and `ITEM-DECORATOR-001` move from `Planned` to
`Implemented`. Neither promotes to `Verified` on this branch: promotion
requires a named released `oxide-batch` version, per the ledger's own
promotion rule, which #146 does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

`TEST-JOB-001`, `TEST-STEP-001`, `TEST-SCOPE-001`, and `TEST-REPO-001`'s
cross-consumer closure notes are updated: #146 is the later M6 component
issue #145 required to close.
