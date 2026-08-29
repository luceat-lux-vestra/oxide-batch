# M6 component conformance/failure matrix

**Owner:** [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153)

This is the shipped-component coverage matrix the M6 exit criteria require:
every first-party component #143–#152 shipped, what scenarios apply to it,
and where the evidence for each applicable scenario actually lives. A row
that says a scenario is covered names the real test file and function; a row
that says a scenario is not applicable says why, in terms of the component's
own documented contract, not by omission.

Coverage lives in two places, and both were checked for every component
below: `crates/oxide-batch/tests/` (the framework crate's own integration
tests) and `crates/oxide-batch-test/tests/` (the public test-kit crate's own
test suite, which is where most item-component functional and restart
evidence actually lives — an earlier pass in this campaign checked only the
former and wrongly concluded several components were untested; this document
corrects that).

## How to read a row

- **Applicable scenarios** are judged from the component's own doc comment
  (most declare their state/checkpoint/restartability/failure contract
  explicitly) — not every scenario applies to every component, and this
  matrix does not force one.
- **Evidence** is a real file:function reference, or `N/A — <reason>`.
- **Restart/crash** is called out on its own because it is the scenario this
  campaign is most concerned is not silently missing for a stateful
  component. It is real `PostgreSQL`-fixture evidence unless noted.

## Typed component traits and erasure

| Component | Normal | Malformed | Failure/rollback | Restart/crash | Notes |
|---|---|---|---|---|---|
| `ItemReader<I>` / `ItemProcessor<I,O>` / `ItemWriter<I>` (traits) | Covered via every concrete impl below | — | — | — | Trait-level contract, not independently testable; conformance is per-impl. |
| `BoxedReader<I>` / `BoxedProcessor<I,O>` / `BoxedWriter<I>` | `item_components_allocation.rs`, `gate_h_allocation.rs`, all Gate B scenarios (`gate_b_01`–`08`) | Inherits delegate's | Inherits delegate's | Gate B (`gate_b_05`–`08`): real process-kill restart under the Boxed representation, cross-representation restart in `gate_b_08` | Representation-equivalence with the typed path is Gate B's entire subject, not restated per-component here. |
| `ItemStream` / `BoxedStream` | `chunk.rs`, `chunk_runtime.rs`, every restartable component's own stream tests below | Schema/codec mismatch: `postgres_schema_rejection.rs`, `postgres_schema_upgrade.rs` | Stream `open`/`update`/`close` failure injection: `oxide_batch_test::inject::InjectedStream` — exercised in restart tests throughout `crates/oxide-batch-test/tests/postgres_*.rs` | Every restart test below | Base restart-state abstraction; not a "component" a user selects, so scenario applicability is inherited by whatever uses it. |

## Standard processors/delegates/classifiers/composites (#146)

| Component | Normal/functional | Failure (preserved buffer / retry) | Stop | Restart/crash | Notes |
|---|---|---|---|---|---|
| `IterReader`, `IdentityProcessor`, `NoopWriter` (`basic.rs`) | `crates/oxide-batch-test/tests/item_components_basic.rs` | — | — | `N/A — holds no durable state of its own (module doc: "ephemeral, in-memory... declare no restart capability beyond a paired ItemStream")` | Fixture-grade by design. |
| `FilterProcessor` | `crates/oxide-batch-test/tests/item_components_decorators.rs` (via `ChainProcessor`/direct) | — | — | `N/A — stateless per-item decision, no buffer, restartability is exactly the delegate's` | |
| `PeekReader` | `item_components_decorators.rs::peek_*` (5 tests: returns-without-consuming, stable repeat, read-after-peek, EOF stability, stop preservation) | `peek_failure_is_not_cached_and_retries_the_delegate` | `peek_preserves_stop` | `N/A — in-memory one-item lookahead buffer only, restartability exactly the delegate's (component doc comment states this explicitly)` | |
| `AggregatingReader` | `item_components_decorators.rs::aggregate_emits_exactly_at_the_bound`, `..._a_partial_final_group_then_stable_end_of_input`, `..._empty_input_is_end_of_input` | `aggregate_failure_does_not_emit_a_truncated_aggregate`, `aggregate_retry_after_failure_resumes_the_preserved_buffer` | `aggregate_stop_discards_the_partial_group` | `N/A — component doc: "Restartability: exactly the delegate's"; the in-flight buffer is never reported as read progress until a full/final group is returned, so restart resumes the delegate from its own last committed position and re-aggregates from there. Bound enforcement: aggregate_never_buffers_beyond_its_bound.` | |
| `ClassifyingProcessor`, `ClassifyingWriter` | `crates/oxide-batch-test/tests/item_components_classify.rs` (9 tests: routing by key, per-delegate isolation, unmapped-key handling, homogeneous vs `Boxed*`-erased heterogeneous delegates) | Covered within the same file (delegate failure propagation) | — | `N/A — the classifier holds no state beyond its (stateless) key function; each delegate's own restart evidence applies unchanged (same delegate types as elsewhere in this matrix)` | |
| `CompositeReader`, `ChainProcessor`, `FanOutWriter` | `crates/oxide-batch-test/tests/item_components_composite.rs` (11 tests), `crates/oxide-batch/tests/item_components_equivalence.rs`, 1 inline test each in `composite.rs` | Delegate failure propagation covered in the same files | — | `N/A — pure composition wrappers, no state of their own; delegates' restart evidence applies unchanged` | Doc comment: "Every type here is a monomorphized decorator... A wrapper's advertised capability is the meet (intersection) of its [delegates']" (Gate E). |
| `SynchronizedProcessor`, `SynchronizedWriter` | `item_components_decorators.rs::synchronized_processor_still_delegates_every_call_correctly`, `synchronized_writer_delegates_and_preserves_the_write_context` | — | — | `N/A — a concurrency-serialization wrapper, no durable state; delegate's restart evidence applies unchanged` | Concurrency guarantee itself: `synchronized_processor_allows_at_most_one_delegate_call_in_flight`, `synchronized_writer_allows_at_most_one_delegate_call_in_flight`, contrasted with `unsynchronized_delegate_allows_concurrent_calls_control`/`unsynchronized_writer_allows_concurrent_calls_control` as a negative control proving the test would catch a regression. |
| `ValidatingProcessor` | `crates/oxide-batch-test/tests/item_components_classify.rs` (shares the file; 1 dedicated test) | Validator rejection path covered in the same file | — | `N/A — stateless per-item validation` | |

## Restartable flat-file / structured-file components

| Component | Normal | Malformed | Restart/crash | Notes |
|---|---|---|---|---|
| `DelimitedReader`/`DelimitedWriter` (#147) | `crates/oxide-batch-test/tests/item_components_delimited.rs`, inline tests in `delimited.rs` | `crates/oxide-batch-test/tests/item_components_flat_file_fault.rs` | `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs::delimited_reader_restarts_after_the_last_committed_record_never_mid_multiline`, `::delimited_writer_truncates_uncommitted_tail_and_resumes_exactly_once` | Real `PostgreSQL` fixture, real truncation/resume. |
| `FixedWidthReader`/`FixedWidthWriter` (#147) | `crates/oxide-batch-test/tests/item_components_fixed_width.rs`, inline tests | Same file (multi-byte UTF-8 boundary rejection is its own typed error, per component doc) | `postgres_flat_file_restart.rs` (shared file/harness with delimited, byte-offset restart position) | |
| `JsonArrayReader`/`JsonArrayWriter` (#148) | `crates/oxide-batch-test/tests/item_components_json_array.rs`, inline tests | `crates/oxide-batch-test/tests/item_components_json_fault.rs` | `crates/oxide-batch-test/tests/postgres_json_restart.rs::json_array_reader_restarts_after_the_last_committed_element_never_mid_element`, `::json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`, `::json_array_writer_fails_closed_when_the_file_is_shorter_than_committed` | |
| `JsonLinesReader`/`JsonLinesWriter` (#148) | `crates/oxide-batch-test/tests/item_components_jsonl.rs`, inline tests | Same fault file as JSON array | `postgres_json_restart.rs::jsonl_reader_restarts_after_the_last_committed_line`, `::jsonl_writer_truncates_uncommitted_tail_and_resumes_exactly_once`, `::jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed` | |

## Multi-resource and object-store components (#150)

| Component | Normal | Restart/crash | Notes |
|---|---|---|---|
| `MultiResourceReader` | `crates/oxide-batch/src/item_components/multi_resource.rs` inline tests | `crates/oxide-batch-test/tests/postgres_multi_resource_restart.rs::multi_resource_reader_restarts_across_a_resource_boundary_crash`, `::multi_resource_reader_completes_across_a_resource_boundary_with_no_crash` | Real `PostgreSQL` fixture, real resource-boundary crash via injected stop. |
| `MultiResourceWriter` | Inline tests in `multi_resource.rs` | `crates/oxide-batch-test/tests/postgres_multi_resource_restart.rs::multi_resource_writer_restarts_across_a_resource_boundary_crash` (**added by this campaign** — confirmed absent before: the writer's own contract doc declares real durable checkpoint state, "supplied explicitly at construction, same as `multi_resource_reader`," but had no restart/crash evidence anywhere in either test crate, unlike its reader counterpart) | Mirrors the reader's own crash test exactly: a stop injected on the write batch that would trigger resource rollover, restart resumes into the next resource with no duplication and no skip. |
| `InMemoryObjectStore` | 20 inline tests in `object_store.rs`, `crates/oxide-batch/tests/*` (get/put/stat/list, bounded/version-token semantics) | `N/A — this module is the provider-neutral capability contract fixture, explicitly documented as not a durable backing store ("No cloud SDK integration ships here... full S3/Azure/GCS certification remains M9"). It has no disk/database persistence to restart against; a real crash/restart claim would only be meaningful once a real provider adapter (M9) exists to restart against.` | |

## PostgreSQL components (#149)

| Component | Normal | Restart/crash | Notes |
|---|---|---|---|
| `PostgresCursorReader` | `crates/oxide-batch/tests/postgres_item_components_cursor.rs`, 3 inline tests | `crates/oxide-batch/tests/postgres_item_components_crash_recovery.rs`; own doc comment: "A PostgreSQL server-side cursor does not survive a crash — a fresh process has no cursor and no transaction," restart re-executes the `DECLARE CURSOR` from the durable keyset position rather than resuming a session | The "does not survive" fact is itself the tested contract, not an evidence gap. |
| `PostgresPagingReader` | `crates/oxide-batch/tests/*`, 2 inline tests | `postgres_completion_policy_restart.rs` and the shared keyset-restart coverage `postgres_paging`/`postgres_cursor` both build on (`postgres_keyset.rs`'s shared column/position plumbing) | No server-side session to lose — each page is independent, so "restart" is just resuming the keyset predicate, exercised alongside cursor restart. |
| `PostgresBatchWriter` | `crates/oxide-batch/tests/*`, 1 inline test | Atomicity with checkpoint/counters: Gate B `gate_b_01`–`08` (real enlisted-transaction writer, same underlying mechanism); `postgres_fault_crash_recovery.rs` | Requires an enlisted transaction structurally (no connection field of its own) — see Gate B's harness `BusinessWriter`, which uses exactly this same enlistment pattern and caught a real non-enlistment bug during this campaign (see Gate B evidence). |

## Completion policies

| Component | Normal | Restart/persistence | Notes |
|---|---|---|---|
| `ItemCountCompletionPolicy`, `TimeCompletionPolicy` | `crates/oxide-batch/tests/*`, inline tests | `N/A — stateless threshold policies, no persisted decision` | |
| `CompositeCompletionPolicy` | `crates/oxide-batch/tests/*` (3 files), inline test | Inherits member policies' restart evidence (composition, not independent state) | |
| `AdaptiveCompletionPolicy` | `crates/oxide-batch/tests/chunk_builder.rs`, `chunk_runtime.rs`, inline test | `crates/oxide-batch/tests/postgres_completion_policy_restart.rs` | Real `PostgreSQL` fixture; the one completion policy with genuinely persisted, restart-relevant decision state. |

## Pipeline/job construction and orchestration

| Component | Evidence |
|---|---|
| `ChunkPipelineBuilder` (#152) | `crates/oxide-batch/tests/chunk_builder.rs` (incl. `typed_and_boxed_pipelines_share_one_fingerprint`), Gate B `gate_b_08` extends this to real-database restart-selection equivalence. |
| `ChunkJob` / `ChunkStep` | `crates/oxide-batch/tests/chunk.rs`, `chunk_runtime.rs`, `chunk_fault_runtime.rs`, `fault_policy.rs`, `fault_state.rs`, plus every restart/crash file cited throughout this matrix (all built on this runtime). |
| `FlowJob` | `crates/oxide-batch/tests/*` (18 files reference it) — flow-level conformance, restart, and partition/split crash recovery (`postgres_flow_crash_recovery.rs`, `postgres_local_partition_crash_recovery.rs`, `postgres_local_split_crash_recovery.rs`). |
| Item listeners (`ReadListener`/`ProcessListener`/`WriteListener`/`RetryListener`/`SkipListener`) | `crates/oxide-batch/tests/item_listener_allocation.rs` (Gate F allocation companion), plus functional coverage in `chunk_runtime.rs`/`chunk_fault_runtime.rs`. Boxed allocation is a kept M6 decision (Gate F), not a gap. |

## `oxide-batch-test` public test-kit surface

Covered by its own crate's test suite dogfooding itself: `crates/oxide-batch-test/tests/*` (19 files, including `process_fixture.rs` for the real-SIGKILL process harness `restart_harness.rs`/`postgres_fixture.rs` build on, and `gate_g_scenarios.rs` for the #145 Gate G evidence). Not restated component-by-component here; the test-kit tutorial documentation (a separate M6 deliverable) is where this surface is described for users.

## Known gap, not closed in this pass

None found requiring new production API. The one confirmed evidence gap this
matrix found (`MultiResourceWriter` restart/crash) was small, contained
within the test crate, and closed directly (see above) — it did not require
a production-code change, only a test using the writer's already-shipped
public API. No other stateful/restartable component was found with a
restart/crash claim unsupported by real evidence.
