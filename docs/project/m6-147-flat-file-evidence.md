# M6 Restartable Flat-File Components Evidence

**State:** Complete on merge

**Issue:** [#147](https://github.com/luceat-lux-vestra/oxide-batch/issues/147)

This record maps issue #147's `IO-FLAT-001` slice -- restartable
delimited/CSV and fixed-width readers and writers -- to production types and
deterministic test evidence. It builds directly on the accepted ADR-0008
contracts, the M6 `ItemStream`/component-state contract (#144), the M3
fault-tolerance classification, and the #146 standard composition catalog's
conventions; it does not reopen ADR-0008, the `ItemStream` lifecycle, or
`FailureCategory`, and it does not implement JSON/JSONL (#148), PostgreSQL
cursor/paging/SQL components (#149), multi-resource composition (#150),
object storage, or XML/Avro.

**First corrective pass (PR #165):** an independent review of the first version of
this PR found that `DelimitedReader`'s per-record bound was checked only
*after* the parser had already fully materialized a record, that
`FixedWidthReader`'s bounded-line copy could transiently exceed the
configured bound before detecting a too-long line, and that the malformed
fail-policy evidence asserted only the coarse `ChunkFailure::Reader` shape
rather than the actual `FailureCategory` the M3 runtime classified. All three
are fixed in this tree:

- `DelimitedReader` no longer uses `csv::Reader` at all. It now drives
  [`csv_core::Reader`](https://docs.rs/csv-core) -- the incremental, no-I/O
  engine the `csv` crate itself is built on -- directly, growing its
  record-content buffer one parser callback at a time and never past the
  configured bound; the instant a record would exceed it, this reader stops
  copying that record's bytes into memory entirely (switching to a tiny
  fixed discard buffer just to drain the remaining input for forward
  checkpoint proof). See `crates/oxide-batch/tests/item_components_flat_file_allocation.rs`'s
  new oversized-single-record scenario below.
- `FixedWidthReader`'s bounded-line copy helper now caps every copy at the
  remaining budget under the configured bound, so the line buffer's length
  can never exceed it even transiently -- previously a single `BufRead` fill
  chunk (which can be many kilobytes, unrelated to the configured bound) was
  copied in full before the length was checked.
- `item_components_flat_file_fault.rs`'s fail-policy tests now attach a
  `ReadListener` that captures the `FaultDescriptor` the real M3 runtime
  produces at the item-listener boundary (the same pattern #146's
  `item_components_equivalence.rs::CapturingReadListener` established) and
  assert the captured `FailureCategory` is `UserComponent`, not merely that
  the chunk failed with `ChunkFailure::Reader`.
- A full audit of every `bounded`/`never`/`exactly`/`always`/`fail closed`
  rustdoc claim against its actual evidence (below) surfaced one real,
  previously undetected defect: `DelimitedReader`'s header-row consumption
  never advanced the shared restart position, so every subsequent record's
  tracked byte/line offset understated the true file position by exactly the
  header row's byte length whenever `DelimitedDialect::with_headers` was
  set -- corrupting both the durable checkpoint and any later restart's seek
  target. `item_components_delimited.rs::headers_survive_a_restart_that_resumes_mid_file`
  is the new test that caught this (it initially failed against the
  unpatched code, reproducibly landing back on the first data record instead
  of the second); the fix and the test are both in this tree.

**Second corrective pass (PR #165):** a further review of the first
corrective pass found that the raw-byte-span bound it added still checked
only *decoded* content length, that the header row still did not establish
the field-count baseline `with_flexible(false)` checks against, and asked
for the bounded-memory audit to be re-run. All are fixed:

- `DelimitedDialect::with_max_record_bytes` now bounds the record's raw byte
  span (bytes consumed from the source), not merely its decoded
  field-content length. The gap this closes: a record built almost entirely
  of empty fields (e.g. millions of consecutive delimiters) decodes to
  almost no field-content bytes at all, so the first corrective pass's
  output-length-only check never tripped for one, while `csv_core`'s
  per-field end-offset buffer (`ends: Vec<usize>`, one entry per field) kept
  growing on every `OutputEndsFull` regardless -- unbounded, proportional to
  the actual field count, not to the configured bound. The raw-span check is
  now applied every loop iteration, before either the decoded-content buffer
  or the end-offset buffer is allowed to grow further, so a record's
  `consumed` byte count exceeding the bound moves it into discard mode
  immediately regardless of which buffer its bytes would have landed in.
  Since every field boundary consumes at least one raw delimiter byte, this
  also bounds the number of fields (and so `ends`' size) to at most
  `max_record_bytes` entries -- O(the configured bound), never O(the actual
  record's real field count).
- New allocator evidence (`item_components_flat_file_allocation.rs`): a
  single CSV record of ~20 million consecutive commas (~20 MiB, all empty
  fields) read under a 4 KiB bound. This needed its own positive control,
  distinct from the first pass's "materialize the whole line" one: a naive
  reader here has *correctly bounded decoded output* (there is none -- every
  field is empty) but *unbounded field-end-offset storage*, isolating the
  defect this pass fixed from the one the first pass fixed. Measured on the
  development host: the real `DelimitedReader` allocates ~74 KB total
  rejecting the record; the naive-unbounded-`ends` positive control
  allocates ~268 MB reading the identical fixture.
- The header row now establishes `first_field_count` (when
  `with_flexible(false)`, the default) so the first *data* row is checked
  against the header's field count, not against itself as a new,
  self-referential baseline. `item_components_delimited.rs::header_field_count_establishes_the_expected_width_for_the_first_data_row`
  is the new regression test: header `name,age,city` (3 fields), one data
  row `Alice,30` (2 fields, but internally self-consistent) -- this
  reproducibly failed against the unpatched code (the lone data row silently
  became its own accepted baseline) before the fix.
- The bounded/never/exactly rustdoc audit was re-run across `delimited.rs`
  after these changes; every claim referring to the per-record bound now
  says "raw byte span," not "parsed"/"decoded," including
  `DEFAULT_MAX_RECORD_BYTES`, `DelimitedDialect::with_max_record_bytes`, the
  module doc, and `DelimitedReader`'s "Bounded resource" contract bullet.

**Third corrective pass (#167):** an independent review of the merged tree
found that `DelimitedWriter` and `FixedWidthWriter` both had a split-lock
checkpoint-coherence defect -- the same class #148's `JsonArrayWriter` and
`JsonLinesWriter` were fixed for during their own corrective passes:

- Both writers computed the post-write byte position under the file's own
  `Mutex<File>` lock, then released that lock and separately locked a
  `Mutex<u64>` to publish it as the committed checkpoint. The file lock
  serialized the physical writes, but nothing tied the *order* of
  publication to the order of the writes it described: two concurrent
  `write()` calls' file writes are strictly ordered by the file lock, but
  their independently-locked publications are not, so the call that wrote
  *first* could publish its (smaller) committed length *after* the call
  that wrote *second* -- silently regressing the restart checkpoint
  backward below data already durably on disk. `DelimitedWriterStream`/
  `FixedWidthWriterStream`'s `open`/`update` read the same two fields under
  the same non-atomic pairing. The fix unifies each writer's file handle
  and committed byte count into one private state struct
  (`DelimitedWriterState`, `FixedWidthWriterState`) behind a single
  `Mutex`, shared by the writer and its paired stream -- the physical
  write, the `sync_data`, and the committed-count update are now one
  transition, so no publication can land out of the order its own write
  did. `committed_bytes` is now read from the file's own
  `stream_position()` after the write and sync succeed, rather than
  computed independently via `saturating_add(buffer.len())`, so the
  published value can never diverge from where the file's cursor actually
  is.
- Each writer's fix is proven by a deterministic, non-probabilistic
  regression test living inside its own module
  (`delimited.rs`'s and `fixed_width.rs`'s
  `tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit`):
  the test thread locks the writer's own private state mutex directly,
  then spawns a real background thread that calls the public `write()`
  API -- which is thereby guaranteed, by `std::sync::Mutex`'s own
  blocking-acquire semantics rather than by scheduler timing, to make no
  progress until the test thread finishes replicating a first write under
  that same lock and releases it. Both tests were empirically confirmed to
  catch the pre-fix defect: reverting to the actual pre-#167 code and
  forcing the exact interleaving the bug report describes (thread A writes
  and releases the file lock, is "descheduled"; thread B fully completes a
  real `write()` call including its publish; thread A then resumes and
  publishes its own stale, smaller byte count -- sequenced deterministically
  with `thread::join` as the happens-before barrier, no `sleep`, no
  scheduler luck) reproducibly published a checkpoint (2 bytes) below the
  file's actual length (4 bytes) on every run, for both writers.
- This class of defect could not have been caught by a reader-side
  ADR-0008 conformance check or by the existing single-threaded restart
  tests: both require a second, genuinely concurrent caller to observe,
  which is exactly what the new tests force deterministically rather than
  relying on real multi-thread scheduling to sometimes produce.

Per this pass's own scope, #149 (PostgreSQL components) was not started, and
CSV/fixed-width parsing semantics, restart/truncation/fail-closed behavior,
and public API surface are unchanged.

## Public component surface

All new types live under `oxide_batch::item_components` (`delimited` and
`fixed_width` submodules), matching #146's placement convention.

| Family | Type(s) | Module |
| --- | --- | --- |
| Delimited/CSV dialect and record | `DelimitedDialect`, `DelimitedTerminator`, `DelimitedRecord` | `item_components::delimited` |
| Delimited/CSV reader | `DelimitedReader`, `DelimitedReaderStream`, `delimited_reader`, `delimited_file_reader` | `item_components::delimited` |
| Delimited/CSV writer | `DelimitedWriter`, `DelimitedWriterStream`, `delimited_writer` | `item_components::delimited` |
| Fixed-width layout and record | `FixedWidthField`, `FixedWidthLayout`, `FixedWidthTerminator`, `FixedWidthRecord` | `item_components::fixed_width` |
| Fixed-width reader | `FixedWidthReader`, `FixedWidthReaderStream`, `fixed_width_reader`, `fixed_width_file_reader` | `item_components::fixed_width` |
| Fixed-width writer | `FixedWidthWriter`, `FixedWidthWriterStream`, `fixed_width_writer` | `item_components::fixed_width` |

The delimited/CSV parser is the mature [`csv`](https://docs.rs/csv) crate
(narrowly scoped to `crates/oxide-batch/Cargo.toml`, the facade crate --
`cargo xtask deps`'s extracted-crate boundaries govern only
`oxide-batch-core`/`oxide-batch-repository`/`oxide-batch-plan` and do not
restrict it). No `csv` crate type appears in any public signature here:
`DelimitedDialect` and `DelimitedRecord` are OxideBatch-owned wrappers,
verified by `cargo xtask surface`'s facade-disclosure scan finding no new
`csv` link and needing no new `ACCEPTED` entry. Fixed-width parsing needed no
external dependency.

Each type's full standard-component contract (input/output, state/checkpoint,
ordering, restartability, thread safety, reentrancy, transaction/delivery,
bounded resource, cancellation, close, sensitive diagnostics, malformed-input
behavior, support tier, evidence pointer) is documented on the type itself in
`delimited.rs`/`fixed_width.rs`.

## Restart/checkpoint semantics

Both families reuse the existing M6 `ItemStream` contract exactly: each
`*_reader`/`*_writer` constructor returns a `(component, stream, contract)`
triple sharing state through an `Arc`, the same pattern
`oxide-batch-test`'s `restart::range_reader` established for #146's restart
evidence. No second checkpoint channel exists.

**Readers.** Restart position is the parser's own record-boundary position,
never an inferred line count:

- Delimited/CSV: a `(byte, line, record)` triple tracked directly against
  `csv_core::Reader` (see the corrective-pass note above for why this reader
  no longer goes through `csv::Reader` at all). A restart therefore cannot
  land inside a multiline quoted record, because the position *is* the
  parser's own record boundary, not a line count -- and, since the header
  row's own consumption is folded into that same position tracking (the bug
  the corrective pass fixed), a header-enabled dialect's restart position is
  exactly as accurate as a headerless one's.
- Fixed-width: a plain byte offset at the last consumed line boundary.

**Writers.** Committed output progress is the output file's byte length as of
the last committed chunk. On `ItemStream::open`, both writer streams
reconcile the file to that exact length before any further write: a file
*longer* than committed (bytes written but never committed, e.g. a crash or a
rolled-back chunk between write and commit) is truncated; a file *shorter*
than committed is an inconsistent resource and fails closed
(`StreamOpenError` in `FailureCategory::Invariant`) rather than fabricating
progress. Initial (non-restart) execution truncates/creates the target file
fresh. Durability across an OS/power failure (as opposed to a process crash)
is not claimed: each write batch is flushed to the OS (`File::sync_data`),
but no directory-entry fsync is performed -- this is stated in both writers'
rustdoc, not left implicit. Both writers' file handles and committed byte
counts each share one `Mutex`-guarded state struct (see the third
corrective-pass note above): the physical write, the `sync_data`, and the
committed-count update are one atomic transition per writer, never
independently-locked steps whose publication order could diverge from
their write order.

Durable state is namespaced, versioned, bounded, checksummed, and
restartability-declared through the existing `ComponentStateEnvelope`/
`ComponentStateCodec`/`StreamStateContract` machinery -- new
`VersionedStateCodec` implementations only, no new persistence path. State is
declared `StateSensitivity::NonSensitive`: every persisted payload is a
position/byte-count, never record content.

## Malformed-record behavior

Malformed input is a typed `ReaderError` in `FailureCategory::UserComponent`
(policy-eligible, consumable by the existing M3 skip/fail surface), never a
panic and never a second, hidden skip engine:

- Delimited/CSV: a ragged row (field count differs from the baseline --
  the header row's field count if `DelimitedDialect::with_headers` is set,
  otherwise the first data row's -- unless `DelimitedDialect::with_flexible`
  is set), invalid UTF-8 in a field, or a record whose raw byte span exceeds
  the configured bound.
- Fixed-width: a line whose raw byte length is not exactly
  `FixedWidthLayout::record_width`, or a field span that is not valid UTF-8
  (including a multi-byte character split by a field boundary -- widths are
  explicitly byte-oriented, documented and tested, never treated as Unicode
  character offsets). Neither reader ever pads or truncates a short/long
  record; it is always a classified failure.

Every malformed-record `ReaderError` carries `checkpoint_advanced: true`: the
underlying parser has always already consumed the offending bytes (through
the next record/line boundary) by the time the error is observed -- this is
inherent to a forward-only parser, not a defect, and is documented as such. A
configured *retry* therefore re-invokes the reader against the *next*
record, not the same one; the rustdoc for both readers states this and
directs a malformed-record policy toward skip or fail, not retry.

Writers reject a field whose byte length does not match its declared
fixed-width, or a record whose field count does not match the layout, as a
`WriterError` -- never silently truncating or padding.

## Bounded-memory behavior

Streaming is mandatory; neither reader materializes its source file, and
(after the corrective passes) neither reader materializes a single oversized
record before rejecting it either -- regardless of whether that record's
size comes from decoded field content or from an unbounded field count:

- Delimited/CSV: `DelimitedDialect::with_max_record_bytes` caps the raw
  record span accepted by `DelimitedReader`. The decoded-content buffer is
  capped directly by that raw-byte bound; per-field end-offset storage is
  bounded by the number of field boundaries possible within the same raw
  span. Record-dependent parser storage is therefore O(`max_record_bytes`),
  not literally limited to `max_record_bytes` bytes. Once the raw span
  exceeds the bound, retained record-dependent buffers stop growing and the
  remainder is drained through small, fixed discard buffers before the
  classified, forward-proven `ReaderError` is returned.

- Fixed-width: lines are read through a bounded `fill_buf`/`consume` loop
  capped at `FixedWidthLayout::with_max_record_bytes`; each chunk read from
  the source is copied into the line buffer only up to the remaining budget
  under that bound, so the buffer's length never exceeds it even
  transiently, before a line without a terminator within that many bytes
  stops retaining further bytes rather than growing without limit.

`crates/oxide-batch/tests/item_components_flat_file_allocation.rs` proves
both the whole-file and the single-oversized-record claims are real, not
merely "a large file/record happened to work" -- two different measurements
for two different claims:

- **Whole-file, many uniform records:** net retained allocator bytes
  (`stats_alloc`, the same instrumented allocator
  `chunk_allocation.rs`/#146's `item_components_allocation.rs` already use --
  this workspace forbids `unsafe_code`, ruling out a hand-written
  `GlobalAlloc`) across a full streaming pass over a ~500-row and a
  ~300,000-row fixture. Measured on the development host: net retained bytes
  are *identical* between the small and large delimited runs (9,026 bytes
  either way) and between the small and large fixed-width runs (8,508 bytes
  either way) -- proving memory does not scale with file size at all, not
  just "under some threshold." A positive control (`std::fs::read_to_string`
  on the same large file) shows net retained bytes equal to the file size
  (9,677,780 of 9,677,780), proving the harness would have caught a real
  whole-file-materialization regression rather than being insensitive to
  allocation size.
- **One pathological oversized record:** a single CSV record (fixed-width
  line) with a 20 MiB field, read under a 4 KiB configured bound. This needs
  a different measurement than net-retained bytes, because the offending
  buffer is freed the moment the record is rejected, well before any
  after-the-fact net-retained snapshot -- so this instead compares
  cumulative `bytes_allocated` (a running total, not a net) across reading
  *just that one record*. Measured on the development host: the real
  `DelimitedReader`/`FixedWidthReader` allocate 12,868/12,574 bytes total
  rejecting the 20,971,520-byte (20 MiB) record -- three orders of magnitude
  less than the record's own size. A positive control -- a deliberately
  naive "materialize the whole line via `BufRead::read_until`, then check
  its length" reader, the exact shape of the bug this evidence guards
  against -- allocates 33,562,624 bytes reading the *same* fixture,
  confirming the harness can and does observe a large one-shot allocation
  when one genuinely happens.
- **One pathological record of millions of empty fields** (new in the
  second corrective pass): a single CSV record of ~20 million consecutive
  commas, read under the same 4 KiB bound. Decoded field content is zero
  bytes throughout -- this fixture exists specifically to exercise
  `csv_core`'s per-field end-offset buffer in isolation from decoded-content
  growth, the defect the first corrective pass's bound didn't catch.
  Measured on the development host: the real `DelimitedReader` allocates
  74,257 bytes total rejecting the ~20,971,520-field record. A positive
  control that drives `csv_core::Reader` directly with a *correctly
  bounded* output buffer but an *unbounded* end-offset buffer -- isolating
  exactly the fixed defect -- allocates 268,443,648 bytes reading the
  identical fixture.

`a_record_exceeding_the_configured_bound_fails_closed` (both families, in
`item_components_delimited.rs`/`item_components_fixed_width.rs`) is the
complementary *functional* proof: the same record accepted under a generous
bound is rejected, classified, and forward-proven under a tight one.

## `oxide-batch-test` evidence

| Kit facility | Used by |
| --- | --- |
| `ComponentFixture` | `item_components_delimited.rs`, `item_components_fixed_width.rs` |
| `TestStep` | `item_components_delimited.rs` (typed/erased equivalence) |
| `TestJob` + `postgres::PostgresFixture` + `restart`/`inject` | `postgres_flat_file_restart.rs` |
| `inject::{InjectedReader, InjectedTransactions}` | `item_components_flat_file_fault.rs`, `postgres_flat_file_restart.rs` |

`item_components_flat_file_allocation.rs` (under `crates/oxide-batch/tests/`,
not `oxide-batch-test`) cannot depend on the kit itself, for the same
dev-dependency-cycle reason #146's allocation/equivalence files can't (see
[M6 #146 evidence](m6-146-composition-catalog-evidence.md)); it drives the
real production types directly.

`item_components_flat_file_fault.rs` drives a real, hand-assembled
`ChunkStep` with a real `FaultRuntime`/`FaultPolicy` (`TestStep` does not yet
expose a fault-runtime builder), reusing the kit's `StandaloneTransactions`/
`NoCompletion` rather than a hand-rolled transaction manager -- the same
pattern `crates/oxide-batch/tests/chunk_fault_runtime.rs` established.

### Section-by-section evidence map

- **A (basic contracts):** `delimited_reader_produces_expected_records_and_eof`,
  `delimited_writer_produces_exact_expected_bytes`,
  `dialect_delimiter_materially_changes_parsing`,
  `fixed_width_reader_produces_expected_fields_and_eof`,
  `fixed_width_writer_produces_exact_expected_bytes`,
  `layout_field_widths_materially_change_parsing`.
- **B (CSV edge semantics):** `quoted_field_hides_the_delimiter_inside_it`,
  `doubled_quote_escapes_a_literal_quote`,
  `multiline_quoted_field_is_one_record_and_the_next_record_boundary_is_correct`,
  `crlf_and_lf_terminators_parse_identically`,
  `ragged_row_is_a_classified_malformed_failure`,
  `flexible_dialect_accepts_a_ragged_row_that_the_default_dialect_rejects`.
- **C (reader restart):**
  `postgres_flat_file_restart.rs::delimited_reader_restarts_after_the_last_committed_record_never_mid_multiline`
  (two checkpoint offsets, a committed multiline record positioned so a
  line-count-based restart would land inside it, and an uncommitted
  in-flight record proven neither skipped nor duplicated),
  `fixed_width_reader_and_writer_restart_from_the_last_committed_position`,
  and `item_components_delimited.rs::headers_survive_a_restart_that_resumes_mid_file`
  (the real `ItemStream::open`/`update` calls a committing chunk makes,
  isolating the header-survives-restart claim the first corrective pass's bug
  fix addresses).
- **D (writer restart):**
  `delimited_writer_truncates_uncommitted_tail_and_resumes_exactly_once`
  (a real injected pre-commit failure leaves physically-written bytes on
  disk with no corresponding commit; restart truncates them and rewrites the
  record exactly once) and
  `delimited_writer_fails_closed_when_the_file_is_shorter_than_committed`.
  `delimited.rs::tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit`
  and `fixed_width.rs::tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit`
  (see the third corrective-pass note above) add the writer's *concurrent-call*
  checkpoint-coherence evidence: a deterministic, held-lock proof that the
  write/publish transition cannot be interleaved, rather than relying on
  thread-count scheduling luck.
- **E (malformed skip/fail):** `item_components_flat_file_fault.rs`'s four
  tests: `csv_fail_policy_fails_the_step_with_the_expected_classification`,
  `csv_skip_policy_skips_the_malformed_record_and_processes_later_valid_records`,
  and their fixed-width equivalents -- each asserts the exact committed
  count and (for skip) the exact skip count, not merely pass/fail. Since the
  first corrective pass, the two fail-policy tests also attach a
  `ReadListener` that captures the real M3 runtime's `FaultDescriptor` and
  assert `fault.category() == FailureCategory::UserComponent` directly,
  rather than inferring the category from the coarser `ChunkFailure::Reader`
  outcome shape alone. Since the second corrective pass,
  `item_components_delimited.rs::header_field_count_establishes_the_expected_width_for_the_first_data_row`
  extends this section: with `headers(true)` and `flexible(false)`, a first
  data row narrower than the header is itself a classified
  `FailureCategory::UserComponent` malformed-record failure, not a silently
  accepted new baseline.
- **F (bounded memory):**
  `flat_file_readers_do_not_retain_memory_proportional_to_file_size` (whole
  file; one pathological oversized-single-field record with its own positive
  control since the first corrective pass; and one pathological record of
  millions of empty fields with its own, differently-shaped positive control
  since the second corrective pass -- see above) plus
  `a_record_exceeding_the_configured_bound_fails_closed` in both
  `item_components_delimited.rs` and `item_components_fixed_width.rs`.
- **G (typed/erased equivalence):**
  `typed_and_erased_delimited_pipelines_produce_identical_items`: a real
  `DelimitedReader`/`BoxedReader<DelimitedReader>` pair through `TestStep`,
  asserting identical items, identical committed counts, and
  `ChunkExecutionOutcome::Completed` on both paths.
- **H (stream lifecycle):** the restart tests above are also the lifecycle
  evidence: attempt A's failed-commit scenario directly proves a candidate
  `update()` envelope becomes authoritative only after commit (attempt B
  inherits only the prior commit's envelope, not the failed chunk's), and
  every attempt drives `open`/`update`/`close` exclusively through the
  existing production `ChunkStep`/`JobLauncher` path -- no ad hoc state.

## Typed/erased equivalence

See `typed_and_erased_delimited_pipelines_produce_identical_items` above (G).

## Evidence Claim Audit

| Claim | Test/assertion that would fail if the claim were false |
| --- | --- |
| Restart resumes exactly, never mid-multiline-record | `delimited_reader_restarts_after_the_last_committed_record_never_mid_multiline`'s `field0(&writer_b) == ["3", "4", "5"]` assertion (a line-count-based restart would produce a corrupted or shifted first field) |
| Restart never duplicates or omits a record | The same test's combined `field0(&writer_a) ++ field0(&writer_b) == ["1", "multi\nline", "3", "4", "5"]` assertion |
| Writer truncates an uncommitted tail | `delimited_writer_truncates_uncommitted_tail_and_resumes_exactly_once`'s final `std::fs::read(&path) == b"1,a\n2,b\n3,c\n"` (an append-mode writer would produce `"1,a\n2,b\n2,b\n3,c\n"`, a duplicated record) |
| Writer fails closed on a shorter-than-committed file | `delimited_writer_fails_closed_when_the_file_is_shorter_than_committed`'s `BatchStatus::Failed` and unchanged-file assertions |
| Malformed record classified `UserComponent`, skip continues, fail stops | `item_components_flat_file_fault.rs`'s four tests' exact `committed_counts()`/`skip_counts()`/`outcome()` assertions, plus the fail-policy tests' `ReadListener`-captured `fault.category() == FailureCategory::UserComponent` |
| Bounded memory, not whole-file materialization | `flat_file_readers_do_not_retain_memory_proportional_to_file_size`'s identical small/large net-retained bytes, contrasted with the whole-file positive control's file-sized net |
| A single oversized record does not allocate proportionally to its own size before rejection | The same test's oversized-record scenario: real-reader `bytes_allocated` (~12.9 KB) against a 20 MiB single-field record, contrasted with the naive-materialize-then-check positive control's `bytes_allocated` (~32 MiB) on the identical fixture |
| The bound is on raw consumed record bytes, not just decoded field content -- a record of millions of empty fields does not grow the per-field end-offset buffer unboundedly | The same test's millions-of-empty-fields scenario: real-reader `bytes_allocated` (74,257 bytes) against a ~20 MiB, ~21M-field record under a 4 KiB bound, contrasted with a positive control that drives `csv_core` directly with a correctly bounded output buffer but an unbounded `ends` buffer -- isolating exactly the defect the second corrective pass fixed -- which allocates 268,443,648 bytes on the identical fixture |
| Oversized single record fails closed | `a_record_exceeding_the_configured_bound_fails_closed` (both families): a generous bound accepts, a tight bound on the same bytes rejects |
| Header names survive a restart that resumes mid-file | `headers_survive_a_restart_that_resumes_mid_file`'s `second.field("name") == Some("Bob")` after a real `ItemStream::open`/`update` round trip (this test initially failed against the first corrective pass's own pre-fix code, reproducibly resuming at the first data record instead of the second) |
| With headers and non-flexible parsing, the header row -- not the first data row -- establishes the expected field count | `header_field_count_establishes_the_expected_width_for_the_first_data_row`'s `FailureCategory::UserComponent` assertion on a first data row narrower than the header (this test fails against the pre-fix code, which silently accepts the first data row's own width as the baseline) |
| Typed and erased paths agree | `typed_and_erased_delimited_pipelines_produce_identical_items`'s item-for-item and count equality |
| Multi-byte UTF-8 char split by a fixed-width field boundary fails, not silently corrupted | `a_field_boundary_splitting_a_multibyte_char_is_a_classified_failure` |
| A concurrent writer call can never publish a stale (regressed) committed byte count | `delimited.rs::tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit` and `fixed_width.rs::tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit` lock the writer's own state mutex, spawn a real background thread through the public `write()` API, and assert the file and the decoded `ItemStream::update()` checkpoint agree on the full, non-regressed byte length -- the background call is provably blocked, by `Mutex` semantics, until the held-lock write finishes and publishes; both tests were confirmed to fail deterministically (publishing a stale, smaller checkpoint) against the pre-#167 split-lock code under a forced, `join`-sequenced interleaving |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask surface
cargo xtask deps
cargo xtask reconciliation
cargo xtask release-crates
cargo xtask evidence
OXIDEBATCH_POSTGRES_TEST_URL=<url> cargo test -p oxide-batch-test --features postgres --test postgres_flat_file_restart
```

The `postgres_flat_file_restart` target requires `OXIDEBATCH_POSTGRES_TEST_URL`
set to an isolated, migrated database and is skipped otherwise.

## Final M5 evidence retention

All eight M5 campaigns were run for PostgreSQL 15 and 18 against the final semantic tree. All sixteen successful artifacts were re-retained byte-for-byte with workflow-run, producing-job, artifact-digest, and execution-tree provenance. During the final pass, P-003 exposed a campaign-isolation defect: the PostgreSQL service health check was connecting to the measured database. The workflow now probes the default `postgres` database instead; with the observer and ceiling assertion unchanged, PostgreSQL 15 records configured peak 2 / observed peak 2 and the performance campaign passes.

## Ledger disposition

`IO-FLAT-001` moves from `Planned` to `Implemented`. It does not promote to
`Verified` on this branch: promotion requires a named released `oxide-batch`
version, per the ledger's own promotion rule, which this PR does not itself
cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Known intentional scope exclusions

Per issue #147's own out-of-scope list and the driving task: JSON/JSONL
(#148), PostgreSQL cursor/paging/SQL batch components (#149), multi-resource
composition (#150), object storage, XML/Avro (M13), M7 late binding/scopes,
M8 database portability, M9 integration transports, and M10 parallel local
execution.
