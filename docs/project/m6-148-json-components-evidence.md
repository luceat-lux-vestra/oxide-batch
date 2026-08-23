# M6 Restartable JSON / JSONL Components Evidence

**State:** Complete on merge

**Issue:** [#148](https://github.com/luceat-lux-vestra/oxide-batch/issues/148)

This record maps issue #148's `IO-STRUCTURED-001` M6 slice -- streaming
JSONL and bounded-memory top-level JSON-array readers/writers -- to
production types and deterministic test evidence. It builds directly on the
accepted ADR-0008 contracts, the M6 `ItemStream`/component-state contract
(#144), the M3 fault-tolerance classification, and the #147 restartable
flat-file components' conventions; it does not reopen ADR-0008, the
`ItemStream` lifecycle, or `FailureCategory`, and it does not implement XML,
Avro, YAML, MessagePack/CBOR, a generic serde format plugin surface,
PostgreSQL cursor/paging/SQL components (#149), multi-resource composition
(#150), or object storage.

**First corrective pass (PR #166):** an independent review of the first
version of this PR found five defects, all in this tree now:

- `JsonArrayReader::parse_element` accepted a `serde_json::Deserializer`
  success as a proven element boundary the moment the bounded growth buffer
  happened to end at a syntactically-valid prefix -- for a non-self-delineated
  value (a bare number, `true`, `false`, `null`), the *buffer's* end is not
  the *value's* end: `[123,4]` read under `with_max_value_bytes(2)` could
  parse the first two bytes `"12"` as a complete, valid number and emit it,
  silently truncating `123` to `12` and advancing the checkpoint past data
  that was never actually validated -- a real data-corrupting false success,
  not a bounds violation caught late. The fix (`prove_value_framing`) treats
  a parser success as a *candidate* only: the bytes immediately after
  `byte_offset()` must independently prove JSON whitespace followed by `,` or
  `]` -- consulting the real source directly (never re-guessing from the
  bounded buffer alone) when the buffer's own content is exhausted at that
  point. `a_number_exactly_at_the_growth_boundary_is_not_silently_truncated`
  is the regression test, with `[12,4]` under the same 2-byte bound as its
  positive control (a genuinely 2-byte number immediately followed by the
  array's own comma, which must be accepted).
- Trailing non-whitespace bytes after a top-level array's closing `]` (e.g.
  `[1]garbage`, `[]x`, `[1][2]`) were silently accepted as ordinary
  end-of-input. The fix (`consume_closing_bracket`) requires everything after
  `]` to be JSON whitespace followed by true source EOF, failing closed
  otherwise. `trailing_garbage_after_top_level_array_is_rejected_but_json_whitespace_is_allowed`
  covers both the accepted (`[1]`, `[1] \r\n\t`) and rejected (`[1]garbage`,
  `[]x`, `[1][2]`) cases.
- `JsonArrayFormat::with_max_value_bytes(0)`'s documented "largest element
  accepted" contract was violated: the growth-target computation's `.max(1)`
  floor meant a one-byte value could still be read and accepted under a
  nominal zero-byte bound. `parse_element` now checks `max_value_bytes == 0`
  explicitly up front and fails closed before reading anything.
  `a_zero_byte_bound_rejects_every_element` is the regression test.
- `JsonLinesFormat::with_max_record_bytes` is documented to bound a line's
  raw span *excluding* its terminator, but `read_bounded_line` bound-checked
  the CRLF terminator's own `\r` as if it were payload content before
  stripping it, so an input using `\r\n` failed one byte earlier than the
  identical input using bare `\n` under the same configured bound. The fix
  holds a trailing `\r` (including one split across a `fill_buf` refill
  boundary) as pending until the following byte proves whether it is a real
  CRLF terminator or ordinary content, so it is never bound-checked as
  payload when it is the former.
  `max_record_bytes_excludes_lf_and_crlf_terminators`/
  `max_record_bytes_rejects_one_extra_payload_byte_for_lf_and_crlf` prove LF
  and CRLF now behave identically at the exact configured boundary.
- `JsonArrayWriter`'s comma-state decision (whether the batch's first item
  needs a leading comma) was read from a `committed_items` lock taken and
  released *before* the file lock that serializes the physical write and the
  resulting count update -- the file lock only ever serialized bytes, never
  the comma decision derived from the count. Two genuinely concurrent
  `write()` calls (the type is `Send + Sync`; ADR-0008 and this contract's
  own "Thread safety" claim both require this to be safe) could each observe
  `committed_items == 0` before either recorded its own write, producing
  output with a missing separator between elements -- reliably reproducible,
  not a rare timing coincidence: a 16-real-OS-thread stress run hit it on
  every attempt prior to the fix. The fix unifies the file handle and both
  committed counts under one `Mutex<WriterState>`, so the comma decision, the
  physical write, and the count update are one atomic transition.
  `concurrent_writes_to_a_fresh_array_never_lose_or_duplicate_a_comma` drives
  16 real OS threads released simultaneously by a `Barrier` and asserts an
  exact comma count, an exact element count, and a valid re-parse -- an
  invariant that holds deterministically after the fix regardless of
  scheduling order, and that failed on every run before it.

**Second corrective pass (PR #166):** a further review found that neither
reader was retry-safe against a transient (not data) I/O failure: both
could continue from a physical source position no operation had actually
confirmed, and both misclassified a failed seek as `UserComponent` instead
of `FailureCategory::TransientInfrastructure`. Both are fixed:

- `JsonLinesReader`'s one-shot `seeked` flag was set *before* attempting the
  seek it gated, and was never re-armed by a later failure. A failed
  restart seek left it permanently (and wrongly) believing the source was
  correctly positioned; a read that failed partway through a line (after
  some of that line's bytes were already consumed into the buffered
  reader, with no matching checkpoint update) left the reader positioned
  mid-line with nothing to force a re-seek before the next attempt -- a
  retry would continue from that arbitrary mid-record position rather than
  the record's own start. The fix replaces the flag with `needs_seek`,
  armed initially and re-armed by any I/O failure; it is cleared only after
  an actual seek call succeeds, and the seek always physically executes
  when armed -- including for checkpoint byte 0, which the removed
  fast path used to skip entirely.
  `a_failed_restart_seek_is_transient_and_the_retry_reseeks_to_the_checkpoint`
  and `a_mid_record_read_failure_is_transient_and_the_retry_returns_the_complete_record_once`
  are the regression tests, using a deterministic injected-fault `Read + Seek`
  double (`FaultyIo`) whose fault fires exactly once at a named byte
  position, not a timing- or stress-only mechanism; both reliably fail
  against the pre-fix code (confirmed by reverting the fix and re-running
  them) with the wrong `FailureCategory` and, for the mid-record case, a
  corrupted read result.
- `JsonArrayReader` had the same class of gap in two places: `ensure_started`
  classified its own seek failure as `UserComponent`, and
  `restore_read_state` executed its rewind seek with `let _ = ...`,
  discarding the result outright -- if that seek failed, the reader
  proceeded exactly as though it had succeeded, updating
  `consumed_absolute`/framing state to a position the physical source was
  never confirmed to be at. The post-parse "rewind past lookahead overshoot"
  seek had an even sharper version of the same bug: it updated
  `consumed_absolute` *before* attempting the seek, so a failure left that
  field wrong with no restoring call to fix it. The fix adds `reseek_to`,
  which updates the logical position only after its seek call actually
  succeeds and otherwise clears `started` (forcing the next call to
  re-derive its position and framing state from the authoritative
  checkpoint through `ensure_started`, rather than trust an unconfirmed
  cursor) and returns `TransientInfrastructure`; every seek in the reader
  now goes through it.
  `a_failed_post_parse_rewind_seek_is_transient_and_the_retry_returns_the_same_element`
  is the regression test (an injected one-shot seek failure via the
  existing `TracedSource` double, extended with the same fires-once-then-clears
  fault model), asserting the failed call's category, that the persisted
  checkpoint does not advance past the unconfirmed element, and that the
  retry returns that exact element once -- confirmed to fail against the
  pre-fix code with the wrong category.

Per this pass's own scope, #147's flat-file components were not touched
even though an analogous pattern may exist there; that is a separate
finding for a separate pass, not folded into this one.

## Public component surface

All new types live under `oxide_batch::item_components` (`jsonl` and
`json_array` submodules), matching #146/#147's placement convention.

| Family | Type(s) | Module |
| --- | --- | --- |
| JSON Lines format | `JsonLinesFormat`, `JsonLinesTerminator` | `item_components::jsonl` |
| JSON Lines reader | `JsonLinesReader`, `JsonLinesReaderStream`, `jsonl_reader`, `jsonl_file_reader` | `item_components::jsonl` |
| JSON Lines writer | `JsonLinesWriter`, `JsonLinesWriterStream`, `jsonl_writer` | `item_components::jsonl` |
| JSON-array format | `JsonArrayFormat` | `item_components::json_array` |
| JSON-array reader | `JsonArrayReader`, `JsonArrayReaderStream`, `json_array_reader`, `json_array_file_reader` | `item_components::json_array` |
| JSON-array writer | `JsonArrayWriter`, `JsonArrayWriterStream`, `json_array_writer` | `item_components::json_array` |

The public item representation is `serde_json::Value` directly (`I: From<Value>`
for a reader, `I: Into<Value>` for a writer) -- no bespoke JSON AST.
`serde_json` is already a production, workspace-level dependency of the
`oxide-batch` facade crate (used internally by #147's own state codecs), so
this introduces no new dependency; `cargo xtask surface`'s facade-disclosure
scan finds `serde_json::Value` as the only `serde_json` type reaching a
public signature, consistent with the driving task's "no bespoke JSON AST"
instruction. No `csv`/`csv_core`-style crate-internal parser type is exposed
anywhere, and the incremental array-framing technique described below is a
private module-internal adapter, never a second JSON grammar.

Placing `serde_json::Value` in a *public* signature (rather than only behind
#147's internal state codecs) is a new disclosure under the M5 facade
disclosure gate (`docs/api/design-guidelines.md`), whose `cargo xtask surface`
inspection requires a named, accepted ADR before any foreign dependency type
reaches a public signature -- the check's own `ACCEPTED` list is explicit
that it records an already-approved exception rather than approving one.
[ADR-0012](../architecture/decisions/0012-json-item-representation-discloses-serde-json-value.md)
is that decision, scoped narrowly to `serde_json::Value` in
`item_components::jsonl`/`item_components::json_array`; `cargo xtask
surface` passes with exactly the twelve `serde_json::Value` disclosures ADR-0012
covers, and no other new disclosure.

Each type's full standard-component contract (input/output, state/checkpoint,
ordering, restartability, thread safety, reentrancy, transaction/delivery,
bounded resource, cancellation, close, sensitive diagnostics, malformed-input
behavior, support tier, evidence pointer) is documented on the type itself in
`jsonl.rs`/`json_array.rs`.

## Streaming design

### JSONL

One JSON value per line is one record. [`JsonLinesReader`] reads a bounded
line with the same `fill_buf`/`consume` technique
[`FixedWidthReader`](../../crates/oxide-batch/src/item_components/fixed_width.rs)
uses (duplicated, not shared, since the two families evolve independently),
then parses the line's content with `serde_json::from_slice`. Because the
line's bytes are always fully consumed through its terminator (or end of
input) before the parse is attempted, the next record's boundary is
independently knowable *regardless of whether the current line parses* --
this is what makes a malformed JSONL line safely skippable, matching #147's
own malformed-record forward-progress guarantee.

### JSON array

One top-level array element is one item. [`JsonArrayReader`] never
deserializes the whole array into `Vec<Value>`. It owns only byte-level
framing around five ASCII bytes -- `[`, `,`, `]`, and JSON whitespace --
recognizing those never requires string-escape or nesting awareness, so it
is not a second JSON grammar. Every value's own bytes are parsed by
`serde_json` itself: this reader grows an owned, bounded buffer and retries
`serde_json::Deserializer::from_slice(&buffer).into_iter::<Value>().next()`
from the start after each growth step, using the documented public
`StreamDeserializer::byte_offset()`/`Error::is_eof()` idiom. A parser result
is only a candidate boundary: the bytes after `byte_offset()` must also prove
JSON whitespace followed by `,` or `]`. This framing check is essential for
numbers whose valid prefix ends at an arbitrary growth boundary. The framing
bytes are only inspected after `serde_json` has identified the value bytes,
so delimiters inside escaped strings and nested values cannot be mistaken for
top-level separators.

Because `serde_json::Deserializer`'s reader-based lookahead (needed to find
where a bare number/`true`/`false`/`null` ends) cannot be recovered once a
`Deserializer` instance is dropped, this reader parses each element against
its own **owned, in-memory buffer** (grown from the real source), never
directly against a live `Read` stream shared across elements -- and
re-seeks the real source to the exact proven boundary after each element,
discarding whatever the buffer's own growth over-read. This is the concrete
mechanism, not merely an assertion, that makes an exact byte-offset
checkpoint possible without ever materializing the whole array.

### Malformed-input recovery: the locked distinction

- **JSONL**: a malformed line is a typed `ReaderError` in
  `FailureCategory::UserComponent` with `checkpoint_advanced: true` -- the
  line's bytes are already consumed through the terminator, so the next
  line's boundary is proven regardless of whether this line parses.
- **JSON array**: every failure mode -- a missing opening/closing bracket, a
  missing/duplicated separator, a syntactically invalid element, or an
  element exceeding the configured bound -- is reported with
  `checkpoint_advanced: false`. This reader has **no** safe mid-array skip
  path: an element boundary is only ever known by a complete, successful
  parse of that element, so a value that fails to parse (or is abandoned for
  exceeding the bound before a complete parse) leaves this reader with no
  way to prove where the next element begins short of the heuristic
  comma/bracket scanning the design is built to avoid. `crates/oxide-batch/src/chunk.rs`'s
  `ReaderError::with_checkpoint_advanced` documents that a read skip requires
  this proof; `crates/oxide-batch-core/src/fault.rs`'s `decide_skip` enforces
  it (`FaultPhase::Read` requires `evidence.has_forward_checkpoint_proof()`).
  `item_components_json_fault.rs::json_array_skip_policy_still_fails_the_step_because_no_boundary_is_proven`
  proves this directly: a *skip*-configured `FaultRuntime` against
  unrecoverable array framing still fails the step, with zero skips
  recorded, rather than guessing at the next comma/bracket to keep going.

No recoverable-malformed-array case exists in this implementation (the
driving task makes this conditional: "if implementation claims this is
supported"), and this record does not claim one.

## Restart/checkpoint semantics

Both families reuse the existing M6 `ItemStream` contract exactly: each
`*_reader`/`*_writer` constructor returns a `(component, stream, contract)`
triple sharing state through an `Arc`, identical to #147's pattern.

**JSONL reader.** Restart position is a plain byte offset at the last
consumed line boundary -- identical in shape to `FixedWidthReader`'s. A
failed seek or a read that fails partway through a line is
`FailureCategory::TransientInfrastructure` and never advances this
position; it also forces the next call to re-seek to it (even at byte 0)
before reading anything further, so a retry never continues from an
unconfirmed mid-line source position (see the second corrective-pass note
above).

**JSON-array reader.** Restart position is a plain `u64`: the byte offset
immediately after the last successfully parsed element's own bytes, before
any following separator. No parser state beyond this offset is persisted --
the driving task allows storing more if a byte offset alone is
insufficient, but this design's every-read protocol (evaluate
separator-or-close, then parse a value) is fully reconstructible from that
one number plus whether it is zero (meaning "not yet started, consume the
opening bracket first"). A restart therefore never rereads from byte zero
and never infers position from a line or item count. Every seek this
reader performs -- establishing the initial/restored position, and the
post-parse rewind past a lookahead overshoot -- updates the logical
position only if that seek actually succeeds
(`FailureCategory::TransientInfrastructure` otherwise, never advancing the
persisted checkpoint); see the second corrective-pass note above. The instrumented
`restart_instrumentation_observes_byte_zero_rescan_control_and_real_reader_avoids_it`
test records every source seek/read interval after restore and asserts that
the production reader does not seek or read before the persisted boundary;
the same harness's positive control explicitly seeks to zero and rereads the
committed prefix.

**JSONL writer / JSON-array writer.** Committed output progress is the
output file's byte length as of the last committed chunk (the JSON-array
writer *also* persists the committed element count, needed to know whether
the next write's first item still needs a leading comma). On
`ItemStream::open`, both writer streams reconcile the file to that exact
length before any further write: longer-than-committed is truncated (an
uncommitted tail, e.g. a crash or rolled-back chunk between write and
commit); shorter-than-committed is an inconsistent resource and fails
closed (`StreamOpenError` in `FailureCategory::Invariant`). Initial
(non-restart) execution truncates/creates the target file fresh; for the
JSON-array writer, initial execution additionally writes the opening `[`
immediately, as the first byte of committed state. The JSON-array writer's
file handle and both committed counts share one `Mutex<WriterState>` (see
the corrective-pass note above): the comma-state decision, the physical
write, and the count update are one atomic transition, never three
independently-locked steps.

**JSON-array writer close.** [`crate::ItemStream::close`] appends the
closing `]` *only* when [`crate::StreamRuntimeOutcome::Committed`] is
reported (the step attempt's own terminal outcome, not a per-chunk signal --
`crates/oxide-batch/src/chunk_runtime.rs::close_opened_streams` maps
`ChunkExecutionOutcome::Completed` to exactly this value). The on-disk file
is therefore never claimed to be a complete, valid JSON array while a step
attempt is in progress, stopped, or failed; a later attempt resumes
appending elements to the still-open array. Durability across an OS/power
failure is not claimed beyond #147's own boundary: each write batch is
flushed with `File::sync_data`, no directory-entry fsync is performed.

## Bounded-memory behavior

Streaming is mandatory. Neither reader materializes its source. The
configured limits bound raw input accumulation, not every allocation made by
`serde_json` or by the resulting `Value`:

- **JSONL**: bounded by the same `fill_buf`/`consume`-capped line-reading
  loop `FixedWidthReader` uses -- raw retained line bytes never exceed
  `JsonLinesFormat::with_max_record_bytes` (the CRLF terminator is excluded
  from this count). Parser/value memory is record-dependent but remains
  `O(max_record_bytes)` for accepted input; oversized records are rejected
  before source-sized raw accumulation.
- **JSON array**: an element is parsed into an owned buffer grown in
  doubling steps (mirroring #147's `DelimitedReader::output` growth) and
  never past `JsonArrayFormat::with_max_value_bytes`; a value whose raw source
  span would exceed the bound is rejected before source-sized raw
  accumulation. Parser/value memory is record-dependent but remains
  `O(max_value_bytes)` for accepted input. The raw bound applies uniformly to
  giant flat strings, giant nested structures, and heavily escaped content.

`crates/oxide-batch/tests/item_components_json_allocation.rs` provides
allocator evidence with `stats_alloc`, while deliberately keeping the claim
narrower than a total-process-memory bound:

- **Whole-file, many uniform records:** the small/large net-retained
  comparison for both families stays independent of file size, while a
  whole-file positive control retains file-sized storage. This is evidence
  against whole-input materialization, not a claim that total allocator usage
  equals the raw input bound.
- **Oversized flat and nested values:** the real readers reject 20 MiB-scale
  inputs under a 4 KiB raw bound with allocation far below the source; naive
  materialize-then-check/deserialize controls allocate in proportion to the
  source. This proves rejection occurs before source-sized raw accumulation
  and before proportional nested `Value` growth in the reader path.
- **Writer batches:** the same test runs
  `naive_jsonl_batch_bytes_allocated` and
  `naive_json_array_batch_bytes_allocated` against the production writers.
  The controls retain one serialized `Vec<u8>` proportional to the complete
  batch, while direct-to-file writers stay below that allocation threshold;
  item values themselves are primitive caller-owned values in this comparison.

## `oxide-batch-test` evidence

| Kit facility | Used by |
| --- | --- |
| `ComponentFixture` | `item_components_jsonl.rs`, `item_components_json_array.rs` |
| `TestStep` | both (typed/erased equivalence) |
| `TestJob` + `postgres::PostgresFixture` + `restart`/`inject` | `postgres_json_restart.rs` |
| `inject::{InjectedReader, InjectedTransactions}` | `item_components_json_fault.rs`, `postgres_json_restart.rs` |
| `StandaloneTransactions`/`NoCompletion` | `item_components_json_fault.rs` |

`item_components_json_allocation.rs` (under `crates/oxide-batch/tests/`, not
`oxide-batch-test`) cannot depend on the kit itself, for the same
dev-dependency-cycle reason #146/#147's allocation files can't.

### Section-by-section evidence map

- **A (basic contracts):** `jsonl_reader_produces_expected_heterogeneous_values_and_eof`,
  `jsonl_writer_produces_exact_expected_bytes_one_record_per_line`,
  `json_array_reader_produces_expected_heterogeneous_values_in_order_and_eof`,
  `json_array_writer_produces_exact_expected_bytes`,
  `empty_array_produces_immediate_eof`,
  `writer_produces_a_valid_empty_array_with_no_items`.
- **B (edge semantics):** `crlf_and_lf_terminators_parse_identically` (both
  families), `final_line_without_a_terminator_is_still_a_record`,
  `a_trailing_terminator_produces_no_phantom_empty_record`,
  `whitespace_around_the_value_inside_a_line_is_tolerated`,
  `an_empty_line_is_a_classified_malformed_failure`,
  `a_syntactically_invalid_line_is_a_classified_failure_with_forward_progress`,
  `a_line_exceeding_the_configured_bound_fails_closed`,
  `crlf_writer_emits_crlf_terminators`,
  `max_record_bytes_excludes_lf_and_crlf_terminators`,
  `max_record_bytes_rejects_one_extra_payload_byte_for_lf_and_crlf`;
  `pretty_printed_input_with_newlines_and_extra_whitespace_parses_correctly`,
  `delimiters_inside_strings_do_not_affect_framing_and_a_naive_scan_would_be_fooled`
  (asserts a naive raw-comma count differs from the true element count on the
  same fixture the real reader parses correctly),
  `missing_closing_bracket_is_unrecoverable_and_fails_closed`,
  `malformed_element_syntax_is_unrecoverable_and_fails_closed`,
  `missing_separator_between_elements_is_unrecoverable_and_fails_closed`,
  `an_element_exceeding_the_configured_bound_fails_closed`,
  `long_number_crossing_growth_bound_is_one_element_and_restartable`,
  `trailing_garbage_after_top_level_array_is_rejected_but_json_whitespace_is_allowed`,
  `malformed_element_syntax_is_unrecoverable_and_fails_closed` (including
  same-instance retry at the restored boundary),
  `restart_instrumentation_observes_byte_zero_rescan_control_and_real_reader_avoids_it`,
  `restart_resumes_at_the_next_element_boundary_not_byte_zero` (an in-memory,
  non-durable complement to the durable restart evidence below, isolating
  the element-boundary-checkpoint claim through direct `ItemStream::open`/
  `update` calls).
- **C (reader restart):** `postgres_json_restart.rs::jsonl_reader_restarts_after_the_last_committed_line`
  and `::json_array_reader_restarts_after_the_last_committed_element_never_mid_element`
  (a genuine multi-line, comma/bracket/escaped-quote-containing second
  element, a committed prefix, and an uncommitted in-flight element proven
  neither skipped nor duplicated; a positive control shows a naive
  2-line-based resume point lands before the real second element even
  ends). Transient-I/O retry-safety (see the second corrective-pass note
  above) is proven separately with deterministic injected faults:
  `item_components_jsonl.rs::a_failed_restart_seek_is_transient_and_the_retry_reseeks_to_the_checkpoint`,
  `::a_mid_record_read_failure_is_transient_and_the_retry_returns_the_complete_record_once`,
  and `item_components_json_array.rs::a_failed_post_parse_rewind_seek_is_transient_and_the_retry_returns_the_same_element`.
- **D (writer restart):** `postgres_json_restart.rs::jsonl_writer_truncates_uncommitted_tail_and_resumes_exactly_once`,
  `::jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed`,
  `::json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`
  (restart occurs after one committed element, so comma-state correctness is
  observable; final output reparses as the exact expected array), and
  `::json_array_writer_fails_closed_when_the_file_is_shorter_than_committed`.
  `item_components_json_array.rs::concurrent_writes_to_a_fresh_array_never_lose_or_duplicate_a_comma`
  complements this with the writer's *concurrent-call* correctness (see the
  corrective-pass note above): real OS threads, not sequential attempts.
- **E (malformed skip/fail):** `item_components_json_fault.rs`'s five tests:
  `jsonl_fail_policy_fails_the_step_with_the_expected_classification` and
  `jsonl_skip_policy_skips_the_malformed_line_and_processes_later_valid_lines`
  (each asserting exact committed/skip counts and, for the fail case, the
  real M3 runtime's captured `FailureCategory::UserComponent` through a
  `ReadListener`, mirroring #146/#147's `CapturingReadListener`);
  `json_array_fail_policy_fails_the_step_with_the_expected_classification`;
  `json_array_skip_policy_still_fails_the_step_because_no_boundary_is_proven`
  (the direct proof of the locked recoverable-vs-unrecoverable distinction,
  asserting zero skips were ever recorded); and
  `sanity_json_array_well_formed_input_completes_without_any_fault`.
- **F (bounded memory):** see the dedicated section above.
- **G (typed/erased equivalence):** `typed_and_erased_jsonl_pipelines_produce_identical_items`,
  `typed_and_erased_json_array_pipelines_produce_identical_items` -- a real
  reader/`BoxedReader<Reader>` pair through `TestStep`, asserting identical
  items, identical committed counts, and `ChunkExecutionOutcome::Completed`
  on both paths, for non-vacuous heterogeneous data.
- **H/I (stream lifecycle):** the restart tests above are also the
  lifecycle evidence: attempt A's failed-commit/injected-stop scenario
  directly proves a candidate `update()` envelope becomes authoritative
  only after commit, and every attempt drives `open`/`update`/`close`
  exclusively through the existing production `ChunkStep`/`JobLauncher`
  path.

## Typed/erased equivalence

See section G above.

## Evidence Claim Audit

| Claim | Test/assertion that would fail if the claim were false |
| --- | --- |
| A malformed JSONL line is safely skippable; its checkpoint always advances | `an_empty_line_is_a_classified_malformed_failure`/`a_syntactically_invalid_line_is_a_classified_failure_with_forward_progress`'s `has_checkpoint_advanced()` assertions, plus `jsonl_skip_policy_skips_the_malformed_line_and_processes_later_valid_lines`'s exact committed/skip counts |
| A JSON array's malformed structure is never safely skippable | `missing_closing_bracket_is_unrecoverable_and_fails_closed`/`malformed_element_syntax_is_unrecoverable_and_fails_closed`/`missing_separator_between_elements_is_unrecoverable_and_fails_closed`'s `!has_checkpoint_advanced()` assertions, and `json_array_skip_policy_still_fails_the_step_because_no_boundary_is_proven`'s `skip_counts() == 0` assertion under a real skip-configured `FaultRuntime` |
| Delimiters inside strings do not affect array framing | `delimiters_inside_strings_do_not_affect_framing_and_a_naive_scan_would_be_fooled`'s exact three-element result, contrasted with the naive comma count's differing (wrong) result on the identical bytes |
| Array elements are parser/framing-proven boundaries, including long tokens across growth boundaries | `long_number_crossing_growth_bound_is_one_element_and_restartable` asserts the complete number value, exact persisted byte boundary, and restart at the second element |
| A parser success at a bounded-buffer edge is never accepted as a proven element boundary on its own | `a_number_exactly_at_the_growth_boundary_is_not_silently_truncated`'s `[123,4]`-under-2-byte-bound failure (would silently emit `12` without the fix), contrasted with its `[12,4]`-under-2-byte-bound positive control, which must still succeed |
| A zero-byte bound accepts nothing, including a one-byte value | `a_zero_byte_bound_rejects_every_element`'s failure on `[1]` under `with_max_value_bytes(0)` |
| A top-level array rejects trailing non-whitespace after `]` | `trailing_garbage_after_top_level_array_is_rejected_but_json_whitespace_is_allowed` accepts JSON whitespace cases and asserts `UserComponent` failure for garbage cases |
| Array retry restores the last proven framing state | `malformed_element_syntax_is_unrecoverable_and_fails_closed` retries the same malformed element and asserts the same failure category without checkpoint advancement |
| Array restart resumes at a proven element boundary, never mid-element, never byte zero | `restart_instrumentation_observes_byte_zero_rescan_control_and_real_reader_avoids_it` asserts seek/read intervals after restore and a positive control observes the forbidden zero seek/prefix read; `postgres_json_restart.rs` supplies durable restart evidence |
| A failed seek/read is transient infrastructure, never advances the checkpoint, and a retry re-seeks to the authoritative position rather than trusting an unconfirmed cursor | `a_failed_restart_seek_is_transient_and_the_retry_reseeks_to_the_checkpoint`, `a_mid_record_read_failure_is_transient_and_the_retry_returns_the_complete_record_once`, and `a_failed_post_parse_rewind_seek_is_transient_and_the_retry_returns_the_same_element`'s exact `FailureCategory::TransientInfrastructure` assertions, unadvanced-checkpoint assertion, and traced re-seek-to-the-exact-byte assertions -- all three reliably fail against the pre-fix code (confirmed by temporarily reverting each fix) |
| Array restart never duplicates or omits an element | The same test's `combined == [five elements once each]` assertion |
| Array writer resumes comma state correctly after a restart | `json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`'s final `b"[1,2,3]"` byte-exact assertion (a doubled or missing comma, or a re-added opening bracket, would produce different bytes) and its `serde_json::from_slice` reparse |
| Array writer never claims a complete valid JSON array before the step commits | `json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`'s intermediate `b"[1,2"` assertion (no closing bracket) after attempt A |
| Concurrent writer calls cannot lose or duplicate a comma (the comma decision, write, and count update are one atomic transition) | `concurrent_writes_to_a_fresh_array_never_lose_or_duplicate_a_comma`'s exact comma count, exact element count, and successful re-parse across 16 real, `Barrier`-synchronized OS threads -- reliably failed (zero commas) on every run against the pre-fix split-lock implementation |
| Writer fails closed on a shorter-than-committed file | `jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed`/`json_array_writer_fails_closed_when_the_file_is_shorter_than_committed`'s `BatchStatus::Failed` and unchanged-file assertions |
| Reader raw storage is bounded and oversized input is rejected before source-sized accumulation | `json_readers_do_not_retain_memory_proportional_to_input_size` compares small/large streaming retention and reader allocation against whole-input positive controls for flat and nested oversized fixtures |
| LF and CRLF have identical payload-bound semantics | `max_record_bytes_excludes_lf_and_crlf_terminators` accepts an exact-bound valid JSON record with both terminators; `max_record_bytes_rejects_one_extra_payload_byte_for_lf_and_crlf` rejects the same one-byte overflow |
| Writers do not materialize a whole serialized batch | `json_readers_do_not_retain_memory_proportional_to_input_size` compares both production writers with `naive_jsonl_batch_bytes_allocated` and `naive_json_array_batch_bytes_allocated` positive controls |
| Typed and erased paths agree | `typed_and_erased_jsonl_pipelines_produce_identical_items`/`typed_and_erased_json_array_pipelines_produce_identical_items`'s item-for-item and count equality |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy -p oxide-batch -p oxide-batch-test --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask surface
cargo xtask deps
cargo xtask reconciliation
cargo xtask release-crates
cargo xtask evidence
OXIDEBATCH_POSTGRES_TEST_URL=<url> cargo test -p oxide-batch-test --features postgres --test postgres_json_restart
```

The `postgres_json_restart` target requires `OXIDEBATCH_POSTGRES_TEST_URL`
set to an isolated, migrated database and is skipped otherwise.

## Ledger disposition

`IO-STRUCTURED-001` moves from `Planned` to `Implemented` for its M6
JSON/JSONL slice only. XML and Avro remain `Planned` for M13; this record
does not claim they exist. `IO-STRUCTURED-001` does not promote to
`Verified` on this branch: promotion requires a named released
`oxide-batch` version, per the ledger's own promotion rule, which this PR
does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Known intentional scope exclusions

Per issue #148's own out-of-scope list and the driving task: XML, Avro,
YAML, MessagePack/CBOR, a generic serde format plugin surface,
PostgreSQL cursor/paging/SQL batch components (#149), multi-resource
composition and object storage (#150), remote I/O, a compression framework,
file watching, automatic schema inference, a JSON Schema validation
framework, a transformation/mapping DSL, item listener redesign (#151),
pipeline builder/config ergonomics (#152), M7 scope/late binding, M8
portability, and M10 parallel item execution.
