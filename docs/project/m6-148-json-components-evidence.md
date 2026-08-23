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
`StreamDeserializer::byte_offset()`/`Error::is_eof()` idiom `serde_json`'s
own documentation recommends for resuming a partial parse over a byte source
of unknown length. A value's exact byte span is learned only from a
*complete, successful* parse of it; the framing bytes that follow a value are
only ever inspected in the byte range `serde_json` has already told us that
value occupies, which is what makes this reader safe against delimiters
appearing inside escaped strings -- the parser has already consumed them as
part of the value before this module's own framing scan ever looks at what
comes next.

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
consumed line boundary -- identical in shape to `FixedWidthReader`'s.

**JSON-array reader.** Restart position is a plain `u64`: the byte offset
immediately after the last successfully parsed element's own bytes, before
any following separator. No parser state beyond this offset is persisted --
the driving task allows storing more if a byte offset alone is
insufficient, but this design's every-read protocol (evaluate
separator-or-close, then parse a value) is fully reconstructible from that
one number plus whether it is zero (meaning "not yet started, consume the
opening bracket first"). A restart therefore never rereads from byte zero
and never infers position from a line or item count.

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
immediately, as the first byte of committed state.

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

Streaming is mandatory. Neither reader materializes its source, and neither
lets a single oversized value grow its retained buffer past the configured
bound:

- **JSONL**: bounded by the same `fill_buf`/`consume`-capped line-reading
  loop `FixedWidthReader` uses -- the line buffer's length never exceeds
  `JsonLinesFormat::with_max_record_bytes` even transiently. A line within
  the bound is then parsed by `serde_json::from_slice`, whose allocation is
  bounded by the line's own (already-bounded) raw byte span.
- **JSON array**: an element is parsed into an owned buffer grown in
  doubling steps (mirroring #147's `DelimitedReader::output` growth) and
  never past `JsonArrayFormat::with_max_value_bytes`; the buffer is
  (re)parsed with `serde_json::Deserializer::from_slice` after each growth
  step, so retained memory for a single element never exceeds the
  configured bound regardless of the element's real size on disk -- a value
  whose complete parse would need more bytes than the bound is rejected,
  never fully materialized. This applies uniformly to a giant flat string,
  a giant nested structure, and heavily escaped content, because the bound
  gates the *raw source byte span* fed to the parser, before any decoding
  is attempted -- there is no decoded-content-only bound for a pathological
  input to slip past (contrast #147's own first-pass defect, which this
  design avoids structurally rather than by a later fix).

`crates/oxide-batch/tests/item_components_json_allocation.rs` proves both
claims are real, mirroring #147's allocator-instrumented (`stats_alloc`)
methodology exactly. Measured on the development host:

- **Whole-file, many uniform records:** net retained bytes are *identical*
  between the small (500-row, ~22 KB) and large (100,000-row, ~4.88 MB) run
  for both JSONL (8,496 bytes either way) and JSON array (8,762 bytes either
  way) -- memory does not scale with input size at all. A positive control
  (`std::fs::read_to_string` on the same large file) shows net retained
  bytes equal to the file size (4,877,780 of 4,877,780), proving the harness
  would have caught a real whole-file-materialization regression.
- **One pathological oversized value (a 20 MiB string) under a 4 KiB
  bound:** the real `JsonLinesReader`/`JsonArrayReader` allocate
  12,594/12,740 bytes total rejecting it -- three orders of magnitude less
  than the value's own size. A positive control -- JSONL: a naive
  "materialize the whole line via `BufRead::read_until`, then check its
  length" reader; JSON array: a naive "read the whole remaining file, then
  fully deserialize it" reader (the exact shape of the bug this evidence
  guards against) -- allocates 33,562,624 / 41,943,172 bytes reading the
  same fixtures, confirming the harness observes a large one-shot
  allocation when one genuinely happens.
- **One pathological nested/escaped element** (a single array element that
  is itself a ~24.9 MB nested array of two million small escaped strings,
  under the same 4 KiB bound): the real `JsonArrayReader` allocates 52,005
  bytes rejecting it, against the naive whole-file-deserialize control's
  108,886,791 bytes -- proving the bound is enforced before `Vec<Value>`
  growth or string-unescaping bookkeeping is allowed to scale with the
  element's real structure, not only for one contiguous string allocation.

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
  `crlf_writer_emits_crlf_terminators`;
  `pretty_printed_input_with_newlines_and_extra_whitespace_parses_correctly`,
  `delimiters_inside_strings_do_not_affect_framing_and_a_naive_scan_would_be_fooled`
  (asserts a naive raw-comma count differs from the true element count on the
  same fixture the real reader parses correctly),
  `missing_closing_bracket_is_unrecoverable_and_fails_closed`,
  `malformed_element_syntax_is_unrecoverable_and_fails_closed`,
  `missing_separator_between_elements_is_unrecoverable_and_fails_closed`,
  `an_element_exceeding_the_configured_bound_fails_closed`,
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
  ends).
- **D (writer restart):** `postgres_json_restart.rs::jsonl_writer_truncates_uncommitted_tail_and_resumes_exactly_once`,
  `::jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed`,
  `::json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`
  (restart occurs after one committed element, so comma-state correctness is
  observable; final output reparses as the exact expected array), and
  `::json_array_writer_fails_closed_when_the_file_is_shorter_than_committed`.
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
| Array restart resumes at a proven element boundary, never mid-element, never byte zero | `postgres_json_restart.rs`'s `json_array_reader_restarts_after_the_last_committed_element_never_mid_element`: exact committed elements each attempt, the naive 2-line resume-point sanity check, and the combined once-each assertion |
| Array restart never duplicates or omits an element | The same test's `combined == [five elements once each]` assertion |
| Array writer resumes comma state correctly after a restart | `json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`'s final `b"[1,2,3]"` byte-exact assertion (a doubled or missing comma, or a re-added opening bracket, would produce different bytes) and its `serde_json::from_slice` reparse |
| Array writer never claims a complete valid JSON array before the step commits | `json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once`'s intermediate `b"[1,2"` assertion (no closing bracket) after attempt A |
| Writer fails closed on a shorter-than-committed file | `jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed`/`json_array_writer_fails_closed_when_the_file_is_shorter_than_committed`'s `BatchStatus::Failed` and unchanged-file assertions |
| Bounded memory, not whole-input materialization | `json_readers_do_not_retain_memory_proportional_to_input_size`'s identical small/large net-retained bytes for both families, contrasted with the whole-file positive control's file-sized net |
| A single oversized value does not allocate proportionally to its own size before rejection | The same test's oversized-value scenarios: real-reader `bytes_allocated` (~12.6-12.7 KB) against a 20 MiB value, contrasted with each family's naive positive control (~33.6/~41.9 MB) |
| The bound is enforced on the raw source byte span, not only a flat string, so a nested/escaped pathological element is also caught before proportional allocation | The same test's nested-element scenario: real-reader allocation (52,005 bytes) against a ~24.9 MB, two-million-entry nested element, contrasted with the naive whole-file-deserialize control (108,886,791 bytes) |
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
