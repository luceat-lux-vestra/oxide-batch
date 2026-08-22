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

- Delimited/CSV: the `csv` crate's own `(byte, line, record)` triple
  (`csv::Position`), restored via `csv::Reader::seek`, which the `csv` crate
  itself documents as safe to resume a subsequent record from. A restart
  therefore cannot land inside a multiline quoted record, because the
  position *is* the parser's own record boundary, not a line count.
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
rustdoc, not left implicit.

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

- Delimited/CSV: a ragged row (field count differs from the first, unless
  `DelimitedDialect::with_flexible` is set), invalid UTF-8 in a field, or a
  record whose parsed byte span exceeds the configured bound.
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

Streaming is mandatory; neither reader materializes its source file:

- Delimited/CSV: a single record's byte span is capped by
  `DelimitedDialect::with_max_record_bytes` (default 1 MiB, `DEFAULT_MAX_RECORD_BYTES`),
  checked immediately after each parse and rejected (classified, forward-proven)
  before becoming a retained item.
- Fixed-width: lines are read through a bounded `fill_buf`/`consume` loop
  capped at `FixedWidthLayout::with_max_record_bytes`; a line without a
  terminator within that many bytes stops retaining further bytes rather than
  growing without limit.

`crates/oxide-batch/tests/item_components_flat_file_allocation.rs` proves
this is real, not merely "a large file happened to work": it measures net
retained allocator bytes (`stats_alloc`, the same instrumented allocator
`chunk_allocation.rs`/#146's `item_components_allocation.rs` already use --
this workspace forbids `unsafe_code`, ruling out a hand-written
`GlobalAlloc`) across a full streaming pass over a ~500-row and a
~300,000-row fixture. Measured on the development host: net retained bytes
are *identical* between the small and large delimited runs (9,258 bytes
either way) and between the small and large fixed-width runs (8,508 bytes
either way) -- proving memory does not scale with file size at all, not just
"under some threshold." A positive control
(`std::fs::read_to_string` on the same large file) shows net retained bytes
equal to the file size (9,677,780 of 9,677,780), proving the harness would
have caught a real whole-file-materialization regression rather than being
insensitive to allocation size.

Separately, `item_components_delimited.rs`/`item_components_fixed_width.rs`
prove the oversized-single-record bound: the same record accepted under a
generous bound is rejected, classified, and forward-proven under a tight
one.

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
  in-flight record proven neither skipped nor duplicated) and
  `fixed_width_reader_and_writer_restart_from_the_last_committed_position`.
- **D (writer restart):**
  `delimited_writer_truncates_uncommitted_tail_and_resumes_exactly_once`
  (a real injected pre-commit failure leaves physically-written bytes on
  disk with no corresponding commit; restart truncates them and rewrites the
  record exactly once) and
  `delimited_writer_fails_closed_when_the_file_is_shorter_than_committed`.
- **E (malformed skip/fail):** `item_components_flat_file_fault.rs`'s four
  tests: `csv_fail_policy_fails_the_step_with_the_expected_classification`,
  `csv_skip_policy_skips_the_malformed_record_and_processes_later_valid_records`,
  and their fixed-width equivalents -- each asserts the exact committed
  count and (for skip) the exact skip count, not merely pass/fail.
- **F (bounded memory):**
  `flat_file_readers_do_not_retain_memory_proportional_to_file_size` (see
  above) plus `a_record_exceeding_the_configured_bound_fails_closed` in both
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
| Malformed record classified `UserComponent`, skip continues, fail stops | `item_components_flat_file_fault.rs`'s four tests' exact `committed_counts()`/`skip_counts()`/`outcome()` assertions |
| Bounded memory, not whole-file materialization | `flat_file_readers_do_not_retain_memory_proportional_to_file_size`'s identical small/large net-retained bytes, contrasted with the positive control's file-sized net |
| Oversized single record fails closed | `a_record_exceeding_the_configured_bound_fails_closed` (both families): a generous bound accepts, a tight bound on the same bytes rejects |
| Typed and erased paths agree | `typed_and_erased_delimited_pipelines_produce_identical_items`'s item-for-item and count equality |
| Multi-byte UTF-8 char split by a fixed-width field boundary fails, not silently corrupted | `a_field_boundary_splitting_a_multibyte_char_is_a_classified_failure` |

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
