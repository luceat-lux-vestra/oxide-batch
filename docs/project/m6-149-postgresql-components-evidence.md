# M6 PostgreSQL Cursor, Paging, and SQL Batch/Enlisted Writer Components Evidence

**State:** Complete on merge

**Issue:** [#149](https://github.com/luceat-lux-vestra/oxide-batch/issues/149)

This record maps issue #149's `IO-DB-001` M6 `PostgreSQL` slice -- a real
server-side cursor reader, a restartable keyset/paging reader, and a bounded
same-resource enlisted SQL batch writer -- to production types and
deterministic test evidence. It builds directly on the accepted ADR-0008
item-component contract, the M6 `ItemStream`/component-state contract, and
the existing `PostgresChunkTransaction`/`PostgresChunkTransactionManager`
same-resource machinery; it does not reopen any of those, and it does not
implement a generic multi-database abstraction, other backend adapters,
upsert/stored-procedure/ORM forms, or outbox/inbox/effect-journal delivery
modes -- all explicitly M8/M9 scope per the issue.

## What this issue turned out to need, and what it did not

Two facts, established by reading the existing codebase before writing any
new code, shrink this issue's real scope below its own prose:

1. **The M6 `ItemStream`/`ComponentStateEnvelope` mechanism already solves
   the "fetched vs. committed" checkpoint-coherence problem this issue is
   concerned with.** `crate::item_components::json_array::JsonArrayReader`
   is the reference pattern this implementation follows exactly: an
   in-memory position updates after every successful read (not yet
   durable); `ItemStream::update()` runs once per *committing* chunk and its
   result is written into `oxide_batch.ob_component_state` in the same
   physical transaction as the step-execution row
   (`PostgresChunkTransaction::commit_with_component_state`) -- it only
   becomes authoritative if that transaction commits. No new checkpoint
   machinery was needed for either reader.
2. **Same-resource enlistment, rollback, and unknown-commit classification
   are already fully implemented** in `PostgresChunkTransaction`
   (`business_transaction()` unconditionally returns `Some(self)` whenever a
   chunk transaction exists; `commit_postgres_connection()` maps any
   ambiguous `COMMIT` response to `ChunkTransactionError::CommitOutcomeUnknown`,
   never inferring success or rollback). `PostgresBatchWriter` only had to
   *use* `WriteContext::transaction()` correctly -- no transaction-manager
   code, no new commit/rollback/unknown-commit handling.

The real net-new work is therefore: two independent-connection readers with
their own keyset checkpoint schema, one writer that shapes bounded
parameterized SQL against the existing enlisted-transaction port, and the
tests/CI/docs proving all of it.

## Defects found and fixed during implementation

Four real bugs surfaced while writing this component's own integration tests
against a real `PostgreSQL` server. The first two were caught during initial
development; the second two were caught during strict re-review and are
proven by dedicated regression tests, not merely fixed and asserted:

- **A failed row was silently skipped, not deterministically retried.**
  Both readers' `read()` originally popped the next buffered row *before*
  validating it (`extract_keyset` then `map_row`); on either failing, the
  already-popped row was gone, so the *next* `read()` call moved on to the
  row after it instead of retrying the failed one -- silently skipping a
  malformed row rather than failing closed with a deterministic retry, the
  opposite of `JsonArrayReader`'s established malformed-input contract. The
  fix peeks (`VecDeque::front`) and only pops after both the keyset
  extraction and `map_row` succeed. Both
  `postgres_item_components_cursor.rs::malformed_row_fails_closed_without_advancing_the_checkpoint`
  and its paging analog exercise the retry path directly: a failing
  `map_row` is retried against the identical row, and a fresh reader
  restored from the resulting (unchanged) checkpoint still starts at that
  same row once a succeeding `map_row` is substituted.
- **The keyset checkpoint payload violated `ComponentStateEnvelope::encode`'s
  own contract.** `ComponentStateEnvelope::encode` requires a top-level JSON
  *object* (`ComponentStateError::PayloadNotObject` otherwise); the first
  version of the keyset codec encoded a bare top-level JSON array. This was
  caught immediately by
  `postgres_item_components_cursor.rs::restart_resumes_from_the_last_committed_key_without_gap_or_duplicate`
  failing with `StreamUpdateError { category: UserComponent }` the first
  time a real `ItemStream::update()` call ran against a real committed
  envelope path (not just the pure-function unit tests, which encoded and
  decoded the payload directly and so never exercised this constraint). The
  fix wraps the keyset tuple under a `{"keys": [...]}` object.
- **A `FETCH`-level transient failure left the cursor reader permanently
  unretryable.** `PostgresCursorReader::fetch_more()`'s error branch dropped
  the broken transaction (`drop(transaction)`) but never reset `started`;
  the *next* `read()` call's `ensure_started()` saw `started` still `true`
  and skipped re-establishing a connection, so `fetch_more()` ran again
  against an empty `transaction_slot` and failed closed forever with
  `FailureCategory::Invariant` -- a fault a `Read`/`TransientInfrastructure`
  retry rule could never actually recover from, no matter how many retries
  the configured policy allowed. The fix resets `started = false` in that
  same error branch, so the next `read()` re-`DECLARE`s a fresh cursor
  filtered by the last row this reader actually delivered.
  `postgres_item_components_cursor_fault.rs::fetch_level_transient_failure_recovers_without_skip_or_duplicate_through_fault_runtime`
  drives this through the real `ChunkStep`/`FaultRuntime` machinery (not a
  hand-rolled retry loop): a genuine `pg_terminate_backend` against the
  backend actually executing a slow second `FETCH` induces the failure, and
  the test was confirmed to fail with `ChunkExecutionOutcome::Failed(Reader)`
  against the pre-fix code before the fix was applied, then re-confirmed
  passing afterward -- not merely written to match the fixed behavior.
- **`KeysetColumnKind::I64`'s own doc comment overclaimed
  "`bigint`/`integer`-family" coverage.** `extract_keyset`'s `I64` arm called
  a bare `row.try_get::<i64, _>(...)`; `sqlx` decodes strictly by wire type
  and never implicitly widens an `int4` column into a requested `i64`, so
  declaring `KeysetColumn::i64` against an ordinary `integer`/`serial`
  column (not `bigint`/`bigserial`) failed at runtime on every row. The fix
  adds the same `int8`-then-`int4` fallback
  [`PostgresRow::f64`](#row-value-coverage-and-the-keysetgeneral-row-mapping-distinction)
  already established for `f64`/`f32`.
  `postgres_item_components_paging.rs::keyset_i64_column_kind_decodes_from_a_real_int4_column`
  pins this against a real `integer`-typed primary key column, across a page
  boundary (so both decoding and restart-filter binding are exercised, not
  just a single row read).

## Public component surface

All new types live under `oxide_batch::item_components`, feature-gated by
the existing `postgres` Cargo feature (already pulling in `sqlx` 0.9 with
`postgres`/`runtime-tokio`), matching #146/#147/#148's placement convention.

| Family | Type(s) | Module |
| --- | --- | --- |
| Keyset construction/row types (re-exported `pub`) | `KeysetColumn`, `KeysetColumnKind`, `PostgresComponentConfigError`, `PostgresRow` | `item_components::postgres_keyset` (module itself is `pub(crate)`; only these four types cross the public boundary) |
| Cursor reader | `PostgresCursorFormat`, `PostgresCursorReader`, `PostgresCursorReaderStream`, `postgres_cursor_reader` | `item_components::postgres_cursor` |
| Paging/keyset reader | `PostgresPagingFormat`, `PostgresPagingReader`, `PostgresPagingReaderStream`, `postgres_paging_reader` | `item_components::postgres_paging` |
| SQL batch / same-resource enlisted writer | `PostgresBatchMode`, `PostgresBatchWriter`, `POSTGRESQL_MAX_BIND_PARAMETERS`, `postgres_batch_writer` | `item_components::postgres_batch` |

Everything else composes existing `ReaderError`/`WriterError`/
`FailureCategory`/`BusinessTransactionError`/`Stream*Error` -- no redesign of
the error hierarchy, no new repository-capability abstraction, no runtime
`Any`/downcast plumbing, no PostgreSQL-specific backend SPI beyond these four
component types.

**Design correction found by `cargo xtask surface`, not by review.** The
first version of `postgres_cursor_reader`/`postgres_paging_reader` accepted
`sqlx::PgPool` directly and handed `map_row` a `&sqlx::postgres::PgRow` --
reasoned (wrongly) as consistent with `JsonArrayReader<Src>` accepting a
concrete `Src` directly. `docs/api/design-guidelines.md`'s M5 disclosure gate
is more specific than that precedent suggests: it flatly prohibits "a
database driver, connection, pool, row, or SQL fragment type" in *any*
public signature, project-wide -- not merely at the
`ChunkTransaction`/`BusinessTransaction`/`JobRepository` port boundary. The
facade-surface CI job (`quality`'s `cargo xtask surface` step) caught this
immediately: `oxide_batch::postgres_cursor_reader discloses the database
driver crate sqlx_postgres` (and the same for `postgres_paging_reader`), a
hard failure (exit code 1), not an advisory. The fix:

- Both constructors now take `PostgresConfig` (already public, already used
  by `PostgresJobRepository::connect`) instead of a caller-built `PgPool`.
  A new `pub(crate)`-only `PostgresConfig::connect_pool` (in
  `repository/postgres.rs`, reachable from `item_components` because
  `repository::postgres` was widened from a private to a `pub(crate)`
  module) opens the pool internally -- `sqlx::PgPool` itself never appears
  in either function's signature. The cursor reader connects lazily on
  first `read()` (a one-shot connection, matching its one dedicated
  transaction); the paging reader also connects lazily but caches the pool
  across pages, since reconnecting per page would be wasted work its
  cursor sibling doesn't pay.
- `map_row` now takes `&PostgresRow<'_>`, a new `pub` wrapper
  (`item_components::postgres_keyset::PostgresRow`) whose only public
  methods are `text(column)`/`i64(column)`, deliberately matching
  `KeysetColumnKind`'s own narrow vocabulary. Its private field is a
  `&sqlx::postgres::PgRow`, never disclosed. `cargo xtask surface` passes
  clean after this change (`facade surface discloses nothing further`) --
  confirmed by re-running it, not merely inferred from the type signatures.

This is a real instance of the class of gap `docs/api/design-guidelines.md`
itself exists to catch mechanically rather than rely on every contributor
correctly scoping "driver types stay private" from prose alone; it is
recorded here rather than quietly folded into the diff.

## Cursor reader: real server-side streaming, not fetch-all

`PostgresCursorReader` opens a dedicated `sqlx::Transaction<'static, Postgres>`
per instance and issues a real `DECLARE ... NO SCROLL CURSOR WITHOUT HOLD
FOR ...` / `FETCH FORWARD <fetch_size> FROM ...` session -- never a
`fetch_all` materializing the result set. At most `fetch_size` rows are
buffered at once (`postgres_item_components_cursor.rs::streams_bounded_batches_without_materializing_the_full_result_set`
streams 5,000 rows through `fetch_size = 32` and asserts exact, ordered
delivery).

`FETCH`'s row count is a structural literal the reader's own configuration
chose (never business/user data) -- consistent with the existing
`AssertSqlSafe(format!(...))` precedent in `repository/postgres.rs` --
because `PostgreSQL`'s `FETCH` grammar does not accept a bind parameter for
the count, unlike `DECLARE CURSOR`'s own `WHERE ... > $1` predicate, which
does.

**Restart model.** A server-side cursor does not survive a crash: a fresh
process has no cursor and no transaction. This reader never treats its
process-local cursor handle as a durable checkpoint. The durable position is
the last successfully delivered row's ordering-key tuple, persisted through
the paired `PostgresCursorReaderStream` exactly like `JsonArrayReader`'s byte
offset. On restart, a fresh instance always re-`DECLARE`s, filtered by the
restored key (`WHERE (cols...) > (restored...)`).
`postgres_item_components_cursor.rs::restart_resumes_from_the_last_committed_key_without_gap_or_duplicate`
proves this directly: a first attempt commits 7 rows, reads 2 more without
committing, and is abandoned in-process without calling `close()` -- a
same-process stand-in for "the next attempt never sees this reader's state
again," not a claim about surviving an actual killed process. A fresh reader
restored from the committed envelope delivers exactly the uncommitted
remainder once, with no duplicate of the committed prefix. The narrower,
stronger claim -- that this reader's checkpoint is durable across a real
OS-level process kill, not merely in-process abandonment -- is proven
separately in "Real process-kill crash/restart evidence" below.

**Cleanup.** `ItemStream::close` explicitly awaits `transaction.rollback()`
(never a bare `Drop`) so the connection is never handed back to the pool
while `PostgreSQL` still considers it "idle in transaction" --
`postgres_item_components_cursor.rs::close_rolls_back_and_leaves_no_idle_in_transaction_backend`
asserts `pg_stat_activity` shows the backend before `close()` and not after.

**Mid-attempt fault recovery.** A `FETCH` can fail for reasons that are
transient and unrelated to this reader's own logic -- a dropped connection,
a server restart -- and the real M3 fault-tolerance surface
(`FaultRuntime`/`FaultPolicy`) is the framework's mechanism for retrying
exactly that class of failure. `classify_pg_error` maps a connection-severed
`sqlx::Error` (no database error code at all) to
`FailureCategory::TransientInfrastructure`, which a caller-configured
`Read`/`TransientInfrastructure` retry rule can act on -- but only if the
reader's own internal state is actually retry-safe afterward.
`postgres_item_components_cursor_fault.rs::fetch_level_transient_failure_recovers_without_skip_or_duplicate_through_fault_runtime`
proves it is: a real `pg_terminate_backend` against the backend actually
executing a slow second `FETCH` induces a genuine mid-attempt connection
loss, and the real `ChunkStep`/`FaultRuntime` machinery retries the failed
`read()` once, recovering by re-`DECLARE`ing a fresh cursor filtered by the
last row this reader actually delivered -- every row committed exactly once,
no skip from the failed `FETCH`'s abandoned rows, no duplicate from the
retried re-`DECLARE`. See "Defects found and fixed during implementation"
above for the bug this regression test was written against.

## Paging/keyset reader: no `OFFSET`, no held resource

`PostgresPagingReader` never uses `OFFSET`. Each page is an independent,
bounded `WHERE (cols...) > (last...) ORDER BY cols... LIMIT page_size`
statement over the pool -- no transaction, no server-side cursor, so no
resource is held between pages
(`postgres_item_components_paging.rs::no_server_side_resource_is_held_between_pages`
asserts `pg_stat_activity`'s idle-in-transaction count is unchanged across a
page boundary). Unlike `FETCH`'s literal count, `LIMIT` accepts an ordinary
bound parameter.

`postgres_item_components_paging.rs::restart_resumes_from_the_last_committed_key_without_skip_or_duplicate`
additionally inserts a row into the gap between the committed key and the
end of the result set before restarting, and asserts it never appears --
proving positional (`OFFSET`-style) skip/duplicate is structurally
impossible here, not merely untested.
`duplicate_primary_sort_key_is_resolved_by_the_unique_tiebreaker` proves the
composite `(sort_key, id)` order is strict even when every row shares the
same primary sort key.

Both readers share `item_components::postgres_keyset` (`pub(crate)` only):
the composite tuple-comparison SQL fragment builders, the keyset value
codec, and `PostgreSQL` SQLSTATE-class error classification -- the "minimal
internal abstraction to remove duplication among these components" the
issue explicitly allows, not a new public capability surface.

## Server-side timeout semantics on the readers' own connections

`PostgresConfig`'s `statement_timeout`/`lock_timeout`/
`idle_in_transaction_session_timeout` were already enforced on the
framework's metadata connection (`configure_transaction`, transaction-scoped
`SELECT set_config(..., true)` issued after `BEGIN`). The first version of
this PR's two readers did not extend that to their own, independent
business-data connections -- a config value a caller set would silently not
apply to the connection actually running their `base_query`.

The fix is `PostgresConfig::connect_pool` (`pub(crate)`,
`repository/postgres.rs`), which both readers now use to open their pool:
`sqlx::PgPoolOptions::after_connect` runs once per new *physical* connection
(session-scoped, not transaction-scoped, since these readers' connections
are not wrapped in the framework's own transaction machinery) and issues the
same three `SELECT set_config($1, $2, false)` calls `configure_transaction`
uses, with `false` (session-scope) in place of `true` (transaction-scope).

`postgres_item_components_cursor.rs::statement_timeout_is_enforced_on_the_cursor_business_connection`
and `postgres_item_components_paging.rs::statement_timeout_is_enforced_on_the_paging_business_connection`
prove this directly, not by inspecting the connection (which the facade
disclosure gate forbids exposing) but by observing its effect: a
`base_query` whose row generation stalls for 2 seconds
(`SELECT pg_sleep(2), ...`), against a reader configured with a 200ms
`statement_timeout`, is cancelled by `PostgreSQL` itself (SQLSTATE `57014`,
`query_canceled`) well before the 2-second stall would otherwise complete.
`classify_pg_error` maps `57014` to `FailureCategory::Cancelled`, which both
tests assert directly on the returned `ReaderError`.

## Row value coverage and the keyset/general-row-mapping distinction

`PostgresRow` originally exposed only `text()`/`i64()` -- matching
`KeysetColumnKind`'s own two-variant vocabulary, since that was the only
vocabulary either reader's own internals needed at the time. That is too
narrow for `map_row`, a general row-mapping closure a caller supplies for
arbitrary business columns, not just ordering-key columns. `PostgresRow` now
also exposes `i32()`, `bool()`, and `f64()` (with an `f32`-to-`f64` fallback
for `real` columns), covering `PostgreSQL`'s common scalar column types:
`text`/`varchar`, the integer family, `boolean`, and floating-point.

This is a real, and deliberately incomplete, widening -- it does **not**
cover `timestamp[tz]`, `uuid`, `numeric`, `bytea`, `json[b]`, or
array/composite/domain types. Reading one of those columns requires casting
it to a covered type in `base_query` itself (e.g.
`EXTRACT(EPOCH FROM ts)::bigint`, `id::text`). This limitation is stated on
`PostgresRow`'s own public doc comment, not left as a silent gap.

This widening does **not** change `KeysetColumn`'s separate, narrower
`Text`/`I64`-only restriction (see "Documented limitations" below): that
restriction exists for a different reason -- strict total ordering, where a
`bool` key cannot order past two buckets and a `NULL` key breaks tuple
comparison entirely -- and is unaffected by `PostgresRow` covering more
value types for general (non-ordering-key) columns.

## Consistency under concurrent source mutation

Keyset pagination's restart correctness rests on one assumption, documented
on `item_components::postgres_keyset`'s module doc comment (the shared
module both readers build on): a row's `key_columns` values do not change
after that row first becomes visible to a read.

- **Late inserts at or before the committed position are structurally
  invisible** -- inherent to keyset pagination itself (any keyset-based
  reader, not a defect of this implementation), not something a workload
  needing to observe late-arriving rows behind its own cursor can rely on
  this delivery mode for.
- **Mutating a row's `key_columns` values in place, once it has entered the
  read window, is unsafe**: it can produce either a skip (the row's new key
  moves outside every future fetch window) or a duplicate (the row's key
  moves from before the committed position to after it, so a restart
  re-delivers it). Key columns should be values a caller's own business
  logic never updates in place -- an auto-incrementing surrogate key, or an
  immutable creation timestamp paired with a unique tiebreaker.
- **A delete at or before the committed position is always safe for either
  reader**, on restart or otherwise: the row was already excluded or already
  delivered, so its deletion has no observable effect.

**This is where the first version of this record overstated a shared
guarantee that strict re-review caught: it is not true that "inserts after
the current position are delivered by a later page/fetch or after a
restart" for both readers, and it is not true that a delete of a
not-yet-delivered row is "always" absent from the rest of the same attempt
for both readers either.** A `PostgreSQL` server-side cursor and an
independent paged `SELECT` are not the same kind of read, and the two
readers diverge sharply on what an attempt already under way can observe
from a *different*, concurrently committing transaction. This was verified
directly against a real server, not asserted from `PostgreSQL` documentation
alone: `DECLARE` a cursor, `FETCH` part of it, commit an insert and a delete
of a not-yet-delivered row from a *second* session, then `FETCH` the rest
from the *same*, already-open cursor.

- **Paging: every page is a fresh, independently visible statement.**
  `PostgresPagingReader` holds no transaction; under `PostgreSQL`'s default
  `READ COMMITTED` isolation, each page's statement sees every row committed
  before it starts. An insert at a key past the current position, committed
  between two pages of the *same* attempt, **is** delivered by a later page;
  a delete of a not-yet-delivered row is simply absent from the next page.
  `postgres_item_components_paging.rs::insert_between_pages_is_visible_to_a_later_page_in_the_same_attempt`
  proves the insert side directly, without needing a restart.
- **Cursor: one held transaction, one fixed snapshot, for the whole
  attempt.** `PostgresCursorReader` keeps one `DECLARE`d cursor and its
  transaction open for the entire attempt; `PostgreSQL`'s cursors behave as
  the SQL standard's `INSENSITIVE` cursors do, returning rows under the
  snapshot fixed when the portal began executing regardless of what other
  transactions commit afterward. Confirmed directly: an insert at a key past
  the current position, committed by another transaction *after* this
  cursor's snapshot was taken, is **not** delivered by a later `FETCH` on
  the same cursor -- only a restart (a fresh `DECLARE`, a new snapshot)
  picks it up. A delete of a not-yet-delivered row, committed the same way,
  does **not** remove it from a later `FETCH` on the same cursor either --
  the row is still delivered, exactly as `PostgreSQL`'s MVCC snapshot
  isolation guarantees for any statement that began before the delete
  committed; a restart's fresh snapshot omits it.
  `postgres_item_components_cursor.rs::insert_after_declare_is_invisible_to_this_attempt_until_restart`
  proves the insert side directly: the five pre-existing rows are delivered
  by the same attempt with the concurrently inserted row absent, and only a
  restart's fresh reader delivers it.

Neither divergence can cause a skip, a duplicate, or a lost commit -- the
durable checkpoint is always the last row a reader's caller actually
processed, never a row it merely fetched-but-stale. It only changes how
promptly a concurrent insert or delete becomes visible to an attempt already
in progress; a restart always re-establishes a fresh view either way.

## SQL batch writer / same-resource enlisted writer

`PostgresBatchWriter` is deliberately one type for both roles the issue
names: `ItemWriter` has no route to `PostgreSQL` business rows other than
the borrowed `WriteContext::transaction()` path, so a bounded SQL batch
writer *is* the enlisted writer.

- **Enlistment is required, not optional.** `write()` requires
  `context.transaction()` to be `Some`; a non-enlisted call is a typed,
  fail-closed `WriterError` in `FailureCategory::UnsupportedCapability` --
  the selected execution mode did not supply the same-resource enlistment
  this writer requires. The writer has no pool/connection field at all, so
  "never opens a second connection" is a structural guarantee, not just a
  runtime check
  (`postgres_item_components_batch_writer.rs::non_enlisted_write_context_is_rejected_without_a_second_connection`).
- **Two execution modes.** `PostgresBatchMode::MultiRowValues` builds one
  chunked, multi-row `INSERT ... VALUES ($1,$2),($3,$4),...` per `write()`
  call, bounded by a configured `max_parameters_per_statement` (default
  2,000) -- the `PostgreSQL`-specific fast path the issue allows.
  `POSTGRESQL_MAX_BIND_PARAMETERS` (`= 65_535`, `PostgreSQL`'s hard wire-
  protocol ceiling on bind parameters per statement) is now enforced at
  construction: `postgres_batch_writer` rejects any
  `max_parameters_per_statement` above that ceiling with
  `PostgresComponentConfigError::MaxParametersExceedsProtocolLimit`, rather
  than accepting a value that would only fail later, opaquely, against a
  real server. `max_parameters_at_the_protocol_ceiling_is_accepted` and
  `max_parameters_one_over_the_protocol_ceiling_is_rejected` are exact
  boundary tests at the ceiling itself; a third unit test
  (`default_max_parameters_is_well_under_the_protocol_ceiling`) pins the
  default as a compile-time invariant. Because a multi-row statement's
  failure is not reliably attributable to one row, this mode never calls
  `WriterError::with_rolled_back_output`; skip policies cannot target it.
  `postgres_item_components_batch_writer.rs::multi_row_values_writes_every_item_across_chunk_boundaries`
  proves correctness across a batch that spans multiple sub-statements
  within one enlisted transaction. `PostgresBatchMode::PerRowStatements`
  executes one statement per item and does call
  `with_rolled_back_output(index)`, enabling skip-policy compatibility at
  the cost of one round trip per item.
- **Rollback.** `postgres_item_components_batch_writer.rs::constraint_violation_rolls_back_the_whole_chunk_with_no_partial_write`
  drives a real constraint violation through the full `ChunkJob`/
  `JobLauncher` path and asserts zero business rows survive, with the
  durable checkpoint left unadvanced.
- **Unknown commit.** Two distinct fault windows are proven, not conflated:
  - `postgres_item_components_batch_writer.rs::disconnect_before_commit_leaves_writer_statements_uncommitted`
    kills the connection *before* `commit()`'s sequence starts, so the
    step-execution `UPDATE` inside `commit_with_component_state` is the
    statement that fails -- a *known* not-committed outcome
    (`ChunkTransactionError::NotCommitted`). This writer's statements,
    though sent successfully to the now-dead connection, never became
    durable, but the outcome was never in doubt.
  - `postgres_item_components_batch_writer.rs::commit_ambiguity_after_writer_statements_already_executed_is_never_guessed`
    proves the genuinely ambiguous case the issue actually asks about: this
    writer's `INSERT` statements execute and succeed first, then the
    enclosing `COMMIT` itself is interrupted while the backend is
    observably executing it (`pg_stat_activity.query = 'COMMIT'`). A
    `DEFERRABLE INITIALLY DEFERRED` constraint trigger calling `pg_sleep`
    fires *during* `COMMIT` processing (not immediately after the
    triggering `INSERT`), giving a wide, non-racy window in which a second,
    admin connection polls for and terminates that exact backend
    mid-`COMMIT`. The test asserts `ChunkTransactionError::CommitOutcomeUnknown`
    (never `NotCommitted`, since real statements did execute) and that zero
    rows are visible afterward -- `PostgreSQL` rolled the whole, still-open
    transaction back when the session terminated, exactly the outcome this
    writer's contract requires it to never guess at. Both tests are
    genuinely deterministic, not timing-sensitive best-effort races: the
    first observes a connection failure before any commit-sequence
    statement runs at all, and the second's timing window is manufactured
    by the deferred trigger, not raced against real commit latency.
- **No `ItemStream` pairing.** Unlike the readers, this writer owns no local
  restart-relevant state: the enlisted transaction's atomicity is the
  durability mechanism, and the framework's central `Checkpoint` remains a
  job-supplied concern via `PostgresChunkStateProvider`, unrelated to this
  writer's internals.

## Restart evidence through the real launch path

`crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`
mirrors `postgres_json_restart.rs`'s structure exactly: `PostgresFixture`
for durable committed state, `TestJob` for the real production restart
path, and `oxide_batch_test::inject` for distinguishable stop/commit-failure
injection, driving all three new components through `ChunkJob`/
`JobLauncher` rather than calling their `ItemReader`/`ItemWriter`/
`ItemStream` methods directly. The injection mechanism is an in-process,
cooperative stop signal between chunks, not an OS-level process kill -- this
section proves *restart correctness* (a second attempt resumes exactly where
the first attempt's last committed chunk left off), a distinct and narrower
claim than "survives an actual killed process," which is proven separately
below.

- `postgres_cursor_reader_restart_through_the_real_launch_path` /
  `postgres_paging_reader_restart_through_the_real_launch_path`: chunk size
  2 over 5 rows; `InjectedReader` stops the process after row 3 is read but
  before chunk 2 (rows 3-4) commits. A second, uninjected attempt resumes at
  row 3 and completes; the combined delivery across both attempts is exactly
  rows 1-5, once each.
- `postgres_batch_writer_restart_after_precommit_failure`: `InjectedTransactions`
  with `PreCommitAction::Fail` intercepts the first chunk *before*
  `PostgresChunkTransaction::commit_with_component_state` is ever called --
  no `COMMIT` (or anything else in that method) is ever sent, so this is a
  deterministic not-committed case, not a genuine commit-ambiguity case (the
  distinct, genuinely ambiguous case -- real statements executed, then a
  `COMMIT` whose outcome the client cannot observe -- is proven separately;
  see "Unknown commit" above). The writer's statement for item 1 was already
  sent to that now-abandoned transaction; asserted absent afterward. No
  explicit `ROLLBACK` is ever sent by the client here: `PostgresChunkTransaction`'s
  `Drop` calls `close_on_drop()`, which only marks the pooled connection to
  be physically closed rather than returned to the pool -- it does not itself
  issue any SQL. The actual discarding of item 1's statement is `PostgreSQL`'s
  own server-side behavior: when a backend's session ends (via socket close)
  while a transaction is open and uncommitted, the server discards that
  transaction's work, exactly as it would for any client that disconnected
  mid-transaction without ever calling `COMMIT`. A second, uninjected attempt
  reprocesses all three items and commits them exactly once.

This is also the first CI job to run any `oxide-batch-test --features
postgres` test at all: `postgres_json_restart.rs` and
`postgres_flat_file_restart.rs` (#147/#148) are currently wired into no CI
workflow (confirmed by inspecting every `.github/workflows/*.yml` file
before writing this PR's own workflow job). Fixing that pre-existing gap for
the older files is out of scope for #149 and is not folded into this PR's
diff; it is flagged here as a follow-up candidate.

## Real process-kill crash/restart evidence

The evidence above proves restart correctness under in-process, cooperative
abandonment. `crates/oxide-batch/tests/postgres_item_components_crash_recovery.rs`
proves the stronger, distinct claim the issue's "crash" language actually
requires: survival of a genuine, abrupt OS-level process termination, for
both the cursor and paging readers.

- **Mechanism.** The test binary re-execs itself (`Command::new(std::env::current_exe())`)
  with an environment flag selecting a worker mode, mirroring this
  repository's existing M2 crash-worker pattern. The child process connects
  to real `PostgreSQL`, builds a real reader through the low-level
  `PostgresChunkTransactionManager`/`ItemStream` API, reads 7 of 20 seeded
  rows, commits that chunk (durable, committed envelope written), reads 5
  more rows *without* committing them, then calls `std::process::exit(87)`
  directly -- an abrupt, unwind-free termination, not a `Drop`, not a
  simulated abandonment, and not a panic.
- **Parent-side assertions.** The parent process asserts the child's exit
  code is exactly `87` (proving the crash was the genuine cause of process
  end, not an early, silent success), then inspects durable state: exactly
  the 7 committed rows are visible, the step is still `Started`, and the 5
  read-but-uncommitted rows left no trace. A fresh reader restored from the
  inherited committed envelope then delivers exactly rows 8-20 once each --
  no gap from the crash, no duplicate of the 7 already-committed rows.
- **Both readers covered.**
  `cursor_reader_survives_a_real_process_kill_mid_chunk` and
  `paging_reader_survives_a_real_process_kill_mid_chunk` run this identical
  scenario against `PostgresCursorReader` and `PostgresPagingReader`
  respectively; both pass locally against real `PostgreSQL 18.4` and are
  wired into the `postgres-item-components` CI job (PG15/PG18) as a
  dedicated step, separate from the injected-stop restart fixtures above.

## `PostgreSQL` 15/18 verification

The new `postgres-item-components` job in `.github/workflows/ci.yml` runs
the full suite below against both `postgres:15` and `postgres:18` service
containers (`strategy.matrix.postgres: ["15", "18"]`), mirroring the
existing `postgres-repository` job's matrix exactly. Locally, the same
commands were run against a real `PostgreSQL 18.4` (Homebrew) instance
during development (`OXIDEBATCH_POSTGRES_TEST_URL` pointed at a dedicated
scratch database, migrated once via `PostgresMigrator`) -- PG18 evidence is
therefore both locally reproduced and CI-authoritative; PG15 evidence is
CI-authoritative only, per this repository's established convention that
local runs catch mistakes first and CI produces the retained matrix
evidence.

## Bounded-resource behavior

- Cursor reader: at most `fetch_size` rows buffered at once; the full result
  set is never materialized (`streams_bounded_batches_without_materializing_the_full_result_set`).
- Paging reader: at most `page_size` rows buffered at once; no server-side
  resource held between pages (`no_server_side_resource_is_held_between_pages`).
- Batch writer: `MultiRowValues` chunks by `max_parameters_per_statement`;
  `PerRowStatements` is bounded by the chunk size itself. Neither
  accumulates the full step's items.
- Checkpoint state: a keyset tuple (`Text`/`I64` values only), bounded by
  the number of declared key columns -- not proportional to result-set size.

## Documented limitations (not silent gaps)

- Keyset ordering-key columns support `Text` and `I64` only
  (`KeysetColumnKind`) -- deliberately narrower than `BusinessValue`'s five
  variants: a boolean key cannot give a strict total order past two
  buckets, and a `NULL` key breaks row-value tuple comparison entirely.
  `Bytes` (e.g. a `uuid`/`bytea` key) is not supported in this M6 slice. This
  is narrower than, and independent of, `PostgresRow`'s general
  value-reading coverage below -- it constrains only which columns may be
  declared as `KeysetColumn`s, not what `map_row` may read. `I64` covers
  both `bigint`/`bigserial` and `integer`/`serial` columns (an `int8`-then-
  `int4` decode fallback, mirroring `PostgresRow::f64`'s `f64`-then-`f32`
  fallback) -- not `bigint` only.
- The cursor and paging readers diverge on same-attempt visibility of a
  concurrent insert or delete of a not-yet-delivered row (see "Consistency
  under concurrent source mutation" above): the cursor's held snapshot means
  such a change is invisible to the same attempt until a restart, while
  paging's independent per-page statements see it immediately. This is a
  documented, empirically-verified difference in *promptness*, not a
  skip/duplicate/correctness gap -- the durable checkpoint is unaffected
  either way.
- `PostgresRow` (the row-mapping type `map_row` receives) covers `text`,
  the integer family (`i32`/`i64`), `boolean`, and floating-point
  (`f64`/`f32`) columns. It does **not** cover `timestamp[tz]`, `uuid`,
  `numeric`, `bytea`, `json[b]`, or array/composite/domain types; reading
  one of those requires casting it to a covered type in `base_query` itself.
- `PostgresBatchMode::MultiRowValues`'s `max_parameters_per_statement` is
  capped at `PostgreSQL`'s hard 65,535-parameter wire-protocol ceiling
  (`POSTGRESQL_MAX_BIND_PARAMETERS`), enforced at construction, not merely
  documented.
- `PostgresBatchMode::MultiRowValues` never claims
  `WriterError::with_rolled_back_output`; a write-skip policy cannot target
  a multi-row batch failure, only `PerRowStatements`.
- `PostgresBatchWriter` requires same-resource enlistment; it has no
  standalone/non-transactional execution mode by design (see "Rollback"
  above).
- Keyset-based restart correctness assumes `key_columns` values are stable
  once a row is visible to a read; mutating them in place is unsafe (see
  "Consistency under concurrent source mutation" above). This is inherent to
  keyset pagination, not specific to this implementation.
- Upsert, stored-procedure, ORM/repository forms, other database backends,
  and generic multi-database portability remain M8, exactly as issue #149
  scopes them. No claim stronger than what is implemented is made anywhere
  in this record: no "database support complete," no "SQL database
  abstraction implemented," no "exactly-once PostgreSQL," no "all database
  components supported."

## Test commands actually run

```console
cargo fmt --package oxide-batch --package oxide-batch-test -- --check
cargo clippy -p oxide-batch --features postgres --all-targets -- -D warnings
cargo clippy -p oxide-batch-test --features postgres --all-targets -- -D warnings
cargo test -p oxide-batch --features postgres --lib
cargo test -p oxide-batch --features postgres --test postgres_item_components_cursor -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres --test postgres_item_components_paging -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres --test postgres_item_components_cursor_fault -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres --test postgres_item_components_batch_writer -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres --test postgres_item_components_crash_recovery -- --nocapture --test-threads=1
cargo test -p oxide-batch-test --features postgres --test postgres_item_components_db_restart -- --nocapture --test-threads=1
cargo run --package oxide-batch-xtask -- surface
```

All of the above were run locally against a real `PostgreSQL 18.4`
(Homebrew) scratch database (`OXIDEBATCH_POSTGRES_TEST_URL`/
`OXIDEBATCH_POSTGRES_ADMIN_TEST_URL` pointed at it, migrated once via the
existing `postgres_repository.rs::migration_is_idempotent_when_migrator_fixture_is_available`
test) before this PR was opened: the full `oxide-batch` lib unit test suite
(31 tests under the `postgres` feature, 17 of them this PR's own
`item_components::postgres_{keyset,cursor,paging,batch}` unit tests, the
rest pre-existing and unaffected), 8 cursor integration tests, 8 paging
integration tests, 1 real fault-runtime retry regression test, 7
batch-writer integration tests, 3 real process-kill crash/restart tests, and
3 injected-stop restart fixtures, all passing. The
`fetch_level_transient_failure_recovers_without_skip_or_duplicate_through_fault_runtime`
regression was additionally confirmed to *fail* against the pre-fix
`fetch_more()` (`ChunkExecutionOutcome::Failed(Reader)`, not the retried
`Completed` this fix produces) before the fix was restored and re-confirmed
passing, so this is a proven regression test, not merely a passing one.

## Ledger disposition

`IO-DB-001` moves from `Planned` to `Implemented` for its M6 `PostgreSQL`
slice only: the cursor reader, keyset paging reader, and SQL batch/
same-resource enlisted writer. Upsert, stored-procedure, ORM/repository
forms, other database backends, and generic multi-database portability
remain `Planned` for M8; this record does not claim they exist.
`IO-DB-001` does not promote to `Verified` on this branch: promotion
requires a named released `oxide-batch` version, per the ledger's own
promotion rule, which this PR does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Known intentional scope exclusions

Per issue #149's own out-of-scope list: a generic multi-database/backend
abstraction, MySQL/SQLite/SQL Server/Oracle/DB2/HANA adapters, an ORM
abstraction, a cross-resource transaction abstraction, outbox/inbox/effect-
journal delivery modes beyond the accepted same-resource path, upsert and
stored-procedure support, and any future M8 API surface speculatively
implemented ahead of need.
