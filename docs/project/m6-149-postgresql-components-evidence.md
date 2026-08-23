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

Two real bugs surfaced while writing this component's own integration tests
against a real `PostgreSQL` server (not from external review -- this PR's
tests are the evidence for both):

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

## Public component surface

All new types live under `oxide_batch::item_components`, feature-gated by
the existing `postgres` Cargo feature (already pulling in `sqlx` 0.9 with
`postgres`/`runtime-tokio`), matching #146/#147/#148's placement convention.

| Family | Type(s) | Module |
| --- | --- | --- |
| Shared keyset plumbing (`pub(crate)`) | `KeysetColumn`, `KeysetColumnKind`, `PostgresComponentConfigError` (the only genuinely new public error type) | `item_components::postgres_keyset` |
| Cursor reader | `PostgresCursorFormat`, `PostgresCursorReader`, `PostgresCursorReaderStream`, `postgres_cursor_reader` | `item_components::postgres_cursor` |
| Paging/keyset reader | `PostgresPagingFormat`, `PostgresPagingReader`, `PostgresPagingReaderStream`, `postgres_paging_reader` | `item_components::postgres_paging` |
| SQL batch / same-resource enlisted writer | `PostgresBatchMode`, `PostgresBatchWriter`, `postgres_batch_writer` | `item_components::postgres_batch` |

Everything else composes existing `ReaderError`/`WriterError`/
`FailureCategory`/`BusinessTransactionError`/`Stream*Error` -- no redesign of
the error hierarchy, no new repository-capability abstraction, no runtime
`Any`/downcast plumbing, no PostgreSQL-specific backend SPI beyond these four
component types.

Accepting `sqlx::PgPool`/`sqlx::postgres::PgRow` directly in these
constructors (rather than hiding them behind a new wrapper type) is
consistent with existing precedent: `JsonArrayReader<Src>` already accepts a
concrete `Src` directly, and `crates/oxide-batch/tests/postgres_repository.rs`
already builds ad hoc `sqlx::PgPool`s for business data. The "driver types
stay private" principle in `docs/architecture/repository-and-transaction-model.md`
is scoped to the `ChunkTransaction`/`BusinessTransaction`/`JobRepository`
*ports*, not to a `PostgreSQL`-specific item component; inventing a wrapper
type here would be exactly the unneeded "lowest common denominator"
abstraction the issue forbids.

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
committing, is abandoned without `close()` (simulating a crash), and a fresh
reader restored from the committed envelope delivers exactly the
uncommitted remainder once, with no duplicate of the committed prefix.

**Cleanup.** `ItemStream::close` explicitly awaits `transaction.rollback()`
(never a bare `Drop`) so the connection is never handed back to the pool
while `PostgreSQL` still considers it "idle in transaction" --
`postgres_item_components_cursor.rs::close_rolls_back_and_leaves_no_idle_in_transaction_backend`
asserts `pg_stat_activity` shows the backend before `close()` and not after.

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
  2,000, deliberately well under `PostgreSQL`'s 65,535-parameter protocol
  ceiling) -- the `PostgreSQL`-specific fast path the issue allows. Because a
  multi-row statement's failure is not reliably attributable to one row,
  this mode never calls `WriterError::with_rolled_back_output`; skip
  policies cannot target it.
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
- **Unknown commit.** Genuine commit-response ambiguity
  (`ChunkTransactionError::CommitOutcomeUnknown`) is a property of the
  shared `commit_postgres_connection` helper this writer never touches --
  already proven by
  `postgres_repository.rs::disconnect_during_commit_never_guesses_outcome`.
  This writer adds no new commit/rollback code, so no new proof of that
  classification is required; `postgres_item_components_batch_writer.rs::disconnect_before_commit_leaves_writer_statements_uncommitted`
  instead proves the writer's own composition with it: a connection lost
  before any part of `commit()`'s sequence runs is a *known* not-committed
  outcome (`ChunkTransactionError::NotCommitted`, a different, earlier fault
  window than genuine commit-ambiguity), and this writer's statements --
  though sent successfully to the now-dead connection -- never became
  durable.
- **No `ItemStream` pairing.** Unlike the readers, this writer owns no local
  restart-relevant state: the enlisted transaction's atomicity is the
  durability mechanism, and the framework's central `Checkpoint` remains a
  job-supplied concern via `PostgresChunkStateProvider`, unrelated to this
  writer's internals.

## Crash/restart evidence through the real launch path

`crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`
mirrors `postgres_json_restart.rs`'s structure exactly: `PostgresFixture`
for durable committed state, `TestJob` for the real production restart
path, and `oxide_batch_test::inject` for distinguishable stop/commit-failure
injection, driving all three new components through `ChunkJob`/
`JobLauncher` rather than calling their `ItemReader`/`ItemWriter`/
`ItemStream` methods directly.

- `postgres_cursor_reader_restart_through_the_real_launch_path` /
  `postgres_paging_reader_restart_through_the_real_launch_path`: chunk size
  2 over 5 rows; `InjectedReader` stops the process after row 3 is read but
  before chunk 2 (rows 3-4) commits. A second, uninjected attempt resumes at
  row 3 and completes; the combined delivery across both attempts is exactly
  rows 1-5, once each.
- `postgres_batch_writer_restart_after_precommit_failure`: `InjectedTransactions`
  with `PreCommitAction::Fail` intercepts the first chunk's commit before
  `PostgresChunkTransaction::commit_with_component_state` ever runs. The
  writer's statement for item 1 was already sent to that now-abandoned
  transaction; asserted absent afterward (`PostgresChunkTransaction`'s
  `Drop` marks the connection `close_on_drop()`, and `PostgreSQL` rolls back
  an uncommitted transaction on connection loss). A second, uninjected
  attempt reprocesses all three items and commits them exactly once.

This is also the first CI job to run any `oxide-batch-test --features
postgres` test at all: `postgres_json_restart.rs` and
`postgres_flat_file_restart.rs` (#147/#148) are currently wired into no CI
workflow (confirmed by inspecting every `.github/workflows/*.yml` file
before writing this PR's own workflow job). Fixing that pre-existing gap for
the older files is out of scope for #149 and is not folded into this PR's
diff; it is flagged here as a follow-up candidate.

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
  `Bytes` (e.g. a `uuid`/`bytea` key) is not supported in this M6 slice.
- `PostgresBatchMode::MultiRowValues` never claims
  `WriterError::with_rolled_back_output`; a write-skip policy cannot target
  a multi-row batch failure, only `PerRowStatements`.
- `PostgresBatchWriter` requires same-resource enlistment; it has no
  standalone/non-transactional execution mode by design (see "Rollback"
  above).
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
cargo test -p oxide-batch --features postgres --test postgres_item_components_batch_writer -- --nocapture --test-threads=1
cargo test -p oxide-batch-test --features postgres --test postgres_item_components_db_restart -- --nocapture --test-threads=1
```

All of the above were run locally against a real `PostgreSQL 18.4`
(Homebrew) scratch database (`OXIDEBATCH_POSTGRES_TEST_URL`/
`OXIDEBATCH_POSTGRES_ADMIN_TEST_URL` pointed at it, migrated once via the
existing `postgres_repository.rs::migration_is_idempotent_when_migrator_fixture_is_available`
test) before this PR was opened: the full `oxide-batch` lib unit test suite
(28 tests under the `postgres` feature, 14 of them this PR's new
`item_components::postgres_{keyset,cursor,paging,batch}` unit tests, the
rest pre-existing and unaffected), 6 cursor integration tests, 5 paging
integration tests, 6 batch-writer integration tests, and 3 crash/restart
fixtures, all passing.

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
