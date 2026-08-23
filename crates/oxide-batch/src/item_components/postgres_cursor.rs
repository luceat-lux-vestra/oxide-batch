//! A real `PostgreSQL` server-side cursor reader (#149, `IO-DB-001` M6
//! `PostgreSQL` slice).
//!
//! [`PostgresCursorReader`] streams rows through an actual `DECLARE
//! CURSOR`/`FETCH` session, never materializing the full result set: memory
//! is bounded by [`PostgresCursorFormat::with_fetch_size`], one small batch
//! of buffered rows at a time.
//!
//! # Restart model
//!
//! A `PostgreSQL` server-side cursor does not survive a crash -- a fresh
//! process has no cursor and no transaction. This reader therefore never
//! treats its process-local cursor handle as a durable checkpoint. Instead,
//! the *logical* position it commits is the last successfully delivered
//! row's ordering-key tuple (see [`crate::item_components::KeysetColumn`]),
//! persisted through the paired [`PostgresCursorReaderStream`] exactly like
//! [`crate::item_components::JsonArrayReader`]'s byte offset: updated in
//! memory after every successful read, made durable only at a committing
//! chunk's [`crate::ItemStream::update`] boundary, and authoritative only if
//! that chunk transaction actually commits. On restart, this reader's first
//! `read()` call re-`DECLARE`s a fresh cursor filtered by the restored key
//! (`WHERE (cols...) > (restored...)`), so a crash mid-chunk re-reads
//! whatever that chunk had not yet committed rather than skipping or
//! silently duplicating past a committed boundary.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, Postgres};

use crate::item_components::postgres_keyset::{
    self, CursorKeysetSchema, KeysetColumn, KeysetPosition, PostgresComponentConfigError,
    PostgresRow, classify_pg_error, extract_keyset, keyset_predicate_sql, order_by_sql,
    validate_key_columns,
};
use crate::{
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    FailureCategory, PostgresConfig, ReadContext, ReadOutcome, ReaderError,
    RestartabilityDeclaration, StateLimits, StreamCloseContext, StreamCloseError,
    StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamStateContract,
    StreamUpdateContext, StreamUpdateError,
};

/// The default `FETCH` batch size: bounds how many rows this reader ever
/// holds in memory at once.
pub const DEFAULT_FETCH_SIZE: usize = 500;

const CURSOR_NAME: &str = "oxide_batch_cursor";

/// A bounded, `OxideBatch`-owned cursor format configuration.
#[derive(Clone, Copy, Debug)]
pub struct PostgresCursorFormat {
    fetch_size: usize,
}

impl PostgresCursorFormat {
    /// The default format: [`DEFAULT_FETCH_SIZE`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            fetch_size: DEFAULT_FETCH_SIZE,
        }
    }

    /// Sets how many rows one `FETCH` round trip retrieves, bounding this
    /// reader's memory to `O(fetch_size)` regardless of result-set size.
    #[must_use]
    pub const fn with_fetch_size(mut self, fetch_size: usize) -> Self {
        self.fetch_size = fetch_size;
        self
    }
}

impl Default for PostgresCursorFormat {
    fn default() -> Self {
        Self::new()
    }
}

fn cursor_keyset_codec() -> DefaultComponentCodec<CursorKeysetSchema> {
    #[allow(
        clippy::unwrap_used,
        reason = "fixed literal identities cannot fail validation"
    )]
    DefaultComponentCodec::new(
        CursorKeysetSchema,
        CodecId::new("oxide-batch.postgres-cursor-reader-position-codec").unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    // Deliberately left at the codec's fail-safe default
    // (`StateSensitivity::Sensitive`), unlike
    // `crate::item_components::json_array`'s byte-offset checkpoint: an
    // ordering-key tuple can carry real business-key values (an email, a
    // name used as a natural key), not just an opaque position.
}

fn declare_cursor_sql(
    base_query: &str,
    key_columns: &[KeysetColumn],
    has_restored: bool,
) -> String {
    let order_by = order_by_sql(key_columns);
    if has_restored {
        let predicate = keyset_predicate_sql(key_columns, 1);
        format!(
            "DECLARE {CURSOR_NAME} NO SCROLL CURSOR WITHOUT HOLD FOR \
             SELECT * FROM ({base_query}) AS ob_cursor_source WHERE {predicate} ORDER BY {order_by}"
        )
    } else {
        format!(
            "DECLARE {CURSOR_NAME} NO SCROLL CURSOR WITHOUT HOLD FOR \
             SELECT * FROM ({base_query}) AS ob_cursor_source ORDER BY {order_by}"
        )
    }
}

/// A restartable [`crate::ItemReader`] over a real `PostgreSQL` server-side
/// cursor. See the module documentation for the streaming and restart
/// design.
///
/// # Contract
///
/// - **Input/output**: `base_query` must be a full `SELECT` (no trailing
///   `ORDER BY`/`LIMIT`) whose projection includes every declared
///   `key_columns` column by name; `map_row` converts each row to `I`.
/// - **State/checkpoint**: the last delivered row's ordering-key tuple,
///   persisted through the paired [`PostgresCursorReaderStream`]. See the
///   module documentation.
/// - **Ordering**: `ORDER BY` over `key_columns`, which must be a strict
///   total order (composite and `NOT NULL`, e.g. a business key plus a
///   unique tiebreaker) -- a non-unique order can silently skip or
///   re-deliver rows across a restart.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant (owns the cursor's server-side position).
/// - **Transaction/delivery**: not applicable; [`crate::ReadContext`] carries
///   no transaction, so this reader always uses its own dedicated
///   connection/transaction, independent of any enlisted business
///   transaction a writer later participates in.
/// - **Bounded resource**: at most [`PostgresCursorFormat::with_fetch_size`]
///   rows buffered at once; the full result set is never materialized.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls, per
///   [`crate::item_components::json_array`]'s established reader
///   convention.
/// - **Close**: [`crate::ItemStream::close`] rolls back the underlying
///   read-only transaction explicitly and awaits it, so no connection is
///   ever handed back to the pool while `PostgreSQL` still considers it
///   "idle in transaction".
/// - **Sensitive diagnostics**: the checkpoint may carry real business-key
///   values and is declared at the codec's fail-safe
///   [`crate::StateSensitivity::Sensitive`] default.
/// - **Malformed input**: a row missing a declared key column, or a
///   `map_row` failure, is a [`ReaderError`] in
///   [`crate::FailureCategory::UserComponent`] with
///   [`ReaderError::has_checkpoint_advanced`] `false` -- the checkpoint only
///   advances after both the keyset extraction and `map_row` succeed. A
///   failing row is left at the front of the buffered batch rather than
///   consumed, so a direct retry is deterministic: the next `read()`
///   attempts the exact same row again, never silently skipping it.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch/tests/postgres_item_components_cursor.rs`,
///   `crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`.
pub struct PostgresCursorReader<I> {
    config: PostgresConfig,
    base_query: String,
    key_columns: Vec<KeysetColumn>,
    fetch_size: usize,
    #[allow(clippy::type_complexity, reason = "one caller-supplied row mapper")]
    map_row: Arc<dyn Fn(&PostgresRow<'_>) -> Result<I, ReaderError> + Send + Sync>,
    position: Arc<Mutex<KeysetPosition>>,
    transaction_slot: Arc<Mutex<Option<sqlx::Transaction<'static, Postgres>>>>,
    buffered: VecDeque<PgRow>,
    started: bool,
    done: bool,
}

impl<I> PostgresCursorReader<I> {
    /// Establishes this instance's dedicated pool, transaction, and
    /// server-side cursor on first use, filtered by whatever position was
    /// restored (or unfiltered on initial execution). A restarted attempt
    /// always runs this again from a fresh process -- see the module
    /// documentation.
    async fn ensure_started(&mut self) -> Result<(), ReaderError> {
        if self.started {
            return Ok(());
        }
        let restored = self
            .position
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let pool =
            self.config.connect_pool().await.map_err(|_| {
                ReaderError::with_category(FailureCategory::TransientInfrastructure)
            })?;
        let mut transaction = pool
            .begin()
            .await
            .map_err(|error| ReaderError::with_category(classify_pg_error(&error)))?;
        let sql = declare_cursor_sql(&self.base_query, &self.key_columns, !restored.is_empty());
        let mut query = sqlx::query(AssertSqlSafe(sql));
        if !restored.is_empty() {
            query = postgres_keyset::bind_keyset(query, restored.values());
        }
        if let Err(error) = query.execute(&mut *transaction).await {
            let category = classify_pg_error(&error);
            let _ = transaction.rollback().await;
            return Err(ReaderError::with_category(category));
        }
        *self
            .transaction_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(transaction);
        self.started = true;
        Ok(())
    }

    /// Refills [`Self::buffered`] with one more `FETCH` batch. The
    /// transaction is moved out of the shared slot for the duration of the
    /// round trip (never held across `.await` inside a lock guard) and
    /// moved back only on success; a failure drops it without a further
    /// round trip, since the connection is presumed unhealthy.
    async fn fetch_more(&mut self) -> Result<(), ReaderError> {
        if self.done {
            return Ok(());
        }
        let Some(mut transaction) = self
            .transaction_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return Err(ReaderError::with_category(FailureCategory::Invariant));
        };
        let sql = format!("FETCH FORWARD {} FROM {CURSOR_NAME}", self.fetch_size);
        match sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&mut *transaction)
            .await
        {
            Ok(rows) => {
                if rows.is_empty() {
                    self.done = true;
                } else {
                    self.buffered.extend(rows);
                }
                *self
                    .transaction_slot
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(transaction);
                Ok(())
            }
            Err(error) => {
                let category = classify_pg_error(&error);
                drop(transaction);
                Err(ReaderError::with_category(category))
            }
        }
    }
}

impl<I> crate::ItemReader<I> for PostgresCursorReader<I>
where
    I: Send + 'static,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.ensure_started().await?;
        if self.buffered.is_empty() && !self.done {
            self.fetch_more().await?;
        }
        let Some(row) = self.buffered.front() else {
            return Ok(ReadOutcome::EndOfInput);
        };
        let key = extract_keyset(row, &self.key_columns)?;
        let item = (self.map_row)(&PostgresRow::new(row))?;
        // Only consumed -- and the checkpoint only allowed to advance --
        // once both the keyset extraction and `map_row` have succeeded. A
        // forward-only server cursor cannot "unfetch" a row, but this
        // buffered row itself is left in place on failure, so the next
        // `read()` deterministically retries the exact same row rather than
        // silently skipping past it.
        self.buffered.pop_front();
        *self.position.lock().unwrap_or_else(PoisonError::into_inner) = key;
        Ok(ReadOutcome::Item(item))
    }
}

/// The [`crate::ItemStream`] half of a [`PostgresCursorReader`].
pub struct PostgresCursorReaderStream {
    position: Arc<Mutex<KeysetPosition>>,
    transaction_slot: Arc<Mutex<Option<sqlx::Transaction<'static, Postgres>>>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for PostgresCursorReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = cursor_keyset_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<KeysetPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = restored;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = KeysetPosition::none();
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = cursor_keyset_codec();
        let current = self
            .position
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &current,
            &codec,
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        let transaction = self
            .transaction_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(transaction) = transaction {
            // Read-only cursor: rollback vs. commit is semantically
            // irrelevant, but an explicit, awaited rollback -- never a bare
            // `Drop` -- ensures the connection is never handed back to the
            // pool while PostgreSQL still considers it "idle in
            // transaction".
            let _ = transaction.rollback().await;
        }
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Builds a `(reader, stream, contract)` triple over a `PostgreSQL`
/// connection described by `config`, namespaced under `identity`.
///
/// `config` is stored, not connected: the reader opens its own dedicated
/// pool/connection lazily on first `read()`, never at construction (the
/// pool never crosses this function's own signature -- see
/// `docs/api/design-guidelines.md`'s disclosure gate).
///
/// `base_query` must be a full `SELECT` (no trailing `ORDER BY`/`LIMIT`)
/// whose projection includes every `key_columns` column by name.
///
/// # Errors
///
/// Returns [`PostgresComponentConfigError`] when `key_columns` is empty, a
/// column name is not a safe SQL identifier, or `format`'s fetch size is
/// zero.
pub fn postgres_cursor_reader<I>(
    config: PostgresConfig,
    base_query: impl Into<String>,
    key_columns: Vec<KeysetColumn>,
    format: PostgresCursorFormat,
    map_row: impl Fn(&PostgresRow<'_>) -> Result<I, ReaderError> + Send + Sync + 'static,
    identity: ComponentStreamIdentity,
) -> Result<
    (
        PostgresCursorReader<I>,
        PostgresCursorReaderStream,
        StreamStateContract,
    ),
    PostgresComponentConfigError,
>
where
    I: Send + 'static,
{
    validate_key_columns(&key_columns)?;
    if format.fetch_size == 0 {
        return Err(PostgresComponentConfigError::InvalidFetchSize);
    }
    let position = Arc::new(Mutex::new(KeysetPosition::none()));
    let transaction_slot = Arc::new(Mutex::new(None));
    let reader = PostgresCursorReader {
        config,
        base_query: base_query.into(),
        key_columns,
        fetch_size: format.fetch_size,
        map_row: Arc::new(map_row),
        position: Arc::clone(&position),
        transaction_slot: Arc::clone(&transaction_slot),
        buffered: VecDeque::new(),
        started: false,
        done: false,
    };
    let stream = PostgresCursorReaderStream {
        position,
        transaction_slot,
        namespace: identity,
    };
    let contract = StreamStateContract::new(cursor_keyset_codec());
    Ok((reader, stream, contract))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn declare_cursor_sql_omits_the_predicate_on_initial_execution() {
        let columns = [KeysetColumn::i64("id")];
        let sql = declare_cursor_sql("SELECT id FROM t", &columns, false);
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY id"));
    }

    #[test]
    fn declare_cursor_sql_includes_the_composite_predicate_on_restart() {
        let columns = [KeysetColumn::text("email"), KeysetColumn::i64("id")];
        let sql = declare_cursor_sql("SELECT email, id FROM t", &columns, true);
        assert!(sql.contains("WHERE (email, id) > ($1, $2)"));
        assert!(sql.contains("ORDER BY email, id"));
    }
}
