//! A restartable `PostgreSQL` keyset/paging reader (#149, `IO-DB-001` M6
//! `PostgreSQL` slice).
//!
//! [`PostgresPagingReader`] never uses `OFFSET`: each page is an independent,
//! bounded `WHERE (cols...) > (last...) ORDER BY cols... LIMIT page_size`
//! query, so page cost does not grow as later pages are read and no
//! server-side resource (transaction, cursor) is held between pages -- unlike
//! [`crate::item_components::postgres_cursor::PostgresCursorReader`], which
//! trades that independence for a real streamed session. Restart, checkpoint
//! ownership, and the strict-total-order requirement on `key_columns` are
//! otherwise identical between the two readers; see that module's
//! documentation for the shared reasoning.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, PgPool};

use crate::item_components::postgres_keyset::{
    self, KeysetColumn, KeysetPosition, PagingKeysetSchema, PostgresComponentConfigError,
    classify_pg_error, extract_keyset, keyset_predicate_sql, order_by_sql, validate_key_columns,
};
use crate::{
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    FailureCategory, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration, StateLimits,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
};

/// The default page size.
pub const DEFAULT_PAGE_SIZE: usize = 500;

/// A bounded, `OxideBatch`-owned paging format configuration.
#[derive(Clone, Copy, Debug)]
pub struct PostgresPagingFormat {
    page_size: usize,
}

impl PostgresPagingFormat {
    /// The default format: [`DEFAULT_PAGE_SIZE`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    /// Sets how many rows one page query retrieves.
    #[must_use]
    pub const fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }
}

impl Default for PostgresPagingFormat {
    fn default() -> Self {
        Self::new()
    }
}

fn paging_keyset_codec() -> DefaultComponentCodec<PagingKeysetSchema> {
    #[allow(
        clippy::unwrap_used,
        reason = "fixed literal identities cannot fail validation"
    )]
    DefaultComponentCodec::new(
        PagingKeysetSchema,
        CodecId::new("oxide-batch.postgres-paging-reader-position-codec").unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    // Left at the fail-safe `StateSensitivity::Sensitive` default for the
    // same reason as `postgres_cursor::cursor_keyset_codec`: an ordering-key
    // tuple can carry real business-key values.
}

/// Builds the bounded page query. The `LIMIT` value is always the last bind
/// parameter (`$1` on initial execution, `$(key_columns.len() + 1)` once a
/// keyset predicate is present) -- unlike `FETCH`'s literal count, `LIMIT`
/// accepts an ordinary bound parameter.
fn page_query_sql(base_query: &str, key_columns: &[KeysetColumn], has_restored: bool) -> String {
    let order_by = order_by_sql(key_columns);
    if has_restored {
        let predicate = keyset_predicate_sql(key_columns, 1);
        let limit_param = key_columns.len() + 1;
        format!(
            "SELECT * FROM ({base_query}) AS ob_page_source WHERE {predicate} \
             ORDER BY {order_by} LIMIT ${limit_param}"
        )
    } else {
        format!("SELECT * FROM ({base_query}) AS ob_page_source ORDER BY {order_by} LIMIT $1")
    }
}

/// A restartable [`crate::ItemReader`] over independent, bounded `PostgreSQL`
/// keyset pages. See the module documentation for the paging and restart
/// design.
///
/// # Contract
///
/// - **Input/output**: `base_query` must be a full `SELECT` (no trailing
///   `ORDER BY`/`LIMIT`) whose projection includes every declared
///   `key_columns` column by name; `map_row` converts each row to `I`.
/// - **State/checkpoint**: the last delivered row's ordering-key tuple,
///   persisted through the paired [`PostgresPagingReaderStream`] exactly like
///   [`crate::item_components::postgres_cursor::PostgresCursorReader`]'s.
/// - **Ordering**: `ORDER BY` over `key_columns`, which must be a strict
///   total order (composite and `NOT NULL`).
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant.
/// - **Transaction/delivery**: not applicable; each page is an independent
///   statement over the pool, never a held transaction.
/// - **Bounded resource**: at most [`PostgresPagingFormat::with_page_size`]
///   rows buffered at once; no server-side resource held between pages.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls.
/// - **Close**: a no-op; this reader holds no resource that needs closing.
/// - **Sensitive diagnostics**: declared at the codec's fail-safe
///   [`crate::StateSensitivity::Sensitive`] default.
/// - **Malformed input**: identical to
///   [`crate::item_components::postgres_cursor::PostgresCursorReader`]'s.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch/tests/postgres_item_components_paging.rs`,
///   `crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`.
pub struct PostgresPagingReader<I> {
    pool: PgPool,
    base_query: String,
    key_columns: Vec<KeysetColumn>,
    page_size: usize,
    #[allow(clippy::type_complexity, reason = "one caller-supplied row mapper")]
    map_row: Arc<dyn Fn(&PgRow) -> Result<I, ReaderError> + Send + Sync>,
    position: Arc<Mutex<KeysetPosition>>,
    buffered: VecDeque<PgRow>,
    exhausted: bool,
}

impl<I> PostgresPagingReader<I> {
    async fn fetch_next_page(&mut self) -> Result<(), ReaderError> {
        if self.exhausted {
            return Ok(());
        }
        let restored = self
            .position
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let sql = page_query_sql(&self.base_query, &self.key_columns, !restored.is_empty());
        let mut query = sqlx::query(AssertSqlSafe(sql));
        if !restored.is_empty() {
            query = postgres_keyset::bind_keyset(query, restored.values());
        }
        let page_size = i64::try_from(self.page_size)
            .map_err(|_| ReaderError::with_category(FailureCategory::Invariant))?;
        query = query.bind(page_size);
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|error| ReaderError::with_category(classify_pg_error(&error)))?;
        if rows.len() < self.page_size {
            self.exhausted = true;
        }
        self.buffered.extend(rows);
        Ok(())
    }
}

impl<I> crate::ItemReader<I> for PostgresPagingReader<I>
where
    I: Send + 'static,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        if self.buffered.is_empty() && !self.exhausted {
            self.fetch_next_page().await?;
        }
        let Some(row) = self.buffered.front() else {
            return Ok(ReadOutcome::EndOfInput);
        };
        let key = extract_keyset(row, &self.key_columns)?;
        let item = (self.map_row)(row)?;
        // See `postgres_cursor::PostgresCursorReader::read` for why the row
        // is only popped -- and the checkpoint only advanced -- after both
        // succeed: a failure must deterministically retry the same row.
        self.buffered.pop_front();
        *self.position.lock().unwrap_or_else(PoisonError::into_inner) = key;
        Ok(ReadOutcome::Item(item))
    }
}

/// The [`crate::ItemStream`] half of a [`PostgresPagingReader`].
pub struct PostgresPagingReaderStream {
    position: Arc<Mutex<KeysetPosition>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for PostgresPagingReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = paging_keyset_codec();
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
        let codec = paging_keyset_codec();
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
        // No server-side resource is ever held between pages, so there is
        // nothing to release here.
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Builds a `(reader, stream, contract)` triple over `pool`, namespaced
/// under `identity`.
///
/// `base_query` must be a full `SELECT` (no trailing `ORDER BY`/`LIMIT`)
/// whose projection includes every `key_columns` column by name.
///
/// # Errors
///
/// Returns [`PostgresComponentConfigError`] when `key_columns` is empty, a
/// column name is not a safe SQL identifier, or `format`'s page size is
/// zero.
pub fn postgres_paging_reader<I>(
    pool: PgPool,
    base_query: impl Into<String>,
    key_columns: Vec<KeysetColumn>,
    format: PostgresPagingFormat,
    map_row: impl Fn(&PgRow) -> Result<I, ReaderError> + Send + Sync + 'static,
    identity: ComponentStreamIdentity,
) -> Result<
    (
        PostgresPagingReader<I>,
        PostgresPagingReaderStream,
        StreamStateContract,
    ),
    PostgresComponentConfigError,
>
where
    I: Send + 'static,
{
    validate_key_columns(&key_columns)?;
    if format.page_size == 0 {
        return Err(PostgresComponentConfigError::InvalidFetchSize);
    }
    let position = Arc::new(Mutex::new(KeysetPosition::none()));
    let reader = PostgresPagingReader {
        pool,
        base_query: base_query.into(),
        key_columns,
        page_size: format.page_size,
        map_row: Arc::new(map_row),
        position: Arc::clone(&position),
        buffered: VecDeque::new(),
        exhausted: false,
    };
    let stream = PostgresPagingReaderStream {
        position,
        namespace: identity,
    };
    let contract = StreamStateContract::new(paging_keyset_codec());
    Ok((reader, stream, contract))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn page_query_sql_omits_the_predicate_on_initial_execution() {
        let columns = [KeysetColumn::i64("id")];
        let sql = page_query_sql("SELECT id FROM t", &columns, false);
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("LIMIT $1"));
    }

    #[test]
    fn page_query_sql_places_limit_after_the_composite_predicate() {
        let columns = [KeysetColumn::text("email"), KeysetColumn::i64("id")];
        let sql = page_query_sql("SELECT email, id FROM t", &columns, true);
        assert!(sql.contains("WHERE (email, id) > ($1, $2)"));
        assert!(sql.contains("LIMIT $3"));
    }
}
