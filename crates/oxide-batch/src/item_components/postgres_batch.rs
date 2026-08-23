//! A bounded, same-resource enlisted `PostgreSQL` SQL batch writer (#149,
//! `IO-DB-001` M6 `PostgreSQL` slice).
//!
//! [`PostgresBatchWriter`] is deliberately one type for both the "SQL batch
//! writer" and the "same-resource enlisted writer" the issue names: an
//! [`crate::ItemWriter`] has no route to `PostgreSQL` business rows other
//! than the borrowed [`crate::WriteContext::transaction`] path, so a bounded
//! SQL batch writer *is* the enlisted writer. It requires an enlisted
//! transaction and never opens a connection of its own -- it has no such
//! field at all, a structural guarantee, not just a runtime check -- and it
//! never commits or rolls back: that remains the enclosing
//! [`crate::ChunkTransaction`]'s job, already implemented (same-resource
//! enlistment, rollback, and unknown-commit classification) by the existing
//! `PostgreSQL` chunk-transaction adapter. This writer only shapes bounded,
//! parameterized SQL against the borrowed
//! [`crate::BusinessTransaction`] port.

use crate::item_components::postgres_keyset::PostgresComponentConfigError;
use crate::{
    BusinessStatement, BusinessTransaction, BusinessTransactionError, BusinessValue,
    FailureCategory, WriteContext, WriteOutcome, WriterError,
};

/// The default maximum bind-parameter count for one
/// [`PostgresBatchMode::MultiRowValues`] statement: deliberately well under
/// `PostgreSQL`'s hard 65,535-parameter protocol ceiling, leaving headroom
/// for wide rows and keeping generated SQL text/plan size reasonable.
pub const DEFAULT_MAX_PARAMETERS_PER_STATEMENT: usize = 2000;

/// How [`PostgresBatchWriter`] shapes one `write()` call's batch into SQL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresBatchMode {
    /// One chunked, multi-row `INSERT ... VALUES ($1,$2),($3,$4),...` per
    /// sub-batch, bounded by `max_parameters_per_statement` -- the
    /// `PostgreSQL`-specific fast path. Because a multi-row statement's
    /// failure is not reliably attributable to one row (`PostgreSQL` aborts
    /// the whole transaction on any statement error, and a constraint
    /// violation does not always name which bound row conflicted), this
    /// mode never calls [`WriterError::with_rolled_back_output`]; a
    /// write-skip policy cannot target it.
    MultiRowValues {
        /// The maximum total bind-parameter count for one statement.
        max_parameters_per_statement: usize,
    },
    /// One parameterized statement per item. Slower (one round trip per
    /// item within the transaction) but every failure is attributable to
    /// exactly one item's index, so this mode calls
    /// [`WriterError::with_rolled_back_output`] and is compatible with a
    /// write-skip policy.
    PerRowStatements,
}

impl PostgresBatchMode {
    /// [`Self::MultiRowValues`] at [`DEFAULT_MAX_PARAMETERS_PER_STATEMENT`].
    #[must_use]
    pub const fn multi_row_values() -> Self {
        Self::MultiRowValues {
            max_parameters_per_statement: DEFAULT_MAX_PARAMETERS_PER_STATEMENT,
        }
    }
}

fn classify(error: BusinessTransactionError) -> WriterError {
    match error {
        BusinessTransactionError::Infrastructure => {
            WriterError::with_category(FailureCategory::TransientInfrastructure)
        }
        BusinessTransactionError::Rejected => {
            WriterError::with_category(FailureCategory::UserComponent)
        }
        BusinessTransactionError::Cancelled => {
            WriterError::with_category(FailureCategory::Cancelled)
        }
    }
}

/// A bounded, same-resource enlisted `PostgreSQL` SQL batch writer. See the
/// module documentation for why this is one type covering both the "SQL
/// batch writer" and "same-resource enlisted writer" roles.
///
/// # Contract
///
/// - **Input/output**: `bind` extracts one row's parameter values from an
///   item; `insert_prefix` (e.g. `"INSERT INTO t (a, b) VALUES"`) plus an
///   optional `conflict_clause` (e.g. `"ON CONFLICT (a) DO NOTHING"`) are
///   fixed SQL text supplied by calling Rust code, never business data.
///   Values are always separately bound (`$1, $2, ...`), never interpolated
///   into SQL text.
/// - **State/checkpoint**: none; this writer owns no restart-relevant state
///   of its own. The enlisted transaction's atomicity is the durability
///   mechanism, and the framework's central checkpoint remains a
///   job-supplied concern unrelated to this writer's internals.
/// - **Ordering**: writes items in the order supplied.
/// - **Thread safety**: `Send + Sync`; shared across the chunk (`&self`).
/// - **Reentrancy**: reentrant; holds no per-call mutable state.
/// - **Transaction/delivery**: requires
///   [`crate::WriteContext::is_enlisted`]. A non-enlisted call is a typed,
///   fail-closed [`WriterError`] in
///   [`crate::FailureCategory::UnsupportedCapability`] -- the selected
///   execution mode did not supply the same-resource enlistment this writer
///   requires -- and this writer never opens a connection or transaction of
///   its own to compensate.
/// - **Bounded resource**: [`PostgresBatchMode::MultiRowValues`] chunks by
///   `max_parameters_per_statement`; [`PostgresBatchMode::PerRowStatements`]
///   is bounded by the chunk size itself. Neither ever accumulates the full
///   step's items.
/// - **Cancellation**: honors the call-scoped stop token before writing.
/// - **Malformed input**: not applicable to `bind`'s output shape beyond a
///   configuration-time [`PostgresComponentConfigError`]; a `bind` output
///   whose length does not match the declared `columns_per_row` is a
///   [`WriterError`] in [`crate::FailureCategory::Invariant`] (a caller
///   configuration bug, not a database failure).
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch/tests/postgres_item_components_batch_writer.rs`,
///   `crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`.
pub struct PostgresBatchWriter<I> {
    insert_prefix: String,
    conflict_clause: Option<String>,
    columns_per_row: usize,
    mode: PostgresBatchMode,
    #[allow(clippy::type_complexity, reason = "one caller-supplied row binder")]
    bind: Box<dyn for<'a> Fn(&'a I) -> Vec<BusinessValue<'a>> + Send + Sync>,
}

impl<I> PostgresBatchWriter<I> {
    /// Builds the fixed `INSERT` text for `row_count` rows worth of
    /// placeholders, starting at `$1`.
    fn insert_sql(&self, row_count: usize) -> String {
        let mut param = 1usize;
        let rows = (0..row_count)
            .map(|_| {
                let placeholders = (0..self.columns_per_row)
                    .map(|_| {
                        let placeholder = format!("${param}");
                        param += 1;
                        placeholder
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({placeholders})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        match &self.conflict_clause {
            Some(conflict) => format!("{} {rows} {conflict}", self.insert_prefix),
            None => format!("{} {rows}", self.insert_prefix),
        }
    }

    async fn write_multi_row<'a>(
        &'a self,
        transaction: &mut dyn BusinessTransaction,
        items: &'a [I],
        max_parameters_per_statement: usize,
    ) -> Result<WriteOutcome, WriterError> {
        let rows_per_statement = (max_parameters_per_statement / self.columns_per_row).max(1);
        for chunk in items.chunks(rows_per_statement) {
            let mut values = Vec::with_capacity(chunk.len() * self.columns_per_row);
            for item in chunk {
                values.extend((self.bind)(item));
            }
            if values.len() != chunk.len() * self.columns_per_row {
                return Err(WriterError::with_category(FailureCategory::Invariant));
            }
            let text = self.insert_sql(chunk.len());
            let statement = BusinessStatement::new(&text, &values);
            transaction.execute(statement).await.map_err(classify)?;
        }
        Ok(WriteOutcome::Written)
    }

    async fn write_per_row<'a>(
        &'a self,
        transaction: &mut dyn BusinessTransaction,
        items: &'a [I],
    ) -> Result<WriteOutcome, WriterError> {
        let text = self.insert_sql(1);
        for (index, item) in items.iter().enumerate() {
            let values = (self.bind)(item);
            if values.len() != self.columns_per_row {
                return Err(WriterError::with_category(FailureCategory::Invariant));
            }
            let statement = BusinessStatement::new(&text, &values);
            transaction
                .execute(statement)
                .await
                .map_err(|error| classify(error).with_rolled_back_output(index))?;
        }
        Ok(WriteOutcome::Written)
    }
}

impl<I> crate::ItemWriter<I> for PostgresBatchWriter<I>
where
    I: Send + Sync,
{
    async fn write<'a>(
        &'a self,
        items: &'a [I],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        if items.is_empty() {
            return Ok(WriteOutcome::Written);
        }
        let transaction = context
            .transaction()
            .ok_or_else(|| WriterError::with_category(FailureCategory::UnsupportedCapability))?;
        match self.mode {
            PostgresBatchMode::MultiRowValues {
                max_parameters_per_statement,
            } => {
                self.write_multi_row(transaction, items, max_parameters_per_statement)
                    .await
            }
            PostgresBatchMode::PerRowStatements => self.write_per_row(transaction, items).await,
        }
    }
}

/// Builds a bounded, same-resource enlisted `PostgreSQL` SQL batch writer.
///
/// `insert_prefix` and `conflict_clause` are fixed SQL text supplied by
/// calling Rust code (e.g. `"INSERT INTO t (a, b) VALUES"` and
/// `"ON CONFLICT (a) DO NOTHING"`); `bind` extracts one row's separately
/// bound parameter values from an item, in the same order `insert_prefix`'s
/// column list expects.
///
/// # Errors
///
/// Returns [`PostgresComponentConfigError`] when `columns_per_row` is zero,
/// or [`PostgresBatchMode::MultiRowValues`]'s `max_parameters_per_statement`
/// is smaller than `columns_per_row`.
pub fn postgres_batch_writer<I>(
    insert_prefix: impl Into<String>,
    conflict_clause: Option<impl Into<String>>,
    columns_per_row: usize,
    mode: PostgresBatchMode,
    bind: impl for<'a> Fn(&'a I) -> Vec<BusinessValue<'a>> + Send + Sync + 'static,
) -> Result<PostgresBatchWriter<I>, PostgresComponentConfigError> {
    if columns_per_row == 0 {
        return Err(PostgresComponentConfigError::InvalidColumnsPerRow);
    }
    if let PostgresBatchMode::MultiRowValues {
        max_parameters_per_statement,
    } = mode
        && max_parameters_per_statement < columns_per_row
    {
        return Err(PostgresComponentConfigError::InvalidMaxParameters);
    }
    Ok(PostgresBatchWriter {
        insert_prefix: insert_prefix.into(),
        conflict_clause: conflict_clause.map(Into::into),
        columns_per_row,
        mode,
        bind: Box::new(bind),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn writer(mode: PostgresBatchMode) -> PostgresBatchWriter<i64> {
        postgres_batch_writer(
            "INSERT INTO t (id) VALUES",
            None::<&str>,
            1,
            mode,
            |item: &i64| vec![BusinessValue::i64(*item)],
        )
        .unwrap()
    }

    #[test]
    fn insert_sql_places_one_placeholder_group_per_row() {
        let writer = writer(PostgresBatchMode::multi_row_values());
        assert_eq!(writer.insert_sql(1), "INSERT INTO t (id) VALUES ($1)");
        assert_eq!(
            writer.insert_sql(3),
            "INSERT INTO t (id) VALUES ($1), ($2), ($3)"
        );
    }

    #[test]
    fn insert_sql_appends_the_conflict_clause() {
        let writer = postgres_batch_writer(
            "INSERT INTO t (id) VALUES",
            Some("ON CONFLICT (id) DO NOTHING"),
            1,
            PostgresBatchMode::multi_row_values(),
            |item: &i64| vec![BusinessValue::i64(*item)],
        )
        .unwrap();
        assert_eq!(
            writer.insert_sql(1),
            "INSERT INTO t (id) VALUES ($1) ON CONFLICT (id) DO NOTHING"
        );
    }

    #[test]
    fn zero_columns_per_row_is_rejected() {
        let result = postgres_batch_writer(
            "INSERT INTO t (id) VALUES",
            None::<&str>,
            0,
            PostgresBatchMode::multi_row_values(),
            |item: &i64| vec![BusinessValue::i64(*item)],
        );
        assert_eq!(
            result.err(),
            Some(PostgresComponentConfigError::InvalidColumnsPerRow)
        );
    }

    #[test]
    fn max_parameters_smaller_than_columns_per_row_is_rejected() {
        let result = postgres_batch_writer(
            "INSERT INTO t (a, b) VALUES",
            None::<&str>,
            2,
            PostgresBatchMode::MultiRowValues {
                max_parameters_per_statement: 1,
            },
            |item: &i64| vec![BusinessValue::i64(*item), BusinessValue::i64(*item)],
        );
        assert_eq!(
            result.err(),
            Some(PostgresComponentConfigError::InvalidMaxParameters)
        );
    }
}
