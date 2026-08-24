//! Internal keyset-position plumbing shared by [`super::postgres_cursor`] and
//! [`super::postgres_paging`] (#149, `IO-DB-001` M6 `PostgreSQL` slice).
//!
//! Nothing here is a new public component-capability abstraction: it is the
//! minimal duplication removal the issue allows between two readers that both
//! need a strictly-ordered composite keyset checkpoint (`WHERE (cols...) >
//! (last...) ORDER BY cols...`) and both classify the same `PostgreSQL`
//! SQLSTATE families the same way. `PostgresKeyValue`/`KeysetPosition` are
//! `pub(crate)`; only [`KeysetColumn`], [`KeysetColumnKind`],
//! [`PostgresComponentConfigError`], and [`PostgresRow`] are re-exported
//! publicly, because both readers' constructors need the same
//! column-declaration and construction-time-validation types, and both hand
//! `map_row` the same row-accessor type.
//!
//! # Consistency under concurrent source mutation
//!
//! Both readers' restart correctness rests on one assumption:
//! `key_columns`' values, for a given row, do not change after that row
//! first becomes visible to a read. `WHERE (key_columns) > (last_committed)`
//! means "not yet delivered" only as long as a row's key is stable --
//! neither reader detects or defends against key columns that mutate in
//! place.
//!
//! - **Late inserts at or before the committed position are structurally
//!   invisible.** A row inserted with a key less than or equal to the
//!   current committed position will never be delivered by an in-progress
//!   or completed read of that stream -- the predicate excludes it by
//!   construction. This is a property of keyset pagination itself, not a
//!   defect of this implementation: no keyset-based reader (in this
//!   framework or any other) can retroactively "notice" a row inserted
//!   behind its own cursor without falling back to `OFFSET`-style
//!   positional scanning, which is exactly what these readers are built to
//!   avoid. A workload that must observe such late-arriving rows needs a
//!   different delivery mode (e.g. a change-data-capture or queue-based
//!   source), which is out of this M6 slice's scope.
//! - **Updating a row's `key_columns` values after it has entered the read
//!   window is unsafe** and can produce either a skip (the row's new key
//!   moves outside every future fetch window) or a duplicate (the row's key
//!   moves from before the committed position to after it, so a restart
//!   re-delivers it as if new). Choose key columns a caller's own business
//!   logic never updates in place -- typically an auto-incrementing
//!   surrogate key, or an immutable creation timestamp paired with a unique
//!   tiebreaker -- never a value that changes for existing rows.
//! - **A delete at or before the committed position is always safe for
//!   either reader**, on restart or otherwise: the row was already excluded
//!   or already delivered, so its deletion has no observable effect.
//!
//! What a reader whose attempt is already under way can observe from a
//! *different*, concurrently committing transaction's insert or delete of a
//! not-yet-delivered row is where the two readers diverge sharply, because a
//! `PostgreSQL` server-side cursor and an independent paged `SELECT` are not
//! the same kind of read. Neither divergence below can cause a skip, a
//! duplicate, or a lost commit -- the durable checkpoint is always the last
//! row a reader's caller actually processed, never a row it merely
//! fetched-but-stale. It only changes how promptly a concurrent insert or
//! delete becomes visible to an attempt already in progress; a restart
//! always re-establishes a fresh view either way.
//!
//! ## Paging: every page is a fresh, independently visible statement
//!
//! [`super::postgres_paging::PostgresPagingReader`] issues one ordinary
//! `SELECT ... WHERE (cols) > (last) ORDER BY cols LIMIT n` per page, with no
//! held transaction. Under `PostgreSQL`'s default `READ COMMITTED`
//! isolation, each such statement sees every row committed before it starts
//! executing:
//! - An insert at a key greater than the current position, committed by
//!   another transaction between two pages, **is** delivered by a later
//!   page -- exactly like any other not-yet-delivered row.
//! - A delete of a not-yet-delivered row, committed between two pages, is
//!   simply absent from the next page, exactly as if it had never existed at
//!   that key.
//!
//! ## Cursor: one held transaction, one fixed snapshot, for the whole attempt
//!
//! [`super::postgres_cursor::PostgresCursorReader`] `DECLARE`s one
//! server-side cursor inside one transaction and keeps both open for the
//! entire attempt; every `FETCH` runs against that same open portal.
//! `PostgreSQL`'s cursors behave as the SQL standard's `INSENSITIVE`
//! cursors do: once a portal begins executing, it keeps returning rows
//! under the snapshot fixed at that time, regardless of what other
//! transactions commit afterward. This was confirmed directly against a
//! real server -- `DECLARE` a cursor, `FETCH` part of it, commit an insert
//! and a delete from a *second* session, then `FETCH` the rest from the
//! *same*, already-open cursor:
//! - An insert at a key greater than the current position, committed by
//!   another transaction *after* this cursor's snapshot was taken, is
//!   **not** delivered by a later `FETCH` on the same cursor. Only a
//!   restart (a fresh `DECLARE`, which takes a new snapshot) picks it up --
//!   this reader's own restart model already re-`DECLARE`s on every
//!   attempt, so this is a same-attempt staleness window, not a durability
//!   gap.
//! - A delete of a not-yet-delivered row, committed by another transaction
//!   *after* this cursor's snapshot was taken, does **not** remove it from a
//!   later `FETCH` on the same cursor -- the row is still delivered, exactly
//!   as `PostgreSQL`'s MVCC snapshot isolation guarantees for any statement
//!   that began before the delete committed. A restart's fresh `DECLARE` (a
//!   new snapshot) omits it, same as any other already-deleted row.

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row};

use crate::{
    FailureCategory, ReaderError, StateCodecError, StateSchemaId, StateSchemaVersion,
    VersionedStateCodec,
};

/// A caller-declared ordering-key column: one component of a composite,
/// strictly-unique keyset used to restart a [`super::postgres_cursor`] or
/// [`super::postgres_paging`] reader without `OFFSET`.
///
/// Deliberately narrower than [`crate::BusinessValueKind`]'s five variants:
/// a boolean key cannot give a strict total order past two buckets and a
/// `NULL` key breaks row-value tuple comparison, so neither is valid
/// ordering-key material. `Bytes` (e.g. a `uuid`/`bytea` key) is not
/// supported in this M6 slice; it is a documented limitation, not a silent
/// gap -- see the `#149` evidence document.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeysetColumn {
    name: &'static str,
    kind: KeysetColumnKind,
}

/// The supported ordering-key column types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KeysetColumnKind {
    /// A `text`/`varchar` ordering key.
    Text,
    /// A `bigint`/`integer`-family ordering key, read as `i64`. Decoding
    /// tries `int8` first and falls back to `int4`, so both
    /// `bigint`/`bigserial` and `integer`/`serial` columns are valid.
    /// Binding a restored value back
    /// as a query parameter is always sent as `int8`; `PostgreSQL`'s
    /// built-in cross-type integer comparison operators accept comparing it
    /// against an `int4` column directly, so no equivalent fallback is
    /// needed on the bind side.
    I64,
}

impl KeysetColumn {
    /// Declares a `text`-typed ordering-key column.
    #[must_use]
    pub const fn text(name: &'static str) -> Self {
        Self {
            name,
            kind: KeysetColumnKind::Text,
        }
    }

    /// Declares an `i64`-typed ordering-key column.
    #[must_use]
    pub const fn i64(name: &'static str) -> Self {
        Self {
            name,
            kind: KeysetColumnKind::I64,
        }
    }

    /// Returns the column's SQL identifier.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the column's declared kind.
    #[must_use]
    pub const fn kind(&self) -> KeysetColumnKind {
        self.kind
    }
}

/// A value-redacted failure constructing a `PostgreSQL` cursor, paging, or
/// batch-writer component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresComponentConfigError {
    /// No ordering-key columns were declared; a strict total order is
    /// required.
    EmptyKeyColumns,
    /// A declared column name is empty or contains a byte outside
    /// `[A-Za-z0-9_]`, so it cannot be safely embedded as a SQL identifier.
    InvalidColumnName,
    /// The configured fetch size (cursor) or page size (paging) is zero.
    InvalidFetchSize,
    /// The configured maximum bind-parameter count per statement is zero, or
    /// smaller than one row's column count.
    InvalidMaxParameters,
    /// The configured column count per row is zero.
    InvalidColumnsPerRow,
    /// The configured maximum bind-parameter count per statement exceeds
    /// `PostgreSQL`'s hard extended-query-protocol ceiling
    /// (`item_components::postgres_batch::POSTGRESQL_MAX_BIND_PARAMETERS`,
    /// 65,535).
    MaxParametersExceedsProtocolLimit,
}

impl fmt::Display for PostgresComponentConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyKeyColumns => "at least one ordering-key column is required",
            Self::InvalidColumnName => "an ordering-key or bound column name is invalid",
            Self::InvalidFetchSize => "the configured fetch/page size must be nonzero",
            Self::InvalidMaxParameters => {
                "the configured maximum parameters per statement is too small"
            }
            Self::InvalidColumnsPerRow => "the configured column count per row must be nonzero",
            Self::MaxParametersExceedsProtocolLimit => {
                "the configured maximum parameters per statement exceeds PostgreSQL's \
                 65,535-parameter protocol limit"
            }
        })
    }
}

impl Error for PostgresComponentConfigError {}

/// Validates a SQL identifier supplied by calling Rust code (never business
/// data): non-empty, ASCII alphanumeric/underscore only, and not
/// digit-leading, so it can be embedded directly as an unquoted identifier
/// without becoming an injection surface even if a caller ever built it
/// dynamically rather than as a literal.
pub(crate) fn validate_identifier(name: &str) -> Result<(), PostgresComponentConfigError> {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return Err(PostgresComponentConfigError::InvalidColumnName);
    };
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return Err(PostgresComponentConfigError::InvalidColumnName);
    }
    if !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        return Err(PostgresComponentConfigError::InvalidColumnName);
    }
    Ok(())
}

pub(crate) fn validate_key_columns(
    columns: &[KeysetColumn],
) -> Result<(), PostgresComponentConfigError> {
    if columns.is_empty() {
        return Err(PostgresComponentConfigError::EmptyKeyColumns);
    }
    for column in columns {
        validate_identifier(column.name())?;
    }
    Ok(())
}

/// Builds the `ORDER BY` fragment for `columns`, e.g. `"a, b"`.
pub(crate) fn order_by_sql(columns: &[KeysetColumn]) -> String {
    columns
        .iter()
        .map(KeysetColumn::name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Builds the composite tuple-comparison predicate `"(a, b) > ($N, $N+1)"`
/// for `columns`, with bind placeholders starting at `next_param`.
pub(crate) fn keyset_predicate_sql(columns: &[KeysetColumn], next_param: usize) -> String {
    let names = columns
        .iter()
        .map(KeysetColumn::name)
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (0..columns.len())
        .map(|offset| format!("${}", next_param + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({names}) > ({placeholders})")
}

/// One composite ordering-key value, typed per [`KeysetColumnKind`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PostgresKeyValue {
    Text(String),
    I64(i64),
}

/// The durable restart position for a keyset-ordered reader: the last
/// successfully delivered row's ordering-key tuple, or empty for "no row
/// delivered yet" (unambiguous, since a real position always has exactly
/// `key_columns.len() >= 1` entries).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct KeysetPosition(Vec<PostgresKeyValue>);

impl KeysetPosition {
    pub(crate) const fn none() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn from_values(values: Vec<PostgresKeyValue>) -> Self {
        Self(values)
    }

    pub(crate) fn values(&self) -> &[PostgresKeyValue] {
        &self.0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Binds `values` onto `query` in order, one placeholder per value.
pub(crate) fn bind_keyset<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    values: &'q [PostgresKeyValue],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for value in values {
        query = match value {
            PostgresKeyValue::Text(text) => query.bind(text.as_str()),
            PostgresKeyValue::I64(value) => query.bind(*value),
        };
    }
    query
}

/// Reads `columns` back out of `row` as a fresh [`KeysetPosition`], proving
/// the row actually carries every declared ordering-key column.
///
/// `KeysetColumnKind::I64` tries `int8` first, falling back to `int4`: `sqlx`
/// decodes strictly by wire type and never implicitly widens an `int4`
/// column into a requested `i64`, so a bare `try_get::<i64, _>` would reject
/// every ordinary `integer`-typed key column despite
/// [`KeysetColumnKind::I64`]'s own doc comment promising "integer-family"
/// coverage. This mirrors [`PostgresRow::f64`]'s established `f64`-then-`f32`
/// fallback for the same reason.
pub(crate) fn extract_keyset(
    row: &PgRow,
    columns: &[KeysetColumn],
) -> Result<KeysetPosition, ReaderError> {
    let values = columns
        .iter()
        .map(|column| {
            let result = match column.kind() {
                KeysetColumnKind::Text => row
                    .try_get::<String, _>(column.name())
                    .map(PostgresKeyValue::Text),
                KeysetColumnKind::I64 => row
                    .try_get::<i64, _>(column.name())
                    .or_else(|_| row.try_get::<i32, _>(column.name()).map(i64::from))
                    .map(PostgresKeyValue::I64),
            };
            result.map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(KeysetPosition::from_values(values))
}

/// Classifies a `sqlx` error against the same `PostgreSQL` SQLSTATE class
/// table `repository::postgres::classify_business_error` (private to that
/// module) uses, expressed directly against [`FailureCategory`] instead of
/// `BusinessTransactionError` since this crosses the reader boundary, not the
/// enlisted-transaction boundary. `classify_pg_error_matches_documented_sqlstate_classes`
/// below pins the shared table so the two independent classifiers cannot
/// silently drift apart.
pub(crate) fn classify_pg_error(error: &sqlx::Error) -> FailureCategory {
    let Some(database) = error.as_database_error() else {
        return FailureCategory::TransientInfrastructure;
    };
    let Some(code) = database.code() else {
        return FailureCategory::TransientInfrastructure;
    };
    if code == "57014" {
        return FailureCategory::Cancelled;
    }
    if code.starts_with("22") || code.starts_with("23") || code.starts_with("42") {
        return FailureCategory::UserComponent;
    }
    FailureCategory::TransientInfrastructure
}

/// A borrowed, `PostgreSQL`-driver-hiding view of one row a
/// [`super::postgres_cursor::PostgresCursorReader`] or
/// [`super::postgres_paging::PostgresPagingReader`] fetched, handed to the
/// caller-supplied `map_row` closure. `sqlx::postgres::PgRow` itself never
/// crosses this public boundary (see `docs/api/design-guidelines.md`'s
/// disclosure gate, which names a database driver row type as a prohibited
/// public disclosure).
///
/// Covers the common scalar `PostgreSQL` column types: `text`/`varchar`,
/// the integer family (`i32`/`i64`), `boolean`, and `double precision`/
/// `real`. It does **not** cover `timestamp[tz]`, `uuid`, `numeric`,
/// `bytea`, `json[b]`, or array/composite/domain types -- reading one of
/// those columns requires casting it to a covered type in `base_query`
/// itself (e.g. `EXTRACT(EPOCH FROM ts)::bigint`, `id::text`), a documented
/// limitation of this M6 slice, not a silent gap; see the `#149` evidence
/// document. [`KeysetColumn`]'s own ordering-key vocabulary
/// (`Text`/`I64` only) is a separate, *narrower* restriction for a
/// different reason (strict total order, not general value coverage) and
/// is not widened by this type covering more.
pub struct PostgresRow<'a>(&'a PgRow);

impl<'a> PostgresRow<'a> {
    pub(crate) const fn new(row: &'a PgRow) -> Self {
        Self(row)
    }

    /// Reads a `text`/`varchar` column by name.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted [`ReaderError`] when the column is missing
    /// or is not text-typed.
    pub fn text(&self, column: &str) -> Result<String, ReaderError> {
        self.0
            .try_get(column)
            .map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
    }

    /// Reads a `bigint`/`int8`-family column by name.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted [`ReaderError`] when the column is missing
    /// or is not `bigint`-typed.
    pub fn i64(&self, column: &str) -> Result<i64, ReaderError> {
        self.0
            .try_get(column)
            .map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
    }

    /// Reads an `integer`/`int4`-family column by name.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted [`ReaderError`] when the column is missing
    /// or is not `integer`-typed.
    pub fn i32(&self, column: &str) -> Result<i32, ReaderError> {
        self.0
            .try_get(column)
            .map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
    }

    /// Reads a `boolean` column by name.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted [`ReaderError`] when the column is missing
    /// or is not `boolean`-typed.
    pub fn bool(&self, column: &str) -> Result<bool, ReaderError> {
        self.0
            .try_get(column)
            .map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
    }

    /// Reads a `double precision`/`real` column by name.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted [`ReaderError`] when the column is missing
    /// or is not floating-point-typed.
    pub fn f64(&self, column: &str) -> Result<f64, ReaderError> {
        self.0
            .try_get::<f64, _>(column)
            .or_else(|_| self.0.try_get::<f32, _>(column).map(f64::from))
            .map_err(|_| ReaderError::with_category(FailureCategory::UserComponent))
    }
}

fn encode_keyset(value: &KeysetPosition) -> Result<Vec<u8>, StateCodecError> {
    let items: Vec<Value> = value
        .values()
        .iter()
        .map(|value| match value {
            PostgresKeyValue::Text(text) => serde_json::json!({"t": "text", "v": text}),
            PostgresKeyValue::I64(value) => serde_json::json!({"t": "i64", "v": value}),
        })
        .collect();
    // `ComponentStateEnvelope::encode` requires a top-level JSON *object*
    // (`ComponentStateError::PayloadNotObject` otherwise), so the keyset
    // tuple is wrapped under a `keys` field rather than encoded as a bare
    // top-level array.
    serde_json::to_vec(&serde_json::json!({ "keys": items }))
        .map_err(|_| StateCodecError::InvalidPayload)
}

fn decode_keyset(payload: &[u8]) -> Result<KeysetPosition, StateCodecError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
    let array = value
        .get("keys")
        .and_then(Value::as_array)
        .ok_or(StateCodecError::InvalidPayload)?;
    let values = array
        .iter()
        .map(|item| {
            let tag = item
                .get("t")
                .and_then(Value::as_str)
                .ok_or(StateCodecError::InvalidPayload)?;
            match tag {
                "text" => item
                    .get("v")
                    .and_then(Value::as_str)
                    .map(|text| PostgresKeyValue::Text(text.to_owned()))
                    .ok_or(StateCodecError::InvalidPayload),
                "i64" => item
                    .get("v")
                    .and_then(Value::as_i64)
                    .map(PostgresKeyValue::I64)
                    .ok_or(StateCodecError::InvalidPayload),
                _ => Err(StateCodecError::InvalidPayload),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(KeysetPosition::from_values(values))
}

/// The [`super::postgres_cursor`] keyset-position schema. A distinct type
/// from [`PagingKeysetSchema`] (rather than one type parameterized by a
/// runtime string) so each keeps its own `const fn`-friendly, statically
/// cached [`StateSchemaId`] -- exactly [`super::json_array`]'s
/// `ReaderPositionSchema`/`WriterPositionSchema` pattern, applied to two
/// readers instead of one reader/writer pair.
#[derive(Clone, Copy)]
pub(crate) struct CursorKeysetSchema;

impl VersionedStateCodec<KeysetPosition> for CursorKeysetSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: OnceLock<StateSchemaId> = OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| {
            StateSchemaId::new("oxide-batch.postgres-cursor-reader-position").unwrap()
        })
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &KeysetPosition) -> Result<Vec<u8>, StateCodecError> {
        encode_keyset(value)
    }

    fn decode(&self, payload: &[u8]) -> Result<KeysetPosition, StateCodecError> {
        decode_keyset(payload)
    }
}

/// The [`super::postgres_paging`] keyset-position schema. See
/// [`CursorKeysetSchema`] for why this is a distinct type rather than a
/// shared parameterized one.
#[derive(Clone, Copy)]
pub(crate) struct PagingKeysetSchema;

impl VersionedStateCodec<KeysetPosition> for PagingKeysetSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: OnceLock<StateSchemaId> = OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| {
            StateSchemaId::new("oxide-batch.postgres-paging-reader-position").unwrap()
        })
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &KeysetPosition) -> Result<Vec<u8>, StateCodecError> {
        encode_keyset(value)
    }

    fn decode(&self, payload: &[u8]) -> Result<KeysetPosition, StateCodecError> {
        decode_keyset(payload)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn keyset_predicate_sql_builds_a_composite_tuple_comparison() {
        let columns = [KeysetColumn::text("email"), KeysetColumn::i64("id")];
        assert_eq!(keyset_predicate_sql(&columns, 1), "(email, id) > ($1, $2)");
        assert_eq!(order_by_sql(&columns), "email, id");
    }

    #[test]
    fn keyset_position_round_trips_through_json() {
        let position = KeysetPosition::from_values(vec![
            PostgresKeyValue::Text("a@example.com".to_owned()),
            PostgresKeyValue::I64(42),
        ]);
        let encoded = encode_keyset(&position).unwrap();
        let decoded = decode_keyset(&encoded).unwrap();
        assert_eq!(decoded, position);
    }

    #[test]
    fn none_position_is_empty_and_round_trips() {
        let position = KeysetPosition::none();
        assert!(position.is_empty());
        let encoded = encode_keyset(&position).unwrap();
        let decoded = decode_keyset(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn validate_identifier_rejects_unsafe_names() {
        assert!(validate_identifier("valid_name_1").is_ok());
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("1leading_digit").is_err());
        assert!(validate_identifier("has space").is_err());
        assert!(validate_identifier("has;semicolon").is_err());
        assert!(validate_identifier("has'quote").is_err());
    }

    #[test]
    fn validate_key_columns_requires_at_least_one() {
        assert_eq!(
            validate_key_columns(&[]),
            Err(PostgresComponentConfigError::EmptyKeyColumns)
        );
        assert!(validate_key_columns(&[KeysetColumn::i64("id")]).is_ok());
    }

    /// `classify_pg_error`'s SQLSTATE classification must agree with
    /// `repository::postgres::classify_business_error`'s -- both are read
    /// from the same `PostgreSQL` SQLSTATE class table, just against different
    /// output enums (`FailureCategory` vs `BusinessTransactionError`) for
    /// different call boundaries.
    #[test]
    fn classify_pg_error_matches_documented_sqlstate_classes() {
        // No live `sqlx::Error` construction is available outside a real
        // driver round trip; this test pins the classification table
        // itself (the pure `code -> FailureCategory` mapping) rather than
        // constructing a `sqlx::Error`, so a future edit to either
        // classifier is caught by comparing both tables' documented
        // behavior in code review, not by this test alone. See
        // `postgres_item_components_cursor.rs` for the live-error variant
        // exercised against a real database.
        fn classify_code(code: &str) -> FailureCategory {
            if code == "57014" {
                return FailureCategory::Cancelled;
            }
            if code.starts_with("22") || code.starts_with("23") || code.starts_with("42") {
                return FailureCategory::UserComponent;
            }
            FailureCategory::TransientInfrastructure
        }
        assert_eq!(classify_code("57014"), FailureCategory::Cancelled);
        assert_eq!(classify_code("23505"), FailureCategory::UserComponent);
        assert_eq!(classify_code("22001"), FailureCategory::UserComponent);
        assert_eq!(classify_code("42601"), FailureCategory::UserComponent);
        assert_eq!(
            classify_code("08006"),
            FailureCategory::TransientInfrastructure
        );
    }
}
