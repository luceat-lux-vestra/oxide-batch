//! Restartable delimited/CSV file components (#147, `IO-FLAT-001`).
//!
//! [`DelimitedReader`]/[`DelimitedWriter`] are plain [`crate::ItemReader`]/
//! [`crate::ItemWriter`] implementations over any `Read + Seek`/`Write`
//! source, built on the mature [`csv`] parser rather than a hand-rolled
//! splitter -- quoted delimiters, doubled/escaped quotes, and multiline
//! quoted fields are the parser's job, not this module's. Restart position is
//! the parser's own record-boundary byte/line/record triple
//! ([`csv::Position`]), never an inferred line count, so a restart can never
//! land inside a multiline quoted record. Neither type, nor any dialect/state
//! type here, exposes a `csv` crate type in its public signature: dialect
//! configuration and record content are OxideBatch-owned
//! ([`DelimitedDialect`], [`DelimitedRecord`]).
//!
//! Restart state is carried by the paired [`DelimitedReaderStream`]/
//! [`DelimitedWriterStream`] through the existing M6 [`crate::ItemStream`]
//! contract: [`delimited_reader`] and [`delimited_writer`] return a
//! `(component, stream, contract)` triple sharing state through an `Arc`,
//! exactly the pattern `oxide-batch-test`'s `restart::range_reader` uses for
//! a durable, restartable reader -- register the stream under the same
//! [`crate::ComponentStreamIdentity`] the component was built with.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use csv::{Reader as CsvReader, ReaderBuilder, Terminator as CsvTerminator};

use crate::{
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError, StateLimits,
    StateSchemaId, StateSchemaVersion, StateSensitivity, StreamCloseContext, StreamCloseError,
    StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamStateContract,
    StreamUpdateContext, StreamUpdateError, VersionedStateCodec, WriteContext, WriteOutcome,
    WriterError,
};

/// The largest single record this module accepts by default: 1 MiB.
///
/// A record whose parsed byte span exceeds the configured bound is a typed,
/// classified [`ReaderError`] (see `DelimitedReader::read`) rather than an
/// unbounded allocation, so a pathological single record cannot grow this
/// component's retained memory without limit.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;

/// The record terminator [`DelimitedWriter`] emits.
///
/// A reader never needs this: `csv`'s default terminator recognizes both
/// `\r\n` and bare `\n` on input, so no reader-side configuration is offered
/// for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DelimitedTerminator {
    /// Bare `\n`.
    Lf,
    /// `\r\n`.
    CrLf,
}

/// A bounded, OxideBatch-owned delimited/CSV dialect configuration.
///
/// Covers the basics [`csv`] itself supports -- delimiter, quote character,
/// optional non-doubled escape byte, doubled-quote escaping, a header row,
/// ragged-row tolerance, and the output terminator -- without reproducing
/// every option the underlying parser exposes.
#[derive(Clone, Copy, Debug)]
pub struct DelimitedDialect {
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    double_quote: bool,
    has_headers: bool,
    flexible: bool,
    terminator: DelimitedTerminator,
    max_record_bytes: usize,
}

impl DelimitedDialect {
    /// Standard comma-separated dialect: `,` delimiter, `"` quote, doubled-quote
    /// escaping, no header row, ragged rows rejected, `\n` output terminator,
    /// and [`DEFAULT_MAX_RECORD_BYTES`].
    #[must_use]
    pub const fn csv() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: None,
            double_quote: true,
            has_headers: false,
            flexible: false,
            terminator: DelimitedTerminator::Lf,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
        }
    }

    /// Sets the field delimiter byte.
    #[must_use]
    pub const fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Sets the quote byte.
    #[must_use]
    pub const fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Sets an optional non-doubled escape byte (e.g. `\`).
    #[must_use]
    pub const fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Sets whether a doubled quote (`""`) escapes a literal quote.
    #[must_use]
    pub const fn with_double_quote(mut self, enabled: bool) -> Self {
        self.double_quote = enabled;
        self
    }

    /// Sets whether the first record is a header row, excluded from items and
    /// available through [`DelimitedRecord::field`].
    #[must_use]
    pub const fn with_headers(mut self, enabled: bool) -> Self {
        self.has_headers = enabled;
        self
    }

    /// Sets whether records with a field count different from the first are
    /// accepted rather than classified as malformed.
    #[must_use]
    pub const fn with_flexible(mut self, enabled: bool) -> Self {
        self.flexible = enabled;
        self
    }

    /// Sets the record terminator [`DelimitedWriter`] emits.
    #[must_use]
    pub const fn with_terminator(mut self, terminator: DelimitedTerminator) -> Self {
        self.terminator = terminator;
        self
    }

    /// Sets the largest single record's parsed byte span the reader accepts.
    #[must_use]
    pub const fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    fn reader_builder(self) -> ReaderBuilder {
        let mut builder = ReaderBuilder::new();
        builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .escape(self.escape)
            .double_quote(self.double_quote)
            .has_headers(self.has_headers)
            .flexible(self.flexible);
        builder
    }

    fn writer_builder(self) -> csv::WriterBuilder {
        let mut builder = csv::WriterBuilder::new();
        builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .double_quote(self.double_quote)
            .flexible(self.flexible)
            .terminator(match self.terminator {
                DelimitedTerminator::Lf => CsvTerminator::Any(b'\n'),
                DelimitedTerminator::CrLf => CsvTerminator::CRLF,
            });
        if let Some(escape) = self.escape {
            builder.escape(escape);
        }
        builder
    }
}

impl Default for DelimitedDialect {
    fn default() -> Self {
        Self::csv()
    }
}

/// One decoded delimited/CSV record: an OxideBatch-owned, Rust-native field
/// list, never a `csv` crate type.
///
/// Downstream mapping to a domain type is an ordinary
/// [`crate::ItemProcessor`], kept out of this module by design (ADR-0008: no
/// parallel component trait hierarchy, no serde marshalling layer).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelimitedRecord {
    fields: Vec<String>,
    headers: Option<Arc<[String]>>,
}

impl DelimitedRecord {
    /// Builds a record from owned fields, with no header row.
    #[must_use]
    pub const fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            headers: None,
        }
    }

    /// Borrows the record's fields in file order.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Consumes the record, returning its owned fields.
    #[must_use]
    pub fn into_fields(self) -> Vec<String> {
        self.fields
    }

    /// Borrows the field at `index`, if present.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&str> {
        self.fields.get(index).map(String::as_str)
    }

    /// Borrows the field named by the dialect's header row, if headers were
    /// enabled and `name` is a known header.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&str> {
        let headers = self.headers.as_ref()?;
        let index = headers.iter().position(|header| header == name)?;
        self.get(index)
    }

    /// Returns the field count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns whether the record has no fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl From<Vec<String>> for DelimitedRecord {
    fn from(fields: Vec<String>) -> Self {
        Self::new(fields)
    }
}

/// A stable, restartable byte/line/record parser position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoredPosition {
    byte: u64,
    line: u64,
    record: u64,
}

impl StoredPosition {
    /// The position of a fresh, unread parser: byte 0, 1-based line 1, no
    /// records read yet -- matches `csv::Position::new()` exactly.
    const START: Self = Self {
        byte: 0,
        line: 1,
        record: 0,
    };

    fn to_csv(self) -> csv::Position {
        let mut position = csv::Position::new();
        position.set_byte(self.byte);
        position.set_line(self.line);
        position.set_record(self.record);
        position
    }

    fn from_csv(position: &csv::Position) -> Self {
        Self {
            byte: position.byte(),
            line: position.line(),
            record: position.record(),
        }
    }
}

const READER_SCHEMA: &str = "oxide-batch.delimited-reader-position";
const READER_CODEC: &str = "oxide-batch.delimited-reader-position-codec";

#[derive(Clone, Copy)]
struct ReaderPositionSchema;

impl VersionedStateCodec<StoredPosition> for ReaderPositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(READER_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &StoredPosition) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({
            "byte": value.byte,
            "line": value.line,
            "record": value.record,
        }))
        .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<StoredPosition, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let byte = value
            .get("byte")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let line = value
            .get("line")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let record = value
            .get("record")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(StoredPosition { byte, line, record })
    }
}

#[allow(
    clippy::unwrap_used,
    reason = "fixed literal identities cannot fail validation"
)]
fn reader_position_codec() -> DefaultComponentCodec<ReaderPositionSchema> {
    DefaultComponentCodec::new(
        ReaderPositionSchema,
        CodecId::new(READER_CODEC).unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

const WRITER_SCHEMA: &str = "oxide-batch.delimited-writer-position";
const WRITER_CODEC: &str = "oxide-batch.delimited-writer-position-codec";

#[derive(Clone, Copy)]
struct WriterPositionSchema;

impl VersionedStateCodec<u64> for WriterPositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(WRITER_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "committed_bytes": value }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        value
            .get("committed_bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)
    }
}

#[allow(
    clippy::unwrap_used,
    reason = "fixed literal identities cannot fail validation"
)]
fn writer_position_codec() -> DefaultComponentCodec<WriterPositionSchema> {
    DefaultComponentCodec::new(
        WriterPositionSchema,
        CodecId::new(WRITER_CODEC).unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

/// A restartable [`crate::ItemReader`] over any `Read + Seek` delimited/CSV
/// source, built on [`csv::Reader`].
///
/// # Contract
///
/// - **Input/output**: produces [`DelimitedRecord`]; format is the
///   configured [`DelimitedDialect`].
/// - **State/checkpoint**: restart position is the parser's own
///   byte/line/record triple, persisted through the paired
///   [`DelimitedReaderStream`]. Never inferred from a line count: a quoted
///   multiline field cannot desynchronize the position from the actual
///   record boundary, because the position *is* the parser's own boundary.
/// - **Ordering**: preserves file order.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant (owns the parser's mutable state).
/// - **Transaction/delivery**: not applicable (a reader never enlists).
/// - **Bounded resource**: bounded by [`DelimitedDialect::with_max_record_bytes`];
///   a single record whose parsed byte span exceeds the bound is a
///   classified, forward-proven [`ReaderError`] rather than an unbounded
///   allocation. The reader never buffers more than the current record.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls; this reader does not itself block on
///   I/O across an await point beyond one synchronous record read.
/// - **Close**: closed through the paired stream's
///   [`crate::ItemStream::close`]; the reader itself holds no resource that
///   outlives the process (the underlying `Read + Seek` source is owned by
///   the caller-supplied value).
/// - **Sensitive diagnostics**: restart state is position-only (byte/line/record
///   counters), never record content, and is declared
///   [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: a ragged row (when not
///   [`DelimitedDialect::with_flexible`]), invalid UTF-8 in a field, or a
///   record whose byte span exceeds the configured bound is a
///   [`ReaderError`] in [`crate::FailureCategory::UserComponent`] with
///   [`ReaderError::has_checkpoint_advanced`] `true`: the parser has always
///   already advanced past the offending bytes by the time the error is
///   observed (this is inherent to a forward-only parser, not a defect), so
///   a configured retry re-invokes the reader against the *next* record, not
///   the same one -- configure skip or fail for this failure class, not
///   retry.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_delimited.rs`,
///   `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs`.
pub struct DelimitedReader<Src> {
    inner: CsvReader<Src>,
    headers: Option<Arc<[String]>>,
    max_record_bytes: usize,
    position: Arc<Mutex<StoredPosition>>,
    seeked: bool,
}

impl<Src: Read + Seek> DelimitedReader<Src> {
    /// Seeks the parser to a restored non-zero position on the attempt's
    /// first read, and otherwise leaves the parser untouched.
    ///
    /// A fresh (non-restarted) attempt never calls [`csv::Reader::seek`] at
    /// all: `csv`'s own header/first-record bookkeeping already handles that
    /// case correctly, and calling `seek` unconditionally -- even to
    /// byte 0 -- disables that bookkeeping (`seek` unconditionally sets its
    /// own "has this reader ever been seeked" flag, which suppresses the
    /// crate's normal first-record/header handling for the rest of the
    /// reader's life), which would silently misplace the first record.
    fn seek_if_needed(&mut self) -> Result<(), ReaderError> {
        if self.seeked {
            return Ok(());
        }
        self.seeked = true;
        let target = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        if target.byte == 0 {
            return Ok(());
        }
        self.inner
            .seek(target.to_csv())
            .map_err(|_| ReaderError::new())
    }
}

impl<I, Src> crate::ItemReader<I> for DelimitedReader<Src>
where
    I: From<DelimitedRecord> + 'static,
    Src: Read + Seek + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.seek_if_needed()?;
        let mut record = csv::StringRecord::new();
        let before = StoredPosition::from_csv(self.inner.position());
        match self.inner.read_record(&mut record) {
            Ok(true) => {
                let after = StoredPosition::from_csv(self.inner.position());
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                let span = after.byte.saturating_sub(before.byte);
                let max_record_bytes = u64::try_from(self.max_record_bytes).unwrap_or(u64::MAX);
                if span > max_record_bytes {
                    return Err(ReaderError::new().with_checkpoint_advanced(true));
                }
                if self.headers.is_none() && self.inner.has_headers() {
                    self.headers = self
                        .inner
                        .headers()
                        .ok()
                        .map(|headers| headers.iter().map(str::to_owned).collect());
                }
                let fields = record.iter().map(str::to_owned).collect();
                let decoded = DelimitedRecord {
                    fields,
                    headers: self.headers.clone(),
                };
                Ok(ReadOutcome::Item(decoded.into()))
            }
            Ok(false) => Ok(ReadOutcome::EndOfInput),
            Err(error) => {
                let category = if matches!(error.kind(), csv::ErrorKind::Io(_)) {
                    crate::FailureCategory::TransientInfrastructure
                } else {
                    crate::FailureCategory::UserComponent
                };
                let after = StoredPosition::from_csv(self.inner.position());
                let advanced = after.byte > before.byte;
                if advanced {
                    *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                }
                Err(ReaderError::with_category(category).with_checkpoint_advanced(advanced))
            }
        }
    }
}

/// The [`crate::ItemStream`] half of a [`DelimitedReader`]: restores the
/// shared parser position from the last committed envelope on
/// [`open`](crate::ItemStream::open), and reports the current shared
/// position as the candidate on
/// [`update`](crate::ItemStream::update).
pub struct DelimitedReaderStream {
    position: Arc<Mutex<StoredPosition>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for DelimitedReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = reader_position_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<StoredPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = restored;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = StoredPosition::START;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = reader_position_codec();
        let current = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
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
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Builds a `(reader, stream, contract)` triple over any `Read + Seek`
/// source, namespaced under `identity`.
///
/// Register the stream with
/// [`crate::ChunkStep::with_item_stream`] under the same `identity`, and
/// declare `identity` in the job's [`crate::ChunkComponentRevisions`] via
/// [`crate::ChunkComponentRevisions::with_stream_revision`] so the runtime
/// resolves the reader's paired stream to the durable namespace a restart
/// inherits from.
pub fn delimited_reader<I, Src>(
    source: Src,
    dialect: DelimitedDialect,
    identity: ComponentStreamIdentity,
) -> (
    DelimitedReader<Src>,
    DelimitedReaderStream,
    StreamStateContract,
)
where
    I: From<DelimitedRecord> + 'static,
    Src: Read + Seek + Send,
{
    let position = Arc::new(Mutex::new(StoredPosition::START));
    let inner = dialect.reader_builder().from_reader(source);
    let reader = DelimitedReader {
        inner,
        headers: None,
        max_record_bytes: dialect.max_record_bytes,
        position: Arc::clone(&position),
        seeked: false,
    };
    let stream = DelimitedReaderStream {
        position,
        namespace: identity,
    };
    let contract = StreamStateContract::new(reader_position_codec());
    (reader, stream, contract)
}

/// Opens `path` for a restartable [`DelimitedReader<File>`].
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn delimited_file_reader<I>(
    path: impl AsRef<Path>,
    dialect: DelimitedDialect,
    identity: ComponentStreamIdentity,
) -> io::Result<(
    DelimitedReader<File>,
    DelimitedReaderStream,
    StreamStateContract,
)>
where
    I: From<DelimitedRecord> + 'static,
{
    let file = File::open(path)?;
    Ok(delimited_reader::<I, File>(file, dialect, identity))
}

/// A restartable [`crate::ItemWriter`] over a local file, built on
/// [`csv::Writer`].
///
/// # Contract
///
/// - **Input/output**: accepts `I: Into<`[`DelimitedRecord`]`>`; writes the
///   configured [`DelimitedDialect`].
/// - **State/checkpoint**: committed output progress is the file's byte
///   length as of the last committed chunk, persisted through the paired
///   [`DelimitedWriterStream`]. On restart, `DelimitedWriterStream::open`
///   reconciles the file to that exact length before any further write:
///   trailing bytes beyond the committed length (written but never
///   committed, e.g. a crash between write and commit) are truncated, never
///   treated as authoritative; a file *shorter* than the committed length is
///   an inconsistent resource and fails closed
///   ([`StreamOpenError`]) rather than fabricating progress. Initial
///   (non-restart) execution truncates/creates the target file fresh.
/// - **Ordering**: writes items in the order supplied.
/// - **Thread safety**: `Send + Sync`; an internal `Mutex` serializes the
///   shared file handle exactly as [`crate::item_components::sync`]
///   documents for a writer whose resource is only correct under serialized
///   access.
/// - **Reentrancy**: not reentrant with itself against the same path from a
///   second concurrent attempt; restart reconciliation assumes exclusive
///   ownership of the file for the duration of one attempt.
/// - **Transaction/delivery**: does not enlist in
///   [`crate::WriteContext::enlisted`]; file bytes are not part of the
///   OxideBatch-owned business transaction. Durability across an OS/power
///   failure (as opposed to a process crash) is not claimed: each write is
///   flushed to the OS (`sync_data`), but no directory-entry fsync is
///   performed.
/// - **Bounded resource**: one file handle; buffers at most one write batch.
/// - **Cancellation**: honors the call-scoped stop token before writing.
/// - **Close**: nothing beyond the paired stream's
///   [`crate::ItemStream::close`]; the file is not explicitly closed by this
///   writer (dropped with the reader/writer pair at the end of the process).
/// - **Sensitive diagnostics**: restart state is a byte count, never record
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: not applicable; a writer never rejects an
///   already-typed item.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_delimited.rs`,
///   `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs`.
pub struct DelimitedWriter {
    file: Arc<Mutex<File>>,
    dialect: DelimitedDialect,
    committed_bytes: Arc<Mutex<u64>>,
}

impl<I> crate::ItemWriter<I> for DelimitedWriter
where
    I: Send + Sync + Clone + Into<DelimitedRecord>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut csv_writer = self.dialect.writer_builder().from_writer(&mut buffer);
            for item in items {
                let record: DelimitedRecord = item.clone().into();
                csv_writer
                    .write_record(record.fields())
                    .map_err(|_| WriterError::new())?;
            }
            csv_writer.flush().map_err(|_| WriterError::new())?;
        }
        let file = Arc::clone(&self.file);
        let committed_bytes = Arc::clone(&self.committed_bytes);
        let mut guard = file.lock().unwrap_or_else(PoisonError::into_inner);
        guard.write_all(&buffer).map_err(|_| WriterError::new())?;
        guard.sync_data().map_err(|_| WriterError::new())?;
        drop(guard);
        let mut committed = committed_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *committed = committed.saturating_add(buffer.len() as u64);
        Ok(WriteOutcome::Written)
    }
}

/// The [`crate::ItemStream`] half of a [`DelimitedWriter`]: reconciles the
/// shared file to the last committed byte length on
/// [`open`](crate::ItemStream::open), and reports the current shared byte
/// length as the candidate on [`update`](crate::ItemStream::update).
pub struct DelimitedWriterStream {
    file: Arc<Mutex<File>>,
    committed_bytes: Arc<Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for DelimitedWriterStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = writer_position_codec();
        let (target, outcome) = if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<u64>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            (restored, StreamOpenOutcome::Restored)
        } else {
            (0, StreamOpenOutcome::Initial)
        };
        let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        let actual_len = file.metadata().map_err(|_| StreamOpenError::new())?.len();
        if actual_len < target {
            return Err(StreamOpenError::with_category(
                crate::FailureCategory::Invariant,
            ));
        }
        if actual_len != target {
            file.set_len(target).map_err(|_| StreamOpenError::new())?;
        }
        file.seek(SeekFrom::Start(target))
            .map_err(|_| StreamOpenError::new())?;
        drop(file);
        *self
            .committed_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = target;
        Ok(outcome)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = writer_position_codec();
        let current = *self
            .committed_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
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
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Opens (creating if absent) `path` for a restartable
/// `(writer, stream, contract)` triple, namespaced under `identity`.
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn delimited_writer(
    path: impl AsRef<Path>,
    dialect: DelimitedDialect,
    identity: ComponentStreamIdentity,
) -> io::Result<(DelimitedWriter, DelimitedWriterStream, StreamStateContract)> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let file = Arc::new(Mutex::new(file));
    let committed_bytes = Arc::new(Mutex::new(0));
    let writer = DelimitedWriter {
        file: Arc::clone(&file),
        dialect,
        committed_bytes: Arc::clone(&committed_bytes),
    };
    let stream = DelimitedWriterStream {
        file,
        committed_bytes,
        namespace: identity,
    };
    let contract = StreamStateContract::new(writer_position_codec());
    Ok((writer, stream, contract))
}
