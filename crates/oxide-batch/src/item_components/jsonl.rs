//! Restartable JSON Lines (JSONL) file components (#148, `IO-STRUCTURED-001`
//! M6 slice).
//!
//! One JSON value per line is one record: [`JsonLinesReader`] reads a bounded
//! line (mirroring [`crate::item_components::fixed_width`]'s line-reading
//! technique exactly -- see that module for the shared reasoning) and parses
//! its content with [`serde_json::from_slice`]. A malformed line is always
//! consumed through its line boundary before the parse is attempted, so its
//! next record boundary is independently knowable regardless of whether the
//! line's own content parses -- this is what makes a malformed JSONL record
//! safely skippable, unlike [`crate::item_components::json_array`]'s
//! per-element framing (see that module's documentation for why array
//! elements cannot make the same claim).
//!
//! Restart position is a plain byte offset at the last consumed line
//! boundary, tracked exactly like
//! [`crate::item_components::fixed_width::FixedWidthReader`]'s. Restart state
//! is carried by the paired [`JsonLinesReaderStream`]/[`JsonLinesWriterStream`]
//! through the existing M6 [`crate::ItemStream`] contract: [`jsonl_reader`]
//! and [`jsonl_writer`] return a `(component, stream, contract)` triple
//! sharing state through an `Arc`, the same pattern
//! [`crate::item_components::delimited`]/[`crate::item_components::fixed_width`]
//! use.
//!
//! The public item representation is [`serde_json::Value`] directly (`I: From<Value>`
//! for the reader, `I: Into<Value>` for the writer) -- no bespoke JSON AST:
//! `serde_json` is already a production dependency of this crate, and no
//! `serde_json` type other than `Value` and its own [`serde_json::Error`]
//! appears in a public signature here.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value;

use crate::{
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    FailureCategory, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StateSensitivity,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

/// The largest single line this module accepts by default: 1 MiB.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;

/// The record terminator [`JsonLinesWriter`] emits; a reader accepts either
/// on input regardless of this setting (see [`JsonLinesReader`]'s contract).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JsonLinesTerminator {
    /// Bare `\n`.
    Lf,
    /// `\r\n`.
    CrLf,
}

impl JsonLinesTerminator {
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::CrLf => b"\r\n",
        }
    }
}

/// A bounded, OxideBatch-owned JSON Lines format configuration.
#[derive(Clone, Copy, Debug)]
pub struct JsonLinesFormat {
    max_record_bytes: usize,
    terminator: JsonLinesTerminator,
}

impl JsonLinesFormat {
    /// The default format: [`DEFAULT_MAX_RECORD_BYTES`], `\n` output
    /// terminator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            terminator: JsonLinesTerminator::Lf,
        }
    }

    /// Sets the largest single line's raw byte span (excluding terminator)
    /// the reader accepts before failing closed.
    #[must_use]
    pub const fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    /// Sets the record terminator [`JsonLinesWriter`] emits.
    #[must_use]
    pub const fn with_terminator(mut self, terminator: JsonLinesTerminator) -> Self {
        self.terminator = terminator;
        self
    }
}

impl Default for JsonLinesFormat {
    fn default() -> Self {
        Self::new()
    }
}

const READER_SCHEMA: &str = "oxide-batch.jsonl-reader-position";
const READER_CODEC: &str = "oxide-batch.jsonl-reader-position-codec";

#[derive(Clone, Copy)]
struct ReaderPositionSchema;

impl VersionedStateCodec<u64> for ReaderPositionSchema {
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

    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "byte": value }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        value
            .get("byte")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)
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

const WRITER_SCHEMA: &str = "oxide-batch.jsonl-writer-position";
const WRITER_CODEC: &str = "oxide-batch.jsonl-writer-position-codec";

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
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        value
            .get("committed_bytes")
            .and_then(Value::as_u64)
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

/// One bounded read of a line from a buffered source, at most `max_bytes`
/// retained regardless of how long the underlying line is.
///
/// Identical in shape to
/// [`crate::item_components::fixed_width`]'s own bounded-line reader -- see
/// that module's documentation for the reasoning -- duplicated rather than
/// shared across the two independently evolving families.
enum BoundedLine {
    /// A line was read, excluding its terminator.
    Line { bytes: Vec<u8>, consumed: u64 },
    /// The source was already exhausted; nothing was read.
    Eof,
    /// The line's raw byte span (before terminator) exceeded `max_bytes`.
    /// `consumed` is the total bytes through and including the terminator
    /// (or end of input), so the caller can still prove forward progress.
    TooLong { consumed: u64 },
}

/// Copies at most `max_bytes.saturating_sub(buf.len())` bytes of `segment`
/// into `buf`, so `buf.len()` can never exceed `max_bytes` -- not even
/// transiently. Sets `*too_long` and discards `buf`'s content the moment
/// `segment` would need more room than that.
fn copy_bounded(buf: &mut Vec<u8>, segment: &[u8], max_bytes: usize, too_long: &mut bool) {
    if *too_long {
        return;
    }
    let budget = max_bytes.saturating_sub(buf.len());
    if segment.len() <= budget {
        buf.extend_from_slice(segment);
    } else {
        buf.extend_from_slice(&segment[..budget]);
        *too_long = true;
        buf.clear();
        buf.shrink_to_fit();
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R, max_bytes: usize) -> io::Result<BoundedLine> {
    let mut buf: Vec<u8> = Vec::new();
    let mut consumed_total: u64 = 0;
    let mut too_long = false;
    let mut pending_cr = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if pending_cr {
                copy_bounded(&mut buf, b"\r", max_bytes, &mut too_long);
            }
            if consumed_total == 0 {
                return Ok(BoundedLine::Eof);
            }
            return Ok(if too_long {
                BoundedLine::TooLong {
                    consumed: consumed_total,
                }
            } else {
                BoundedLine::Line {
                    bytes: buf,
                    consumed: consumed_total,
                }
            });
        }
        if pending_cr {
            if available[0] == b'\n' {
                reader.consume(1);
                consumed_total += 1;
                return Ok(if too_long {
                    BoundedLine::TooLong {
                        consumed: consumed_total,
                    }
                } else {
                    BoundedLine::Line {
                        bytes: buf,
                        consumed: consumed_total,
                    }
                });
            }
            copy_bounded(&mut buf, b"\r", max_bytes, &mut too_long);
            pending_cr = false;
        }
        if let Some(newline) = available.iter().position(|&byte| byte == b'\n') {
            let payload_end =
                newline.saturating_sub(usize::from(newline > 0 && available[newline - 1] == b'\r'));
            copy_bounded(
                &mut buf,
                &available[..payload_end],
                max_bytes,
                &mut too_long,
            );
            let take = newline + 1;
            reader.consume(take);
            consumed_total += take as u64;
            return Ok(if too_long {
                BoundedLine::TooLong {
                    consumed: consumed_total,
                }
            } else {
                BoundedLine::Line {
                    bytes: buf,
                    consumed: consumed_total,
                }
            });
        }
        if available.last() == Some(&b'\r') {
            let payload_end = available.len() - 1;
            copy_bounded(
                &mut buf,
                &available[..payload_end],
                max_bytes,
                &mut too_long,
            );
            reader.consume(payload_end);
            consumed_total += payload_end as u64;
            reader.consume(1);
            consumed_total += 1;
            pending_cr = true;
            continue;
        }
        copy_bounded(&mut buf, available, max_bytes, &mut too_long);
        let chunk_len = available.len();
        reader.consume(chunk_len);
        consumed_total += chunk_len as u64;
    }
}

/// A restartable [`crate::ItemReader`] over any `Read + Seek` JSON Lines
/// source.
///
/// # Contract
///
/// - **Input/output**: produces one [`serde_json::Value`] per line.
/// - **State/checkpoint**: restart position is a plain byte offset at the
///   last consumed line boundary, persisted through the paired
///   [`JsonLinesReaderStream`] -- identical in shape to
///   [`crate::item_components::fixed_width::FixedWidthReader`]'s. A failed
///   seek or a read that fails partway through a line is
///   [`crate::FailureCategory::TransientInfrastructure`], never advances
///   this position, and forces the *next* call to re-seek to it (even when
///   it is byte 0) before reading anything -- a retry never continues from
///   an unconfirmed mid-line source position, so it can neither duplicate a
///   committed line nor resume inside one.
/// - **Ordering**: preserves file order.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: raw line bytes are read through the same bounded
///   `fill_buf`/`consume` loop
///   [`crate::item_components::fixed_width::FixedWidthReader`] uses, capped
///   at [`JsonLinesFormat::with_max_record_bytes`]: each chunk read from the
///   source is copied into the line buffer only up to the remaining budget
///   under that bound. Record-dependent parser/value allocations remain
///   `O(max_record_bytes)` for accepted input, while an oversized line is
///   rejected before source-sized raw accumulation. See
///   `crates/oxide-batch/tests/item_components_json_allocation.rs` for the
///   allocator-level positive controls.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls.
/// - **Close**: closed through the paired stream's
///   [`crate::ItemStream::close`].
/// - **Sensitive diagnostics**: restart state is a byte offset, never record
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: a line whose content does not parse as one JSON
///   value (including an empty line, and a line whose raw byte length
///   exceeds the configured bound) is a [`ReaderError`] in
///   [`crate::FailureCategory::UserComponent`] with
///   [`ReaderError::has_checkpoint_advanced`] `true`: the line's bytes
///   (through the next line terminator, or end of input) are always consumed
///   before the parse is attempted, so the next record's boundary is
///   independently knowable regardless of whether this line parses --
///   configure skip or fail for this failure class, not retry (a retry
///   re-invokes the reader against the *next* line, not the same one). A
///   file's final line is a record whether or not it carries a trailing
///   terminator; a file that ends exactly after a terminator produces no
///   phantom trailing empty record.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_jsonl.rs`,
///   `crates/oxide-batch-test/tests/postgres_json_restart.rs`.
pub struct JsonLinesReader<Src> {
    source: BufReader<Src>,
    max_record_bytes: usize,
    position: Arc<Mutex<u64>>,
    /// `true` until the source's physical position is *confirmed* to match
    /// the authoritative `position`: armed initially, and re-armed by any
    /// operation that leaves that match unproven (a failed seek, or a read
    /// failure partway through a line, which may have already consumed some
    /// of that line's bytes into the buffered reader without a matching
    /// commit). Never cleared until an actual `seek` call succeeds -- a
    /// retry must never trust an unconfirmed position, including byte 0.
    needs_seek: bool,
}

impl<Src: Read + Seek> JsonLinesReader<Src> {
    fn seek_if_needed(&mut self) -> Result<(), ReaderError> {
        if !self.needs_seek {
            return Ok(());
        }
        let target = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        self.source
            .seek(SeekFrom::Start(target))
            .map_err(|_| ReaderError::with_category(FailureCategory::TransientInfrastructure))?;
        self.needs_seek = false;
        Ok(())
    }
}

impl<I, Src> crate::ItemReader<I> for JsonLinesReader<Src>
where
    I: From<Value> + 'static,
    Src: Read + Seek + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.seek_if_needed()?;
        let before = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        let Ok(outcome) = read_bounded_line(&mut self.source, self.max_record_bytes) else {
            // The line may be partially consumed into the buffered reader
            // at an arbitrary mid-record position with no matching
            // checkpoint update -- never resume from there. A retry must
            // re-seek to `before` (the last proven boundary, including 0)
            // rather than continue from wherever this failed attempt left
            // the source.
            self.needs_seek = true;
            return Err(ReaderError::with_category(
                FailureCategory::TransientInfrastructure,
            ));
        };
        match outcome {
            BoundedLine::Eof => Ok(ReadOutcome::EndOfInput),
            BoundedLine::TooLong { consumed } => {
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = before + consumed;
                Err(ReaderError::new().with_checkpoint_advanced(true))
            }
            BoundedLine::Line { bytes, consumed } => {
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = before + consumed;
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|_| ReaderError::new().with_checkpoint_advanced(true))?;
                Ok(ReadOutcome::Item(value.into()))
            }
        }
    }
}

/// The [`crate::ItemStream`] half of a [`JsonLinesReader`].
pub struct JsonLinesReaderStream {
    position: Arc<Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for JsonLinesReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = reader_position_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<u64>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = restored;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = 0;
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
pub fn jsonl_reader<I, Src>(
    source: Src,
    format: JsonLinesFormat,
    identity: ComponentStreamIdentity,
) -> (
    JsonLinesReader<Src>,
    JsonLinesReaderStream,
    StreamStateContract,
)
where
    I: From<Value> + 'static,
    Src: Read + Seek + Send,
{
    let position = Arc::new(Mutex::new(0));
    let reader = JsonLinesReader {
        source: BufReader::new(source),
        max_record_bytes: format.max_record_bytes,
        position: Arc::clone(&position),
        needs_seek: true,
    };
    let stream = JsonLinesReaderStream {
        position,
        namespace: identity,
    };
    let contract = StreamStateContract::new(reader_position_codec());
    (reader, stream, contract)
}

/// Opens `path` for a restartable [`JsonLinesReader<File>`].
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn jsonl_file_reader<I>(
    path: impl AsRef<Path>,
    format: JsonLinesFormat,
    identity: ComponentStreamIdentity,
) -> io::Result<(
    JsonLinesReader<File>,
    JsonLinesReaderStream,
    StreamStateContract,
)>
where
    I: From<Value> + 'static,
{
    let file = File::open(path)?;
    Ok(jsonl_reader::<I, File>(file, format, identity))
}

/// A restartable [`crate::ItemWriter`] over a local JSON Lines file.
///
/// # Contract
///
/// - **Input/output**: accepts `I: Into<`[`serde_json::Value`]`>`; writes
///   exactly one JSON value plus terminator per item.
/// - **State/checkpoint**: committed output progress is the file's byte
///   length as of the last committed chunk, persisted through the paired
///   [`JsonLinesWriterStream`], reconciled identically to
///   [`crate::item_components::delimited::DelimitedWriter`] (trailing
///   uncommitted bytes truncated on restart; a shorter-than-committed file
///   fails closed).
/// - **Ordering**: writes items in the order supplied.
/// - **Thread safety**: `Send + Sync`; internal `Mutex` serializes the
///   shared file handle.
/// - **Reentrancy**: not reentrant against the same path from a second
///   concurrent attempt.
/// - **Transaction/delivery**: does not enlist; file bytes are outside the
///   OxideBatch-owned business transaction. No directory-entry fsync is
///   performed (each write batch is flushed with `File::sync_data`).
/// - **Bounded resource**: one file handle; serializes each item directly to
///   the file and does not materialize the serialized write batch.
/// - **Cancellation**: honors the call-scoped stop token before writing.
/// - **Close**: nothing beyond the paired stream's
///   [`crate::ItemStream::close`] -- each line is independently complete, so
///   no closing punctuation is deferred to close time (contrast
///   [`crate::item_components::json_array::JsonArrayWriter`]).
/// - **Sensitive diagnostics**: restart state is a byte count, never record
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: not applicable; a writer never rejects an
///   already-typed item (serializing a [`serde_json::Value`] cannot fail in
///   practice, since a `Value` can never hold a non-finite number).
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_jsonl.rs`,
///   `crates/oxide-batch-test/tests/postgres_json_restart.rs`.
pub struct JsonLinesWriter {
    file: Arc<Mutex<File>>,
    terminator: JsonLinesTerminator,
    committed_bytes: Arc<Mutex<u64>>,
}

impl<I> crate::ItemWriter<I> for JsonLinesWriter
where
    I: Send + Sync + Clone + Into<Value>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let mut guard = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        for item in items {
            let value: Value = item.clone().into();
            serde_json::to_writer(&mut *guard, &value).map_err(|_| WriterError::new())?;
            guard
                .write_all(self.terminator.bytes())
                .map_err(|_| WriterError::new())?;
        }
        guard.sync_data().map_err(|_| WriterError::new())?;
        let candidate_bytes = guard.stream_position().map_err(|_| WriterError::new())?;
        drop(guard);
        let mut committed = self
            .committed_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *committed = candidate_bytes;
        Ok(WriteOutcome::Written)
    }
}

/// The [`crate::ItemStream`] half of a [`JsonLinesWriter`].
pub struct JsonLinesWriterStream {
    file: Arc<Mutex<File>>,
    committed_bytes: Arc<Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for JsonLinesWriterStream {
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
            return Err(StreamOpenError::with_category(FailureCategory::Invariant));
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
pub fn jsonl_writer(
    path: impl AsRef<Path>,
    format: JsonLinesFormat,
    identity: ComponentStreamIdentity,
) -> io::Result<(JsonLinesWriter, JsonLinesWriterStream, StreamStateContract)> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let file = Arc::new(Mutex::new(file));
    let committed_bytes = Arc::new(Mutex::new(0));
    let writer = JsonLinesWriter {
        file: Arc::clone(&file),
        terminator: format.terminator,
        committed_bytes: Arc::clone(&committed_bytes),
    };
    let stream = JsonLinesWriterStream {
        file,
        committed_bytes,
        namespace: identity,
    };
    let contract = StreamStateContract::new(writer_position_codec());
    Ok((writer, stream, contract))
}
