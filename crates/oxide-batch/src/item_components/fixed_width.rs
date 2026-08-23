//! Restartable fixed-width file components (#147, `IO-FLAT-001`).
//!
//! Field widths are byte-oriented, not Unicode character-oriented: a
//! declared width of `n` selects exactly `n` bytes of the record's raw line,
//! and each selected span is then decoded as UTF-8 independently. A field
//! boundary that lands inside a multi-byte UTF-8 character is a typed,
//! classified [`ReaderError`], not silently misinterpreted text -- see
//! `FixedWidthReader::read`.
//!
//! Restart position is a plain byte offset into the source, tracked at
//! record (line) boundaries only, so it is deterministic regardless of field
//! content. Restart state is carried by the paired
//! [`FixedWidthReaderStream`]/[`FixedWidthWriterStream`] through the
//! existing M6 [`crate::ItemStream`] contract, mirroring
//! [`crate::item_components::delimited`]'s reader/writer restart design
//! exactly (parser-position vs. byte-offset is the only structural
//! difference between the two families).

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use crate::{
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    FailureCategory, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StateSensitivity,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

/// The largest single record this module accepts by default: 1 MiB.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;

/// The record terminator [`FixedWidthWriter`] emits; a reader accepts either
/// on input regardless of this setting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FixedWidthTerminator {
    /// Bare `\n`.
    Lf,
    /// `\r\n`.
    CrLf,
}

impl FixedWidthTerminator {
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::CrLf => b"\r\n",
        }
    }
}

/// One byte-width field in a [`FixedWidthLayout`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixedWidthField {
    name: Option<String>,
    width: usize,
}

impl FixedWidthField {
    /// Declares an unnamed field of `width` bytes.
    #[must_use]
    pub const fn new(width: usize) -> Self {
        Self { name: None, width }
    }

    /// Declares a named field of `width` bytes, addressable through
    /// [`FixedWidthRecord::field`].
    #[must_use]
    pub fn named(name: impl Into<String>, width: usize) -> Self {
        Self {
            name: Some(name.into()),
            width,
        }
    }

    /// Returns the field's declared byte width.
    #[must_use]
    pub const fn width(&self) -> usize {
        self.width
    }

    /// Borrows the field's declared name, if any.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// A bounded, OxideBatch-owned fixed-width record layout.
///
/// Widths are explicitly byte-oriented (see the module documentation): the
/// layout never treats a byte offset as a Unicode character offset.
#[derive(Clone, Debug)]
pub struct FixedWidthLayout {
    fields: Vec<FixedWidthField>,
    record_width: usize,
    terminator: FixedWidthTerminator,
    max_record_bytes: usize,
}

impl FixedWidthLayout {
    /// Builds a layout from ordered field widths.
    ///
    /// # Panics
    ///
    /// Panics if `fields` is empty: a zero-field layout could never
    /// distinguish a valid record from a malformed one.
    #[must_use]
    pub fn new(fields: Vec<FixedWidthField>) -> Self {
        assert!(!fields.is_empty(), "a fixed-width layout needs >= 1 field");
        let record_width = fields.iter().map(FixedWidthField::width).sum();
        Self {
            fields,
            record_width,
            terminator: FixedWidthTerminator::Lf,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
        }
    }

    /// Sets the record terminator [`FixedWidthWriter`] emits.
    #[must_use]
    pub const fn with_terminator(mut self, terminator: FixedWidthTerminator) -> Self {
        self.terminator = terminator;
        self
    }

    /// Sets the largest single record's raw byte span the reader accepts
    /// before failing closed.
    #[must_use]
    pub const fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    /// Borrows the ordered field declarations.
    #[must_use]
    pub fn fields(&self) -> &[FixedWidthField] {
        &self.fields
    }

    /// Returns the exact byte width one well-formed record occupies,
    /// excluding its terminator.
    #[must_use]
    pub const fn record_width(&self) -> usize {
        self.record_width
    }
}

/// One decoded fixed-width record: an OxideBatch-owned, Rust-native field
/// list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixedWidthRecord {
    fields: Vec<String>,
    names: Option<Arc<[Option<String>]>>,
}

impl FixedWidthRecord {
    /// Builds a record from owned fields, with no name lookup.
    #[must_use]
    pub const fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            names: None,
        }
    }

    /// Borrows the record's fields in layout order.
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

    /// Borrows the field named by the layout, if the layout named it.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&str> {
        let names = self.names.as_ref()?;
        let index = names
            .iter()
            .position(|candidate| candidate.as_deref() == Some(name))?;
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

impl From<Vec<String>> for FixedWidthRecord {
    fn from(fields: Vec<String>) -> Self {
        Self::new(fields)
    }
}

const READER_SCHEMA: &str = "oxide-batch.fixed-width-reader-position";
const READER_CODEC: &str = "oxide-batch.fixed-width-reader-position-codec";

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
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        value
            .get("byte")
            .and_then(serde_json::Value::as_u64)
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

const WRITER_SCHEMA: &str = "oxide-batch.fixed-width-writer-position";
const WRITER_CODEC: &str = "oxide-batch.fixed-width-writer-position-codec";

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

/// One bounded read of a line from a buffered source, at most
/// `max_bytes` retained regardless of how long the underlying line is.
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
/// transiently between this call and the caller's own bound check. Sets
/// `*too_long` and discards `buf`'s content (never its already-bounded
/// capacity growth) the moment `segment` would need more room than that.
///
/// This is the fix that makes "copies no more than the configured bound
/// before entering discard mode" literally true: the previous version of
/// this routine copied a whole `BufRead` fill chunk (which can be many
/// kilobytes, entirely unrelated to `max_bytes`) into `buf` first and only
/// checked the length afterwards, so a single oversized line could briefly
/// occupy far more memory than the configured bound before being rejected.
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
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
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
        if let Some(newline) = available.iter().position(|&byte| byte == b'\n') {
            copy_bounded(&mut buf, &available[..newline], max_bytes, &mut too_long);
            let take = newline + 1;
            reader.consume(take);
            consumed_total += take as u64;
            return Ok(if too_long {
                BoundedLine::TooLong {
                    consumed: consumed_total,
                }
            } else {
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
                BoundedLine::Line {
                    bytes: buf,
                    consumed: consumed_total,
                }
            });
        }
        copy_bounded(&mut buf, available, max_bytes, &mut too_long);
        let chunk_len = available.len();
        reader.consume(chunk_len);
        consumed_total += chunk_len as u64;
    }
}

/// A restartable [`crate::ItemReader`] over any `Read + Seek` fixed-width
/// source.
///
/// # Contract
///
/// - **Input/output**: produces [`FixedWidthRecord`]; format is the
///   configured [`FixedWidthLayout`], byte-oriented (see module
///   documentation).
/// - **State/checkpoint**: restart position is a plain byte offset at the
///   last consumed record boundary, persisted through the paired
///   [`FixedWidthReaderStream`].
/// - **Ordering**: preserves file order.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: a line is read through a bounded loop capped at
///   [`FixedWidthLayout::with_max_record_bytes`]: each chunk read from the
///   source is copied into the line buffer only up to the remaining budget
///   under that bound, so the buffer's length never exceeds
///   `max_record_bytes` even transiently, before the line is known to be
///   too long and copying stops entirely for its remainder (see
///   `FixedWidthReader::read`). See
///   `crates/oxide-batch/tests/item_components_flat_file_allocation.rs` for
///   the allocator-level evidence.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls.
/// - **Close**: closed through the paired stream's
///   [`crate::ItemStream::close`].
/// - **Sensitive diagnostics**: restart state is a byte offset, never record
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: a line whose raw byte length (excluding
///   terminator) is not exactly [`FixedWidthLayout::record_width`], a field
///   span that is not valid UTF-8 (including a multi-byte character split by
///   a field boundary), or a line exceeding the configured bound is a
///   [`ReaderError`] in [`crate::FailureCategory::UserComponent`] with
///   [`ReaderError::has_checkpoint_advanced`] `true` -- the offending bytes
///   through the next terminator (or end of input) are always consumed
///   before the error is observed, so forward progress is real. As with
///   [`crate::item_components::delimited`], a configured retry re-invokes the
///   reader against the *next* line, not the same one; configure skip or
///   fail for this failure class. This reader never pads or truncates a
///   short/long line -- it is always a classified failure.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_fixed_width.rs`,
///   `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs`.
pub struct FixedWidthReader<Src> {
    source: BufReader<Src>,
    layout: FixedWidthLayout,
    names: Option<Arc<[Option<String>]>>,
    position: Arc<Mutex<u64>>,
    seeked: bool,
}

impl<Src: Read + Seek> FixedWidthReader<Src> {
    fn seek_if_needed(&mut self) -> Result<(), ReaderError> {
        if self.seeked {
            return Ok(());
        }
        self.seeked = true;
        let target = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        if target == 0 {
            return Ok(());
        }
        self.source
            .seek(SeekFrom::Start(target))
            .map_err(|_| ReaderError::new())?;
        Ok(())
    }
}

impl<I, Src> crate::ItemReader<I> for FixedWidthReader<Src>
where
    I: From<FixedWidthRecord> + 'static,
    Src: Read + Seek + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.seek_if_needed()?;
        let before = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        let outcome = read_bounded_line(&mut self.source, self.layout.max_record_bytes)
            .map_err(|_| ReaderError::with_category(FailureCategory::TransientInfrastructure))?;
        match outcome {
            BoundedLine::Eof => Ok(ReadOutcome::EndOfInput),
            BoundedLine::TooLong { consumed } => {
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = before + consumed;
                Err(ReaderError::new().with_checkpoint_advanced(true))
            }
            BoundedLine::Line { bytes, consumed } => {
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = before + consumed;
                if bytes.len() != self.layout.record_width() {
                    return Err(ReaderError::new().with_checkpoint_advanced(true));
                }
                let mut fields = Vec::with_capacity(self.layout.fields.len());
                let mut offset = 0usize;
                for field in &self.layout.fields {
                    let span = &bytes[offset..offset + field.width()];
                    let text = std::str::from_utf8(span)
                        .map_err(|_| ReaderError::new().with_checkpoint_advanced(true))?;
                    fields.push(text.to_owned());
                    offset += field.width();
                }
                let record = FixedWidthRecord {
                    fields,
                    names: self.names.clone(),
                };
                Ok(ReadOutcome::Item(record.into()))
            }
        }
    }
}

/// The [`crate::ItemStream`] half of a [`FixedWidthReader`].
pub struct FixedWidthReaderStream {
    position: Arc<Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for FixedWidthReaderStream {
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
pub fn fixed_width_reader<I, Src>(
    source: Src,
    layout: FixedWidthLayout,
    identity: ComponentStreamIdentity,
) -> (
    FixedWidthReader<Src>,
    FixedWidthReaderStream,
    StreamStateContract,
)
where
    I: From<FixedWidthRecord> + 'static,
    Src: Read + Seek + Send,
{
    let position = Arc::new(Mutex::new(0));
    let names: Option<Arc<[Option<String>]>> =
        if layout.fields.iter().any(|field| field.name.is_some()) {
            Some(
                layout
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            )
        } else {
            None
        };
    let reader = FixedWidthReader {
        source: BufReader::new(source),
        layout,
        names,
        position: Arc::clone(&position),
        seeked: false,
    };
    let stream = FixedWidthReaderStream {
        position,
        namespace: identity,
    };
    let contract = StreamStateContract::new(reader_position_codec());
    (reader, stream, contract)
}

/// Opens `path` for a restartable [`FixedWidthReader<File>`].
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn fixed_width_file_reader<I>(
    path: impl AsRef<Path>,
    layout: FixedWidthLayout,
    identity: ComponentStreamIdentity,
) -> io::Result<(
    FixedWidthReader<File>,
    FixedWidthReaderStream,
    StreamStateContract,
)>
where
    I: From<FixedWidthRecord> + 'static,
{
    let file = File::open(path)?;
    Ok(fixed_width_reader::<I, File>(file, layout, identity))
}

/// A restartable [`crate::ItemWriter`] over a local fixed-width file.
///
/// # Contract
///
/// - **Input/output**: accepts `I: Into<`[`FixedWidthRecord`]`>`; writes the
///   configured [`FixedWidthLayout`].
/// - **State/checkpoint**: committed output progress is the file's byte
///   length as of the last committed chunk, persisted through the paired
///   [`FixedWidthWriterStream`], reconciled identically to
///   [`crate::item_components::delimited::DelimitedWriter`] (trailing
///   uncommitted bytes truncated on restart; a shorter-than-committed file
///   fails closed).
/// - **Ordering**: writes items in the order supplied.
/// - **Thread safety**: `Send + Sync`; a single internal `Mutex` guards the
///   file handle *and* the committed byte count together, so the physical
///   write, the `sync_data`, and the committed-count update form one
///   coherent, serialized transition -- a concurrent call can never publish
///   a stale committed length after a later call's write has already
///   landed.
/// - **Reentrancy**: not reentrant against the same path from a second
///   concurrent attempt.
/// - **Transaction/delivery**: does not enlist; file bytes are outside the
///   OxideBatch-owned business transaction. No directory-entry fsync is
///   performed (see [`crate::item_components::delimited::DelimitedWriter`]'s
///   equivalent note).
/// - **Bounded resource**: one file handle; buffers at most one write batch.
/// - **Cancellation**: honors the call-scoped stop token before writing.
/// - **Close**: nothing beyond the paired stream's
///   [`crate::ItemStream::close`].
/// - **Sensitive diagnostics**: restart state is a byte count, never record
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: a supplied field whose byte length does not
///   exactly match its declared [`FixedWidthField::width`] is a
///   [`WriterError`] -- this writer never silently pads or truncates a
///   field to fit.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_fixed_width.rs`,
///   `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs`.
pub struct FixedWidthWriter {
    state: Arc<Mutex<FixedWidthWriterState>>,
    layout: FixedWidthLayout,
}

/// The file and committed byte count a [`FixedWidthWriter`]/
/// [`FixedWidthWriterStream`] pair shares under one lock, so the physical
/// write and the committed-count update are never independently observable.
struct FixedWidthWriterState {
    file: File,
    committed_bytes: u64,
}

impl<I> crate::ItemWriter<I> for FixedWidthWriter
where
    I: Send + Sync + Clone + Into<FixedWidthRecord>,
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
        for item in items {
            let record: FixedWidthRecord = item.clone().into();
            if record.fields.len() != self.layout.fields.len() {
                return Err(WriterError::new());
            }
            for (field, declared) in record.fields.iter().zip(&self.layout.fields) {
                if field.len() != declared.width() {
                    return Err(WriterError::new());
                }
                buffer.extend_from_slice(field.as_bytes());
            }
            buffer.extend_from_slice(self.layout.terminator.bytes());
        }
        // The physical write and the committed-count update both happen
        // under this one lock: no other call can publish a committed byte
        // count until this entire transition -- write, sync, and commit --
        // completes.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .file
            .write_all(&buffer)
            .map_err(|_| WriterError::new())?;
        state.file.sync_data().map_err(|_| WriterError::new())?;
        let candidate_bytes = state
            .file
            .stream_position()
            .map_err(|_| WriterError::new())?;
        state.committed_bytes = candidate_bytes;
        Ok(WriteOutcome::Written)
    }
}

/// The [`crate::ItemStream`] half of a [`FixedWidthWriter`].
pub struct FixedWidthWriterStream {
    state: Arc<Mutex<FixedWidthWriterState>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for FixedWidthWriterStream {
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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let actual_len = state
            .file
            .metadata()
            .map_err(|_| StreamOpenError::new())?
            .len();
        if actual_len < target {
            return Err(StreamOpenError::with_category(FailureCategory::Invariant));
        }
        if actual_len != target {
            state
                .file
                .set_len(target)
                .map_err(|_| StreamOpenError::new())?;
        }
        state
            .file
            .seek(SeekFrom::Start(target))
            .map_err(|_| StreamOpenError::new())?;
        state.committed_bytes = target;
        Ok(outcome)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = writer_position_codec();
        let current = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .committed_bytes;
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
pub fn fixed_width_writer(
    path: impl AsRef<Path>,
    layout: FixedWidthLayout,
    identity: ComponentStreamIdentity,
) -> io::Result<(
    FixedWidthWriter,
    FixedWidthWriterStream,
    StreamStateContract,
)> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let state = Arc::new(Mutex::new(FixedWidthWriterState {
        file,
        committed_bytes: 0,
    }));
    let writer = FixedWidthWriter {
        state: Arc::clone(&state),
        layout,
    };
    let stream = FixedWidthWriterStream {
        state,
        namespace: identity,
    };
    let contract = StreamStateContract::new(writer_position_codec());
    Ok((writer, stream, contract))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::thread;

    use super::*;
    use crate::{ComponentStatePayload, ItemStream, ItemWriter, StopSource};

    fn identity() -> ComponentStreamIdentity {
        ComponentStreamIdentity::new("oxide-batch.fixed-width-writer-unit-test")
            .expect("static identity is valid")
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time moves forward")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "oxide-batch-167-fixed-width-unit-{name}-{nonce}.txt"
        ))
    }

    fn committed_byte(envelope: &ComponentStateEnvelope) -> u64 {
        let ComponentStatePayload::Inline(payload) = envelope.payload().unwrap() else {
            panic!("writer checkpoints must use an inline payload");
        };
        serde_json::from_slice::<serde_json::Value>(&payload)
            .unwrap()
            .get("committed_bytes")
            .and_then(serde_json::Value::as_u64)
            .expect("writer checkpoint committed_bytes")
    }

    /// A structural smoke test for the shared single-state mutex, mirroring
    /// `delimited::tests::concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit`:
    /// proves the physical write and the committed-count update happen as
    /// one coherent, serialized transition, not two independently lockable
    /// steps. Both writes here are equal-sized single records, so every
    /// committed position observed lands on a record boundary by
    /// construction -- this alone does not reproduce the historical
    /// additive-publish defect, which required *unequal*-size writes to
    /// expose an intermediate checkpoint that is a record boundary without
    /// being a complete write-call prefix. That reproduction, and its
    /// restart proof, live in
    /// `crates/oxide-batch/tests/writer_checkpoint_coherence.rs`; see
    /// `docs/project/m6-167-writer-checkpoint-coherence.md` for the full
    /// account.
    #[test]
    fn concurrent_write_is_fully_serialized_by_one_lock_never_a_stale_commit() {
        let path = temp_path("serialized");
        let layout = FixedWidthLayout::new(vec![FixedWidthField::new(1)]);
        let (writer, stream, _contract) = fixed_width_writer(&path, layout, identity()).unwrap();
        let writer = Arc::new(writer);
        let (_open_stop_source, open_stop_token) = StopSource::new();
        futures_executor::block_on(stream.open(StreamOpenContext::new(None, &open_stop_token)))
            .unwrap();

        let mut guard = writer.state.lock().unwrap_or_else(PoisonError::into_inner);

        let writer_for_thread = Arc::clone(&writer);
        let handle = thread::spawn(move || {
            let (_stop_source, stop_token) = StopSource::new();
            futures_executor::block_on(writer_for_thread.write(
                &[FixedWidthRecord::new(vec!["2".to_owned()])],
                WriteContext::non_transactional(&stop_token),
            ))
            .unwrap();
        });

        guard.file.write_all(b"1\n").unwrap();
        guard.file.sync_data().unwrap();
        guard.committed_bytes = guard.file.stream_position().unwrap();
        drop(guard);

        handle.join().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            bytes,
            b"1\n2\n".to_vec(),
            "both writes must land on disk exactly once each, in the order the lock serialized \
             them"
        );

        let envelope = futures_executor::block_on(
            stream.update(crate::StreamUpdateContext::new(&open_stop_token)),
        )
        .unwrap();
        let committed = committed_byte(&envelope);
        assert_eq!(
            committed,
            bytes.len() as u64,
            "the published checkpoint must equal the real file length, never a stale, earlier \
             write's byte count"
        );
        assert_eq!(
            committed % 2,
            0,
            "every committed position must land on a record_width(1) + terminator(1) boundary"
        );

        let _ = std::fs::remove_file(&path);
    }
}
