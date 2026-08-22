//! Restartable delimited/CSV file components (#147, `IO-FLAT-001`).
//!
//! [`DelimitedReader`] drives [`csv_core::Reader`] directly (the incremental,
//! no-I/O parsing engine the `csv` crate itself is built on) rather than
//! `csv::Reader`'s own convenience API, specifically so this reader can
//! enforce [`DelimitedDialect::with_max_record_bytes`] *during* parsing,
//! against the record's *raw* byte span -- not merely its decoded
//! field-content length. Both the decoded-content buffer and the per-field
//! end-offset buffer this reader accumulates a record into are never grown
//! past what that bound allows, so a pathological oversized record (whether
//! it's one enormous field, or millions of empty ones -- a case decoded
//! content alone could never bound) is detected and rejected without ever
//! copying more than the configured bound into memory for it, not merely
//! rejected after being fully materialized. [`DelimitedWriter`] uses the
//! higher-level [`csv::Writer`], which has no equivalent unbounded-growth
//! concern (it serializes already-bounded, caller-supplied items).
//!
//! Quoted delimiters, doubled/escaped quotes, and multiline quoted fields
//! remain the parser's job, not this module's. Restart position is the
//! parser's own record-boundary byte/line/record triple, never an inferred
//! line count, so a restart can never land inside a multiline quoted record.
//! Neither type, nor any dialect/state type here, exposes a `csv`/`csv_core`
//! crate type in its public signature: dialect configuration and record
//! content are OxideBatch-owned ([`DelimitedDialect`], [`DelimitedRecord`]).
//!
//! Restart state is carried by the paired [`DelimitedReaderStream`]/
//! [`DelimitedWriterStream`] through the existing M6 [`crate::ItemStream`]
//! contract: [`delimited_reader`] and [`delimited_writer`] return a
//! `(component, stream, contract)` triple sharing state through an `Arc`,
//! exactly the pattern `oxide-batch-test`'s `restart::range_reader` uses for
//! a durable, restartable reader -- register the stream under the same
//! [`crate::ComponentStreamIdentity`] the component was built with.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use csv::Terminator as CsvTerminator;
use csv_core::{ReadRecordResult, Reader as CoreReader};

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
/// A record whose *raw* byte span (the bytes consumed from the source for
/// it, not merely its decoded field-content length) exceeds the configured
/// bound is a typed, classified [`ReaderError`] (see `DelimitedReader::read`)
/// rather than an unbounded allocation: the incremental parser is checked
/// against this bound as each raw byte is consumed, so this component never
/// copies more than this many bytes into memory for a single record --
/// whether that memory would hold decoded field content or per-field
/// end-offset bookkeeping -- however large the record actually is on disk,
/// and regardless of whether its size comes from one huge field or from an
/// unbounded number of small/empty ones.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;

/// The fixed-size scratch buffers used once a record is known to exceed
/// [`DelimitedDialect::with_max_record_bytes`], so that continuing to drain
/// its remaining bytes from the input (to preserve forward checkpoint
/// progress) never itself allocates in proportion to the oversized record.
/// Both are stack-allocated, fixed at compile time, and never resized:
/// draining a record with millions of fields still makes progress a few
/// thousand field-ends at a time rather than one at a time, without ever
/// costing more than these two constant-size arrays.
const DISCARD_OUTPUT_BYTES: usize = 4096;
const DISCARD_ENDS_LEN: usize = 1024;

/// The first output-buffer growth step for a fresh record, before doubling.
const INITIAL_GROWTH_BYTES: usize = 256;

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
    /// available through [`DelimitedRecord::field`]. When set, the header
    /// row's own field count -- not the first *data* row's -- is the
    /// baseline [`Self::with_flexible`]`(false)` checks later rows against.
    #[must_use]
    pub const fn with_headers(mut self, enabled: bool) -> Self {
        self.has_headers = enabled;
        self
    }

    /// Sets whether records with a field count different from the baseline
    /// (the header row's field count if [`Self::with_headers`] is set,
    /// otherwise the first data row's) are accepted rather than classified
    /// as malformed.
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

    /// Sets the largest single record's *raw* byte span (bytes consumed
    /// from the source, not decoded field-content length) the reader
    /// accepts.
    #[must_use]
    pub const fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    /// Builds the incremental, no-I/O `csv_core` parser for this dialect.
    ///
    /// `has_headers`/`flexible` are not `csv_core` concepts -- they're
    /// applied by [`DelimitedReader`] itself, above this low-level parser.
    fn core_reader(self) -> CoreReader {
        let mut builder = csv_core::ReaderBuilder::new();
        builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .escape(self.escape)
            .double_quote(self.double_quote);
        builder.build()
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
    /// records read yet.
    const START: Self = Self {
        byte: 0,
        line: 1,
        record: 0,
    };
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

/// One low-level, mechanical outcome of parsing a single record from
/// wherever [`csv_core::Reader`] currently is. Carries no knowledge of
/// headers, flexibility, or UTF-8 -- those are [`DelimitedReader::read`]'s
/// job, layered on top.
enum RawRecord {
    /// A complete, in-bound record. Its decoded field bytes are in
    /// [`DelimitedReader::output`], sliced by [`DelimitedReader::ends`].
    Fields {
        /// Bytes consumed from the source for this record (its byte span).
        consumed: u64,
    },
    /// A complete record whose raw byte span exceeded
    /// [`DelimitedDialect::with_max_record_bytes`]. No field content or
    /// per-field end-offset bookkeeping was retained for it.
    Oversized {
        /// Bytes consumed from the source for this record (its byte span).
        consumed: u64,
    },
    /// The source is exhausted.
    Eof,
}

/// A restartable [`crate::ItemReader`] over any `Read + Seek` delimited/CSV
/// source, built directly on [`csv_core::Reader`] (see the module
/// documentation for why).
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
/// - **Bounded resource**: bounded by [`DelimitedDialect::with_max_record_bytes`],
///   checked against the record's *raw* byte span (the bytes consumed from
///   the source for it), not merely its decoded field-content length -- a
///   record built almost entirely of empty fields decodes to almost no
///   content bytes, so a decoded-only bound could never catch one with an
///   unbounded field count. Both the decoded-content buffer and the
///   per-field end-offset buffer this reader accumulates a record into are
///   grown incrementally, one parser callback at a time, and neither is ever
///   grown past what the configured raw-byte bound allows: once the raw
///   span consumed for the current record would exceed it, this reader
///   stops copying that record's bytes into either buffer at all (switching
///   to small, fixed discard buffers just to drain the remaining input for
///   forward checkpoint proof) and reports it as a classified,
///   forward-proven [`ReaderError`] instead. The bound is therefore enforced
///   *during* parsing, not applied as an after-the-fact check against an
///   already-fully-materialized record -- see
///   `crates/oxide-batch/tests/item_components_flat_file_allocation.rs` for
///   the allocator-level evidence, including a record of millions of empty
///   fields that exercises the end-offset buffer specifically.
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
///   `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs`,
///   `crates/oxide-batch/tests/item_components_flat_file_allocation.rs`.
pub struct DelimitedReader<Src> {
    source: BufReader<Src>,
    core: CoreReader,
    /// Reused across records; its capacity naturally settles at the largest
    /// in-bound record seen so far, and is never grown past
    /// `max_record_bytes`.
    output: Vec<u8>,
    /// Cumulative end offsets of each field within `output`, per
    /// [`csv_core::Reader::read_record`]'s contract.
    ends: Vec<usize>,
    max_record_bytes: usize,
    has_headers: bool,
    flexible: bool,
    first_field_count: Option<usize>,
    headers: Option<Arc<[String]>>,
    position: Arc<Mutex<StoredPosition>>,
    seeked: bool,
}

impl<Src: Read> DelimitedReader<Src> {
    /// Parses exactly one record from wherever the parser currently is,
    /// growing [`Self::output`] and [`Self::ends`] one step at a time and
    /// never past `max_record_bytes` -- bounding the record's *raw* byte
    /// span, not merely its decoded field-content length.
    ///
    /// That distinction matters: a record built almost entirely of empty
    /// fields (e.g. millions of consecutive delimiters) decodes to almost no
    /// field-content bytes at all, so a bound checked only against decoded
    /// output would never trip, while [`Self::ends`] -- one `usize` per
    /// field boundary -- still grows without limit. Bounding on `consumed`
    /// (the record's raw input byte count, checked every loop iteration,
    /// before either buffer is allowed to grow further) catches this: CSV
    /// unescaping only ever shrinks or preserves byte count field-by-field
    /// (a doubled quote `""` decodes to one `"`), so the raw span is always
    /// an upper bound on decoded content, and every field boundary consumes
    /// at least one raw delimiter byte, so bounding the raw span also bounds
    /// the number of fields (and therefore `ends`' size) to at most
    /// `max_record_bytes` entries -- O(the configured bound), never O(the
    /// actual oversized record's real size).
    ///
    /// Once a record is known to exceed the bound, this switches to writing
    /// into small, fixed, stack-allocated scratch buffers for the remainder
    /// of that record: `csv_core::Reader` tracks each field's logical
    /// position independently of whatever buffer it's told to write into
    /// (its own documentation states end positions are "constructed as if
    /// there was a single contiguous buffer in memory containing the entire
    /// row"), so reusing a tiny fixed buffer across calls is sufficient to
    /// keep draining input correctly without retaining the oversized
    /// content -- this is what makes the bound apply *during* parsing.
    fn read_raw_record(&mut self) -> io::Result<RawRecord> {
        self.output.clear();
        self.ends.clear();
        let mut output_len = 0usize;
        let mut ends_len = 0usize;
        let mut consumed: u64 = 0;
        let mut oversized = false;
        let max_record_bytes = u64::try_from(self.max_record_bytes).unwrap_or(u64::MAX);
        let mut discard_output = [0u8; DISCARD_OUTPUT_BYTES];
        let mut discard_ends = [0usize; DISCARD_ENDS_LEN];

        loop {
            let input = self.source.fill_buf()?;
            let (result, nin, nout, nend) = if oversized {
                self.core
                    .read_record(input, &mut discard_output, &mut discard_ends)
            } else {
                self.core.read_record(
                    input,
                    &mut self.output[output_len..],
                    &mut self.ends[ends_len..],
                )
            };
            self.source.consume(nin);
            consumed += nin as u64;
            if !oversized {
                output_len += nout;
                ends_len += nend;
                // Checked immediately, before this iteration's `match` is
                // allowed to grow either buffer any further: this is the
                // one gate that bounds both `output` and `ends` to the raw
                // span, regardless of which of the two a given record's
                // bytes happen to land in.
                if consumed > max_record_bytes {
                    oversized = true;
                }
            }
            match result {
                ReadRecordResult::InputEmpty => {}
                ReadRecordResult::OutputFull => {
                    if !oversized {
                        let grown = if self.output.is_empty() {
                            INITIAL_GROWTH_BYTES
                        } else {
                            self.output.len().saturating_mul(2)
                        }
                        .min(self.max_record_bytes);
                        if grown <= self.output.len() {
                            oversized = true;
                        } else {
                            self.output.resize(grown, 0);
                        }
                    }
                }
                ReadRecordResult::OutputEndsFull => {
                    if !oversized {
                        let grown = self.ends.len().max(16) * 2;
                        self.ends.resize(grown, 0);
                    }
                }
                ReadRecordResult::Record => {
                    return Ok(if oversized {
                        RawRecord::Oversized { consumed }
                    } else {
                        self.output.truncate(output_len);
                        self.ends.truncate(ends_len);
                        RawRecord::Fields { consumed }
                    });
                }
                ReadRecordResult::End => return Ok(RawRecord::Eof),
            }
        }
    }

    /// Decodes `self.output`/`self.ends` (populated by a just-completed,
    /// in-bound [`Self::read_raw_record`] call) into owned UTF-8 fields.
    fn decode_fields(&self) -> Result<Vec<String>, ReaderError> {
        let mut fields = Vec::with_capacity(self.ends.len());
        let mut start = 0usize;
        for &end in &self.ends {
            let slice = self
                .output
                .get(start..end)
                .ok_or_else(|| ReaderError::new().with_checkpoint_advanced(true))?;
            let text = std::str::from_utf8(slice)
                .map_err(|_| ReaderError::new().with_checkpoint_advanced(true))?;
            fields.push(text.to_owned());
            start = end;
        }
        Ok(fields)
    }
}

impl<Src: Read + Seek> DelimitedReader<Src> {
    /// On the attempt's first read: consumes and caches the header row (if
    /// [`DelimitedDialect::with_headers`] is set) from wherever the source
    /// currently is, then seeks to a restored non-zero position, if any.
    ///
    /// The header row is always read *before* seeking (from byte 0, where a
    /// freshly opened source starts), exactly so header names remain
    /// available after a restart that resumes mid-file.
    fn ensure_headers_and_seek(&mut self) -> Result<(), ReaderError> {
        if self.seeked {
            return Ok(());
        }
        self.seeked = true;
        // Captured *before* the header row is read: this is the position
        // `ItemStream::open` actually restored (zero for initial execution,
        // nonzero for a restart), never the header row's own consumption.
        let target = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        if self.has_headers {
            match self.read_raw_record().map_err(|_| {
                ReaderError::with_category(crate::FailureCategory::TransientInfrastructure)
            })? {
                RawRecord::Fields { consumed } => {
                    let fields = self.decode_fields()?;
                    // The header row establishes the expected field count
                    // for `DelimitedDialect::with_flexible(false)` (the
                    // default): without this, the *first data row* would
                    // silently become its own baseline instead, and a data
                    // row ragged relative to the header -- but internally
                    // consistent with itself, e.g. a single short first row
                    // -- would never be caught.
                    if !self.flexible {
                        self.first_field_count = Some(fields.len());
                    }
                    self.headers = Some(fields.into_iter().collect());
                    if target.byte == 0 {
                        // Initial execution: there is nothing to seek past
                        // below, so the header row's own consumption must
                        // become part of the baseline position here --
                        // otherwise every later record's tracked position
                        // would understate the true file offset by exactly
                        // the header row's byte length, corrupting both the
                        // durable checkpoint and any later restart's seek
                        // target.
                        let mut position =
                            self.position.lock().unwrap_or_else(PoisonError::into_inner);
                        position.byte = consumed;
                        position.line = self.core.line();
                    }
                    // A restart (target.byte > 0) leaves `self.position`
                    // untouched here: it already holds the correct resume
                    // point, and the header re-read above exists only to
                    // populate `self.headers` -- the seek below moves past
                    // it to the real resume point.
                }
                RawRecord::Oversized { .. } => {
                    return Err(ReaderError::new().with_checkpoint_advanced(true));
                }
                RawRecord::Eof => {}
            }
        }
        if target.byte > 0 {
            self.source
                .seek(SeekFrom::Start(target.byte))
                .map_err(|_| ReaderError::new())?;
            self.core.reset();
            self.core.set_line(target.line);
        }
        Ok(())
    }
}

impl<I, Src> crate::ItemReader<I> for DelimitedReader<Src>
where
    I: From<DelimitedRecord> + 'static,
    Src: Read + Seek + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.ensure_headers_and_seek()?;
        let before = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        let outcome = self.read_raw_record().map_err(|_| {
            ReaderError::with_category(crate::FailureCategory::TransientInfrastructure)
        })?;
        match outcome {
            RawRecord::Eof => Ok(ReadOutcome::EndOfInput),
            RawRecord::Oversized { consumed } => {
                let after = StoredPosition {
                    byte: before.byte + consumed,
                    line: self.core.line(),
                    record: before.record + 1,
                };
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                Err(ReaderError::new().with_checkpoint_advanced(true))
            }
            RawRecord::Fields { consumed } => {
                let after = StoredPosition {
                    byte: before.byte + consumed,
                    line: self.core.line(),
                    record: before.record + 1,
                };
                let field_count = self.ends.len();
                if !self.flexible {
                    match self.first_field_count {
                        None => self.first_field_count = Some(field_count),
                        Some(expected) if expected != field_count => {
                            *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                            return Err(ReaderError::new().with_checkpoint_advanced(true));
                        }
                        Some(_) => {}
                    }
                }
                let fields = match self.decode_fields() {
                    Ok(fields) => fields,
                    Err(error) => {
                        *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                        return Err(error);
                    }
                };
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) = after;
                let decoded = DelimitedRecord {
                    fields,
                    headers: self.headers.clone(),
                };
                Ok(ReadOutcome::Item(decoded.into()))
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
    let reader = DelimitedReader {
        source: BufReader::new(source),
        core: dialect.core_reader(),
        output: Vec::new(),
        ends: Vec::new(),
        max_record_bytes: dialect.max_record_bytes,
        has_headers: dialect.has_headers,
        flexible: dialect.flexible,
        first_field_count: None,
        headers: None,
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
