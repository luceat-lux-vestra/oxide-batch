//! Restartable streaming top-level JSON-array file components (#148,
//! `IO-STRUCTURED-001` M6 slice).
//!
//! One top-level array element is one item. [`JsonArrayReader`] streams
//! elements without ever deserializing the whole array into `Vec<Value>`:
//! this module owns only the byte-level framing around `[`, `,`, `]`, and
//! JSON whitespace -- recognizing this small set of ASCII framing bytes never
//! requires understanding string escaping or nesting, so it is not "a second
//! JSON grammar" -- while every value's own bytes are parsed by
//! [`serde_json`] itself, via the documented, public
//! [`serde_json::Deserializer::from_slice`] plus
//! [`serde_json::StreamDeserializer::byte_offset`] idiom for resuming a
//! partial parse: grow an owned, bounded buffer and retry parsing it from the
//! start until `serde_json` reports a complete value with a candidate byte
//! length. That candidate is accepted only after the following framing is
//! proven to be JSON whitespace plus `,` or `]`; a parser result alone is not
//! enough because a buffer can end in the middle of a number token. This is
//! what makes the reader safe against delimiters appearing inside escaped
//! strings: framing bytes are only ever inspected after `serde_json` has
//! identified the value bytes.
//!
//! # Malformed-input recovery
//!
//! Every element boundary this reader reports is *proven* by a complete,
//! successful `serde_json` parse of that element's own bytes. This is also
//! why this reader has exactly one failure mode, always fail-closed, never a
//! safe mid-array skip: the moment a value fails to parse (a genuine syntax
//! error) or exceeds the configured bound (never fully parsed, so its true
//! length is never learned), this reader has no way to prove where the next
//! element begins without falling back to exactly the kind of heuristic
//! comma/bracket scanning the design this module implements is built to
//! avoid -- scanning for the next unescaped `,`/`]` byte requires the same
//! string-escaping awareness this module deliberately keeps inside
//! `serde_json` alone. A missing separator between two otherwise-valid
//! elements is the same situation: the byte where a `,` or `]` was expected
//! is a proven boundary for the *previous* element, but what follows it is
//! unrecognized syntax, not a recoverable single malformed item. Every one of
//! these conditions is therefore reported with
//! [`crate::ReaderError::has_checkpoint_advanced`] `false`: the M3
//! fault-tolerance runtime requires that proof before a skip policy is
//! honored (see `crates/oxide-batch/src/chunk.rs`'s
//! `ReaderError::with_checkpoint_advanced` documentation), so a skip policy
//! configured against this failure class cannot silently resynchronize past
//! it -- it fails the step, exactly like an unconfigured skip would.
//!
//! Restart is at a parser-proven complete-element boundary: the persisted
//! checkpoint is the exact byte offset immediately after the last
//! successfully parsed element's own bytes (before any following separator),
//! so a fresh reader restored from it resumes by looking for `,`-then-value
//! or `]`, never by re-reading the array from byte zero or counting emitted
//! items.
//!
//! The public item representation is [`serde_json::Value`] directly, exactly
//! as [`crate::item_components::jsonl`] uses it -- see that module's
//! documentation for why no bespoke JSON AST is introduced.

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
    StreamOpenOutcome, StreamRuntimeOutcome, StreamStateContract, StreamUpdateContext,
    StreamUpdateError, VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

/// The largest single element this module accepts by default: 1 MiB.
pub const DEFAULT_MAX_VALUE_BYTES: usize = 1024 * 1024;

/// The first output-buffer growth step for a fresh element, before doubling.
const INITIAL_GROWTH_BYTES: usize = 256;

/// A bounded, OxideBatch-owned JSON-array format configuration.
#[derive(Clone, Copy, Debug)]
pub struct JsonArrayFormat {
    max_value_bytes: usize,
}

impl JsonArrayFormat {
    /// The default format: [`DEFAULT_MAX_VALUE_BYTES`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
        }
    }

    /// Sets the largest single element's raw byte span the reader accepts
    /// before failing closed.
    #[must_use]
    pub const fn with_max_value_bytes(mut self, max_value_bytes: usize) -> Self {
        self.max_value_bytes = max_value_bytes;
        self
    }
}

impl Default for JsonArrayFormat {
    fn default() -> Self {
        Self::new()
    }
}

const READER_SCHEMA: &str = "oxide-batch.json-array-reader-position";
const READER_CODEC: &str = "oxide-batch.json-array-reader-position-codec";

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

/// A restartable writer's committed byte length and element count: both are
/// needed to resume comma-state correctly (see [`JsonArrayWriter`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WriterPosition {
    committed_bytes: u64,
    committed_items: u64,
}

impl WriterPosition {
    /// The position of a freshly created output: just the opening bracket.
    const START: Self = Self {
        committed_bytes: 1,
        committed_items: 0,
    };
}

const WRITER_SCHEMA: &str = "oxide-batch.json-array-writer-position";
const WRITER_CODEC: &str = "oxide-batch.json-array-writer-position-codec";

#[derive(Clone, Copy)]
struct WriterPositionSchema;

impl VersionedStateCodec<WriterPosition> for WriterPositionSchema {
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

    fn encode(&self, value: &WriterPosition) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({
            "committed_bytes": value.committed_bytes,
            "committed_items": value.committed_items,
        }))
        .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<WriterPosition, StateCodecError> {
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let committed_bytes = value
            .get("committed_bytes")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let committed_items = value
            .get("committed_items")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(WriterPosition {
            committed_bytes,
            committed_items,
        })
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

/// Skips ASCII JSON whitespace (space, tab, LF, CR) on `reader` without
/// consuming the first non-whitespace byte, returning it (or `None` at true
/// EOF) together with how many whitespace bytes were consumed.
fn skip_whitespace<R: BufRead>(reader: &mut R) -> io::Result<(u64, Option<u8>)> {
    let mut skipped: u64 = 0;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((skipped, None));
        }
        let stop = available
            .iter()
            .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'));
        if let Some(index) = stop {
            let byte = available[index];
            reader.consume(index);
            skipped += index as u64;
            return Ok((skipped, Some(byte)));
        }
        let length = available.len();
        reader.consume(length);
        skipped += length as u64;
    }
}

/// Reads from `reader` into `buffer` until `buffer.len() == target` or the
/// source is exhausted, returning whether true EOF was hit first. Never
/// copies more into `buffer` than the remaining budget under `target` in any
/// single step, so `buffer.len()` never exceeds `target`.
fn grow_to<R: BufRead>(reader: &mut R, buffer: &mut Vec<u8>, target: usize) -> io::Result<bool> {
    while buffer.len() < target {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(true);
        }
        let budget = target - buffer.len();
        let take = available.len().min(budget);
        buffer.extend_from_slice(&available[..take]);
        reader.consume(take);
    }
    Ok(false)
}

/// The outcome of attempting to parse one bounded element.
enum ParsedElement {
    /// A complete, in-bound element. `consumed` is its exact raw byte span.
    Value { value: Value, consumed: u64 },
    /// The element's raw byte span exceeded the configured bound before a
    /// complete parse was achieved, or its syntax is invalid, or input ended
    /// before a complete value was found. All three are reported identically
    /// (see the module documentation): this reader cannot distinguish "the
    /// value is enormous" from "the value is malformed" without exceeding
    /// the configured bound to find out, and refuses to do that.
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueFraming {
    /// The parser-proven value is followed by a valid array delimiter.
    Delimited,
    /// The source ended at an ambiguous value prefix, so more bytes are
    /// needed before the value can be accepted as complete.
    NeedMore,
    /// Bytes after the parser-proven value cannot frame a top-level array
    /// element.
    Invalid,
}

/// A restartable [`crate::ItemReader`] over any `Read + Seek` top-level
/// JSON-array source. See the module documentation for the streaming and
/// malformed-input design.
///
/// # Contract
///
/// - **Input/output**: produces one [`serde_json::Value`] per top-level array
///   element.
/// - **State/checkpoint**: restart position is the byte offset immediately
///   after the last successfully parsed element, persisted through the
///   paired [`JsonArrayReaderStream`]. A restart resumes by evaluating
///   separator-or-close at that exact byte, never by re-reading from byte
///   zero or counting emitted items. Every seek this reader performs --
///   establishing the initial/restored position, and rewinding past a
///   parser lookahead overshoot after a successful element -- updates the
///   logical position only if that seek actually succeeds
///   ([`crate::FailureCategory::TransientInfrastructure`] otherwise,
///   never advancing the persisted checkpoint); a failed seek forces the
///   next call to fully re-derive its position and framing state from the
///   authoritative checkpoint rather than trust an unconfirmed cursor.
/// - **Ordering**: preserves array order.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`).
/// - **Reentrancy**: not reentrant (owns the parse position).
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: the raw input buffer used to frame one element is
///   grown in doubling steps and never past
///   [`JsonArrayFormat::with_max_value_bytes`]. Parser/value allocations are
///   record-dependent but remain `O(max_value_bytes)` for accepted input; a
///   value whose raw source span would exceed the bound is rejected before
///   source-sized raw accumulation. See
///   `crates/oxide-batch/tests/item_components_json_allocation.rs`.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls.
/// - **Close**: closed through the paired stream's
///   [`crate::ItemStream::close`].
/// - **Sensitive diagnostics**: restart state is a byte offset, never element
///   content, and is declared [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: a missing opening/closing bracket, a missing or
///   duplicated separator, a syntactically invalid element, or an element
///   exceeding the configured bound is a [`ReaderError`] in
///   [`crate::FailureCategory::UserComponent`] with
///   [`ReaderError::has_checkpoint_advanced`] `false` -- see the module
///   documentation for why this reader has exactly one failure mode, always
///   fail-closed, never a safe mid-array skip. Configure fail for this
///   failure class; skip cannot be honored (the M3 runtime requires forward
///   checkpoint proof). A direct retry is deterministic: this reader restores
///   the source position and framing state to the last proven boundary before
///   returning the error, so the same malformed element is attempted again.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_json_array.rs`,
///   `crates/oxide-batch-test/tests/postgres_json_restart.rs`.
pub struct JsonArrayReader<Src> {
    source: BufReader<Src>,
    buffer: Vec<u8>,
    max_value_bytes: usize,
    position: Arc<Mutex<u64>>,
    consumed_absolute: u64,
    expect_separator: bool,
    done: bool,
    started: bool,
}

impl<Src: Read + Seek> JsonArrayReader<Src> {
    /// Seeks the physical source to `target` and records the new logical
    /// position *only if that seek actually succeeds*. A failed seek never
    /// updates `consumed_absolute`/`position` to pretend it landed there --
    /// it clears `started` instead, so the next call re-derives everything
    /// (seek target, and whether to redo the opening-bracket dance or arm
    /// separator-or-close directly) from the authoritative shared
    /// checkpoint via [`Self::ensure_started`], rather than trusting a
    /// physical cursor no operation has actually confirmed.
    fn reseek_to(&mut self, target: u64) -> Result<(), ReaderError> {
        if self.source.seek(SeekFrom::Start(target)).is_err() {
            self.started = false;
            return Err(ReaderError::with_category(
                FailureCategory::TransientInfrastructure,
            ));
        }
        self.consumed_absolute = target;
        Ok(())
    }

    /// Restores framing state to a previously proven boundary after a
    /// failed read/parse attempt. Propagates a failure of the restoring
    /// seek itself (see [`Self::reseek_to`]) rather than ever proceeding as
    /// though an unconfirmed rewind succeeded.
    fn restore_read_state(
        &mut self,
        position: u64,
        expect_separator: bool,
    ) -> Result<(), ReaderError> {
        self.reseek_to(position)?;
        self.expect_separator = expect_separator;
        self.done = false;
        Ok(())
    }

    /// Accepts a closing bracket only when all remaining source bytes are
    /// JSON whitespace and then true EOF. The first non-whitespace byte is
    /// left unread so the caller can restore the last proven boundary on
    /// failure.
    fn consume_closing_bracket(&mut self) -> Result<(), ReaderError> {
        self.source.consume(1);
        self.consumed_absolute += 1;
        let (skipped, byte) = skip_whitespace(&mut self.source)
            .map_err(|_| ReaderError::with_category(FailureCategory::TransientInfrastructure))?;
        self.consumed_absolute += skipped;
        if byte.is_some() {
            return Err(ReaderError::new());
        }
        self.done = true;
        Ok(())
    }

    /// Proves the framing immediately after a parser-proven value. Only the
    /// byte range after `byte_offset()` is inspected; nested or quoted
    /// delimiters are therefore never considered framing. A non-whitespace
    /// byte immediately adjacent to the buffer may still be a continuation of
    /// a bare JSON token (most importantly a number), so that case requests
    /// another bounded growth step. Once whitespace has intervened, a
    /// non-delimiter is definitively invalid rather than something to scan
    /// past heuristically.
    fn prove_value_framing(&mut self, offset: usize) -> Result<ValueFraming, ReaderError> {
        let suffix = &self.buffer[offset..];
        let first_non_whitespace = suffix
            .iter()
            .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'));
        if let Some(index) = first_non_whitespace {
            return Ok(match suffix[index] {
                b',' | b']' => ValueFraming::Delimited,
                _ if index == 0 => ValueFraming::NeedMore,
                _ => ValueFraming::Invalid,
            });
        }

        let buffered_whitespace = !suffix.is_empty();
        let (skipped, byte) = skip_whitespace(&mut self.source)
            .map_err(|_| ReaderError::with_category(FailureCategory::TransientInfrastructure))?;
        Ok(match byte {
            Some(b',' | b']') => ValueFraming::Delimited,
            Some(_) if buffered_whitespace || skipped > 0 => ValueFraming::Invalid,
            Some(_) => ValueFraming::NeedMore,
            None => ValueFraming::Invalid,
        })
    }

    /// Runs once per instance: on initial execution, seeks to byte 0 (a
    /// no-op) and consumes the opening bracket (and, for an empty array, the
    /// closing one too); on restart, seeks to the persisted checkpoint and
    /// arms separator-or-close evaluation directly, never re-consuming a
    /// bracket that a previous attempt already proved.
    fn ensure_started(&mut self) -> Result<(), ReaderError> {
        if self.started {
            return Ok(());
        }
        let target = *self.position.lock().unwrap_or_else(PoisonError::into_inner);
        self.reseek_to(target)?;
        if target == 0 {
            let (skipped, byte) = skip_whitespace(&mut self.source).map_err(|_| {
                ReaderError::with_category(FailureCategory::TransientInfrastructure)
            })?;
            self.consumed_absolute += skipped;
            if byte != Some(b'[') {
                return Err(ReaderError::new());
            }
            self.source.consume(1);
            self.consumed_absolute += 1;
            let (skipped, byte) = skip_whitespace(&mut self.source).map_err(|_| {
                ReaderError::with_category(FailureCategory::TransientInfrastructure)
            })?;
            self.consumed_absolute += skipped;
            match byte {
                Some(b']') => {
                    self.consume_closing_bracket()?;
                }
                Some(_) => {
                    self.expect_separator = false;
                }
                None => return Err(ReaderError::new()),
            }
            self.started = true;
            Ok(())
        } else {
            self.expect_separator = true;
            self.started = true;
            Ok(())
        }
    }

    /// Evaluates the byte at the current position as either a separator
    /// (more elements follow) or the array's closing bracket (done),
    /// consuming exactly that one byte. Any other byte, or true EOF, means
    /// framing is no longer trustworthy.
    fn resolve_separator(&mut self) -> Result<bool, ReaderError> {
        let (skipped, byte) = skip_whitespace(&mut self.source)
            .map_err(|_| ReaderError::with_category(FailureCategory::TransientInfrastructure))?;
        self.consumed_absolute += skipped;
        match byte {
            Some(b']') => {
                self.consume_closing_bracket()?;
                Ok(false)
            }
            Some(b',') => {
                self.source.consume(1);
                self.consumed_absolute += 1;
                let (skipped, byte) = skip_whitespace(&mut self.source).map_err(|_| {
                    ReaderError::with_category(FailureCategory::TransientInfrastructure)
                })?;
                self.consumed_absolute += skipped;
                if byte.is_none() {
                    return Err(ReaderError::new());
                }
                Ok(true)
            }
            _ => Err(ReaderError::new()),
        }
    }

    /// Parses exactly one element from wherever the source currently is,
    /// growing [`Self::buffer`] and never past `max_value_bytes`. Returns
    /// the element and its exact raw byte span, or [`ParsedElement::FailClosed`].
    fn parse_element(&mut self) -> Result<ParsedElement, ReaderError> {
        if self.max_value_bytes == 0 {
            return Ok(ParsedElement::FailClosed);
        }
        self.buffer.clear();
        let mut target = INITIAL_GROWTH_BYTES.min(self.max_value_bytes);
        loop {
            let hit_eof = grow_to(&mut self.source, &mut self.buffer, target).map_err(|_| {
                ReaderError::with_category(FailureCategory::TransientInfrastructure)
            })?;
            let mut stream =
                serde_json::Deserializer::from_slice(&self.buffer).into_iter::<Value>();
            let result = stream.next();
            let offset = stream.byte_offset();
            drop(stream);
            match result {
                Some(Ok(value)) => match self.prove_value_framing(offset) {
                    Ok(ValueFraming::Delimited) => {
                        return Ok(ParsedElement::Value {
                            value,
                            consumed: offset as u64,
                        });
                    }
                    Ok(ValueFraming::NeedMore)
                        if !hit_eof && self.buffer.len() < self.max_value_bytes =>
                    {
                        let next = target.saturating_mul(2).min(self.max_value_bytes);
                        if next <= target {
                            return Ok(ParsedElement::FailClosed);
                        }
                        target = next;
                    }
                    Ok(ValueFraming::NeedMore | ValueFraming::Invalid) => {
                        return Ok(ParsedElement::FailClosed);
                    }
                    Err(error) => return Err(error),
                },
                Some(Err(error))
                    if error.is_eof() && !hit_eof && self.buffer.len() < self.max_value_bytes =>
                {
                    let next = target.saturating_mul(2).min(self.max_value_bytes);
                    if next <= target {
                        return Ok(ParsedElement::FailClosed);
                    }
                    target = next;
                }
                _ => return Ok(ParsedElement::FailClosed),
            }
        }
    }
}

impl<I, Src> crate::ItemReader<I> for JsonArrayReader<Src>
where
    I: From<Value> + 'static,
    Src: Read + Seek + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.ensure_started()?;
        if self.done {
            return Ok(ReadOutcome::EndOfInput);
        }
        let checkpoint_before_read = self.consumed_absolute;
        let expect_separator_before_read = self.expect_separator;
        if self.expect_separator {
            match self.resolve_separator() {
                Ok(true) => {}
                Ok(false) => return Ok(ReadOutcome::EndOfInput),
                Err(error) => {
                    self.restore_read_state(checkpoint_before_read, expect_separator_before_read)?;
                    return Err(error);
                }
            }
        }
        match self.parse_element() {
            Ok(ParsedElement::Value { value, consumed }) => {
                // `parse_element`'s bounded growth may have read further
                // ahead from `source` than the element's own bytes (its
                // growth target is not tailored to this element's exact
                // size); rewind to the true logical position so the next
                // call -- whether `resolve_separator` or a restart's fresh
                // seek -- starts exactly where this element's bytes end,
                // never mid-lookahead. The target is computed *before*
                // attempting that seek, and `consumed_absolute`/`position`
                // are updated only if it actually succeeds (see
                // `reseek_to`): a failed rewind must never advance the
                // persisted checkpoint past an element the physical source
                // was never actually confirmed to be positioned after.
                let new_position = self.consumed_absolute + consumed;
                self.reseek_to(new_position)?;
                *self.position.lock().unwrap_or_else(PoisonError::into_inner) =
                    self.consumed_absolute;
                self.expect_separator = true;
                Ok(ReadOutcome::Item(value.into()))
            }
            Ok(ParsedElement::FailClosed) => {
                self.restore_read_state(checkpoint_before_read, expect_separator_before_read)?;
                Err(ReaderError::new())
            }
            Err(error) => {
                self.restore_read_state(checkpoint_before_read, expect_separator_before_read)?;
                Err(error)
            }
        }
    }
}

/// The [`crate::ItemStream`] half of a [`JsonArrayReader`].
pub struct JsonArrayReaderStream {
    position: Arc<Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for JsonArrayReaderStream {
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
pub fn json_array_reader<I, Src>(
    source: Src,
    format: JsonArrayFormat,
    identity: ComponentStreamIdentity,
) -> (
    JsonArrayReader<Src>,
    JsonArrayReaderStream,
    StreamStateContract,
)
where
    I: From<Value> + 'static,
    Src: Read + Seek + Send,
{
    let position = Arc::new(Mutex::new(0));
    let reader = JsonArrayReader {
        source: BufReader::new(source),
        buffer: Vec::new(),
        max_value_bytes: format.max_value_bytes,
        position: Arc::clone(&position),
        consumed_absolute: 0,
        expect_separator: false,
        done: false,
        started: false,
    };
    let stream = JsonArrayReaderStream {
        position,
        namespace: identity,
    };
    let contract = StreamStateContract::new(reader_position_codec());
    (reader, stream, contract)
}

/// Opens `path` for a restartable [`JsonArrayReader<File>`].
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn json_array_file_reader<I>(
    path: impl AsRef<Path>,
    format: JsonArrayFormat,
    identity: ComponentStreamIdentity,
) -> io::Result<(
    JsonArrayReader<File>,
    JsonArrayReaderStream,
    StreamStateContract,
)>
where
    I: From<Value> + 'static,
{
    let file = File::open(path)?;
    Ok(json_array_reader::<I, File>(file, format, identity))
}

/// The file and committed counts a [`JsonArrayWriter`]/[`JsonArrayWriterStream`]
/// pair shares under one lock, so a comma decision, its physical write, and
/// the resulting count update can never be observed half-applied.
struct WriterState {
    file: File,
    committed_bytes: u64,
    committed_items: u64,
}

/// A restartable [`crate::ItemWriter`] over a local file, producing a valid
/// top-level JSON array.
///
/// # Contract
///
/// - **Input/output**: accepts `I: Into<`[`serde_json::Value`]`>`; each write
///   batch appends its items' compact JSON encoding, comma-separated,
///   directly after the previously written content -- items are streamed
///   incrementally, never retained in memory until close.
/// - **State/checkpoint**: committed state is the file's committed byte
///   length *and* committed element count, persisted through the paired
///   [`JsonArrayWriterStream`]; both are needed because the element count is
///   what tells a resumed writer whether its first item still needs a
///   leading comma. Reconciled identically to
///   [`crate::item_components::delimited::DelimitedWriter`] otherwise
///   (trailing uncommitted bytes truncated on restart; a shorter-than
///   committed file fails closed). Initial execution writes the opening
///   `[` immediately.
/// - **Ordering**: writes items in the order supplied.
/// - **Thread safety**: `Send + Sync`; a single internal `Mutex` guards the
///   file handle *and* the committed byte/item counts together, so the
///   comma-state decision, the physical write, and the count update form one
///   coherent, serialized transition -- a concurrent call can never observe
///   the pre-write item count while another call's write is in flight.
/// - **Reentrancy**: not reentrant against the same path from a second
///   concurrent attempt.
/// - **Transaction/delivery**: does not enlist; file bytes are outside the
///   OxideBatch-owned business transaction. No directory-entry fsync is
///   performed.
/// - **Bounded resource**: one file handle; serializes each item directly to
///   the file and does not materialize the serialized write batch.
/// - **Cancellation**: honors the call-scoped stop token before writing.
/// - **Close**: [`crate::ItemStream::close`] appends the closing `]` *only*
///   when [`crate::StreamRuntimeOutcome::Committed`] is reported -- the file
///   is never claimed to be a complete, valid JSON array while the step
///   attempt is in progress, stopped, or failed; a later attempt resumes
///   appending elements to the still-open array exactly as before.
/// - **Sensitive diagnostics**: restart state is a byte count and an element
///   count, never element content, and is declared
///   [`crate::StateSensitivity::NonSensitive`].
/// - **Malformed input**: not applicable; a writer never rejects an
///   already-typed item.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_json_array.rs`,
///   `crates/oxide-batch-test/tests/postgres_json_restart.rs`.
pub struct JsonArrayWriter {
    state: Arc<Mutex<WriterState>>,
}

impl<I> crate::ItemWriter<I> for JsonArrayWriter
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
        if items.is_empty() {
            return Ok(WriteOutcome::Written);
        }
        // The comma decision, the physical write, and the committed-count
        // update all happen under this one lock: no other call can observe
        // `committed_items` (to decide its own leading comma) or write to
        // `file` until this entire transition completes.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut has_items = state.committed_items > 0;
        for item in items {
            if has_items {
                state.file.write_all(b",").map_err(|_| WriterError::new())?;
            }
            has_items = true;
            let value: Value = item.clone().into();
            serde_json::to_writer(&mut state.file, &value).map_err(|_| WriterError::new())?;
        }
        state.file.sync_data().map_err(|_| WriterError::new())?;
        let candidate_bytes = state
            .file
            .stream_position()
            .map_err(|_| WriterError::new())?;
        state.committed_bytes = candidate_bytes;
        state.committed_items = state.committed_items.saturating_add(items.len() as u64);
        Ok(WriteOutcome::Written)
    }
}

/// The [`crate::ItemStream`] half of a [`JsonArrayWriter`].
pub struct JsonArrayWriterStream {
    state: Arc<Mutex<WriterState>>,
    namespace: ComponentStreamIdentity,
}

impl crate::ItemStream for JsonArrayWriterStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = writer_position_codec();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (target, outcome) = if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<WriterPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            let actual_len = state
                .file
                .metadata()
                .map_err(|_| StreamOpenError::new())?
                .len();
            if actual_len < restored.committed_bytes {
                return Err(StreamOpenError::with_category(FailureCategory::Invariant));
            }
            if actual_len != restored.committed_bytes {
                state
                    .file
                    .set_len(restored.committed_bytes)
                    .map_err(|_| StreamOpenError::new())?;
            }
            (restored, StreamOpenOutcome::Restored)
        } else {
            // No committed state exists yet: this is a fresh attempt, so the
            // file's pre-existing length (if any, e.g. garbage left by a
            // crashed attempt that never durably committed anything) is not
            // authoritative -- start over exactly as
            // `crate::item_components::delimited::DelimitedWriter` does.
            state.file.set_len(0).map_err(|_| StreamOpenError::new())?;
            state
                .file
                .seek(SeekFrom::Start(0))
                .map_err(|_| StreamOpenError::new())?;
            state
                .file
                .write_all(b"[")
                .map_err(|_| StreamOpenError::new())?;
            state.file.sync_data().map_err(|_| StreamOpenError::new())?;
            (WriterPosition::START, StreamOpenOutcome::Initial)
        };
        state
            .file
            .seek(SeekFrom::Start(target.committed_bytes))
            .map_err(|_| StreamOpenError::new())?;
        state.committed_bytes = target.committed_bytes;
        state.committed_items = target.committed_items;
        Ok(outcome)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = writer_position_codec();
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let current = WriterPosition {
            committed_bytes: state.committed_bytes,
            committed_items: state.committed_items,
        };
        drop(state);
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
        context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        if context.outcome() == StreamRuntimeOutcome::Committed {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let committed_bytes = state.committed_bytes;
            state
                .file
                .seek(SeekFrom::Start(committed_bytes))
                .map_err(|_| StreamCloseError::new())?;
            state
                .file
                .write_all(b"]")
                .map_err(|_| StreamCloseError::new())?;
            state
                .file
                .sync_data()
                .map_err(|_| StreamCloseError::new())?;
        }
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Opens (creating if absent) `path` for a restartable
/// `(writer, stream, contract)` triple, namespaced under `identity`.
///
/// # Errors
///
/// Returns the [`io::Error`] opening `path` produces.
pub fn json_array_writer(
    path: impl AsRef<Path>,
    identity: ComponentStreamIdentity,
) -> io::Result<(JsonArrayWriter, JsonArrayWriterStream, StreamStateContract)> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let state = Arc::new(Mutex::new(WriterState {
        file,
        committed_bytes: 0,
        committed_items: 0,
    }));
    let writer = JsonArrayWriter {
        state: Arc::clone(&state),
    };
    let stream = JsonArrayWriterStream {
        state,
        namespace: identity,
    };
    let contract = StreamStateContract::new(writer_position_codec());
    Ok((writer, stream, contract))
}
