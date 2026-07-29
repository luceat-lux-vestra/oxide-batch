//! Runtime-neutral chunk component and transaction-enlistment contracts.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use crate::{BoxFuture, Checkpoint, ExecutionContext, StopToken};

/// A nonzero item limit for one chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkSize(NonZeroU32);

impl ChunkSize {
    /// Constructs a nonzero chunk size.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ZeroSize`] when `value` is zero.
    pub fn new(value: u32) -> Result<Self, ChunkError> {
        NonZeroU32::new(value).map(Self).ok_or(ChunkError::ZeroSize)
    }

    /// Returns the configured item limit.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// A checked non-negative item or transaction count.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkCount(u64);

impl ChunkCount {
    /// The zero count.
    pub const ZERO: Self = Self(0);

    /// Constructs a count.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds two counts without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::CountOverflow`] when the sum exceeds `u64`.
    pub fn checked_add(self, other: Self) -> Result<Self, ChunkError> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(ChunkError::CountOverflow)
    }

    /// Increments this count without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::CountOverflow`] at `u64::MAX`.
    pub fn checked_increment(self) -> Result<Self, ChunkError> {
        self.checked_add(Self(1))
    }
}

/// Validated item counts within one open chunk.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkCounts {
    read: ChunkCount,
    processed: ChunkCount,
    written: ChunkCount,
    filtered: ChunkCount,
}

impl ChunkCounts {
    /// Validates a complete count snapshot.
    ///
    /// `processed` counts items that produced writer input; `filtered` counts
    /// items intentionally producing no output. Their sum cannot exceed
    /// `read`, and `written` cannot exceed `processed`.
    ///
    /// # Errors
    ///
    /// Returns a typed overflow or invalid-state classification.
    pub fn new(
        read: ChunkCount,
        processed: ChunkCount,
        written: ChunkCount,
        filtered: ChunkCount,
    ) -> Result<Self, ChunkError> {
        let classified = processed.checked_add(filtered)?;
        if classified > read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        if written > processed {
            return Err(ChunkError::WrittenExceedsProcessed);
        }
        Ok(Self {
            read,
            processed,
            written,
            filtered,
        })
    }

    /// Returns the read count.
    #[must_use]
    pub const fn read(self) -> ChunkCount {
        self.read
    }

    /// Returns the successfully processed count.
    #[must_use]
    pub const fn processed(self) -> ChunkCount {
        self.processed
    }

    /// Returns the acknowledged writer-input count.
    #[must_use]
    pub const fn written(self) -> ChunkCount {
        self.written
    }

    /// Returns the filtered count.
    #[must_use]
    pub const fn filtered(self) -> ChunkCount {
        self.filtered
    }

    /// Adds two snapshots and revalidates their aggregate invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed overflow or invalid-state classification.
    pub fn checked_add(self, other: Self) -> Result<Self, ChunkError> {
        Self::new(
            self.read.checked_add(other.read)?,
            self.processed.checked_add(other.processed)?,
            self.written.checked_add(other.written)?,
            self.filtered.checked_add(other.filtered)?,
        )
    }
}

/// Mutable, invariant-preserving progress for one bounded chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkProgress {
    size: ChunkSize,
    counts: ChunkCounts,
}

impl ChunkProgress {
    /// Starts an empty chunk with a validated nonzero size.
    #[must_use]
    pub const fn new(size: ChunkSize) -> Self {
        Self {
            size,
            counts: ChunkCounts {
                read: ChunkCount::ZERO,
                processed: ChunkCount::ZERO,
                written: ChunkCount::ZERO,
                filtered: ChunkCount::ZERO,
            },
        }
    }

    /// Restores a validated in-memory progress snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::SizeExceeded`] when `counts.read()` exceeds the
    /// configured chunk size.
    pub fn from_counts(size: ChunkSize, counts: ChunkCounts) -> Result<Self, ChunkError> {
        if counts.read().get() > u64::from(size.get()) {
            return Err(ChunkError::SizeExceeded);
        }
        Ok(Self { size, counts })
    }

    /// Returns the configured size.
    #[must_use]
    pub const fn size(self) -> ChunkSize {
        self.size
    }

    /// Returns the current validated counts.
    #[must_use]
    pub const fn counts(self) -> ChunkCounts {
        self.counts
    }

    /// Returns whether no more items may be read into this chunk.
    #[must_use]
    pub fn is_full(self) -> bool {
        self.counts.read().get() == u64::from(self.size.get())
    }

    /// Records one successfully read item.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::SizeExceeded`] when the chunk is already full,
    /// or [`ChunkError::CountOverflow`] on arithmetic exhaustion.
    pub fn record_read(&mut self) -> Result<(), ChunkError> {
        if self.is_full() {
            return Err(ChunkError::SizeExceeded);
        }
        self.counts.read = self.counts.read.checked_increment()?;
        Ok(())
    }

    /// Classifies one previously read item as successfully processed.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ClassifiedExceedsRead`] when no unclassified read
    /// item remains, or [`ChunkError::CountOverflow`] on exhaustion.
    pub fn record_processed(&mut self) -> Result<(), ChunkError> {
        let next = self.counts.processed.checked_increment()?;
        let classified = next.checked_add(self.counts.filtered)?;
        if classified > self.counts.read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        self.counts.processed = next;
        Ok(())
    }

    /// Classifies one previously read item as filtered.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ClassifiedExceedsRead`] when no unclassified read
    /// item remains, or [`ChunkError::CountOverflow`] on exhaustion.
    pub fn record_filtered(&mut self) -> Result<(), ChunkError> {
        let next = self.counts.filtered.checked_increment()?;
        let classified = self.counts.processed.checked_add(next)?;
        if classified > self.counts.read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        self.counts.filtered = next;
        Ok(())
    }

    /// Records writer acknowledgement for `count` processed items.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::WrittenExceedsProcessed`] when the aggregate
    /// exceeds processed output, or [`ChunkError::CountOverflow`] on
    /// exhaustion.
    pub fn record_written(&mut self, count: ChunkCount) -> Result<(), ChunkError> {
        let next = self.counts.written.checked_add(count)?;
        if next > self.counts.processed {
            return Err(ChunkError::WrittenExceedsProcessed);
        }
        self.counts.written = next;
        Ok(())
    }
}

/// Stable chunk-size and count failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkError {
    /// Chunk size was zero.
    ZeroSize,
    /// Count arithmetic exceeded `u64`.
    CountOverflow,
    /// Processed plus filtered count exceeded read count.
    ClassifiedExceedsRead,
    /// Written count exceeded successfully processed count.
    WrittenExceedsProcessed,
    /// Read count exceeded the configured chunk size.
    SizeExceeded,
}

impl fmt::Display for ChunkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroSize => "chunk size must be nonzero",
            Self::CountOverflow => "chunk count arithmetic overflowed",
            Self::ClassifiedExceedsRead => "processed and filtered counts exceed the read count",
            Self::WrittenExceedsProcessed => "written count exceeds the processed count",
            Self::SizeExceeded => "read count exceeds the configured chunk size",
        })
    }
}

impl Error for ChunkError {}

/// Borrowed call state for a reader.
#[derive(Clone, Copy, Debug)]
pub struct ReadContext<'a> {
    stop: &'a StopToken,
}

impl<'a> ReadContext<'a> {
    /// Constructs a reader call scope.
    #[must_use]
    pub const fn new(stop: &'a StopToken) -> Self {
        Self { stop }
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// One item-reader call outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadOutcome<I> {
    /// The next input item.
    Item(I),
    /// The source is exhausted normally.
    EndOfInput,
    /// Cooperative stop was observed before another item was produced.
    Stopped,
}

/// A stateful asynchronous item source.
pub trait ItemReader<I>: Send {
    /// Reads at most one item while borrowing the reader and call scope.
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>>;
}

/// Borrowed call state for a processor.
#[derive(Clone, Copy, Debug)]
pub struct ProcessContext<'a> {
    stop: &'a StopToken,
}

impl<'a> ProcessContext<'a> {
    /// Constructs a processor call scope.
    #[must_use]
    pub const fn new(stop: &'a StopToken) -> Self {
        Self { stop }
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// One item-processor call outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProcessOutcome<O> {
    /// An output item was produced.
    Item(O),
    /// The input was intentionally filtered without producing output.
    Filtered,
    /// Cooperative stop was observed before output was produced.
    Stopped,
}

/// A dynamically dispatchable asynchronous item transformer.
pub trait ItemProcessor<I, O>: Send + Sync {
    /// Processes one borrowed item.
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>>;
}

/// A stable bound-value type for enlisted business statements.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BusinessValue<'a>(BusinessValueInner<'a>);

#[derive(Clone, Copy, Eq, PartialEq)]
enum BusinessValueInner<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
    I64(i64),
    Bool(bool),
    Null,
}

impl<'a> BusinessValue<'a> {
    /// Constructs a borrowed UTF-8 value.
    #[must_use]
    pub const fn text(value: &'a str) -> Self {
        Self(BusinessValueInner::Text(value))
    }

    /// Constructs a borrowed byte value.
    #[must_use]
    pub const fn bytes(value: &'a [u8]) -> Self {
        Self(BusinessValueInner::Bytes(value))
    }

    /// Constructs a signed integer value.
    #[must_use]
    pub const fn i64(value: i64) -> Self {
        Self(BusinessValueInner::I64(value))
    }

    /// Constructs a boolean value.
    #[must_use]
    pub const fn boolean(value: bool) -> Self {
        Self(BusinessValueInner::Bool(value))
    }

    /// Constructs a database null value.
    #[must_use]
    pub const fn null() -> Self {
        Self(BusinessValueInner::Null)
    }

    /// Returns the stable value kind.
    #[must_use]
    pub const fn kind(self) -> BusinessValueKind {
        match self.0 {
            BusinessValueInner::Text(_) => BusinessValueKind::Text,
            BusinessValueInner::Bytes(_) => BusinessValueKind::Bytes,
            BusinessValueInner::I64(_) => BusinessValueKind::I64,
            BusinessValueInner::Bool(_) => BusinessValueKind::Bool,
            BusinessValueInner::Null => BusinessValueKind::Null,
        }
    }

    /// Borrows the UTF-8 value when present.
    #[must_use]
    pub const fn as_text(self) -> Option<&'a str> {
        match self.0 {
            BusinessValueInner::Text(value) => Some(value),
            _ => None,
        }
    }

    /// Borrows the byte value when present.
    #[must_use]
    pub const fn as_bytes(self) -> Option<&'a [u8]> {
        match self.0 {
            BusinessValueInner::Bytes(value) => Some(value),
            _ => None,
        }
    }

    /// Returns the signed integer when present.
    #[must_use]
    pub const fn as_i64(self) -> Option<i64> {
        match self.0 {
            BusinessValueInner::I64(value) => Some(value),
            _ => None,
        }
    }

    /// Returns the boolean when present.
    #[must_use]
    pub const fn as_bool(self) -> Option<bool> {
        match self.0 {
            BusinessValueInner::Bool(value) => Some(value),
            _ => None,
        }
    }
}

impl fmt::Debug for BusinessValue<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BusinessValue")
            .field("kind", &self.kind())
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Stable discriminator for a bound business value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BusinessValueKind {
    /// UTF-8 text.
    Text,
    /// Arbitrary bytes.
    Bytes,
    /// Signed 64-bit integer.
    I64,
    /// Boolean.
    Bool,
    /// Database null.
    Null,
}

/// A parameterized business write borrowed for one transaction call.
pub struct BusinessStatement<'a> {
    text: &'a str,
    values: &'a [BusinessValue<'a>],
}

impl<'a> BusinessStatement<'a> {
    /// Constructs a statement from SQL text and separately bound values.
    ///
    /// The `PostgreSQL` adapter binds every value; it never interpolates these
    /// values into `text`.
    #[must_use]
    pub const fn new(text: &'a str, values: &'a [BusinessValue<'a>]) -> Self {
        Self { text, values }
    }

    /// Borrows statement text for the authorized database adapter.
    #[must_use]
    pub const fn text(&self) -> &'a str {
        self.text
    }

    /// Borrows separately bound values.
    #[must_use]
    pub const fn values(&self) -> &'a [BusinessValue<'a>] {
        self.values
    }
}

impl fmt::Debug for BusinessStatement<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BusinessStatement")
            .field("text", &"<redacted>")
            .field("value_count", &self.values.len())
            .finish()
    }
}

/// Successful effect from one enlisted business statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BusinessWriteResult {
    rows_affected: u64,
}

impl BusinessWriteResult {
    /// Constructs a result reported by a transaction adapter.
    #[must_use]
    pub const fn new(rows_affected: u64) -> Self {
        Self { rows_affected }
    }

    /// Returns the database-reported affected-row count.
    #[must_use]
    pub const fn rows_affected(self) -> u64 {
        self.rows_affected
    }
}

/// OxideBatch-owned port for the currently enlisted business transaction.
///
/// The durable adapter owns the concrete transaction and lends this port to a
/// writer only for the call. `SQLx` pool, connection, row, error, and transaction
/// types do not cross this boundary.
pub trait BusinessTransaction: Send {
    /// Executes one parameterized business write.
    fn execute<'a>(
        &'a mut self,
        statement: BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>>;
}

/// Stable, value-redacted enlisted-transaction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BusinessTransactionError {
    /// The transaction cannot safely continue after an infrastructure failure.
    Infrastructure,
    /// The statement was rejected permanently.
    Rejected,
    /// Cooperative cancellation interrupted the operation.
    Cancelled,
}

impl fmt::Display for BusinessTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Infrastructure => "business transaction infrastructure failed",
            Self::Rejected => "business transaction statement was rejected",
            Self::Cancelled => "business transaction operation was cancelled",
        })
    }
}

impl Error for BusinessTransactionError {}

/// Borrowed call state for a writer.
pub struct WriteContext<'a> {
    stop: &'a StopToken,
    transaction: Option<&'a mut dyn BusinessTransaction>,
}

impl<'a> WriteContext<'a> {
    /// Constructs a non-enlisted writer call scope.
    #[must_use]
    pub const fn non_transactional(stop: &'a StopToken) -> Self {
        Self {
            stop,
            transaction: None,
        }
    }

    /// Constructs a writer call enlisted in an OxideBatch-owned transaction.
    #[must_use]
    pub fn enlisted(stop: &'a StopToken, transaction: &'a mut dyn BusinessTransaction) -> Self {
        Self {
            stop,
            transaction: Some(transaction),
        }
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(&self) -> &'a StopToken {
        self.stop
    }

    /// Reborrows the enlisted transaction when one is present.
    #[must_use]
    pub fn transaction(&mut self) -> Option<&mut (dyn BusinessTransaction + 'a)> {
        self.transaction.as_deref_mut()
    }

    /// Returns whether this call participates in the chunk transaction.
    #[must_use]
    pub const fn is_enlisted(&self) -> bool {
        self.transaction.is_some()
    }
}

impl fmt::Debug for WriteContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteContext")
            .field("stop_requested", &self.stop.is_stop_requested())
            .field("enlisted", &self.transaction.is_some())
            .finish()
    }
}

/// One item-writer call outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WriteOutcome {
    /// Every supplied item was accepted by the writer.
    Written,
    /// Cooperative stop was observed before the batch was accepted.
    Stopped,
}

/// A dynamically dispatchable asynchronous batch writer.
pub trait ItemWriter<I>: Send + Sync {
    /// Writes one borrowed, nonempty batch.
    ///
    /// A durable `PostgreSQL` step supplies an enlisted transaction in
    /// `context`. External writers receive a non-transactional context and
    /// retain the documented at-least-once boundary.
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>>;
}

/// Read-only evidence passed after the chunk transaction commits.
#[derive(Clone, Copy, Debug)]
pub struct ChunkCompletionContext<'a> {
    checkpoint: &'a Checkpoint,
    execution_context: &'a ExecutionContext,
    counts: ChunkCounts,
    stop: &'a StopToken,
}

impl<'a> ChunkCompletionContext<'a> {
    /// Constructs committed chunk evidence.
    #[must_use]
    pub const fn new(
        checkpoint: &'a Checkpoint,
        execution_context: &'a ExecutionContext,
        counts: ChunkCounts,
        stop: &'a StopToken,
    ) -> Self {
        Self {
            checkpoint,
            execution_context,
            counts,
            stop,
        }
    }

    /// Borrows the committed checkpoint.
    #[must_use]
    pub const fn checkpoint(self) -> &'a Checkpoint {
        self.checkpoint
    }

    /// Borrows the committed execution context.
    #[must_use]
    pub const fn execution_context(self) -> &'a ExecutionContext {
        self.execution_context
    }

    /// Returns the committed chunk counts.
    #[must_use]
    pub const fn counts(self) -> ChunkCounts {
        self.counts
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// Post-commit acknowledgement from a chunk-completion component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkCompletionOutcome {
    /// The component observed and acknowledged the durable commit.
    Acknowledged,
    /// Stop was observed after commit; the commit remains authoritative.
    StoppedAfterCommit,
}

/// An asynchronous observer called only after a durable chunk commit.
pub trait ChunkCompletion: Send + Sync {
    /// Acknowledges committed state without becoming a correctness authority.
    fn after_commit<'a>(
        &'a self,
        context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>>;
}

macro_rules! component_error {
    ($name:ident, $message:literal) => {
        #[doc = $message]
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $name;

        impl $name {
            /// Constructs a value-redacted component failure.
            #[must_use]
            pub const fn new() -> Self {
                Self
            }

            /// Classifies an arbitrary user error without retaining its
            /// payload or display text.
            #[must_use]
            pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
                drop(error);
                Self
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str($message)
            }
        }

        impl Error for $name {}
    };
}

component_error!(ReaderError, "item reader failed");
component_error!(ProcessorError, "item processor failed");
component_error!(WriterError, "item writer failed");
component_error!(ChunkCompletionError, "chunk completion callback failed");
