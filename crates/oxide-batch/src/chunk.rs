//! Runtime-neutral chunk component and transaction-enlistment contracts.

use std::error::Error;
use std::fmt;
use std::future::Future;

use crate::{
    BoxFuture, Checkpoint, ChunkCounts, ExecutionContext, FailureCategory, FaultProgress,
    JobExecutionId, SkipCounts, StepExecutionId, StopToken,
};

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
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item reader for `{I}`",
    label = "this component cannot read `{I}`",
    note = "implement `ItemReader<{I}>` with `async fn read(&mut self, context: ReadContext<'_>)`",
    note = "the returned future must be `Send`: do not hold a non-`Send` value across an await"
)]
pub trait ItemReader<I>: Send {
    /// Reads at most one item while borrowing the reader and call scope.
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a;
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

/// A shared asynchronous item transformer.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item processor from `{I}` to `{O}`",
    label = "this component cannot process `{I}` into `{O}`",
    note = "implement `ItemProcessor<{I}, {O}>` with `async fn process(&self, item: &{I}, context: ProcessContext<'_>)`",
    note = "a processor is shared across the chunk, so it takes `&self` and must be `Sync`"
)]
pub trait ItemProcessor<I, O>: Send + Sync {
    /// Processes one borrowed item.
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> impl Future<Output = Result<ProcessOutcome<O>, ProcessorError>> + Send + 'a;
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

/// Evidence returned after one chunk transaction is known to have committed.
///
/// The receipt owns the durable checkpoint and execution context so
/// post-commit observers cannot borrow adapter-internal transaction state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkCommitReceipt {
    checkpoint: Checkpoint,
    execution_context: ExecutionContext,
}

impl ChunkCommitReceipt {
    /// Constructs committed durable-state evidence.
    #[must_use]
    pub const fn new(checkpoint: Checkpoint, execution_context: ExecutionContext) -> Self {
        Self {
            checkpoint,
            execution_context,
        }
    }

    /// Borrows the committed reader checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Borrows the committed execution context.
    #[must_use]
    pub const fn execution_context(&self) -> &ExecutionContext {
        &self.execution_context
    }
}

/// Stable, payload-redacted chunk-transaction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkTransactionError {
    /// The operation is known not to have committed.
    NotCommitted,
    /// The adapter cannot determine whether commit reached durable storage.
    CommitOutcomeUnknown,
}

impl fmt::Display for ChunkTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotCommitted => "chunk transaction did not commit",
            Self::CommitOutcomeUnknown => "chunk transaction commit outcome is unknown",
        })
    }
}

impl Error for ChunkTransactionError {}

/// The fault-tolerance progress one chunk commit makes authoritative.
///
/// The values are deltas contributed by a single chunk attempt. A durable
/// adapter adds them to the committed totals it read when the transaction
/// began, so replaying an uncommitted chunk after a crash cannot double-count.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ChunkFaultProgress {
    skips: SkipCounts,
    no_rollbacks: u64,
}

impl ChunkFaultProgress {
    /// The progress of a chunk that accepted no skip.
    pub const NONE: Self = Self {
        skips: SkipCounts::ZERO,
        no_rollbacks: 0,
    };

    /// Constructs the delta contributed by one chunk attempt.
    #[must_use]
    pub const fn new(skips: SkipCounts, no_rollbacks: u64) -> Self {
        Self {
            skips,
            no_rollbacks,
        }
    }

    /// Returns the per-phase skips this chunk accepted.
    #[must_use]
    pub const fn skips(self) -> SkipCounts {
        self.skips
    }

    /// Returns the accepted commit-safe skips this chunk committed.
    #[must_use]
    pub const fn no_rollbacks(self) -> u64 {
        self.no_rollbacks
    }
}

/// One adapter-owned transaction for a bounded chunk attempt.
///
/// The runtime invokes the writer while this value is open, then commits the
/// supplied checked counters or rolls the transaction back. Implementations
/// keep database-driver and serialization types private.
pub trait ChunkTransaction: Send {
    /// Reborrows an enlisted business transaction when the selected delivery
    /// mode supports same-resource atomicity.
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction>;

    /// Commits business work and the supplied progress, returning the durable
    /// checkpoint and context that became authoritative.
    ///
    /// `fault` carries the skips this chunk accepted. A durable adapter also
    /// clears the retained fault state of the superseded checkpoint generation
    /// in this transaction, so a skip, its counters, and the checkpoint that
    /// makes it authoritative commit or roll back together.
    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>>;

    /// Rolls back all provisional work in this chunk attempt.
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>>;
}

/// Repository execution identity for one launched chunk transaction.
///
/// Standalone chunk execution has no durable execution graph and therefore
/// uses [`ChunkTransactionManager::begin`]. The repository-backed launcher
/// supplies this context through [`ChunkTransactionManager::begin_for`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkTransactionContext {
    job_execution_id: JobExecutionId,
    step_execution_id: StepExecutionId,
}

impl ChunkTransactionContext {
    /// Constructs a durable chunk-transaction scope.
    #[must_use]
    pub const fn new(job_execution_id: JobExecutionId, step_execution_id: StepExecutionId) -> Self {
        Self {
            job_execution_id,
            step_execution_id,
        }
    }

    /// Returns the enclosing job execution.
    #[must_use]
    pub const fn job_execution_id(self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the step execution whose progress is committed.
    #[must_use]
    pub const fn step_execution_id(self) -> StepExecutionId {
        self.step_execution_id
    }
}

/// Committed step progress one chunk-step attempt inherits.
///
/// A restart resumes bounded policy limits and stable retry-key identity from
/// the durable state its attempt inherited, so a retry budget is not refilled
/// by restarting the process.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct InheritedStepProgress {
    read_ordinal: u64,
    checkpoint_digest: [u8; 32],
    fault: FaultProgress,
}

impl InheritedStepProgress {
    /// The progress a standalone or first-attempt chunk step inherits.
    pub const NONE: Self = Self {
        read_ordinal: 0,
        checkpoint_digest: [0; 32],
        fault: FaultProgress::NONE,
    };

    /// Constructs inherited progress from durable step state.
    #[must_use]
    pub const fn new(read_ordinal: u64, checkpoint_digest: [u8; 32], fault: FaultProgress) -> Self {
        Self {
            read_ordinal,
            checkpoint_digest,
            fault,
        }
    }

    /// Returns the stable reader ordinal the next chunk continues from.
    #[must_use]
    pub const fn read_ordinal(self) -> u64 {
        self.read_ordinal
    }

    /// Returns the digest of the last committed checkpoint.
    ///
    /// Retry keys are derived from this generation, so an inherited digest
    /// makes a reserved ordinal resumable after restart.
    #[must_use]
    pub const fn checkpoint_digest(self) -> [u8; 32] {
        self.checkpoint_digest
    }

    /// Returns the inherited committed fault-tolerance totals.
    #[must_use]
    pub const fn fault(self) -> FaultProgress {
        self.fault
    }
}

/// Begins isolated adapter-owned chunk transactions.
pub trait ChunkTransactionManager: Send + Sync {
    /// Starts one transaction for a bounded chunk attempt.
    fn begin(&self)
    -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>>;

    /// Returns the durable progress this step attempt inherits.
    ///
    /// The default suits managers without durable state. A durable adapter
    /// overrides it and fails closed rather than restarting bounded policy
    /// limits from zero.
    fn inherited_progress(
        &self,
        _context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<InheritedStepProgress, ChunkTransactionError>> {
        Box::pin(std::future::ready(Ok(InheritedStepProgress::NONE)))
    }

    /// Starts one transaction bound to a durable repository execution.
    ///
    /// The default preserves managers that do not need repository identity.
    /// Durable adapters override this method and reject unbound
    /// [`Self::begin`] calls rather than guessing an execution target.
    fn begin_for(
        &self,
        _context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        self.begin()
    }
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

/// A shared asynchronous batch writer.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item writer for `{I}`",
    label = "this component cannot write `{I}`",
    note = "implement `ItemWriter<{I}>` with `async fn write(&self, items: &[{I}], context: WriteContext<'_>)`",
    note = "a writer that delegates to another component must tie its lifetimes: `async fn write<'a>(&'a self, items: &'a [{I}], context: WriteContext<'a>)`"
)]
pub trait ItemWriter<I>: Send + Sync {
    /// Writes one borrowed, nonempty batch.
    ///
    /// A durable step supplies an enlisted transaction in `context`, borrowed
    /// for exactly this call.
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> impl Future<Output = Result<WriteOutcome, WriterError>> + Send + 'a;
}

/// The dyn-compatible mirror of the public component contract.
///
/// Nothing here is exported. Its only implementors are the blanket impls
/// below, so no external crate can observe or depend on this shape, and the
/// single `Box::pin` per call is the only boxing this item-component erasure
/// boundary introduces. Other ADR-0002 extension points — tasklets,
/// transactions, listeners, and the rest — remain boxed by design and are
/// unaffected by and unrelated to this module.
mod sealed {
    use super::{ItemProcessor, ItemReader, ItemWriter};
    use crate::{
        BoxFuture, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
        ReaderError, WriteContext, WriteOutcome, WriterError,
    };

    pub trait ReaderObject<I>: Send {
        fn read_boxed<'a>(
            &'a mut self,
            context: ReadContext<'a>,
        ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>>;
    }

    impl<I, R: ItemReader<I>> ReaderObject<I> for R {
        fn read_boxed<'a>(
            &'a mut self,
            context: ReadContext<'a>,
        ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>> {
            Box::pin(self.read(context))
        }
    }

    pub trait ProcessorObject<I, O>: Send + Sync {
        fn process_boxed<'a>(
            &'a self,
            item: &'a I,
            context: ProcessContext<'a>,
        ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>>;
    }

    impl<I, O, P: ItemProcessor<I, O>> ProcessorObject<I, O> for P {
        fn process_boxed<'a>(
            &'a self,
            item: &'a I,
            context: ProcessContext<'a>,
        ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>> {
            Box::pin(self.process(item, context))
        }
    }

    pub trait WriterObject<I>: Send + Sync {
        fn write_boxed<'a>(
            &'a self,
            items: &'a [I],
            context: WriteContext<'a>,
        ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>>;
    }

    impl<I, W: ItemWriter<I>> WriterObject<I> for W {
        fn write_boxed<'a>(
            &'a self,
            items: &'a [I],
            context: WriteContext<'a>,
        ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
            Box::pin(self.write(items, context))
        }
    }
}

/// A reader of any concrete type, behind one dynamic dispatch.
///
/// Constructing one is the explicit, greppable point where the pipeline stops
/// being monomorphized and starts paying a boxed future per call.
pub struct BoxedReader<I>(Box<dyn sealed::ReaderObject<I>>);

impl<I> BoxedReader<I> {
    /// Erases a concrete reader.
    pub fn new<R: ItemReader<I> + 'static>(reader: R) -> Self {
        Self(Box::new(reader))
    }
}

impl<I> ItemReader<I> for BoxedReader<I> {
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a {
        self.0.read_boxed(context)
    }
}

/// A processor of any concrete type, behind one dynamic dispatch.
pub struct BoxedProcessor<I, O>(Box<dyn sealed::ProcessorObject<I, O>>);

impl<I, O> BoxedProcessor<I, O> {
    /// Erases a concrete processor.
    pub fn new<P: ItemProcessor<I, O> + 'static>(processor: P) -> Self {
        Self(Box::new(processor))
    }
}

impl<I, O> ItemProcessor<I, O> for BoxedProcessor<I, O> {
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> impl Future<Output = Result<ProcessOutcome<O>, ProcessorError>> + Send + 'a {
        self.0.process_boxed(item, context)
    }
}

/// A writer of any concrete type, behind one dynamic dispatch.
pub struct BoxedWriter<I>(Box<dyn sealed::WriterObject<I>>);

impl<I> BoxedWriter<I> {
    /// Erases a concrete writer.
    pub fn new<W: ItemWriter<I> + 'static>(writer: W) -> Self {
        Self(Box::new(writer))
    }
}

impl<I> ItemWriter<I> for BoxedWriter<I> {
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> impl Future<Output = Result<WriteOutcome, WriterError>> + Send + 'a {
        self.0.write_boxed(items, context)
    }
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
    (
        $name:ident,
        $message:literal
        $(, $field:ident : $field_type:ty = $field_default:expr, $field_docs:literal)* $(,)?
    ) => {
        #[doc = $message]
        ///
        /// The adapter translates its own typed error into a stable
        /// [`FailureCategory`] at this boundary. The payload, display text, and
        /// source chain are dropped, so classification never inspects them.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name {
            category: FailureCategory,
            $(
                #[doc = $field_docs]
                $field: $field_type,
            )*
        }

        impl $name {
            /// Constructs a value-redacted [`FailureCategory::UserComponent`]
            /// failure.
            #[must_use]
            pub const fn new() -> Self {
                Self {
                    category: FailureCategory::UserComponent,
                    $($field: $field_default,)*
                }
            }

            /// Constructs a failure that declares its own stable category.
            ///
            /// A category that is not policy-eligible fails closed: the fault
            /// is never retried or skipped.
            #[must_use]
            pub const fn with_category(category: FailureCategory) -> Self {
                Self {
                    category,
                    $($field: $field_default,)*
                }
            }

            /// Classifies an arbitrary user error without retaining its
            /// payload or display text.
            #[must_use]
            pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
                drop(error);
                Self::new()
            }

            /// Returns the stable category supplied by the adapter.
            #[must_use]
            pub const fn category(self) -> FailureCategory {
                self.category
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
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

component_error!(
    ReaderError,
    "item reader failed",
    checkpoint_advanced: bool = false,
    "Whether the reader proved its checkpoint moved past one failed input.",
);
component_error!(ProcessorError, "item processor failed");
component_error!(
    WriterError,
    "item writer failed",
    rolled_back_output: Option<usize> = None,
    "The located, known-rolled-back output index, when the writer supplied one.",
);
component_error!(ChunkCompletionError, "chunk completion callback failed");

impl ReaderError {
    /// Records that the reader moved its checkpoint past exactly one failed
    /// input.
    ///
    /// A read skip requires this proof. Without it a repeated failure at the
    /// same position fails the step instead of skipping forever.
    #[must_use]
    pub const fn with_checkpoint_advanced(mut self, advanced: bool) -> Self {
        self.checkpoint_advanced = advanced;
        self
    }

    /// Returns whether the reader proved forward checkpoint progress.
    #[must_use]
    pub const fn has_checkpoint_advanced(self) -> bool {
        self.checkpoint_advanced
    }
}

impl WriterError {
    /// Records that the batch is known to have rolled back and identifies the
    /// single failed output by its zero-based index in the supplied batch.
    ///
    /// A write skip requires this evidence. An unlocated, partially visible, or
    /// ambiguous write cannot be skipped.
    #[must_use]
    pub const fn with_rolled_back_output(mut self, index: usize) -> Self {
        self.rolled_back_output = Some(index);
        self
    }

    /// Returns the located failed output index, when the writer supplied one.
    #[must_use]
    pub const fn rolled_back_output(self) -> Option<usize> {
        self.rolled_back_output
    }
}
