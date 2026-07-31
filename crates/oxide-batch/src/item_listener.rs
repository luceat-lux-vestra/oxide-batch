//! Item, retry, and skip listener contracts for the M3 chunk slice.
//!
//! These listeners are authoritative interceptors. A before failure prevents
//! its component call, and an after, error, retry, or skip failure prevents an
//! uncommitted chunk from committing. Every already-entered reverse callback
//! still runs so failures can be aggregated, and a panic is classified exactly
//! like a returned error.
//!
//! [`ItemListenerSet`] owns the ordering, nesting, aggregation, and panic
//! rules. It performs no repository, transaction, or component work, so the
//! chunk runtime keeps sole ownership of policy and commit decisions.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures_util::FutureExt;

use crate::{
    BoxFuture, ChunkCount, ExecutionCorrelation, FaultDescriptor, ListenerError,
    ListenerFailureKind, StopToken,
};

/// The largest accepted number of listeners in one family.
const MAX_LISTENERS: usize = 32;

fn ready_ok<'a>() -> BoxFuture<'a, Result<(), ListenerError>> {
    Box::pin(std::future::ready(Ok(())))
}

/// Read-only execution data supplied at an item, retry, or skip callback.
///
/// The context deliberately excludes job parameters, execution-context values,
/// and durable state so a listener cannot copy them into diagnostics.
#[derive(Clone, Copy, Debug)]
pub struct ItemListenerContext<'a> {
    correlation: &'a ExecutionCorrelation,
    chunk_sequence: ChunkCount,
    stop: &'a StopToken,
}

impl<'a> ItemListenerContext<'a> {
    /// Constructs the borrowed callback scope.
    #[must_use]
    pub const fn new(
        correlation: &'a ExecutionCorrelation,
        chunk_sequence: ChunkCount,
        stop: &'a StopToken,
    ) -> Self {
        Self {
            correlation,
            chunk_sequence,
            stop,
        }
    }

    /// Borrows the bounded execution correlation.
    #[must_use]
    pub const fn correlation(self) -> &'a ExecutionCorrelation {
        self.correlation
    }

    /// Returns the zero-based chunk sequence within the step attempt.
    #[must_use]
    pub const fn chunk_sequence(self) -> ChunkCount {
        self.chunk_sequence
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// The result visible to a retry-completion callback.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RetryOutcome {
    /// A re-invocation succeeded.
    Recovered,
    /// A re-invocation failed again.
    Failed,
}

/// Observes reader invocations for one item.
pub trait ReadListener<I>: Send + Sync {
    /// Runs in registration order before the reader is invoked.
    fn before_read<'a>(
        &'a self,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = context;
        ready_ok()
    }

    /// Runs in reverse registration order after one item is read.
    fn after_read<'a>(
        &'a self,
        item: &'a I,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (item, context);
        ready_ok()
    }

    /// Runs in reverse registration order for every failed reader call.
    ///
    /// This includes a call that a later retry completes successfully.
    fn on_read_error<'a>(
        &'a self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (fault, context);
        ready_ok()
    }
}

/// Observes processor invocations for one item.
pub trait ProcessListener<I, O>: Send + Sync {
    /// Runs in registration order before the processor is invoked.
    fn before_process<'a>(
        &'a self,
        input: &'a I,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (input, context);
        ready_ok()
    }

    /// Runs in reverse registration order after the processor returns.
    ///
    /// A filtered input has no output.
    fn after_process<'a>(
        &'a self,
        input: &'a I,
        output: Option<&'a O>,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (input, output, context);
        ready_ok()
    }

    /// Runs in reverse registration order for every failed processor call.
    fn on_process_error<'a>(
        &'a self,
        input: &'a I,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (input, fault, context);
        ready_ok()
    }
}

/// Observes writer invocations for one output batch.
pub trait WriteListener<O>: Send + Sync {
    /// Runs in registration order before the writer is invoked.
    fn before_write<'a>(
        &'a self,
        outputs: &'a [O],
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (outputs, context);
        ready_ok()
    }

    /// Runs in reverse registration order after the batch is accepted.
    fn after_write<'a>(
        &'a self,
        outputs: &'a [O],
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (outputs, context);
        ready_ok()
    }

    /// Runs in reverse registration order for every failed writer call.
    fn on_write_error<'a>(
        &'a self,
        outputs: &'a [O],
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (outputs, fault, context);
        ready_ok()
    }
}

/// Observes the retry scope around one failed invocation.
pub trait RetryListener: Send + Sync {
    /// Runs in registration order after reservation and before backoff.
    fn before_retry<'a>(
        &'a self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (fault, context);
        ready_ok()
    }

    /// Runs in reverse registration order after a re-invocation completes.
    fn after_retry<'a>(
        &'a self,
        fault: FaultDescriptor,
        outcome: RetryOutcome,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (fault, outcome, context);
        ready_ok()
    }

    /// Runs in reverse registration order once the retry budget is spent.
    fn on_retry_exhausted<'a>(
        &'a self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (fault, context);
        ready_ok()
    }
}

/// Confirms one accepted skip immediately before the accepting commit.
///
/// A skip callback runs at most once in one chunk attempt. A known rollback or
/// a crash may cause another invocation on replay, so only work enlisted in
/// the accepting transaction has exactly one committed effect.
pub trait SkipListener<I, O>: Send + Sync {
    /// Confirms a skipped read.
    fn on_skip_in_read<'a>(
        &'a self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (fault, context);
        ready_ok()
    }

    /// Confirms a skipped input.
    fn on_skip_in_process<'a>(
        &'a self,
        input: &'a I,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (input, fault, context);
        ready_ok()
    }

    /// Confirms a skipped output.
    fn on_skip_in_write<'a>(
        &'a self,
        output: &'a O,
        fault: FaultDescriptor,
        context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let _ = (output, fault, context);
        ready_ok()
    }
}

/// The callback boundary where an item, retry, or skip listener failed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ItemListenerPhase {
    /// A read before-callback.
    BeforeRead,
    /// A read after-callback.
    AfterRead,
    /// A read error callback.
    ReadError,
    /// A process before-callback.
    BeforeProcess,
    /// A process after-callback.
    AfterProcess,
    /// A process error callback.
    ProcessError,
    /// A write before-callback.
    BeforeWrite,
    /// A write after-callback.
    AfterWrite,
    /// A write error callback.
    WriteError,
    /// A retry before-callback.
    BeforeRetry,
    /// A retry completion callback.
    AfterRetry,
    /// A retry exhaustion callback.
    RetryExhausted,
    /// A skip confirmation callback.
    Skip,
}

impl ItemListenerPhase {
    /// Returns the stable low-cardinality telemetry name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeRead => "before_read",
            Self::AfterRead => "after_read",
            Self::ReadError => "read_error",
            Self::BeforeProcess => "before_process",
            Self::AfterProcess => "after_process",
            Self::ProcessError => "process_error",
            Self::BeforeWrite => "before_write",
            Self::AfterWrite => "after_write",
            Self::WriteError => "write_error",
            Self::BeforeRetry => "before_retry",
            Self::AfterRetry => "after_retry",
            Self::RetryExhausted => "retry_exhausted",
            Self::Skip => "skip",
        }
    }
}

impl fmt::Display for ItemListenerPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One value-redacted item, retry, or skip listener failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ItemListenerFailure {
    phase: ItemListenerPhase,
    registration_index: usize,
    kind: ListenerFailureKind,
}

impl ItemListenerFailure {
    /// Constructs a classified callback failure.
    #[must_use]
    pub const fn new(
        phase: ItemListenerPhase,
        registration_index: usize,
        kind: ListenerFailureKind,
    ) -> Self {
        Self {
            phase,
            registration_index,
            kind,
        }
    }

    /// Returns the callback boundary.
    #[must_use]
    pub const fn phase(self) -> ItemListenerPhase {
        self.phase
    }

    /// Returns the zero-based listener registration index.
    #[must_use]
    pub const fn registration_index(self) -> usize {
        self.registration_index
    }

    /// Returns whether the boundary returned an error or panicked.
    #[must_use]
    pub const fn kind(self) -> ListenerFailureKind {
        self.kind
    }
}

/// The outcome of one registration-order before-callback pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BeforeCallbackOutcome {
    entered: usize,
    failure: Option<ItemListenerFailure>,
}

impl BeforeCallbackOutcome {
    /// Returns how many listeners completed their before callback.
    ///
    /// The matching reverse pass runs exactly these listeners, so a listener
    /// that failed or never ran receives no completion callback.
    #[must_use]
    pub const fn entered(self) -> usize {
        self.entered
    }

    /// Returns the first failure, when the pass stopped early.
    #[must_use]
    pub const fn failure(self) -> Option<ItemListenerFailure> {
        self.failure
    }

    /// Returns whether every registered listener completed.
    #[must_use]
    pub const fn is_ok(self) -> bool {
        self.failure.is_none()
    }
}

/// Failure to register a bounded listener family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ItemListenerError {
    /// One listener family exceeded its registration bound.
    TooManyListeners {
        /// The largest accepted number of listeners in one family.
        max: usize,
    },
}

impl fmt::Display for ItemListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyListeners { max } => {
                write!(formatter, "a listener family exceeds {max} listeners")
            }
        }
    }
}

impl std::error::Error for ItemListenerError {}

/// A bounded, ordered registration of the M3 item listener families.
///
/// ```
/// use std::sync::Arc;
///
/// use oxide_batch::{
///     BoxFuture, ItemListenerContext, ItemListenerSet, ListenerError, ReadListener,
/// };
///
/// struct AuditReads;
///
/// impl ReadListener<u32> for AuditReads {
///     fn after_read<'a>(
///         &'a self,
///         item: &'a u32,
///         _context: ItemListenerContext<'a>,
///     ) -> BoxFuture<'a, Result<(), ListenerError>> {
///         let accepted = *item < 1_000;
///         Box::pin(async move {
///             if accepted {
///                 Ok(())
///             } else {
///                 Err(ListenerError::new())
///             }
///         })
///     }
/// }
///
/// let listeners = ItemListenerSet::<u32, String>::new()
///     .with_read_listener(Arc::new(AuditReads))?;
/// assert_eq!(listeners.read_listeners(), 1);
/// # Ok::<(), oxide_batch::ItemListenerError>(())
/// ```
pub struct ItemListenerSet<I, O> {
    read: Vec<Arc<dyn ReadListener<I>>>,
    process: Vec<Arc<dyn ProcessListener<I, O>>>,
    write: Vec<Arc<dyn WriteListener<O>>>,
    retry: Vec<Arc<dyn RetryListener>>,
    skip: Vec<Arc<dyn SkipListener<I, O>>>,
}

impl<I, O> ItemListenerSet<I, O> {
    /// The largest accepted number of listeners in one family.
    pub const MAX_LISTENERS: usize = MAX_LISTENERS;

    /// Constructs an empty registration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            read: Vec::new(),
            process: Vec::new(),
            write: Vec::new(),
            retry: Vec::new(),
            skip: Vec::new(),
        }
    }

    /// Registers one read listener.
    ///
    /// # Errors
    ///
    /// Returns [`ItemListenerError::TooManyListeners`] beyond the bound.
    pub fn with_read_listener(
        mut self,
        listener: Arc<dyn ReadListener<I>>,
    ) -> Result<Self, ItemListenerError> {
        push_bounded(&mut self.read, listener)?;
        Ok(self)
    }

    /// Registers one process listener.
    ///
    /// # Errors
    ///
    /// Returns [`ItemListenerError::TooManyListeners`] beyond the bound.
    pub fn with_process_listener(
        mut self,
        listener: Arc<dyn ProcessListener<I, O>>,
    ) -> Result<Self, ItemListenerError> {
        push_bounded(&mut self.process, listener)?;
        Ok(self)
    }

    /// Registers one write listener.
    ///
    /// # Errors
    ///
    /// Returns [`ItemListenerError::TooManyListeners`] beyond the bound.
    pub fn with_write_listener(
        mut self,
        listener: Arc<dyn WriteListener<O>>,
    ) -> Result<Self, ItemListenerError> {
        push_bounded(&mut self.write, listener)?;
        Ok(self)
    }

    /// Registers one retry listener.
    ///
    /// # Errors
    ///
    /// Returns [`ItemListenerError::TooManyListeners`] beyond the bound.
    pub fn with_retry_listener(
        mut self,
        listener: Arc<dyn RetryListener>,
    ) -> Result<Self, ItemListenerError> {
        push_bounded(&mut self.retry, listener)?;
        Ok(self)
    }

    /// Registers one skip listener.
    ///
    /// # Errors
    ///
    /// Returns [`ItemListenerError::TooManyListeners`] beyond the bound.
    pub fn with_skip_listener(
        mut self,
        listener: Arc<dyn SkipListener<I, O>>,
    ) -> Result<Self, ItemListenerError> {
        push_bounded(&mut self.skip, listener)?;
        Ok(self)
    }

    /// Returns whether no listener is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read.is_empty()
            && self.process.is_empty()
            && self.write.is_empty()
            && self.retry.is_empty()
            && self.skip.is_empty()
    }

    /// Returns the number of registered read listeners.
    #[must_use]
    pub fn read_listeners(&self) -> usize {
        self.read.len()
    }

    /// Returns the number of registered process listeners.
    #[must_use]
    pub fn process_listeners(&self) -> usize {
        self.process.len()
    }

    /// Returns the number of registered write listeners.
    #[must_use]
    pub fn write_listeners(&self) -> usize {
        self.write.len()
    }

    /// Returns the number of registered retry listeners.
    #[must_use]
    pub fn retry_listeners(&self) -> usize {
        self.retry.len()
    }

    /// Returns the number of registered skip listeners.
    #[must_use]
    pub fn skip_listeners(&self) -> usize {
        self.skip.len()
    }
}

/// The callback passes borrow items across `await`, so both item types must be
/// shareable. Registration and inspection remain available for any item type.
impl<I, O> ItemListenerSet<I, O>
where
    I: Sync,
    O: Sync,
{
    /// Runs read before-callbacks in registration order.
    pub async fn before_read(&self, context: ItemListenerContext<'_>) -> BeforeCallbackOutcome {
        forward_pass(self.read.len(), ItemListenerPhase::BeforeRead, |index| {
            Box::pin(invoke(move || self.read[index].before_read(context)))
        })
        .await
    }

    /// Runs read after-callbacks for `entered` listeners in reverse order.
    pub async fn after_read(
        &self,
        entered: usize,
        item: &I,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.read.len()),
            ItemListenerPhase::AfterRead,
            |index| Box::pin(invoke(move || self.read[index].after_read(item, context))),
        )
        .await
    }

    /// Runs read error callbacks for `entered` listeners in reverse order.
    pub async fn on_read_error(
        &self,
        entered: usize,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.read.len()),
            ItemListenerPhase::ReadError,
            |index| {
                Box::pin(invoke(move || {
                    self.read[index].on_read_error(fault, context)
                }))
            },
        )
        .await
    }

    /// Runs process before-callbacks in registration order.
    pub async fn before_process(
        &self,
        input: &I,
        context: ItemListenerContext<'_>,
    ) -> BeforeCallbackOutcome {
        forward_pass(
            self.process.len(),
            ItemListenerPhase::BeforeProcess,
            |index| {
                Box::pin(invoke(move || {
                    self.process[index].before_process(input, context)
                }))
            },
        )
        .await
    }

    /// Runs process after-callbacks for `entered` listeners in reverse order.
    pub async fn after_process(
        &self,
        entered: usize,
        input: &I,
        output: Option<&O>,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.process.len()),
            ItemListenerPhase::AfterProcess,
            |index| {
                Box::pin(invoke(move || {
                    self.process[index].after_process(input, output, context)
                }))
            },
        )
        .await
    }

    /// Runs process error callbacks for `entered` listeners in reverse order.
    pub async fn on_process_error(
        &self,
        entered: usize,
        input: &I,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.process.len()),
            ItemListenerPhase::ProcessError,
            |index| {
                Box::pin(invoke(move || {
                    self.process[index].on_process_error(input, fault, context)
                }))
            },
        )
        .await
    }

    /// Runs write before-callbacks in registration order.
    pub async fn before_write(
        &self,
        outputs: &[O],
        context: ItemListenerContext<'_>,
    ) -> BeforeCallbackOutcome {
        forward_pass(self.write.len(), ItemListenerPhase::BeforeWrite, |index| {
            Box::pin(invoke(move || {
                self.write[index].before_write(outputs, context)
            }))
        })
        .await
    }

    /// Runs write after-callbacks for `entered` listeners in reverse order.
    pub async fn after_write(
        &self,
        entered: usize,
        outputs: &[O],
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.write.len()),
            ItemListenerPhase::AfterWrite,
            |index| {
                Box::pin(invoke(move || {
                    self.write[index].after_write(outputs, context)
                }))
            },
        )
        .await
    }

    /// Runs write error callbacks for `entered` listeners in reverse order.
    pub async fn on_write_error(
        &self,
        entered: usize,
        outputs: &[O],
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.write.len()),
            ItemListenerPhase::WriteError,
            |index| {
                Box::pin(invoke(move || {
                    self.write[index].on_write_error(outputs, fault, context)
                }))
            },
        )
        .await
    }

    /// Runs retry before-callbacks in registration order.
    pub async fn before_retry(
        &self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> BeforeCallbackOutcome {
        forward_pass(self.retry.len(), ItemListenerPhase::BeforeRetry, |index| {
            Box::pin(invoke(move || {
                self.retry[index].before_retry(fault, context)
            }))
        })
        .await
    }

    /// Runs retry completion callbacks for `entered` listeners in reverse order.
    pub async fn after_retry(
        &self,
        entered: usize,
        fault: FaultDescriptor,
        outcome: RetryOutcome,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.retry.len()),
            ItemListenerPhase::AfterRetry,
            |index| {
                Box::pin(invoke(move || {
                    self.retry[index].after_retry(fault, outcome, context)
                }))
            },
        )
        .await
    }

    /// Runs retry exhaustion callbacks for `entered` listeners in reverse order.
    pub async fn on_retry_exhausted(
        &self,
        entered: usize,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        reverse_pass(
            entered.min(self.retry.len()),
            ItemListenerPhase::RetryExhausted,
            |index| {
                Box::pin(invoke(move || {
                    self.retry[index].on_retry_exhausted(fault, context)
                }))
            },
        )
        .await
    }

    /// Confirms a skipped read in registration order.
    pub async fn on_skip_in_read(
        &self,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        ordered_pass(self.skip.len(), |index| {
            Box::pin(invoke(move || {
                self.skip[index].on_skip_in_read(fault, context)
            }))
        })
        .await
    }

    /// Confirms a skipped input in registration order.
    pub async fn on_skip_in_process(
        &self,
        input: &I,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        ordered_pass(self.skip.len(), |index| {
            Box::pin(invoke(move || {
                self.skip[index].on_skip_in_process(input, fault, context)
            }))
        })
        .await
    }

    /// Confirms a skipped output in registration order.
    pub async fn on_skip_in_write(
        &self,
        output: &O,
        fault: FaultDescriptor,
        context: ItemListenerContext<'_>,
    ) -> Vec<ItemListenerFailure> {
        ordered_pass(self.skip.len(), |index| {
            Box::pin(invoke(move || {
                self.skip[index].on_skip_in_write(output, fault, context)
            }))
        })
        .await
    }
}

impl<I, O> Default for ItemListenerSet<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Clone for ItemListenerSet<I, O> {
    fn clone(&self) -> Self {
        Self {
            read: self.read.clone(),
            process: self.process.clone(),
            write: self.write.clone(),
            retry: self.retry.clone(),
            skip: self.skip.clone(),
        }
    }
}

impl<I, O> fmt::Debug for ItemListenerSet<I, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ItemListenerSet")
            .field("read", &self.read.len())
            .field("process", &self.process.len())
            .field("write", &self.write.len())
            .field("retry", &self.retry.len())
            .field("skip", &self.skip.len())
            .finish()
    }
}

fn push_bounded<T>(listeners: &mut Vec<T>, listener: T) -> Result<(), ItemListenerError> {
    if listeners.len() == MAX_LISTENERS {
        return Err(ItemListenerError::TooManyListeners { max: MAX_LISTENERS });
    }
    listeners.push(listener);
    Ok(())
}

async fn invoke<'a, F>(make: F) -> Result<(), ListenerFailureKind>
where
    F: FnOnce() -> BoxFuture<'a, Result<(), ListenerError>>,
{
    let future = catch_unwind(AssertUnwindSafe(make)).map_err(|_| ListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ListenerFailureKind::Error),
        Err(_) => Err(ListenerFailureKind::Panic),
    }
}

async fn forward_pass<'a, F>(
    count: usize,
    phase: ItemListenerPhase,
    mut call: F,
) -> BeforeCallbackOutcome
where
    F: FnMut(usize) -> BoxFuture<'a, Result<(), ListenerFailureKind>>,
{
    for index in 0..count {
        if let Err(kind) = call(index).await {
            return BeforeCallbackOutcome {
                entered: index,
                failure: Some(ItemListenerFailure::new(phase, index, kind)),
            };
        }
    }
    BeforeCallbackOutcome {
        entered: count,
        failure: None,
    }
}

async fn reverse_pass<'a, F>(
    entered: usize,
    phase: ItemListenerPhase,
    mut call: F,
) -> Vec<ItemListenerFailure>
where
    F: FnMut(usize) -> BoxFuture<'a, Result<(), ListenerFailureKind>>,
{
    let mut failures = Vec::new();
    for index in (0..entered).rev() {
        if let Err(kind) = call(index).await {
            failures.push(ItemListenerFailure::new(phase, index, kind));
        }
    }
    failures
}

async fn ordered_pass<'a, F>(count: usize, mut call: F) -> Vec<ItemListenerFailure>
where
    F: FnMut(usize) -> BoxFuture<'a, Result<(), ListenerFailureKind>>,
{
    let mut failures = Vec::new();
    for index in 0..count {
        if let Err(kind) = call(index).await {
            failures.push(ItemListenerFailure::new(
                ItemListenerPhase::Skip,
                index,
                kind,
            ));
        }
    }
    failures
}
