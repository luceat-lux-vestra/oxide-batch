//! Failure, panic, and cooperative-stop injection at named lifecycle points.
//!
//! Every wrapper here delegates to a real component when its
//! [`Trigger`] has not fired, and only ever substitutes behavior at the exact
//! configured point -- it never reimplements reader/processor/writer/stream
//! logic. An injected failure/panic is always recorded in an [`InjectionLog`]
//! with its [`InjectionId`] before it takes effect, so a test can prove a
//! failure it observed was the one it injected rather than an unrelated
//! genuine framework defect (issue #145's own requirement).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use oxide_batch::{
    BoxFuture, BusinessTransaction, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress,
    ChunkTransaction, ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager,
    ComponentStateEnvelope, FailureCategory, InheritedStepProgress, ItemProcessor, ItemReader,
    ItemStream, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StopSource, StreamCloseContext, StreamCloseError, StreamCloseOutcome,
    StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamUpdateContext, StreamUpdateError,
    WriteContext, WriteOutcome, WriterError,
};

/// A bounded, test-owned marker distinguishing an injected failure from a
/// genuine framework defect.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct InjectionId(u64);

impl InjectionId {
    /// Constructs a marker from a caller-chosen value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw marker value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for InjectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "injection#{}", self.0)
    }
}

/// The lifecycle point an injected effect fired at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InjectionPoint {
    /// [`oxide_batch::ItemReader::read`].
    Read,
    /// [`oxide_batch::ItemProcessor::process`].
    Process,
    /// [`oxide_batch::ItemWriter::write`].
    Write,
    /// [`oxide_batch::ItemStream::open`].
    StreamOpen,
    /// [`oxide_batch::ItemStream::update`].
    StreamUpdate,
    /// [`oxide_batch::ItemStream::close`].
    StreamClose,
    /// [`oxide_batch::ChunkTransaction::commit`]/[`commit_with_component_state`](oxide_batch::ChunkTransaction::commit_with_component_state).
    PreCommit,
}

/// The effect an injected trigger produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InjectionEffect {
    /// A typed failure was returned.
    Failed,
    /// The component panicked.
    Panicked,
    /// Cooperative stop was requested.
    Stopped,
}

/// One recorded injection firing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InjectionEvent {
    id: InjectionId,
    point: InjectionPoint,
    effect: InjectionEffect,
}

impl InjectionEvent {
    const fn new(id: InjectionId, point: InjectionPoint, effect: InjectionEffect) -> Self {
        Self { id, point, effect }
    }

    /// Returns the marker of the injection that fired.
    #[must_use]
    pub const fn id(self) -> InjectionId {
        self.id
    }

    /// Returns the lifecycle point the injection fired at.
    #[must_use]
    pub const fn point(self) -> InjectionPoint {
        self.point
    }

    /// Returns the effect the injection produced.
    #[must_use]
    pub const fn effect(self) -> InjectionEffect {
        self.effect
    }
}

/// A shared, append-only record of every injection that has fired.
///
/// Cloning shares the same underlying log, so a fixture can hand one log to
/// several wrapped components and inspect every firing together after a run.
#[derive(Clone, Debug, Default)]
pub struct InjectionLog {
    events: Arc<Mutex<Vec<InjectionEvent>>>,
}

impl InjectionLog {
    /// Builds an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, event: InjectionEvent) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }

    /// Returns a snapshot of every firing recorded so far, in firing order.
    #[must_use]
    pub fn events(&self) -> Vec<InjectionEvent> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Returns whether the given marker has fired.
    #[must_use]
    pub fn fired(&self, id: InjectionId) -> bool {
        self.events().into_iter().any(|event| event.id() == id)
    }
}

/// A one-shot call-count trigger: fires exactly once, on the call whose
/// zero-based index equals the configured count.
///
/// `Trigger::immediately()` fires on the first call (before any item is
/// observed); `Trigger::after(n)` fires on the `(n + 1)`th call, i.e. after
/// `n` calls have already passed through untouched.
#[derive(Debug)]
pub struct Trigger {
    fire_at: u64,
    calls: AtomicU64,
}

impl Trigger {
    /// Fires after `count` untouched calls.
    #[must_use]
    pub const fn after(count: u64) -> Self {
        Self {
            fire_at: count,
            calls: AtomicU64::new(0),
        }
    }

    /// Fires on the very first call.
    #[must_use]
    pub const fn immediately() -> Self {
        Self::after(0)
    }

    fn should_fire(&self) -> bool {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        index == self.fire_at
    }
}

/// What an injected component call does once its [`Trigger`] fires.
#[non_exhaustive]
pub enum ComponentAction {
    /// Returns a value-redacted failure in the given stable category.
    Fail(FailureCategory),
    /// Panics through the real production panic boundary.
    Panic,
    /// Requests cooperative stop and returns the component's `Stopped`
    /// outcome, exactly as production cooperative-stop semantics require.
    Stop(StopSource),
}

fn record_and_panic(log: &InjectionLog, id: InjectionId, point: InjectionPoint) -> ! {
    log.record(InjectionEvent::new(id, point, InjectionEffect::Panicked));
    #[allow(
        clippy::panic,
        reason = "this function's entire purpose is production panic-boundary injection"
    )]
    {
        panic!("oxide-batch-test: injected panic ({id}) at {point:?}");
    }
}

/// An [`ItemReader`] that substitutes a configured [`ComponentAction`] once
/// its [`Trigger`] fires, and otherwise delegates to the wrapped reader.
pub struct InjectedReader<R> {
    inner: R,
    trigger: Trigger,
    action: ComponentAction,
    id: InjectionId,
    log: InjectionLog,
}

impl<R> InjectedReader<R> {
    /// Wraps `inner` with an injection that fires once its trigger matches.
    #[must_use]
    pub const fn new(
        inner: R,
        trigger: Trigger,
        action: ComponentAction,
        id: InjectionId,
        log: InjectionLog,
    ) -> Self {
        Self {
            inner,
            trigger,
            action,
            id,
            log,
        }
    }
}

impl<I: 'static, R: ItemReader<I>> ItemReader<I> for InjectedReader<R> {
    async fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> Result<ReadOutcome<I>, ReaderError> {
        if self.trigger.should_fire() {
            match &self.action {
                ComponentAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Read,
                        InjectionEffect::Failed,
                    ));
                    return Err(ReaderError::with_category(*category));
                }
                ComponentAction::Panic => {
                    record_and_panic(&self.log, self.id, InjectionPoint::Read);
                }
                ComponentAction::Stop(source) => {
                    source.request_stop();
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Read,
                        InjectionEffect::Stopped,
                    ));
                    return Ok(ReadOutcome::Stopped);
                }
            }
        }
        self.inner.read(context).await
    }
}

/// An [`ItemProcessor`] that substitutes a configured [`ComponentAction`]
/// once its [`Trigger`] fires, and otherwise delegates to the wrapped
/// processor.
pub struct InjectedProcessor<P> {
    inner: P,
    trigger: Trigger,
    action: ComponentAction,
    id: InjectionId,
    log: InjectionLog,
}

impl<P> InjectedProcessor<P> {
    /// Wraps `inner` with an injection that fires once its trigger matches.
    #[must_use]
    pub const fn new(
        inner: P,
        trigger: Trigger,
        action: ComponentAction,
        id: InjectionId,
        log: InjectionLog,
    ) -> Self {
        Self {
            inner,
            trigger,
            action,
            id,
            log,
        }
    }
}

impl<I: Sync + 'static, O: 'static, P: ItemProcessor<I, O>> ItemProcessor<I, O>
    for InjectedProcessor<P>
{
    async fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> Result<ProcessOutcome<O>, ProcessorError> {
        if self.trigger.should_fire() {
            match &self.action {
                ComponentAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Process,
                        InjectionEffect::Failed,
                    ));
                    return Err(ProcessorError::with_category(*category));
                }
                ComponentAction::Panic => {
                    record_and_panic(&self.log, self.id, InjectionPoint::Process);
                }
                ComponentAction::Stop(source) => {
                    source.request_stop();
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Process,
                        InjectionEffect::Stopped,
                    ));
                    return Ok(ProcessOutcome::Stopped);
                }
            }
        }
        self.inner.process(item, context).await
    }
}

/// An [`ItemWriter`] that substitutes a configured [`ComponentAction`] once
/// its [`Trigger`] fires, and otherwise delegates to the wrapped writer.
pub struct InjectedWriter<W> {
    inner: W,
    trigger: Trigger,
    action: ComponentAction,
    id: InjectionId,
    log: InjectionLog,
}

impl<W> InjectedWriter<W> {
    /// Wraps `inner` with an injection that fires once its trigger matches.
    #[must_use]
    pub const fn new(
        inner: W,
        trigger: Trigger,
        action: ComponentAction,
        id: InjectionId,
        log: InjectionLog,
    ) -> Self {
        Self {
            inner,
            trigger,
            action,
            id,
            log,
        }
    }
}

impl<I: Sync + 'static, W: ItemWriter<I>> ItemWriter<I> for InjectedWriter<W> {
    async fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        if self.trigger.should_fire() {
            match &self.action {
                ComponentAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Write,
                        InjectionEffect::Failed,
                    ));
                    return Err(WriterError::with_category(*category));
                }
                ComponentAction::Panic => {
                    record_and_panic(&self.log, self.id, InjectionPoint::Write);
                }
                ComponentAction::Stop(source) => {
                    source.request_stop();
                    self.log.record(InjectionEvent::new(
                        self.id,
                        InjectionPoint::Write,
                        InjectionEffect::Stopped,
                    ));
                    return Ok(WriteOutcome::Stopped);
                }
            }
        }
        self.inner.write(items, context).await
    }
}

/// What an injected [`ItemStream`] call does once fired: `Stop` does not
/// apply here because the stream contract has no stopped outcome.
#[non_exhaustive]
pub enum StreamAction {
    /// Returns a value-redacted failure in the given stable category.
    Fail(FailureCategory),
    /// Panics through the real production panic boundary.
    Panic,
}

/// An [`ItemStream`] that can fail or panic at `open`, `update`, and/or
/// `close`, independently, and otherwise delegates to the wrapped stream.
///
/// A configured `open`/`close` injection fires at most once, matching the
/// production contract's own at-most-once-per-attempt call sites for those
/// two methods. A configured `update` injection fires on *every* `update`
/// call: the production contract calls `update` once per *committing chunk
/// attempt*, not once per step attempt, so a multi-chunk step can call it
/// several times.
pub struct InjectedStream<S> {
    inner: S,
    open: Option<(StreamAction, InjectionId)>,
    update: Option<(StreamAction, InjectionId)>,
    close: Option<(StreamAction, InjectionId)>,
    log: InjectionLog,
}

impl<S> InjectedStream<S> {
    /// Wraps `inner` with no injected lifecycle points.
    #[must_use]
    pub const fn new(inner: S, log: InjectionLog) -> Self {
        Self {
            inner,
            open: None,
            update: None,
            close: None,
            log,
        }
    }

    /// Injects an action at `open`.
    #[must_use]
    pub fn with_open(mut self, action: StreamAction, id: InjectionId) -> Self {
        self.open = Some((action, id));
        self
    }

    /// Injects an action at `update`.
    #[must_use]
    pub fn with_update(mut self, action: StreamAction, id: InjectionId) -> Self {
        self.update = Some((action, id));
        self
    }

    /// Injects an action at `close`.
    #[must_use]
    pub fn with_close(mut self, action: StreamAction, id: InjectionId) -> Self {
        self.close = Some((action, id));
        self
    }
}

impl<S: ItemStream> ItemStream for InjectedStream<S> {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        if let Some((action, id)) = &self.open {
            match action {
                StreamAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        *id,
                        InjectionPoint::StreamOpen,
                        InjectionEffect::Failed,
                    ));
                    return Err(StreamOpenError::with_category(*category));
                }
                StreamAction::Panic => {
                    record_and_panic(&self.log, *id, InjectionPoint::StreamOpen);
                }
            }
        }
        self.inner.open(context).await
    }

    async fn update(
        &self,
        context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        if let Some((action, id)) = &self.update {
            match action {
                StreamAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        *id,
                        InjectionPoint::StreamUpdate,
                        InjectionEffect::Failed,
                    ));
                    return Err(StreamUpdateError::with_category(*category));
                }
                StreamAction::Panic => {
                    record_and_panic(&self.log, *id, InjectionPoint::StreamUpdate);
                }
            }
        }
        self.inner.update(context).await
    }

    async fn close(
        &self,
        context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        if let Some((action, id)) = &self.close {
            match action {
                StreamAction::Fail(category) => {
                    self.log.record(InjectionEvent::new(
                        *id,
                        InjectionPoint::StreamClose,
                        InjectionEffect::Failed,
                    ));
                    return Err(StreamCloseError::with_category(*category));
                }
                StreamAction::Panic => {
                    record_and_panic(&self.log, *id, InjectionPoint::StreamClose);
                }
            }
        }
        self.inner.close(context).await
    }
}

/// What an injected pre-commit call does once fired.
///
/// There is no `Panic` variant: unlike the reader/processor/writer/stream
/// boundary, the framework wraps no `catch_unwind` around
/// [`ChunkTransaction::commit`]/[`commit_with_component_state`](ChunkTransaction::commit_with_component_state) --
/// a `ChunkTransactionManager` is adapter-owned infrastructure, not a
/// panic-isolated user component, so a panic injected here would only ever
/// be a raw unwind, not a production panic-to-typed-failure conversion.
#[non_exhaustive]
pub enum PreCommitAction {
    /// Returns [`ChunkTransactionError::NotCommitted`] instead of calling
    /// through to the real commit, so the runtime rolls the attempt back.
    Fail,
}

/// A [`ChunkTransactionManager`] decorator that fails the commit of the
/// chunk attempt whose one-based begin ordinal matches `at`, and otherwise
/// delegates every call to the wrapped manager.
///
/// This is the real pre-commit failure point: unlike
/// [`ChunkListener::before_chunk`](oxide_batch::ChunkListener::before_chunk)
/// (which the production contract documents as running *before the
/// transaction begins* -- before the reader, processor, writer, or
/// `ItemStream::update` for that chunk ever run), this decorator only
/// intercepts [`ChunkTransaction::commit`]/[`commit_with_component_state`](ChunkTransaction::commit_with_component_state),
/// after the chunk's item work and candidate component-state envelope
/// already exist. A fired injection proves that candidate work is rolled
/// back and never durably committed -- not merely that the chunk never
/// started.
pub struct InjectedTransactions<M> {
    inner: M,
    at: u64,
    action: PreCommitAction,
    id: InjectionId,
    log: InjectionLog,
    begins: AtomicU64,
}

impl<M> InjectedTransactions<M> {
    /// Wraps `inner`, failing the commit of the `at`-th chunk attempt this
    /// manager begins (one-based, in `begin`/`begin_for` call order).
    #[must_use]
    pub const fn new(
        inner: M,
        at: u64,
        action: PreCommitAction,
        id: InjectionId,
        log: InjectionLog,
    ) -> Self {
        Self {
            inner,
            at,
            action,
            id,
            log,
            begins: AtomicU64::new(0),
        }
    }

    fn should_fire(&self) -> bool {
        let ordinal = self.begins.fetch_add(1, Ordering::SeqCst) + 1;
        ordinal == self.at
    }
}

impl<M: ChunkTransactionManager> ChunkTransactionManager for InjectedTransactions<M> {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let fire = self.should_fire();
        Box::pin(async move {
            let inner = self.inner.begin().await?;
            Ok(Box::new(InjectedTransaction {
                inner,
                fire,
                action: &self.action,
                id: self.id,
                log: &self.log,
            }) as Box<dyn ChunkTransaction>)
        })
    }

    fn begin_for(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let fire = self.should_fire();
        Box::pin(async move {
            let inner = self.inner.begin_for(context).await?;
            Ok(Box::new(InjectedTransaction {
                inner,
                fire,
                action: &self.action,
                id: self.id,
                log: &self.log,
            }) as Box<dyn ChunkTransaction>)
        })
    }

    fn inherited_progress(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<InheritedStepProgress, ChunkTransactionError>> {
        self.inner.inherited_progress(context)
    }

    fn inherited_component_state(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
        self.inner.inherited_component_state(context)
    }
}

struct InjectedTransaction<'a> {
    inner: Box<dyn ChunkTransaction + 'a>,
    fire: bool,
    action: &'a PreCommitAction,
    id: InjectionId,
    log: &'a InjectionLog,
}

impl ChunkTransaction for InjectedTransaction<'_> {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        self.inner.business_transaction()
    }

    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        if self.fire {
            let PreCommitAction::Fail = self.action;
            self.log.record(InjectionEvent::new(
                self.id,
                InjectionPoint::PreCommit,
                InjectionEffect::Failed,
            ));
            return Box::pin(async { Err(ChunkTransactionError::NotCommitted) });
        }
        self.inner.commit(counts, fault)
    }

    fn commit_with_component_state<'a>(
        &'a mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
        component_state: &'a [ComponentStateEnvelope],
    ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        if self.fire {
            let PreCommitAction::Fail = self.action;
            self.log.record(InjectionEvent::new(
                self.id,
                InjectionPoint::PreCommit,
                InjectionEffect::Failed,
            ));
            return Box::pin(async { Err(ChunkTransactionError::NotCommitted) });
        }
        self.inner
            .commit_with_component_state(counts, fault, component_state)
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        self.inner.rollback()
    }
}
