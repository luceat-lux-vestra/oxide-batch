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
    BoxFuture, ChunkAttemptOutcome, ChunkCount, ChunkListener, ChunkListenerContext,
    ChunkListenerError, ComponentStateEnvelope, FailureCategory, ItemProcessor, ItemReader,
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
    /// [`oxide_batch::ChunkListener::before_chunk`], immediately before commit.
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
/// Each lifecycle point fires at most once (the contract already calls each
/// method at most once per step attempt).
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

/// What an injected [`ChunkListener::before_chunk`] pre-commit call does once
/// fired.
#[non_exhaustive]
pub enum PreCommitAction {
    /// Fails the chunk listener boundary, which forces a rollback before
    /// commit.
    Fail,
    /// Panics through the real production panic boundary.
    Panic,
}

/// A [`ChunkListener`] that fails or panics immediately before the chunk
/// whose sequence matches `at`, and otherwise observes silently.
///
/// This is the pre-commit failure point: `before_chunk` runs before the
/// adapter's transaction commits, so a fired injection here proves no
/// business, checkpoint, or component-state write became durable.
pub struct InjectedPreCommit {
    at: ChunkCount,
    action: PreCommitAction,
    id: InjectionId,
    log: InjectionLog,
}

impl InjectedPreCommit {
    /// Injects `action` immediately before the chunk attempt numbered `at`.
    #[must_use]
    pub const fn new(
        at: ChunkCount,
        action: PreCommitAction,
        id: InjectionId,
        log: InjectionLog,
    ) -> Self {
        Self {
            at,
            action,
            id,
            log,
        }
    }
}

impl ChunkListener for InjectedPreCommit {
    fn before_chunk<'a>(
        &'a self,
        context: ChunkListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        Box::pin(async move {
            if context.sequence() == self.at {
                match self.action {
                    PreCommitAction::Fail => {
                        self.log.record(InjectionEvent::new(
                            self.id,
                            InjectionPoint::PreCommit,
                            InjectionEffect::Failed,
                        ));
                        return Err(ChunkListenerError::new());
                    }
                    PreCommitAction::Panic => {
                        record_and_panic(&self.log, self.id, InjectionPoint::PreCommit);
                    }
                }
            }
            Ok(())
        })
    }

    fn after_chunk<'a>(
        &'a self,
        _context: ChunkListenerContext<'a>,
        _outcome: ChunkAttemptOutcome,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        Box::pin(async { Ok(()) })
    }
}
