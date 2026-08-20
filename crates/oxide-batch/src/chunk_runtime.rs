//! Deterministic single-threaded chunk-step orchestration.

use std::fmt;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::runtime::{lower_one_step, one_step_node};
use crate::{
    BackoffOutcome, BoxFuture, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkFaultProgress,
    ChunkSize, ChunkTransaction, ChunkTransactionContext, ChunkTransactionError,
    ChunkTransactionManager, CompiledExecutionPlan, DefinitionError, DefinitionIdentity,
    DefinitionRevision, ExecutionCorrelation, FailureCategory, FailureId, FailureSummary,
    FaultDecision, FaultDescriptor, FaultEvidence, FaultPhase, FaultProgress, FaultRuntime,
    InFlightPolicy, InheritedStepProgress, ItemListenerContext, ItemListenerFailure,
    ItemListenerSet, ItemProcessor, ItemReader, ItemWriter, JobExecutionListener, JobLauncher,
    JobName, JobParameters, LaunchError, LaunchReport, LifecycleEventKind, ListenerFailureKind,
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    RetryCounts, RetryKey, RetryOrdinal, RetryOutcome, RetryReservation, RollbackDisposition,
    SkipCounts, StepComponents, StepExecutionListener, StepName, StopToken, Tasklet,
    TaskletContext, TaskletError, TaskletJob, TaskletOutcome, TaskletStep, WriteContext,
    WriteOutcome, WriterError,
};

/// A validated one-step chunk definition.
pub struct ChunkStep<I, O, R, P, W> {
    name: StepName,
    size: ChunkSize,
    reader: R,
    processor: P,
    writer: W,
    transactions: Arc<dyn ChunkTransactionManager>,
    completion: Arc<dyn ChunkCompletion>,
    listeners: Vec<Arc<dyn ChunkListener>>,
    step_listeners: Vec<Arc<dyn StepExecutionListener>>,
    item_listeners: ItemListenerSet<I, O>,
    fault: Option<FaultRuntime>,
    in_flight_policy: InFlightPolicy,
    definition_digest: [u8; 32],
}

impl<I, O, R, P, W> ChunkStep<I, O, R, P, W>
where
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    /// Constructs a chunk step from facade-owned component and transaction
    /// ports.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: StepName,
        size: ChunkSize,
        reader: R,
        processor: P,
        writer: W,
        transactions: Arc<dyn ChunkTransactionManager>,
        completion: Arc<dyn ChunkCompletion>,
    ) -> Self {
        Self {
            name,
            size,
            reader,
            processor,
            writer,
            transactions,
            completion,
            listeners: Vec::new(),
            step_listeners: Vec::new(),
            item_listeners: ItemListenerSet::new(),
            fault: None,
            in_flight_policy: InFlightPolicy::FinishChunk,
            definition_digest: [0; 32],
        }
    }

    /// Registers a chunk listener in deterministic before-order.
    #[must_use]
    pub fn with_chunk_listener(mut self, listener: Arc<dyn ChunkListener>) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Installs the authoritative item, retry, and skip listener families.
    ///
    /// The set replaces any previously installed families.
    #[must_use]
    pub fn with_item_listeners(mut self, listeners: ItemListenerSet<I, O>) -> Self {
        self.item_listeners = listeners;
        self
    }

    /// Installs bounded retry, backoff, skip, and rollback behavior.
    ///
    /// Without a fault runtime every component failure fails the step after a
    /// known rollback, which is the M2 behavior.
    #[must_use]
    pub fn with_fault_runtime(mut self, fault: FaultRuntime) -> Self {
        self.fault = Some(fault);
        self
    }

    /// Registers a step listener in deterministic before-order.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn StepExecutionListener>) -> Self {
        self.step_listeners.push(listener);
        self
    }

    /// Borrows the step name.
    #[must_use]
    pub const fn name(&self) -> &StepName {
        &self.name
    }

    /// Executes this step deterministically on the caller's async runtime.
    ///
    /// The reader is stateful, so the definition is mutably borrowed for the
    /// duration of the run. Only counts returned by successful transaction
    /// commits appear in the report. `correlation` identifies the execution for
    /// item, retry, and skip listeners; it never reaches a component.
    pub async fn execute(
        &mut self,
        correlation: &ExecutionCorrelation,
        stop: &StopToken,
    ) -> ChunkExecutionReport
    where
        I: Send + Sync,
        O: Send + Sync,
    {
        execute_chunk_step(self, correlation, stop, None, |_| {}).await
    }
}

impl<I, O, R, P, W> fmt::Debug for ChunkStep<I, O, R, P, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkStep")
            .field("name", &self.name)
            .field("size", &self.size)
            .field("chunk_listener_count", &self.listeners.len())
            .field("step_listener_count", &self.step_listeners.len())
            .finish_non_exhaustive()
    }
}

/// A validated single-step chunk job definition.
pub struct ChunkJob<I, O, R, P, W> {
    name: JobName,
    step_name: StepName,
    plan: CompiledExecutionPlan,
    tasklet: Arc<ChunkTasklet<I, O, R, P, W>>,
    step_listeners: Vec<Arc<dyn StepExecutionListener>>,
    listeners: Vec<Arc<dyn JobExecutionListener>>,
}

impl<I, O, R, P, W> ChunkJob<I, O, R, P, W> {
    /// Constructs a chunk job with explicit restart-relevant revisions.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError::ManifestEncoding`] if the bounded canonical
    /// manifest cannot be encoded, and
    /// [`DefinitionError::DeliveryModeMismatch`] when an installed fault
    /// runtime declares a different delivery mode than the restart contract.
    pub fn new(
        name: JobName,
        mut step: ChunkStep<I, O, R, P, W>,
        revision: DefinitionRevision,
        components: &ChunkComponentRevisions,
    ) -> Result<Self, DefinitionError>
    where
        R: ItemReader<I> + Send + 'static,
        P: ItemProcessor<I, O> + Send + 'static,
        W: ItemWriter<O> + Send + 'static,
    {
        if let Some(fault) = step.fault.as_ref()
            && fault.delivery_mode() != components.delivery_mode()
        {
            return Err(DefinitionError::DeliveryModeMismatch);
        }
        let step_name = step.name.clone();
        let definition =
            DefinitionIdentity::chunk(&name, &step_name, step.size, revision, components)?;
        step.in_flight_policy = components.in_flight_policy();
        step.definition_digest = *definition.manifest_digest();
        let mut node = one_step_node(
            &step_name,
            StepComponents::Chunk {
                size: step.size,
                revisions: Box::new(components.clone()),
            },
        )?;
        if let Some(fault) = step.fault.as_ref() {
            node = node.with_fault_policy(fault.policy().clone());
        }
        let plan = lower_one_step(definition, node)?;
        let step_listeners = step.step_listeners.clone();
        Ok(Self {
            name,
            step_name,
            plan,
            tasklet: Arc::new(ChunkTasklet::new(step)),
            step_listeners,
            listeners: Vec::new(),
        })
    }

    /// Borrows the in-memory compatibility plan this wrapper lowers into.
    ///
    /// The plan retains the wrapper's original manifest bytes, format, and
    /// fingerprint and records no durable flow decision.
    #[must_use]
    pub const fn compiled_plan(&self) -> &CompiledExecutionPlan {
        &self.plan
    }

    /// Borrows the exact restart-relevant definition identity.
    #[must_use]
    pub const fn definition_identity(&self) -> &DefinitionIdentity {
        self.plan.definition_identity()
    }

    /// Registers a job listener in deterministic before-order.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn JobExecutionListener>) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Borrows the job name.
    #[must_use]
    pub const fn name(&self) -> &JobName {
        &self.name
    }

    /// Borrows the chunk-step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }
}

impl<I, O, R, P, W> fmt::Debug for ChunkJob<I, O, R, P, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkJob")
            .field("name", &self.name)
            .field("step_name", &self.step_name)
            .field("definition", self.plan.definition_identity())
            .field("listener_count", &self.listeners.len())
            .field("step_listener_count", &self.step_listeners.len())
            .finish_non_exhaustive()
    }
}

impl crate::FlowJob {
    /// Binds a stateful chunk step to one compiled flow node.
    ///
    /// The chunk's size, component revisions, delivery mode, and fault policy
    /// must exactly match the immutable plan. The component is erased only at
    /// the existing tasklet composition boundary; item calls keep the accepted
    /// M3 boxed component contract.
    ///
    /// # Errors
    ///
    /// Returns [`crate::FlowJobError::ComponentMismatch`] when executable and
    /// manifest declarations differ, or the ordinary binding errors for an
    /// unknown, wrong-kind, duplicate, or differently named node.
    pub fn with_chunk_step<I, O, R, P, W>(
        mut self,
        node_id: crate::NodeId,
        mut step: ChunkStep<I, O, R, P, W>,
        revisions: &ChunkComponentRevisions,
    ) -> Result<Self, crate::FlowJobError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        R: ItemReader<I> + Send + 'static,
        P: ItemProcessor<I, O> + Send + 'static,
        W: ItemWriter<O> + Send + 'static,
    {
        let Some(crate::FlowNode::Step(compiled)) = self.compiled_plan().node(&node_id) else {
            return Err(crate::FlowJobError::WrongNodeKind { node: node_id });
        };
        let expected = StepComponents::Chunk {
            size: step.size,
            revisions: Box::new(revisions.clone()),
        };
        if compiled.step_name() != step.name()
            || compiled.components() != &expected
            || compiled.fault_policy() != step.fault.as_ref().map(FaultRuntime::policy)
        {
            return Err(crate::FlowJobError::ComponentMismatch { node: node_id });
        }
        step.definition_digest = *self.compiled_plan().fingerprint();
        step.in_flight_policy = revisions.in_flight_policy();
        let listeners = step.step_listeners.clone();
        let tasklet: Arc<dyn Tasklet> = Arc::new(ChunkTasklet::new(step));
        let mut tasklet_step = TaskletStep::new(compiled.step_name().clone(), tasklet);
        for listener in listeners {
            tasklet_step = tasklet_step.with_listener(listener);
        }
        self.bind_chunk_tasklet(node_id, tasklet_step)?;
        Ok(self)
    }
}

struct ChunkTasklet<I, O, R, P, W> {
    step: AsyncMutex<ChunkStep<I, O, R, P, W>>,
    last_report: Mutex<Option<ChunkExecutionReport>>,
}

impl<I, O, R, P, W> ChunkTasklet<I, O, R, P, W>
where
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    fn new(step: ChunkStep<I, O, R, P, W>) -> Self {
        Self {
            step: AsyncMutex::new(step),
            last_report: Mutex::new(None),
        }
    }

    fn take_last_report(&self) -> Option<ChunkExecutionReport> {
        self.last_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn clear_last_report(&self) {
        *self
            .last_report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl<I, O, R, P, W> Tasklet for ChunkTasklet<I, O, R, P, W>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    R: ItemReader<I> + Send + 'static,
    P: ItemProcessor<I, O> + Send + 'static,
    W: ItemWriter<O> + Send + 'static,
{
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let mut step = self.step.lock().await;
            let transaction_context = ChunkTransactionContext::new(
                context.job_execution_id(),
                context.step_execution_id(),
            );
            let report = execute_chunk_step(
                &mut step,
                context.correlation(),
                context.stop_token(),
                Some(transaction_context),
                |event| match event {
                    ChunkRuntimeEvent::Started(sequence) => {
                        context.emit_chunk_event(LifecycleEventKind::ChunkStarted, sequence);
                    }
                    ChunkRuntimeEvent::Committed(sequence) => {
                        context.emit_chunk_event(LifecycleEventKind::ChunkCommitted, sequence);
                    }
                    ChunkRuntimeEvent::RolledBack(sequence) => {
                        context.emit_chunk_event(LifecycleEventKind::ChunkRolledBack, sequence);
                    }
                    ChunkRuntimeEvent::Unknown(sequence) => {
                        context.emit_chunk_event(LifecycleEventKind::ChunkUnknown, sequence);
                    }
                    ChunkRuntimeEvent::Fault(fault) => context.emit_fault_event(&fault),
                },
            )
            .await;
            let outcome = match report.outcome() {
                ChunkExecutionOutcome::Completed => Ok(TaskletOutcome::Completed),
                ChunkExecutionOutcome::Stopped => Ok(TaskletOutcome::Stopped),
                ChunkExecutionOutcome::Failed(_) => Err(TaskletError::new()),
                ChunkExecutionOutcome::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            };
            if report.terminal_rollback {
                context.acknowledge_terminal_rollback();
            }
            *self
                .last_report
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
            outcome
        })
    }
}

/// Combined repository lifecycle and chunk-orchestration result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkLaunchReport {
    launch: LaunchReport,
    chunk: Option<ChunkExecutionReport>,
}

impl ChunkLaunchReport {
    /// Borrows the persisted job/step lifecycle result.
    #[must_use]
    pub const fn launch(&self) -> &LaunchReport {
        &self.launch
    }

    /// Borrows chunk evidence when user work reached the chunk step.
    ///
    /// A stop or before-listener failure can finish the launch without
    /// invoking the chunk body.
    #[must_use]
    pub const fn chunk(&self) -> Option<&ChunkExecutionReport> {
        self.chunk.as_ref()
    }
}

impl JobLauncher<'_> {
    /// Launches a stateful one-step chunk job through the existing repository,
    /// lifecycle-listener, and event contracts.
    ///
    /// The mutable job borrow prevents concurrent use of one stateful reader.
    ///
    /// # Errors
    ///
    /// Returns [`LaunchError`] when repository metadata cannot reach a final
    /// state. Component failures are persisted and returned in the reports.
    pub async fn launch_chunk<I, O, R, P, W>(
        &self,
        job: &mut ChunkJob<I, O, R, P, W>,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<ChunkLaunchReport, LaunchError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        R: ItemReader<I> + Send + 'static,
        P: ItemProcessor<I, O> + Send + 'static,
        W: ItemWriter<O> + Send + 'static,
    {
        job.tasklet.clear_last_report();
        let tasklet: Arc<dyn Tasklet> = job.tasklet.clone();
        let mut tasklet_step = TaskletStep::new(job.step_name.clone(), tasklet);
        for listener in &job.step_listeners {
            tasklet_step = tasklet_step.with_listener(Arc::clone(listener));
        }
        let mut tasklet_job =
            TaskletJob::from_lowered_plan(job.name.clone(), tasklet_step, job.plan.clone());
        for listener in &job.listeners {
            tasklet_job = tasklet_job.with_listener(Arc::clone(listener));
        }

        let launch = self.launch(&tasklet_job, parameters, stop).await?;
        let chunk = job.tasklet.take_last_report();
        Ok(ChunkLaunchReport { launch, chunk })
    }
}

/// Read-only state supplied at a chunk-listener boundary.
#[derive(Clone, Copy, Debug)]
pub struct ChunkListenerContext<'a> {
    sequence: ChunkCount,
    committed_counts: ChunkCounts,
    stop: &'a StopToken,
}

impl<'a> ChunkListenerContext<'a> {
    const fn new(sequence: ChunkCount, committed_counts: ChunkCounts, stop: &'a StopToken) -> Self {
        Self {
            sequence,
            committed_counts,
            stop,
        }
    }

    /// Returns the nonzero chunk-attempt sequence.
    #[must_use]
    pub const fn sequence(self) -> ChunkCount {
        self.sequence
    }

    /// Returns counts from chunks committed before this attempt.
    #[must_use]
    pub const fn committed_counts(self) -> ChunkCounts {
        self.committed_counts
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// The result visible to an after-chunk listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkAttemptOutcome {
    /// The chunk transaction committed.
    Committed,
    /// The transaction rolled back after a failure.
    RolledBack,
    /// Cooperative stop rolled back the open attempt.
    Stopped,
    /// The commit result is unknown and must not be guessed.
    Unknown,
}

/// A value-redacted chunk-listener failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkListenerError;

impl ChunkListenerError {
    /// Constructs a listener error without retaining application data.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl fmt::Display for ChunkListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("chunk listener failed")
    }
}

impl std::error::Error for ChunkListenerError {}

/// Observes a chunk attempt around its transaction body.
pub trait ChunkListener: Send + Sync {
    /// Runs before the transaction begins.
    fn before_chunk<'a>(
        &'a self,
        context: ChunkListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>>;

    /// Runs after commit, rollback, stop, or an unknown commit result.
    fn after_chunk<'a>(
        &'a self,
        context: ChunkListenerContext<'a>,
        outcome: ChunkAttemptOutcome,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>>;
}

/// Whether a chunk listener returned an error or panicked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkListenerFailureKind {
    /// The listener returned a classified error.
    Error,
    /// The listener panicked before or while its future was polled.
    Panic,
}

/// The listener callback phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkListenerPhase {
    /// Before the transaction body.
    BeforeChunk,
    /// After the transaction outcome.
    AfterChunk,
}

/// One redacted chunk-listener failure in callback execution order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkListenerFailure {
    phase: ChunkListenerPhase,
    registration_index: usize,
    kind: ChunkListenerFailureKind,
}

impl ChunkListenerFailure {
    const fn new(
        phase: ChunkListenerPhase,
        registration_index: usize,
        kind: ChunkListenerFailureKind,
    ) -> Self {
        Self {
            phase,
            registration_index,
            kind,
        }
    }

    /// Returns the callback phase.
    #[must_use]
    pub const fn phase(self) -> ChunkListenerPhase {
        self.phase
    }

    /// Returns the zero-based registration index.
    #[must_use]
    pub const fn registration_index(self) -> usize {
        self.registration_index
    }

    /// Returns whether the listener errored or panicked.
    #[must_use]
    pub const fn kind(self) -> ChunkListenerFailureKind {
        self.kind
    }
}

/// Stable phase classification for a failed chunk step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkFailure {
    /// Checked count arithmetic rejected the attempted update.
    Count,
    /// The reader returned an error.
    Reader,
    /// The reader panicked.
    ReaderPanic,
    /// The processor returned an error.
    Processor,
    /// The processor panicked.
    ProcessorPanic,
    /// The writer returned an error.
    Writer,
    /// The writer panicked.
    WriterPanic,
    /// A chunk transaction could not begin.
    TransactionBegin,
    /// A chunk transaction was known not to commit.
    TransactionCommit,
    /// Rollback itself failed.
    TransactionRollback,
    /// The post-commit completion callback returned an error.
    Completion,
    /// The post-commit completion callback panicked.
    CompletionPanic,
    /// A chunk listener returned an error.
    Listener,
    /// A chunk listener panicked.
    ListenerPanic,
    /// An item, retry, or skip listener returned an error.
    ItemListener,
    /// An item, retry, or skip listener panicked.
    ItemListenerPanic,
    /// A retry ordinal could not be reserved durably.
    RetryReservation,
    /// Durable fault state was unusable, so no component work began.
    FaultState,
    /// The step already retains its maximum unresolved retry keys.
    RetryStateExhausted,
    /// The selected resource cannot honour the declared policy.
    UnsupportedCapability,
}

/// Final result of deterministic chunk orchestration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkExecutionOutcome {
    /// End of input was reached and every open chunk committed.
    Completed,
    /// Cooperative stop was observed at a safe chunk boundary.
    Stopped,
    /// A typed component, listener, count, or transaction failure occurred.
    Failed(ChunkFailure),
    /// Commit outcome is unknown; replay requires durable recovery evidence.
    Unknown,
}

/// In-memory execution evidence returned by a chunk step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkExecutionReport {
    outcome: ChunkExecutionOutcome,
    original_outcome: Option<ChunkExecutionOutcome>,
    committed_counts: ChunkCounts,
    committed_chunks: ChunkCount,
    rolled_back_chunks: ChunkCount,
    listener_failures: Vec<ChunkListenerFailure>,
    item_listener_failures: Vec<ItemListenerFailure>,
    skip_counts: SkipCounts,
    retry_counts: RetryCounts,
    rollback_count: u64,
    no_rollback_count: u64,
    terminal_rollback: bool,
}

impl ChunkExecutionReport {
    /// Returns the final chunk-step result.
    #[must_use]
    pub const fn outcome(&self) -> ChunkExecutionOutcome {
        self.outcome
    }

    /// Returns the result superseded by an after-listener failure.
    #[must_use]
    pub const fn original_outcome(&self) -> Option<ChunkExecutionOutcome> {
        self.original_outcome
    }

    /// Returns aggregate counts from committed chunks only.
    #[must_use]
    pub const fn committed_counts(&self) -> ChunkCounts {
        self.committed_counts
    }

    /// Returns the number of committed chunk transactions.
    #[must_use]
    pub const fn committed_chunks(&self) -> ChunkCount {
        self.committed_chunks
    }

    /// Returns the number of rolled-back chunk transactions.
    #[must_use]
    pub const fn rolled_back_chunks(&self) -> ChunkCount {
        self.rolled_back_chunks
    }

    /// Borrows listener failures in callback execution order.
    #[must_use]
    pub fn listener_failures(&self) -> &[ChunkListenerFailure] {
        &self.listener_failures
    }

    /// Borrows item, retry, and skip listener failures in execution order.
    #[must_use]
    pub fn item_listener_failures(&self) -> &[ItemListenerFailure] {
        &self.item_listener_failures
    }

    /// Returns committed per-phase skip counts.
    ///
    /// A skip appears here only after the chunk that accepted it committed. On
    /// a repository-backed run the totals include the counts this attempt
    /// inherited, because the aggregate skip limit spans every attempt of one
    /// job instance.
    #[must_use]
    pub const fn skip_counts(&self) -> SkipCounts {
        self.skip_counts
    }

    /// Returns per-phase counts of durably reserved retry ordinals.
    ///
    /// The counts include the ordinals this attempt inherited.
    #[must_use]
    pub const fn retry_counts(&self) -> RetryCounts {
        self.retry_counts
    }

    /// Returns framework rollback decisions with a durable acknowledgement.
    ///
    /// A retry reservation and a terminal known rollback each add one. A
    /// database abort caused by process death is not counted. Unlike the skip
    /// and retry counts this value is scoped to the current attempt.
    #[must_use]
    pub const fn rollback_count(&self) -> u64 {
        self.rollback_count
    }

    /// Returns commits that accepted a
    /// [`RollbackDisposition::CommitSafeSkip`], including inherited ones.
    #[must_use]
    pub const fn no_rollback_count(&self) -> u64 {
        self.no_rollback_count
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkRuntimeEvent {
    Started(ChunkCount),
    Committed(ChunkCount),
    RolledBack(ChunkCount),
    Unknown(ChunkCount),
    Fault(FaultRuntimeEvent),
}

/// A post-decision fault observation with only reviewed bounded fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FaultRuntimeEvent {
    pub(crate) kind: LifecycleEventKind,
    pub(crate) sequence: ChunkCount,
    pub(crate) phase: FaultPhase,
    pub(crate) summary: Option<FailureSummary>,
    pub(crate) ordinal: Option<RetryOrdinal>,
    pub(crate) backoff: Option<Duration>,
}

impl FaultRuntimeEvent {
    const fn new(kind: LifecycleEventKind, sequence: ChunkCount, phase: FaultPhase) -> Self {
        Self {
            kind,
            sequence,
            phase,
            summary: None,
            ordinal: None,
            backoff: None,
        }
    }

    const fn with_summary(mut self, summary: FailureSummary) -> Self {
        self.summary = Some(summary);
        self
    }

    const fn with_ordinal(mut self, ordinal: RetryOrdinal) -> Self {
        self.ordinal = Some(ordinal);
        self
    }

    const fn with_backoff(mut self, backoff: Duration) -> Self {
        self.backoff = Some(backoff);
        self
    }
}

struct ExecutionState {
    committed_counts: ChunkCounts,
    committed_chunks: ChunkCount,
    rolled_back_chunks: ChunkCount,
    listener_failures: Vec<ChunkListenerFailure>,
    item_listener_failures: Vec<ItemListenerFailure>,
    skip_counts: SkipCounts,
    retry_counts: RetryCounts,
    rollback_count: u64,
    no_rollback_count: u64,
    terminal_rollback: bool,
    next_failure_id: u64,
}

impl ExecutionState {
    fn new() -> Self {
        Self::inheriting(FaultProgress::NONE)
    }

    /// Starts an attempt from the totals its durable predecessor committed.
    fn inheriting(inherited: FaultProgress) -> Self {
        Self {
            committed_counts: ChunkCounts::default(),
            committed_chunks: ChunkCount::ZERO,
            rolled_back_chunks: ChunkCount::ZERO,
            listener_failures: Vec::new(),
            item_listener_failures: Vec::new(),
            skip_counts: inherited.skips(),
            retry_counts: inherited.retries(),
            rollback_count: 0,
            no_rollback_count: inherited.no_rollbacks(),
            terminal_rollback: false,
            next_failure_id: 0,
        }
    }

    /// Preserves step-scoped fault evidence while resetting per-attempt state.
    fn drain(&mut self) -> Self {
        let mut replacement = Self::new();
        replacement.skip_counts = self.skip_counts;
        replacement.retry_counts = self.retry_counts;
        replacement.rollback_count = self.rollback_count;
        replacement.no_rollback_count = self.no_rollback_count;
        replacement.terminal_rollback = self.terminal_rollback;
        replacement.next_failure_id = self.next_failure_id;
        std::mem::replace(self, replacement)
    }

    fn report(
        self,
        outcome: ChunkExecutionOutcome,
        original_outcome: Option<ChunkExecutionOutcome>,
    ) -> ChunkExecutionReport {
        ChunkExecutionReport {
            outcome,
            original_outcome,
            committed_counts: self.committed_counts,
            committed_chunks: self.committed_chunks,
            rolled_back_chunks: self.rolled_back_chunks,
            listener_failures: self.listener_failures,
            item_listener_failures: self.item_listener_failures,
            skip_counts: self.skip_counts,
            retry_counts: self.retry_counts,
            rollback_count: self.rollback_count,
            no_rollback_count: self.no_rollback_count,
            terminal_rollback: self.terminal_rollback,
        }
    }
}

/// One buffered input retained across the retry replays of a chunk.
struct ItemSlot<I> {
    item: I,
    ordinal: u64,
    skipped: bool,
}

/// One accepted skip awaiting the commit that makes it authoritative.
struct PendingSkip<O> {
    phase: FaultPhase,
    fault: FaultDescriptor,
    disposition: RollbackDisposition,
    slot: Option<usize>,
    output: Option<O>,
}

/// The in-flight retry scope for one key.
struct PendingRetry {
    key: RetryKey,
    fault: FaultDescriptor,
    entered: usize,
}

/// Chunk-scoped work that survives a rollback and its replay.
///
/// A retry rolls the open transaction back and replays the chunk. The reader is
/// stateful and cannot rewind in process, so already-read inputs stay buffered
/// and the replay re-invokes only the components that have not yet succeeded.
struct ChunkBuffer<I, O> {
    slots: Vec<ItemSlot<I>>,
    skips: Vec<PendingSkip<O>>,
    retry: Option<PendingRetry>,
    end_of_input: bool,
    base_ordinal: u64,
    read_ordinal: u64,
    checkpoint_digest: [u8; 32],
}

impl<I, O> ChunkBuffer<I, O> {
    const fn new(base_ordinal: u64, checkpoint_digest: [u8; 32]) -> Self {
        Self {
            slots: Vec::new(),
            skips: Vec::new(),
            retry: None,
            end_of_input: false,
            base_ordinal,
            read_ordinal: base_ordinal,
            checkpoint_digest,
        }
    }
}

/// Returns the fault-tolerance deltas one chunk commit makes authoritative.
fn accepted_fault_progress<O>(skips: &[PendingSkip<O>]) -> Option<ChunkFaultProgress> {
    let mut counts = SkipCounts::ZERO;
    let mut no_rollbacks = 0_u64;
    for skip in skips {
        counts = counts.checked_increment(skip.phase).ok()?;
        if skip.disposition == RollbackDisposition::CommitSafeSkip {
            no_rollbacks = no_rollbacks.checked_add(1)?;
        }
    }
    Some(ChunkFaultProgress::new(counts, no_rollbacks))
}

/// Returns durable skips plus the skips one chunk has not yet committed.
fn projected_skips<O>(committed: SkipCounts, skips: &[PendingSkip<O>]) -> Option<SkipCounts> {
    skips.iter().try_fold(committed, |counts, skip| {
        counts.checked_increment(skip.phase).ok()
    })
}

/// The verdict of one chunk attempt body, before rollback or commit.
enum Verdict {
    /// Every buffered input is classified; the attempt may commit.
    Commit,
    /// A retryable fault ends the attempt, reserves an ordinal, and replays.
    Retry(RetryRequest),
    /// An accepted rollback skip ends the attempt and replays the chunk.
    Replay,
    /// The attempt is terminal.
    Terminal(ChunkExecutionOutcome),
}

/// One retry, reserved after rollback and before backoff.
struct RetryRequest {
    key: RetryKey,
    phase: FaultPhase,
    fault: FaultDescriptor,
    ordinal: RetryOrdinal,
    delay: Duration,
}

/// The result of one complete chunk attempt.
enum AttemptResult {
    /// The chunk transaction committed.
    Committed {
        counts: ChunkCounts,
        receipt: ChunkCommitReceipt,
    },
    /// The attempt rolled back and the chunk replays.
    Replay,
    /// The attempt rolled back and the step is finished.
    RolledBack(ChunkExecutionOutcome),
    /// Rollback itself failed after an earlier outcome.
    RollbackFailed(Option<ChunkExecutionOutcome>),
    /// The commit outcome is unknown and must never be guessed.
    Unknown,
}

/// Borrowed components and policy for one chunk step.
struct Components<'a, I, O, P, W> {
    processor: &'a P,
    writer: &'a W,
    item_listeners: &'a ItemListenerSet<I, O>,
    fault: Option<&'a FaultRuntime>,
    step_name: &'a StepName,
    definition_digest: [u8; 32],
    size: ChunkSize,
}

/// Borrowed call state for one chunk attempt.
#[derive(Clone, Copy)]
struct AttemptScope<'a> {
    correlation: &'a ExecutionCorrelation,
    stop: &'a StopToken,
    sequence: ChunkCount,
}

impl<'a> AttemptScope<'a> {
    const fn listener_context(self) -> ItemListenerContext<'a> {
        ItemListenerContext::new(self.correlation, self.sequence, self.stop)
    }
}

/// Provisional writer input for one attempt.
struct AttemptOutputs<O> {
    values: Vec<O>,
    slots: Vec<usize>,
    filtered: u64,
}

impl<O> AttemptOutputs<O> {
    const fn new() -> Self {
        Self {
            values: Vec::new(),
            slots: Vec::new(),
            filtered: 0,
        }
    }

    fn reset(&mut self) {
        self.values.clear();
        self.slots.clear();
        self.filtered = 0;
    }
}

/// One component invocation classified without inspecting its payload.
enum Invoked<T, E> {
    Completed(T),
    Failed(E),
    Panicked,
}

#[allow(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "the chunk loop keeps the canonical attempt, commit, and stop order visible"
)]
pub(crate) async fn execute_chunk_step<I, O, R, P, W>(
    step: &mut ChunkStep<I, O, R, P, W>,
    correlation: &ExecutionCorrelation,
    stop: &StopToken,
    transaction_context: Option<ChunkTransactionContext>,
    mut emit: impl FnMut(ChunkRuntimeEvent),
) -> ChunkExecutionReport
where
    I: Send + Sync,
    O: Send + Sync,
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let ChunkStep {
        name,
        size,
        reader,
        processor,
        writer,
        transactions,
        completion,
        listeners,
        item_listeners,
        fault,
        in_flight_policy,
        definition_digest,
        ..
    } = step;
    let components = Components {
        processor,
        writer,
        item_listeners,
        fault: fault.as_ref(),
        step_name: name,
        definition_digest: definition_digest.clone(),
        size: *size,
    };

    let inherited = match inherited_progress(
        transactions.as_ref(),
        fault.as_ref(),
        transaction_context,
    )
    .await
    {
        Ok(inherited) => inherited,
        Err(outcome) => return ExecutionState::new().report(outcome, None),
    };
    let base_ordinal = inherited.read_ordinal();
    let mut state = ExecutionState::inheriting(inherited.fault());
    let mut sequence = ChunkCount::ZERO;
    let mut buffer = ChunkBuffer::new(base_ordinal, inherited.checkpoint_digest());

    loop {
        if stop.is_stop_requested() {
            return state.report(ChunkExecutionOutcome::Stopped, None);
        }
        sequence = match sequence.checked_increment() {
            Ok(value) => value,
            Err(_) => {
                return state.report(ChunkExecutionOutcome::Failed(ChunkFailure::Count), None);
            }
        };
        let listener_context = ChunkListenerContext::new(sequence, state.committed_counts, stop);

        if let Some(failure) = run_before_listeners(&listeners, listener_context).await {
            let outcome = listener_failure_outcome(failure.kind());
            state.listener_failures.push(failure);
            return state.report(outcome, None);
        }
        if stop.is_stop_requested() {
            return state.report(ChunkExecutionOutcome::Stopped, None);
        }

        let begun = match transaction_context {
            Some(context) => transactions.begin_for(context).await,
            None => transactions.begin().await,
        };
        let mut transaction = match begun {
            Ok(transaction) => transaction,
            Err(ChunkTransactionError::NotCommitted) => {
                return finish_failed_attempt(
                    listeners,
                    listener_context,
                    ChunkAttemptOutcome::RolledBack,
                    ChunkExecutionOutcome::Failed(ChunkFailure::TransactionBegin),
                    &mut state,
                )
                .await;
            }
            Err(ChunkTransactionError::CommitOutcomeUnknown) => {
                return finish_failed_attempt(
                    listeners,
                    listener_context,
                    ChunkAttemptOutcome::Unknown,
                    ChunkExecutionOutcome::Unknown,
                    &mut state,
                )
                .await;
            }
        };
        emit(ChunkRuntimeEvent::Started(sequence));

        // Once a chunk is open, the definition decides whether a shutdown
        // request remains visible to component calls. The masked token is
        // scoped to this attempt; the real token is consulted immediately
        // after commit so `FinishChunk` never starts another chunk.
        let masked_stop;
        let attempt_stop = match in_flight_policy {
            InFlightPolicy::FinishChunk => {
                let (_, token) = crate::StopSource::new();
                masked_stop = token;
                &masked_stop
            }
            // `InFlightPolicy::RollbackChunk`, and any policy this build does
            // not know: `InFlightPolicy` is `#[non_exhaustive]`, and an
            // unrecognized policy never masks a shutdown request.
            _ => stop,
        };
        let scope = AttemptScope {
            correlation,
            stop: attempt_stop,
            sequence,
        };

        let result = run_attempt(
            &components,
            reader,
            scope,
            &mut buffer,
            &mut state,
            transaction.as_mut(),
            &mut emit,
        )
        .await;
        drop(transaction);

        match result {
            AttemptResult::Committed { counts, receipt } => {
                let Ok(next_counts) = state.committed_counts.checked_add(counts) else {
                    return finish_failed_attempt(
                        listeners,
                        listener_context,
                        ChunkAttemptOutcome::Committed,
                        ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                        &mut state,
                    )
                    .await;
                };
                let Ok(next_chunks) = state.committed_chunks.checked_increment() else {
                    return finish_failed_attempt(
                        listeners,
                        listener_context,
                        ChunkAttemptOutcome::Committed,
                        ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                        &mut state,
                    )
                    .await;
                };
                state.committed_counts = next_counts;
                state.committed_chunks = next_chunks;
                emit(ChunkRuntimeEvent::Committed(sequence));
                emit_committed_skips(&buffer, sequence, &mut emit);

                let end_of_input = buffer.end_of_input;
                let checkpoint_digest = checkpoint_digest(receipt.checkpoint());
                let Some(next_ordinal) =
                    base_ordinal.checked_add(state.committed_counts.read().get())
                else {
                    return finish_failed_attempt(
                        listeners,
                        listener_context,
                        ChunkAttemptOutcome::Committed,
                        ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                        &mut state,
                    )
                    .await;
                };
                buffer = ChunkBuffer::new(next_ordinal, checkpoint_digest);

                let completion_context = ChunkCompletionContext::new(
                    receipt.checkpoint(),
                    receipt.execution_context(),
                    counts,
                    stop,
                );
                let terminal_outcome =
                    match invoke_completion(completion.as_ref(), completion_context).await {
                        Ok(ChunkCompletionOutcome::Acknowledged) => {
                            if stop.is_stop_requested() {
                                Some(ChunkExecutionOutcome::Stopped)
                            } else if end_of_input {
                                Some(ChunkExecutionOutcome::Completed)
                            } else {
                                None
                            }
                        }
                        Ok(ChunkCompletionOutcome::StoppedAfterCommit) => {
                            Some(ChunkExecutionOutcome::Stopped)
                        }
                        Err(failure) => Some(ChunkExecutionOutcome::Failed(failure)),
                    };

                let after_context =
                    ChunkListenerContext::new(sequence, state.committed_counts, stop);
                let after_failures =
                    run_after_listeners(listeners, after_context, ChunkAttemptOutcome::Committed)
                        .await;
                if let Some(first) = after_failures.first().copied() {
                    state.listener_failures.extend(after_failures);
                    let original = terminal_outcome.or(Some(ChunkExecutionOutcome::Completed));
                    return state.report(listener_failure_outcome(first.kind()), original);
                }
                if let Some(outcome) = terminal_outcome {
                    return state.report(outcome, None);
                }
            }
            AttemptResult::Replay => {
                if let Err(report) =
                    record_rolled_back_attempt(listeners, listener_context, &mut state, &mut emit)
                        .await
                {
                    return report;
                }
            }
            AttemptResult::RolledBack(ChunkExecutionOutcome::Completed) => {
                // The final chunk read nothing, so its unused transaction rolls
                // back without counting as a rolled-back attempt.
                emit(ChunkRuntimeEvent::RolledBack(sequence));
                let failures = run_after_listeners(
                    listeners,
                    listener_context,
                    ChunkAttemptOutcome::RolledBack,
                )
                .await;
                if let Some(first) = failures.first().copied() {
                    state.listener_failures.extend(failures);
                    return state
                        .drain()
                        .report(listener_failure_outcome(first.kind()), None);
                }
                return state.report(ChunkExecutionOutcome::Completed, None);
            }
            AttemptResult::RolledBack(outcome) => {
                state.rollback_count = state.rollback_count.saturating_add(1);
                state.terminal_rollback = true;
                state.rolled_back_chunks = match state.rolled_back_chunks.checked_increment() {
                    Ok(count) => count,
                    Err(_) => {
                        return state.drain().report(
                            ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                            Some(outcome),
                        );
                    }
                };
                emit(ChunkRuntimeEvent::RolledBack(sequence));
                let attempt_outcome = match outcome {
                    ChunkExecutionOutcome::Stopped => ChunkAttemptOutcome::Stopped,
                    _ => ChunkAttemptOutcome::RolledBack,
                };
                return finish_failed_attempt(
                    listeners,
                    listener_context,
                    attempt_outcome,
                    outcome,
                    &mut state,
                )
                .await;
            }
            AttemptResult::RollbackFailed(original) => {
                return state.drain().report(
                    ChunkExecutionOutcome::Failed(ChunkFailure::TransactionRollback),
                    original,
                );
            }
            AttemptResult::Unknown => {
                emit(ChunkRuntimeEvent::Unknown(sequence));
                return finish_failed_attempt(
                    listeners,
                    listener_context,
                    ChunkAttemptOutcome::Unknown,
                    ChunkExecutionOutcome::Unknown,
                    &mut state,
                )
                .await;
            }
        }
    }
}

/// Loads the durable progress this attempt inherits and binds durable state.
///
/// A standalone chunk step has no repository execution and inherits nothing. A
/// repository-backed step fails closed rather than restarting a bounded policy
/// limit from zero or deriving retry keys from the wrong checkpoint.
async fn inherited_progress(
    transactions: &dyn ChunkTransactionManager,
    fault: Option<&FaultRuntime>,
    context: Option<ChunkTransactionContext>,
) -> Result<InheritedStepProgress, ChunkExecutionOutcome> {
    let Some(context) = context else {
        return Ok(InheritedStepProgress::NONE);
    };
    if let Some(fault) = fault
        && fault.state().bind(context).await.is_err()
    {
        return Err(ChunkExecutionOutcome::Failed(ChunkFailure::FaultState));
    }
    match transactions.inherited_progress(context).await {
        Ok(inherited) => Ok(inherited),
        Err(ChunkTransactionError::CommitOutcomeUnknown) => Err(ChunkExecutionOutcome::Unknown),
        Err(ChunkTransactionError::NotCommitted) => {
            Err(ChunkExecutionOutcome::Failed(ChunkFailure::FaultState))
        }
    }
}

/// Counts one rolled-back attempt and runs its after-chunk listeners.
async fn record_rolled_back_attempt<E>(
    listeners: &[Arc<dyn ChunkListener>],
    context: ChunkListenerContext<'_>,
    state: &mut ExecutionState,
    emit: &mut E,
) -> Result<(), ChunkExecutionReport>
where
    E: FnMut(ChunkRuntimeEvent),
{
    state.rolled_back_chunks = match state.rolled_back_chunks.checked_increment() {
        Ok(count) => count,
        Err(_) => {
            return Err(state
                .drain()
                .report(ChunkExecutionOutcome::Failed(ChunkFailure::Count), None));
        }
    };
    emit(ChunkRuntimeEvent::RolledBack(context.sequence()));
    let failures = run_after_listeners(listeners, context, ChunkAttemptOutcome::RolledBack).await;
    if let Some(first) = failures.first().copied() {
        state.listener_failures.extend(failures);
        return Err(state
            .drain()
            .report(listener_failure_outcome(first.kind()), None));
    }
    Ok(())
}

async fn run_attempt<I, O, E, P, W, R>(
    components: &Components<'_, I, O, P, W>,
    reader: &mut R,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    transaction: &mut dyn ChunkTransaction,
    emit: &mut E,
) -> AttemptResult
where
    I: Send + Sync,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
    R: ItemReader<I>,
{
    let mut outputs = AttemptOutputs::new();

    let verdict = 'body: {
        if let Some(fault) = components.fault
            && fault.policy().requires_commit_safe_skip()
            && transaction.business_transaction().is_none()
        {
            break 'body Verdict::Terminal(ChunkExecutionOutcome::Failed(
                ChunkFailure::UnsupportedCapability,
            ));
        }

        match read_phase(components, reader, scope, buffer, state, emit).await {
            Verdict::Commit => {}
            other => break 'body other,
        }

        if buffer.slots.is_empty() && buffer.end_of_input && buffer.skips.is_empty() {
            break 'body Verdict::Terminal(ChunkExecutionOutcome::Completed);
        }

        match process_phase(components, scope, buffer, state, &mut outputs, emit).await {
            Verdict::Commit => {}
            other => break 'body other,
        }

        write_phase(
            components,
            scope,
            buffer,
            state,
            &mut outputs,
            transaction,
            emit,
        )
        .await
    };

    match verdict {
        Verdict::Commit => {
            commit_attempt(components, scope, buffer, state, transaction, &outputs).await
        }
        Verdict::Retry(request) => {
            schedule_retry(components, scope, buffer, state, transaction, request, emit).await
        }
        Verdict::Replay => {
            if transaction.rollback().await.is_err() {
                return AttemptResult::RollbackFailed(None);
            }
            AttemptResult::Replay
        }
        Verdict::Terminal(ChunkExecutionOutcome::Unknown) => AttemptResult::Unknown,
        Verdict::Terminal(outcome) => {
            if transaction.rollback().await.is_err() {
                return AttemptResult::RollbackFailed(Some(outcome));
            }
            AttemptResult::RolledBack(outcome)
        }
    }
}

/// Reads until the chunk is full or the input ends.
#[allow(
    clippy::too_many_lines,
    reason = "one phase keeps its listener, classification, and skip order visible"
)]
async fn read_phase<I, O, E, P, W, R>(
    components: &Components<'_, I, O, P, W>,
    reader: &mut R,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    emit: &mut E,
) -> Verdict
where
    I: Send + Sync,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
    R: ItemReader<I>,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
{
    let listener_context = scope.listener_context();
    while !buffer.end_of_input && buffer.slots.len() < components.size.get() as usize {
        if scope.stop.is_stop_requested() {
            return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
        }
        let ordinal = buffer.read_ordinal;
        let key = retry_key(
            components,
            buffer.checkpoint_digest,
            FaultPhase::Read,
            ordinal,
        );

        let before = components
            .item_listeners
            .before_read(listener_context)
            .await;
        if let Some(failure) = before.failure() {
            state.item_listener_failures.push(failure);
            return Verdict::Terminal(item_listener_outcome(failure.kind()));
        }

        match invoke_reader(reader, ReadContext::new(scope.stop)).await {
            Invoked::Completed(ReadOutcome::Item(item)) => {
                let failures = components
                    .item_listeners
                    .after_read(before.entered(), &item, listener_context)
                    .await;
                if let Some(first) = failures.first().copied() {
                    state.item_listener_failures.extend(failures);
                    return Verdict::Terminal(item_listener_outcome(first.kind()));
                }
                if let Some(outcome) = complete_retry(
                    components,
                    listener_context,
                    &mut buffer.retry,
                    state,
                    key,
                    RetryOutcome::Recovered,
                )
                .await
                {
                    return Verdict::Terminal(outcome);
                }
                resolve_key(components, key).await;
                buffer.slots.push(ItemSlot {
                    item,
                    ordinal,
                    skipped: false,
                });
                buffer.read_ordinal = buffer.read_ordinal.saturating_add(1);
            }
            Invoked::Completed(ReadOutcome::EndOfInput) => buffer.end_of_input = true,
            Invoked::Completed(ReadOutcome::Stopped) => {
                return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
            }
            invoked => {
                let (error, panicked) = match invoked {
                    Invoked::Failed(error) => (error, false),
                    _ => (ReaderError::new(), true),
                };
                let terminal = if panicked {
                    ChunkFailure::ReaderPanic
                } else {
                    ChunkFailure::Reader
                };
                let advanced = !panicked && error.has_checkpoint_advanced();
                let Some(fault) = descriptor(
                    components,
                    state,
                    &buffer.skips,
                    FaultPhase::Read,
                    error.category(),
                ) else {
                    return Verdict::Terminal(ChunkExecutionOutcome::Failed(ChunkFailure::Count));
                };
                let fault = with_reserved_ordinal(components, key, fault).await;

                let failures = components
                    .item_listeners
                    .on_read_error(before.entered(), fault, listener_context)
                    .await;
                if let Some(first) = failures.first().copied() {
                    state.item_listener_failures.extend(failures);
                    return Verdict::Terminal(item_listener_outcome(first.kind()));
                }

                let evidence = FaultEvidence::new(advanced, true, advanced);
                let decision = match classify(
                    components,
                    listener_context,
                    &mut buffer.retry,
                    state,
                    key,
                    fault,
                    evidence,
                    scope.sequence,
                    emit,
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(outcome) => return Verdict::Terminal(outcome),
                };
                match decision {
                    FaultDecision::Retry { ordinal, delay } => {
                        return Verdict::Retry(RetryRequest {
                            key,
                            phase: FaultPhase::Read,
                            fault,
                            ordinal,
                            delay,
                        });
                    }
                    FaultDecision::Skip { disposition } => {
                        resolve_key(components, key).await;
                        buffer.read_ordinal = buffer.read_ordinal.saturating_add(1);
                        buffer.skips.push(PendingSkip {
                            phase: FaultPhase::Read,
                            fault,
                            disposition,
                            slot: None,
                            output: None,
                        });
                        if disposition == RollbackDisposition::Rollback {
                            return Verdict::Replay;
                        }
                    }
                    FaultDecision::Unknown => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Unknown);
                    }
                    FaultDecision::Stop => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
                    }
                    // `FaultDecision::FailAndRollback`, and any decision this
                    // build does not know: `FaultDecision` is
                    // `#[non_exhaustive]`, and an unrecognized decision rolls
                    // back and fails rather than committing work or claiming an
                    // unknown commit.
                    _ => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Failed(terminal));
                    }
                }
            }
        }
    }
    Verdict::Commit
}

/// Processes every buffered input that is not already skipped.
#[allow(
    clippy::too_many_lines,
    reason = "one phase keeps its listener, classification, and skip order visible"
)]
async fn process_phase<I, O, E, P, W>(
    components: &Components<'_, I, O, P, W>,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    outputs: &mut AttemptOutputs<O>,
    emit: &mut E,
) -> Verdict
where
    I: Send + Sync,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let listener_context = scope.listener_context();
    outputs.reset();
    for index in 0..buffer.slots.len() {
        if buffer.slots[index].skipped {
            continue;
        }
        if scope.stop.is_stop_requested() {
            return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
        }
        let ordinal = buffer.slots[index].ordinal;
        let key = retry_key(
            components,
            buffer.checkpoint_digest,
            FaultPhase::Process,
            ordinal,
        );

        let before = components
            .item_listeners
            .before_process(&buffer.slots[index].item, listener_context)
            .await;
        if let Some(failure) = before.failure() {
            state.item_listener_failures.push(failure);
            return Verdict::Terminal(item_listener_outcome(failure.kind()));
        }

        let invoked = invoke_processor(
            components.processor,
            &buffer.slots[index].item,
            ProcessContext::new(scope.stop),
        )
        .await;
        match invoked {
            Invoked::Completed(ProcessOutcome::Item(output)) => {
                let failures = components
                    .item_listeners
                    .after_process(
                        before.entered(),
                        &buffer.slots[index].item,
                        Some(&output),
                        listener_context,
                    )
                    .await;
                if let Some(first) = failures.first().copied() {
                    state.item_listener_failures.extend(failures);
                    return Verdict::Terminal(item_listener_outcome(first.kind()));
                }
                if let Some(outcome) = complete_retry(
                    components,
                    listener_context,
                    &mut buffer.retry,
                    state,
                    key,
                    RetryOutcome::Recovered,
                )
                .await
                {
                    return Verdict::Terminal(outcome);
                }
                resolve_key(components, key).await;
                outputs.values.push(output);
                outputs.slots.push(index);
            }
            Invoked::Completed(ProcessOutcome::Filtered) => {
                let failures = components
                    .item_listeners
                    .after_process(
                        before.entered(),
                        &buffer.slots[index].item,
                        None,
                        listener_context,
                    )
                    .await;
                if let Some(first) = failures.first().copied() {
                    state.item_listener_failures.extend(failures);
                    return Verdict::Terminal(item_listener_outcome(first.kind()));
                }
                if let Some(outcome) = complete_retry(
                    components,
                    listener_context,
                    &mut buffer.retry,
                    state,
                    key,
                    RetryOutcome::Recovered,
                )
                .await
                {
                    return Verdict::Terminal(outcome);
                }
                resolve_key(components, key).await;
                outputs.filtered = outputs.filtered.saturating_add(1);
            }
            Invoked::Completed(ProcessOutcome::Stopped) => {
                return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
            }
            invoked => {
                let (error, panicked) = match invoked {
                    Invoked::Failed(error) => (error, false),
                    _ => (ProcessorError::new(), true),
                };
                let terminal = if panicked {
                    ChunkFailure::ProcessorPanic
                } else {
                    ChunkFailure::Processor
                };
                let Some(fault) = descriptor(
                    components,
                    state,
                    &buffer.skips,
                    FaultPhase::Process,
                    error.category(),
                ) else {
                    return Verdict::Terminal(ChunkExecutionOutcome::Failed(ChunkFailure::Count));
                };
                let fault = with_reserved_ordinal(components, key, fault).await;

                let failures = components
                    .item_listeners
                    .on_process_error(
                        before.entered(),
                        &buffer.slots[index].item,
                        fault,
                        listener_context,
                    )
                    .await;
                if let Some(first) = failures.first().copied() {
                    state.item_listener_failures.extend(failures);
                    return Verdict::Terminal(item_listener_outcome(first.kind()));
                }

                // The input is located and no writer effect has started, so the
                // framework owns every piece of process-skip evidence.
                let evidence = FaultEvidence::new(true, true, true);
                let decision = match classify(
                    components,
                    listener_context,
                    &mut buffer.retry,
                    state,
                    key,
                    fault,
                    evidence,
                    scope.sequence,
                    emit,
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(outcome) => return Verdict::Terminal(outcome),
                };
                match decision {
                    FaultDecision::Retry { ordinal, delay } => {
                        return Verdict::Retry(RetryRequest {
                            key,
                            phase: FaultPhase::Process,
                            fault,
                            ordinal,
                            delay,
                        });
                    }
                    FaultDecision::Skip { disposition } => {
                        resolve_key(components, key).await;
                        buffer.slots[index].skipped = true;
                        buffer.skips.push(PendingSkip {
                            phase: FaultPhase::Process,
                            fault,
                            disposition,
                            slot: Some(index),
                            output: None,
                        });
                        if disposition == RollbackDisposition::Rollback {
                            return Verdict::Replay;
                        }
                    }
                    FaultDecision::Unknown => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Unknown);
                    }
                    FaultDecision::Stop => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
                    }
                    // `FaultDecision::FailAndRollback`, and any decision this
                    // build does not know: `FaultDecision` is
                    // `#[non_exhaustive]`, and an unrecognized decision rolls
                    // back and fails rather than committing work or claiming an
                    // unknown commit.
                    _ => {
                        return Verdict::Terminal(ChunkExecutionOutcome::Failed(terminal));
                    }
                }
            }
        }
    }
    Verdict::Commit
}

/// Writes the provisional output batch inside the open transaction.
#[allow(
    clippy::too_many_lines,
    reason = "one phase keeps its listener, classification, and skip order visible"
)]
#[allow(
    clippy::too_many_arguments,
    reason = "the write boundary needs components, scope, buffer, state, outputs, and the transaction"
)]
async fn write_phase<I, O, E, P, W>(
    components: &Components<'_, I, O, P, W>,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    outputs: &mut AttemptOutputs<O>,
    transaction: &mut dyn ChunkTransaction,
    emit: &mut E,
) -> Verdict
where
    I: Send + Sync,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    if outputs.values.is_empty() {
        return Verdict::Commit;
    }
    if scope.stop.is_stop_requested() {
        return Verdict::Terminal(ChunkExecutionOutcome::Stopped);
    }
    let listener_context = scope.listener_context();
    let key = retry_key(
        components,
        buffer.checkpoint_digest,
        FaultPhase::Write,
        buffer.base_ordinal,
    );

    let before = components
        .item_listeners
        .before_write(&outputs.values, listener_context)
        .await;
    if let Some(failure) = before.failure() {
        state.item_listener_failures.push(failure);
        return Verdict::Terminal(item_listener_outcome(failure.kind()));
    }

    let write_context = match transaction.business_transaction() {
        Some(business) => WriteContext::enlisted(scope.stop, business),
        None => WriteContext::non_transactional(scope.stop),
    };
    match invoke_writer(components.writer, &outputs.values, write_context).await {
        Invoked::Completed(WriteOutcome::Written) => {
            let failures = components
                .item_listeners
                .after_write(before.entered(), &outputs.values, listener_context)
                .await;
            if let Some(first) = failures.first().copied() {
                state.item_listener_failures.extend(failures);
                return Verdict::Terminal(item_listener_outcome(first.kind()));
            }
            if let Some(outcome) = complete_retry(
                components,
                listener_context,
                &mut buffer.retry,
                state,
                key,
                RetryOutcome::Recovered,
            )
            .await
            {
                return Verdict::Terminal(outcome);
            }
            resolve_key(components, key).await;
            Verdict::Commit
        }
        Invoked::Completed(WriteOutcome::Stopped) => {
            Verdict::Terminal(ChunkExecutionOutcome::Stopped)
        }
        invoked => {
            let (error, panicked) = match invoked {
                Invoked::Failed(error) => (error, false),
                _ => (WriterError::new(), true),
            };
            let terminal = if panicked {
                ChunkFailure::WriterPanic
            } else {
                ChunkFailure::Writer
            };
            let located = if panicked {
                None
            } else {
                error
                    .rolled_back_output()
                    .filter(|index| *index < outputs.values.len())
            };
            let Some(fault) = descriptor(
                components,
                state,
                &buffer.skips,
                FaultPhase::Write,
                error.category(),
            ) else {
                return Verdict::Terminal(ChunkExecutionOutcome::Failed(ChunkFailure::Count));
            };
            let fault = with_reserved_ordinal(components, key, fault).await;

            let failures = components
                .item_listeners
                .on_write_error(before.entered(), &outputs.values, fault, listener_context)
                .await;
            if let Some(first) = failures.first().copied() {
                state.item_listener_failures.extend(failures);
                return Verdict::Terminal(item_listener_outcome(first.kind()));
            }

            let evidence = FaultEvidence::new(located.is_some(), located.is_some(), false);
            let decision = match classify(
                components,
                listener_context,
                &mut buffer.retry,
                state,
                key,
                fault,
                evidence,
                scope.sequence,
                emit,
            )
            .await
            {
                Ok(decision) => decision,
                Err(outcome) => return Verdict::Terminal(outcome),
            };
            match decision {
                FaultDecision::Retry { ordinal, delay } => Verdict::Retry(RetryRequest {
                    key,
                    phase: FaultPhase::Write,
                    fault,
                    ordinal,
                    delay,
                }),
                FaultDecision::Skip { disposition } => {
                    let Some(index) = located else {
                        return Verdict::Terminal(ChunkExecutionOutcome::Failed(terminal));
                    };
                    resolve_key(components, key).await;
                    let slot = outputs.slots[index];
                    buffer.slots[slot].skipped = true;
                    buffer.skips.push(PendingSkip {
                        phase: FaultPhase::Write,
                        fault,
                        disposition,
                        slot: Some(slot),
                        output: Some(outputs.values.remove(index)),
                    });
                    outputs.slots.remove(index);
                    Verdict::Replay
                }
                FaultDecision::Unknown => Verdict::Terminal(ChunkExecutionOutcome::Unknown),
                FaultDecision::Stop => Verdict::Terminal(ChunkExecutionOutcome::Stopped),
                // `FaultDecision::FailAndRollback`, and any decision this build
                // does not know: `FaultDecision` is `#[non_exhaustive]`, and an
                // unrecognized decision rolls back and fails rather than
                // committing work or claiming an unknown commit.
                _ => Verdict::Terminal(ChunkExecutionOutcome::Failed(terminal)),
            }
        }
    }
}

/// Runs skip callbacks, clears resolved keys, and commits the chunk.
async fn commit_attempt<I, O, P, W>(
    components: &Components<'_, I, O, P, W>,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    transaction: &mut dyn ChunkTransaction,
    outputs: &AttemptOutputs<O>,
) -> AttemptResult
where
    I: Send + Sync,
    O: Send + Sync,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let listener_context = scope.listener_context();
    for skip in &buffer.skips {
        let failures = match (skip.phase, skip.slot, skip.output.as_ref()) {
            (FaultPhase::Process, Some(index), _) => {
                components
                    .item_listeners
                    .on_skip_in_process(&buffer.slots[index].item, skip.fault, listener_context)
                    .await
            }
            (FaultPhase::Write, _, Some(output)) => {
                components
                    .item_listeners
                    .on_skip_in_write(output, skip.fault, listener_context)
                    .await
            }
            _ => {
                components
                    .item_listeners
                    .on_skip_in_read(skip.fault, listener_context)
                    .await
            }
        };
        if let Some(first) = failures.first().copied() {
            state.item_listener_failures.extend(failures);
            let outcome = item_listener_outcome(first.kind());
            if transaction.rollback().await.is_err() {
                return AttemptResult::RollbackFailed(Some(outcome));
            }
            return AttemptResult::RolledBack(outcome);
        }
    }

    let read = ChunkCount::new(buffer.slots.len() as u64);
    let processed = ChunkCount::new(outputs.values.len() as u64);
    let Ok(counts) = ChunkCounts::new(
        read,
        processed,
        processed,
        ChunkCount::new(outputs.filtered),
    ) else {
        let outcome = ChunkExecutionOutcome::Failed(ChunkFailure::Count);
        if transaction.rollback().await.is_err() {
            return AttemptResult::RollbackFailed(Some(outcome));
        }
        return AttemptResult::RolledBack(outcome);
    };

    let Some(accepted) = accepted_fault_progress(&buffer.skips) else {
        let outcome = ChunkExecutionOutcome::Failed(ChunkFailure::Count);
        if transaction.rollback().await.is_err() {
            return AttemptResult::RollbackFailed(Some(outcome));
        }
        return AttemptResult::RolledBack(outcome);
    };

    match transaction.commit(counts, accepted).await {
        Ok(receipt) => {
            let Ok(next_skips) = state.skip_counts.checked_add(accepted.skips()) else {
                return AttemptResult::RolledBack(ChunkExecutionOutcome::Failed(
                    ChunkFailure::Count,
                ));
            };
            state.skip_counts = next_skips;
            state.no_rollback_count = state
                .no_rollback_count
                .saturating_add(accepted.no_rollbacks());
            // The commit that advanced the checkpoint superseded every retry
            // key of the previous generation, so the durable clear is already
            // authoritative and this only prunes process-local bookkeeping.
            if let Some(fault) = components.fault {
                let _ = fault.state().clear_resolved().await;
            }
            AttemptResult::Committed { counts, receipt }
        }
        Err(ChunkTransactionError::NotCommitted) => {
            let outcome = ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit);
            if transaction.rollback().await.is_err() {
                return AttemptResult::RollbackFailed(Some(outcome));
            }
            AttemptResult::RolledBack(outcome)
        }
        Err(ChunkTransactionError::CommitOutcomeUnknown) => AttemptResult::Unknown,
    }
}

/// Rolls back, reserves the retry ordinal durably, then waits for backoff.
#[allow(
    clippy::too_many_arguments,
    reason = "the retry scope needs components, scope, buffer, state, transaction, request, and events"
)]
async fn schedule_retry<I, O, E, P, W>(
    components: &Components<'_, I, O, P, W>,
    scope: AttemptScope<'_>,
    buffer: &mut ChunkBuffer<I, O>,
    state: &mut ExecutionState,
    transaction: &mut dyn ChunkTransaction,
    request: RetryRequest,
    emit: &mut E,
) -> AttemptResult
where
    I: Send + Sync,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let Some(fault_runtime) = components.fault else {
        return AttemptResult::RolledBack(ChunkExecutionOutcome::Failed(
            ChunkFailure::RetryReservation,
        ));
    };
    if transaction.rollback().await.is_err() {
        return AttemptResult::RollbackFailed(None);
    }
    if scope.stop.is_stop_requested() {
        return AttemptResult::RolledBack(ChunkExecutionOutcome::Stopped);
    }

    let reservation = RetryReservation::new(
        request.key,
        request.phase,
        request.fault.category(),
        request.ordinal,
    );
    match fault_runtime.state().reserve(reservation).await {
        Ok(()) => {}
        Err(crate::FaultStateError::CapacityExhausted { .. }) => {
            return AttemptResult::RolledBack(ChunkExecutionOutcome::Failed(
                ChunkFailure::RetryStateExhausted,
            ));
        }
        Err(_) => {
            return AttemptResult::RolledBack(ChunkExecutionOutcome::Failed(
                ChunkFailure::RetryReservation,
            ));
        }
    }
    state.rollback_count = state.rollback_count.saturating_add(1);
    state.retry_counts = state.retry_counts.increment(request.phase);
    emit(ChunkRuntimeEvent::Fault(
        FaultRuntimeEvent::new(
            LifecycleEventKind::RetryReserved,
            scope.sequence,
            request.phase,
        )
        .with_summary(request.fault.summary())
        .with_ordinal(request.ordinal),
    ));
    emit(ChunkRuntimeEvent::Fault(
        FaultRuntimeEvent::new(
            LifecycleEventKind::FaultRollbackCommitted,
            scope.sequence,
            request.phase,
        )
        .with_summary(request.fault.summary()),
    ));

    let listener_context = scope.listener_context();
    let before = components
        .item_listeners
        .before_retry(request.fault, listener_context)
        .await;
    if let Some(failure) = before.failure() {
        state.item_listener_failures.push(failure);
        return AttemptResult::RolledBack(item_listener_outcome(failure.kind()));
    }
    buffer.retry = Some(PendingRetry {
        key: request.key,
        fault: request.fault,
        entered: before.entered(),
    });

    if scope.stop.is_stop_requested() {
        return AttemptResult::RolledBack(ChunkExecutionOutcome::Stopped);
    }
    emit(ChunkRuntimeEvent::Fault(
        FaultRuntimeEvent::new(
            LifecycleEventKind::RetryBackoffStarted,
            scope.sequence,
            request.phase,
        )
        .with_ordinal(request.ordinal)
        .with_backoff(request.delay),
    ));
    if fault_runtime
        .sleeper()
        .sleep(request.delay, scope.stop)
        .await
        == BackoffOutcome::Stopped
    {
        emit(ChunkRuntimeEvent::Fault(
            FaultRuntimeEvent::new(
                LifecycleEventKind::RetryBackoffCancelled,
                scope.sequence,
                request.phase,
            )
            .with_ordinal(request.ordinal)
            .with_backoff(request.delay),
        ));
        return AttemptResult::RolledBack(ChunkExecutionOutcome::Stopped);
    }
    if scope.stop.is_stop_requested() {
        return AttemptResult::RolledBack(ChunkExecutionOutcome::Stopped);
    }
    AttemptResult::Replay
}

/// Runs the retry-completion callback when `key` had a reserved retry.
async fn complete_retry<I, O, P, W>(
    components: &Components<'_, I, O, P, W>,
    listener_context: ItemListenerContext<'_>,
    retry: &mut Option<PendingRetry>,
    state: &mut ExecutionState,
    key: RetryKey,
    outcome: RetryOutcome,
) -> Option<ChunkExecutionOutcome>
where
    I: Send + Sync,
    O: Send + Sync,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let pending = retry.take_if(|pending| pending.key == key)?;
    let failures = components
        .item_listeners
        .after_retry(pending.entered, pending.fault, outcome, listener_context)
        .await;
    let first = failures.first().copied()?;
    state.item_listener_failures.extend(failures);
    Some(item_listener_outcome(first.kind()))
}

/// Decides one fault and runs the exhaustion callback when the budget is spent.
#[allow(
    clippy::too_many_arguments,
    reason = "classification needs components, listeners, retry state, and the fault inputs"
)]
async fn classify<I, O, E, P, W>(
    components: &Components<'_, I, O, P, W>,
    listener_context: ItemListenerContext<'_>,
    retry: &mut Option<PendingRetry>,
    state: &mut ExecutionState,
    key: RetryKey,
    fault: FaultDescriptor,
    evidence: FaultEvidence,
    sequence: ChunkCount,
    emit: &mut E,
) -> Result<FaultDecision, ChunkExecutionOutcome>
where
    I: Send + Sync,
    O: Send + Sync,
    E: FnMut(ChunkRuntimeEvent),
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let entered = retry
        .as_ref()
        .filter(|pending| pending.key == key)
        .map(|pending| pending.entered);
    if entered.is_some()
        && let Some(outcome) = complete_retry(
            components,
            listener_context,
            retry,
            state,
            key,
            RetryOutcome::Failed,
        )
        .await
    {
        return Err(outcome);
    }

    let Some(fault_runtime) = components.fault else {
        return Ok(FaultDecision::FailAndRollback);
    };
    let decision = fault_runtime.policy().decide(&fault, evidence);

    if !decision.is_retry()
        && let Some(entered) = entered
    {
        emit(ChunkRuntimeEvent::Fault(
            FaultRuntimeEvent::new(LifecycleEventKind::RetryExhausted, sequence, fault.phase())
                .with_summary(fault.summary())
                .with_ordinal(fault.retry_ordinal()),
        ));
        let failures = components
            .item_listeners
            .on_retry_exhausted(entered, fault, listener_context)
            .await;
        if let Some(first) = failures.first().copied() {
            state.item_listener_failures.extend(failures);
            return Err(item_listener_outcome(first.kind()));
        }
    }
    Ok(decision)
}

/// Builds the framework-owned classification input for one fault.
fn descriptor<I, O, P, W>(
    components: &Components<'_, I, O, P, W>,
    state: &mut ExecutionState,
    skips: &[PendingSkip<O>],
    phase: FaultPhase,
    category: FailureCategory,
) -> Option<FaultDescriptor>
where
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let delivery_mode = components.fault.map_or(
        crate::ChunkDeliveryMode::AtLeastOnce,
        FaultRuntime::delivery_mode,
    );
    let committed = projected_skips(state.skip_counts, skips)?;
    state.next_failure_id = state.next_failure_id.saturating_add(1);
    let failure_id = FailureId::new(state.next_failure_id).ok()?;
    Some(FaultDescriptor::new(
        phase,
        FailureSummary::new(category, failure_id),
        RetryOrdinal::INITIAL,
        committed,
        true,
        delivery_mode,
    ))
}

/// Replaces the descriptor ordinal with the durably reserved one.
async fn with_reserved_ordinal<I, O, P, W>(
    components: &Components<'_, I, O, P, W>,
    key: RetryKey,
    fault: FaultDescriptor,
) -> FaultDescriptor
where
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let Some(fault_runtime) = components.fault else {
        return fault;
    };
    let ordinal = fault_runtime
        .state()
        .reserved_ordinal(key)
        .await
        .ok()
        .flatten()
        .unwrap_or(RetryOrdinal::INITIAL);
    FaultDescriptor::new(
        fault.phase(),
        fault.summary(),
        ordinal,
        fault.committed_skips(),
        fault.is_transaction_open(),
        fault.delivery_mode(),
    )
}

/// Marks one retry key resolved because its unit of work finished.
async fn resolve_key<I, O, P, W>(components: &Components<'_, I, O, P, W>, key: RetryKey)
where
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    if let Some(fault_runtime) = components.fault {
        let _ = fault_runtime.state().resolve(key).await;
    }
}

fn retry_key<I, O, P, W>(
    components: &Components<'_, I, O, P, W>,
    checkpoint_digest: [u8; 32],
    phase: FaultPhase,
    ordinal: u64,
) -> RetryKey
where
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    RetryKey::derive(
        &components.definition_digest,
        components.step_name,
        phase,
        &checkpoint_digest,
        ordinal,
    )
}

fn checkpoint_digest(checkpoint: &crate::Checkpoint) -> [u8; 32] {
    checkpoint.generation_digest()
}

fn emit_committed_skips<I, O, E>(buffer: &ChunkBuffer<I, O>, sequence: ChunkCount, emit: &mut E)
where
    E: FnMut(ChunkRuntimeEvent),
{
    for skip in &buffer.skips {
        emit(ChunkRuntimeEvent::Fault(
            FaultRuntimeEvent::new(LifecycleEventKind::ItemSkipped, sequence, skip.phase)
                .with_summary(skip.fault.summary()),
        ));
        if skip.disposition == RollbackDisposition::CommitSafeSkip {
            emit(ChunkRuntimeEvent::Fault(
                FaultRuntimeEvent::new(
                    LifecycleEventKind::FaultNoRollbackCommitted,
                    sequence,
                    skip.phase,
                )
                .with_summary(skip.fault.summary()),
            ));
        }
    }
}

const fn item_listener_outcome(kind: ListenerFailureKind) -> ChunkExecutionOutcome {
    match kind {
        ListenerFailureKind::Error => ChunkExecutionOutcome::Failed(ChunkFailure::ItemListener),
        ListenerFailureKind::Panic => {
            ChunkExecutionOutcome::Failed(ChunkFailure::ItemListenerPanic)
        }
    }
}

async fn finish_failed_attempt(
    listeners: &[Arc<dyn ChunkListener>],
    context: ChunkListenerContext<'_>,
    attempt_outcome: ChunkAttemptOutcome,
    outcome: ChunkExecutionOutcome,
    state: &mut ExecutionState,
) -> ChunkExecutionReport {
    let failures = run_after_listeners(listeners, context, attempt_outcome).await;
    if let Some(first) = failures.first().copied() {
        state.listener_failures.extend(failures);
        if outcome == ChunkExecutionOutcome::Unknown {
            return state.drain().report(outcome, None);
        }
        return state
            .drain()
            .report(listener_failure_outcome(first.kind()), Some(outcome));
    }
    state.drain().report(outcome, None)
}

async fn run_before_listeners(
    listeners: &[Arc<dyn ChunkListener>],
    context: ChunkListenerContext<'_>,
) -> Option<ChunkListenerFailure> {
    for (index, listener) in listeners.iter().enumerate() {
        if let Err(kind) = invoke_before_listener(listener.as_ref(), context).await {
            return Some(ChunkListenerFailure::new(
                ChunkListenerPhase::BeforeChunk,
                index,
                kind,
            ));
        }
    }
    None
}

async fn run_after_listeners(
    listeners: &[Arc<dyn ChunkListener>],
    context: ChunkListenerContext<'_>,
    outcome: ChunkAttemptOutcome,
) -> Vec<ChunkListenerFailure> {
    let mut failures = Vec::new();
    for (index, listener) in listeners.iter().enumerate().rev() {
        if let Err(kind) = invoke_after_listener(listener.as_ref(), context, outcome).await {
            failures.push(ChunkListenerFailure::new(
                ChunkListenerPhase::AfterChunk,
                index,
                kind,
            ));
        }
    }
    failures
}

const fn listener_failure_outcome(kind: ChunkListenerFailureKind) -> ChunkExecutionOutcome {
    match kind {
        ChunkListenerFailureKind::Error => ChunkExecutionOutcome::Failed(ChunkFailure::Listener),
        ChunkListenerFailureKind::Panic => {
            ChunkExecutionOutcome::Failed(ChunkFailure::ListenerPanic)
        }
    }
}

async fn invoke_before_listener(
    listener: &dyn ChunkListener,
    context: ChunkListenerContext<'_>,
) -> Result<(), ChunkListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.before_chunk(context)))
        .map_err(|_| ChunkListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ChunkListenerFailureKind::Error),
        Err(_) => Err(ChunkListenerFailureKind::Panic),
    }
}

async fn invoke_after_listener(
    listener: &dyn ChunkListener,
    context: ChunkListenerContext<'_>,
    outcome: ChunkAttemptOutcome,
) -> Result<(), ChunkListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.after_chunk(context, outcome)))
        .map_err(|_| ChunkListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ChunkListenerFailureKind::Error),
        Err(_) => Err(ChunkListenerFailureKind::Panic),
    }
}

struct ReaderInvocation<'a, I, R>(&'a mut R, PhantomData<I>);

impl<'a, I, R> ReaderInvocation<'a, I, R>
where
    R: ItemReader<I>,
{
    fn invoke(
        self,
        context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>> {
        Box::pin(self.0.read(context))
    }
}

async fn invoke_reader<'a, I, R>(
    reader: &'a mut R,
    context: ReadContext<'a>,
) -> Invoked<ReadOutcome<I>, ReaderError>
where
    R: ItemReader<I>,
{
    let invocation = ReaderInvocation(reader, PhantomData);
    let Ok(future) = catch_unwind(AssertUnwindSafe(move || invocation.invoke(context))) else {
        return Invoked::Panicked;
    };
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Invoked::Completed(outcome),
        Ok(Err(error)) => Invoked::Failed(error),
        Err(_) => Invoked::Panicked,
    }
}

async fn invoke_processor<I, O, P>(
    processor: &P,
    item: &I,
    context: ProcessContext<'_>,
) -> Invoked<ProcessOutcome<O>, ProcessorError>
where
    P: ItemProcessor<I, O>,
{
    let Ok(future) = catch_unwind(AssertUnwindSafe(|| processor.process(item, context))) else {
        return Invoked::Panicked;
    };
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Invoked::Completed(outcome),
        Ok(Err(error)) => Invoked::Failed(error),
        Err(_) => Invoked::Panicked,
    }
}

async fn invoke_writer<'a, O, W>(
    writer: &'a W,
    items: &'a [O],
    context: WriteContext<'a>,
) -> Invoked<WriteOutcome, WriterError>
where
    W: ItemWriter<O>,
{
    let Ok(future) = catch_unwind(AssertUnwindSafe(|| writer.write(items, context))) else {
        return Invoked::Panicked;
    };
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Invoked::Completed(outcome),
        Ok(Err(error)) => Invoked::Failed(error),
        Err(_) => Invoked::Panicked,
    }
}

async fn invoke_completion(
    completion: &dyn ChunkCompletion,
    context: ChunkCompletionContext<'_>,
) -> Result<ChunkCompletionOutcome, ChunkFailure> {
    let future = catch_unwind(AssertUnwindSafe(|| completion.after_commit(context)))
        .map_err(|_| ChunkFailure::CompletionPanic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(ChunkFailure::Completion),
        Err(_) => Err(ChunkFailure::CompletionPanic),
    }
}
