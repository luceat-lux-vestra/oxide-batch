//! Deterministic single-threaded chunk-step orchestration.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    BoxFuture, ChunkCompletion, ChunkCompletionContext, ChunkCompletionOutcome, ChunkCount,
    ChunkCounts, ChunkProgress, ChunkSize, ChunkTransactionError, ChunkTransactionManager,
    ItemProcessor, ItemReader, ItemWriter, JobExecutionListener, JobLauncher, JobName,
    JobParameters, LaunchError, LaunchReport, LifecycleEventKind, ProcessContext, ProcessOutcome,
    ReadContext, ReadOutcome, StepExecutionListener, StepName, StopToken, Tasklet, TaskletContext,
    TaskletError, TaskletJob, TaskletOutcome, TaskletStep, WriteContext, WriteOutcome,
};

/// A validated one-step chunk definition.
pub struct ChunkStep<I, O> {
    name: StepName,
    size: ChunkSize,
    reader: Box<dyn ItemReader<I>>,
    processor: Arc<dyn ItemProcessor<I, O>>,
    writer: Arc<dyn ItemWriter<O>>,
    transactions: Arc<dyn ChunkTransactionManager>,
    completion: Arc<dyn ChunkCompletion>,
    listeners: Vec<Arc<dyn ChunkListener>>,
    step_listeners: Vec<Arc<dyn StepExecutionListener>>,
}

impl<I, O> ChunkStep<I, O> {
    /// Constructs a chunk step from facade-owned component and transaction
    /// ports.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: StepName,
        size: ChunkSize,
        reader: Box<dyn ItemReader<I>>,
        processor: Arc<dyn ItemProcessor<I, O>>,
        writer: Arc<dyn ItemWriter<O>>,
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
        }
    }

    /// Registers a chunk listener in deterministic before-order.
    #[must_use]
    pub fn with_chunk_listener(mut self, listener: Arc<dyn ChunkListener>) -> Self {
        self.listeners.push(listener);
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
    /// commits appear in the report.
    pub async fn execute(&mut self, stop: &StopToken) -> ChunkExecutionReport
    where
        I: Send + Sync,
        O: Send + Sync,
    {
        execute_chunk_step(self, stop, |_| {}).await
    }
}

impl<I, O> fmt::Debug for ChunkStep<I, O> {
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
pub struct ChunkJob<I, O> {
    name: JobName,
    step_name: StepName,
    tasklet: Arc<ChunkTasklet<I, O>>,
    step_listeners: Vec<Arc<dyn StepExecutionListener>>,
    listeners: Vec<Arc<dyn JobExecutionListener>>,
}

impl<I, O> ChunkJob<I, O> {
    /// Constructs a single-step chunk job.
    #[must_use]
    pub fn new(name: JobName, step: ChunkStep<I, O>) -> Self {
        let step_name = step.name.clone();
        let step_listeners = step.step_listeners.clone();
        Self {
            name,
            step_name,
            tasklet: Arc::new(ChunkTasklet::new(step)),
            step_listeners,
            listeners: Vec::new(),
        }
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

impl<I, O> fmt::Debug for ChunkJob<I, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkJob")
            .field("name", &self.name)
            .field("step_name", &self.step_name)
            .field("listener_count", &self.listeners.len())
            .field("step_listener_count", &self.step_listeners.len())
            .finish_non_exhaustive()
    }
}

struct ChunkTasklet<I, O> {
    step: AsyncMutex<ChunkStep<I, O>>,
    last_report: Mutex<Option<ChunkExecutionReport>>,
}

impl<I, O> ChunkTasklet<I, O> {
    fn new(step: ChunkStep<I, O>) -> Self {
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

impl<I, O> Tasklet for ChunkTasklet<I, O>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let mut step = self.step.lock().await;
            let report = execute_chunk_step(&mut step, context.stop_token(), |event| match event {
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
            })
            .await;
            let outcome = match report.outcome() {
                ChunkExecutionOutcome::Completed => Ok(TaskletOutcome::Completed),
                ChunkExecutionOutcome::Stopped => Ok(TaskletOutcome::Stopped),
                ChunkExecutionOutcome::Failed(_) => Err(TaskletError::new()),
                ChunkExecutionOutcome::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            };
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
    pub async fn launch_chunk<I, O>(
        &self,
        job: &mut ChunkJob<I, O>,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<ChunkLaunchReport, LaunchError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
    {
        job.tasklet.clear_last_report();
        let tasklet: Arc<dyn Tasklet> = job.tasklet.clone();
        let mut tasklet_step = TaskletStep::new(job.step_name.clone(), tasklet);
        for listener in &job.step_listeners {
            tasklet_step = tasklet_step.with_listener(Arc::clone(listener));
        }
        let mut tasklet_job = TaskletJob::new(job.name.clone(), tasklet_step);
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkRuntimeEvent {
    Started(ChunkCount),
    Committed(ChunkCount),
    RolledBack(ChunkCount),
    Unknown(ChunkCount),
}

struct ExecutionState {
    committed_counts: ChunkCounts,
    committed_chunks: ChunkCount,
    rolled_back_chunks: ChunkCount,
    listener_failures: Vec<ChunkListenerFailure>,
}

impl ExecutionState {
    fn new() -> Self {
        Self {
            committed_counts: ChunkCounts::default(),
            committed_chunks: ChunkCount::ZERO,
            rolled_back_chunks: ChunkCount::ZERO,
            listener_failures: Vec::new(),
        }
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
        }
    }
}

#[allow(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "the executor keeps the canonical read/process/write/commit order visible"
)]
pub(crate) async fn execute_chunk_step<I, O>(
    step: &mut ChunkStep<I, O>,
    stop: &StopToken,
    mut emit: impl FnMut(ChunkRuntimeEvent),
) -> ChunkExecutionReport
where
    I: Send + Sync,
    O: Send + Sync,
{
    let mut state = ExecutionState::new();
    let mut sequence = ChunkCount::ZERO;

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

        if let Some(failure) = run_before_listeners(&step.listeners, listener_context).await {
            let outcome = listener_failure_outcome(failure.kind());
            state.listener_failures.push(failure);
            return state.report(outcome, None);
        }

        let mut transaction = match step.transactions.begin().await {
            Ok(transaction) => transaction,
            Err(ChunkTransactionError::NotCommitted) => {
                let outcome = ChunkExecutionOutcome::Failed(ChunkFailure::TransactionBegin);
                return finish_failed_attempt(
                    &step.listeners,
                    listener_context,
                    ChunkAttemptOutcome::RolledBack,
                    outcome,
                    &mut state,
                )
                .await;
            }
            Err(ChunkTransactionError::CommitOutcomeUnknown) => {
                return finish_failed_attempt(
                    &step.listeners,
                    listener_context,
                    ChunkAttemptOutcome::Unknown,
                    ChunkExecutionOutcome::Unknown,
                    &mut state,
                )
                .await;
            }
        };
        emit(ChunkRuntimeEvent::Started(sequence));

        let mut progress = ChunkProgress::new(step.size);
        let mut outputs = Vec::with_capacity(step.size.get() as usize);
        let mut end_of_input = false;

        while !progress.is_full() {
            if stop.is_stop_requested() {
                return rollback_attempt(
                    transaction.as_mut(),
                    &step.listeners,
                    listener_context,
                    ChunkExecutionOutcome::Stopped,
                    &mut state,
                    sequence,
                    &mut emit,
                )
                .await;
            }

            match invoke_reader(step.reader.as_mut(), ReadContext::new(stop)).await {
                Ok(ReadOutcome::Item(item)) => {
                    if progress.record_read().is_err() {
                        return rollback_attempt(
                            transaction.as_mut(),
                            &step.listeners,
                            listener_context,
                            ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                            &mut state,
                            sequence,
                            &mut emit,
                        )
                        .await;
                    }
                    match invoke_processor(
                        step.processor.as_ref(),
                        &item,
                        ProcessContext::new(stop),
                    )
                    .await
                    {
                        Ok(ProcessOutcome::Item(output)) => {
                            if progress.record_processed().is_err() {
                                return rollback_attempt(
                                    transaction.as_mut(),
                                    &step.listeners,
                                    listener_context,
                                    ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                                    &mut state,
                                    sequence,
                                    &mut emit,
                                )
                                .await;
                            }
                            outputs.push(output);
                        }
                        Ok(ProcessOutcome::Filtered) => {
                            if progress.record_filtered().is_err() {
                                return rollback_attempt(
                                    transaction.as_mut(),
                                    &step.listeners,
                                    listener_context,
                                    ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                                    &mut state,
                                    sequence,
                                    &mut emit,
                                )
                                .await;
                            }
                        }
                        Ok(ProcessOutcome::Stopped) => {
                            return rollback_attempt(
                                transaction.as_mut(),
                                &step.listeners,
                                listener_context,
                                ChunkExecutionOutcome::Stopped,
                                &mut state,
                                sequence,
                                &mut emit,
                            )
                            .await;
                        }
                        Err(failure) => {
                            return rollback_attempt(
                                transaction.as_mut(),
                                &step.listeners,
                                listener_context,
                                ChunkExecutionOutcome::Failed(failure),
                                &mut state,
                                sequence,
                                &mut emit,
                            )
                            .await;
                        }
                    }
                }
                Ok(ReadOutcome::EndOfInput) => {
                    end_of_input = true;
                    break;
                }
                Ok(ReadOutcome::Stopped) => {
                    return rollback_attempt(
                        transaction.as_mut(),
                        &step.listeners,
                        listener_context,
                        ChunkExecutionOutcome::Stopped,
                        &mut state,
                        sequence,
                        &mut emit,
                    )
                    .await;
                }
                Err(failure) => {
                    return rollback_attempt(
                        transaction.as_mut(),
                        &step.listeners,
                        listener_context,
                        ChunkExecutionOutcome::Failed(failure),
                        &mut state,
                        sequence,
                        &mut emit,
                    )
                    .await;
                }
            }
        }

        if progress.counts().read() == ChunkCount::ZERO && end_of_input {
            if transaction.rollback().await.is_err() {
                return state.report(
                    ChunkExecutionOutcome::Failed(ChunkFailure::TransactionRollback),
                    None,
                );
            }
            emit(ChunkRuntimeEvent::RolledBack(sequence));
            let after_failures = run_after_listeners(
                &step.listeners,
                listener_context,
                ChunkAttemptOutcome::RolledBack,
            )
            .await;
            if let Some(first) = after_failures.first().copied() {
                state.listener_failures.extend(after_failures);
                return state.report(listener_failure_outcome(first.kind()), None);
            }
            return state.report(ChunkExecutionOutcome::Completed, None);
        }

        if !outputs.is_empty() {
            let write_context = match transaction.business_transaction() {
                Some(business) => WriteContext::enlisted(stop, business),
                None => WriteContext::non_transactional(stop),
            };
            match invoke_writer(step.writer.as_ref(), &outputs, write_context).await {
                Ok(WriteOutcome::Written) => {
                    let written = match u64::try_from(outputs.len()) {
                        Ok(count) => ChunkCount::new(count),
                        Err(_) => {
                            return rollback_attempt(
                                transaction.as_mut(),
                                &step.listeners,
                                listener_context,
                                ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                                &mut state,
                                sequence,
                                &mut emit,
                            )
                            .await;
                        }
                    };
                    if progress.record_written(written).is_err() {
                        return rollback_attempt(
                            transaction.as_mut(),
                            &step.listeners,
                            listener_context,
                            ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                            &mut state,
                            sequence,
                            &mut emit,
                        )
                        .await;
                    }
                }
                Ok(WriteOutcome::Stopped) => {
                    return rollback_attempt(
                        transaction.as_mut(),
                        &step.listeners,
                        listener_context,
                        ChunkExecutionOutcome::Stopped,
                        &mut state,
                        sequence,
                        &mut emit,
                    )
                    .await;
                }
                Err(failure) => {
                    return rollback_attempt(
                        transaction.as_mut(),
                        &step.listeners,
                        listener_context,
                        ChunkExecutionOutcome::Failed(failure),
                        &mut state,
                        sequence,
                        &mut emit,
                    )
                    .await;
                }
            }
        }

        let counts = progress.counts();
        let Ok(next_committed_counts) = state.committed_counts.checked_add(counts) else {
            return rollback_attempt(
                transaction.as_mut(),
                &step.listeners,
                listener_context,
                ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                &mut state,
                sequence,
                &mut emit,
            )
            .await;
        };
        let Ok(next_committed_chunks) = state.committed_chunks.checked_increment() else {
            return rollback_attempt(
                transaction.as_mut(),
                &step.listeners,
                listener_context,
                ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                &mut state,
                sequence,
                &mut emit,
            )
            .await;
        };
        let receipt = match transaction.commit(counts).await {
            Ok(receipt) => receipt,
            Err(ChunkTransactionError::NotCommitted) => {
                return rollback_attempt(
                    transaction.as_mut(),
                    &step.listeners,
                    listener_context,
                    ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit),
                    &mut state,
                    sequence,
                    &mut emit,
                )
                .await;
            }
            Err(ChunkTransactionError::CommitOutcomeUnknown) => {
                emit(ChunkRuntimeEvent::Unknown(sequence));
                return finish_failed_attempt(
                    &step.listeners,
                    listener_context,
                    ChunkAttemptOutcome::Unknown,
                    ChunkExecutionOutcome::Unknown,
                    &mut state,
                )
                .await;
            }
        };

        state.committed_counts = next_committed_counts;
        state.committed_chunks = next_committed_chunks;
        emit(ChunkRuntimeEvent::Committed(sequence));

        let completion_context = ChunkCompletionContext::new(
            receipt.checkpoint(),
            receipt.execution_context(),
            counts,
            stop,
        );
        let completion = invoke_completion(step.completion.as_ref(), completion_context).await;
        let terminal_outcome = match completion {
            Ok(ChunkCompletionOutcome::Acknowledged) => {
                if stop.is_stop_requested() {
                    Some(ChunkExecutionOutcome::Stopped)
                } else if end_of_input {
                    Some(ChunkExecutionOutcome::Completed)
                } else {
                    None
                }
            }
            Ok(ChunkCompletionOutcome::StoppedAfterCommit) => Some(ChunkExecutionOutcome::Stopped),
            Err(failure) => Some(ChunkExecutionOutcome::Failed(failure)),
        };

        let after_listener_context =
            ChunkListenerContext::new(sequence, state.committed_counts, stop);
        let after_failures = run_after_listeners(
            &step.listeners,
            after_listener_context,
            ChunkAttemptOutcome::Committed,
        )
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
            return std::mem::replace(state, ExecutionState::new()).report(outcome, None);
        }
        return std::mem::replace(state, ExecutionState::new())
            .report(listener_failure_outcome(first.kind()), Some(outcome));
    }
    std::mem::replace(state, ExecutionState::new()).report(outcome, None)
}

async fn rollback_attempt(
    transaction: &mut dyn crate::ChunkTransaction,
    listeners: &[Arc<dyn ChunkListener>],
    context: ChunkListenerContext<'_>,
    outcome: ChunkExecutionOutcome,
    state: &mut ExecutionState,
    sequence: ChunkCount,
    emit: &mut impl FnMut(ChunkRuntimeEvent),
) -> ChunkExecutionReport {
    if transaction.rollback().await.is_err() {
        return std::mem::replace(state, ExecutionState::new()).report(
            ChunkExecutionOutcome::Failed(ChunkFailure::TransactionRollback),
            Some(outcome),
        );
    }
    state.rolled_back_chunks = match state.rolled_back_chunks.checked_increment() {
        Ok(count) => count,
        Err(_) => {
            return std::mem::replace(state, ExecutionState::new()).report(
                ChunkExecutionOutcome::Failed(ChunkFailure::Count),
                Some(outcome),
            );
        }
    };
    emit(ChunkRuntimeEvent::RolledBack(sequence));
    let attempt_outcome = if outcome == ChunkExecutionOutcome::Stopped {
        ChunkAttemptOutcome::Stopped
    } else {
        ChunkAttemptOutcome::RolledBack
    };
    finish_failed_attempt(listeners, context, attempt_outcome, outcome, state).await
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

struct ReaderInvocation<'a, I>(&'a mut dyn ItemReader<I>);

impl<'a, I> ReaderInvocation<'a, I> {
    fn invoke(
        self,
        context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<I>, crate::ReaderError>> {
        self.0.read(context)
    }
}

async fn invoke_reader<'a, I>(
    reader: &'a mut dyn ItemReader<I>,
    context: ReadContext<'a>,
) -> Result<ReadOutcome<I>, ChunkFailure> {
    let invocation = ReaderInvocation(reader);
    let future = catch_unwind(AssertUnwindSafe(move || invocation.invoke(context)))
        .map_err(|_| ChunkFailure::ReaderPanic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(ChunkFailure::Reader),
        Err(_) => Err(ChunkFailure::ReaderPanic),
    }
}

async fn invoke_processor<I, O>(
    processor: &dyn ItemProcessor<I, O>,
    item: &I,
    context: ProcessContext<'_>,
) -> Result<ProcessOutcome<O>, ChunkFailure> {
    let future = catch_unwind(AssertUnwindSafe(|| processor.process(item, context)))
        .map_err(|_| ChunkFailure::ProcessorPanic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(ChunkFailure::Processor),
        Err(_) => Err(ChunkFailure::ProcessorPanic),
    }
}

async fn invoke_writer<'a, O>(
    writer: &'a dyn ItemWriter<O>,
    items: &'a [O],
    context: WriteContext<'a>,
) -> Result<WriteOutcome, ChunkFailure> {
    let future = catch_unwind(AssertUnwindSafe(|| writer.write(items, context)))
        .map_err(|_| ChunkFailure::WriterPanic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(ChunkFailure::Writer),
        Err(_) => Err(ChunkFailure::WriterPanic),
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
