//! Async tasklet execution and cooperative stopping.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::FutureExt;
use tokio::sync::{Notify, Semaphore};

use crate::{
    BatchStatus, BoxFuture, Clock, ExecutionAttempt, ExecutionCorrelation, ExitStatus,
    FailureCategory, FailureSummary, IdGenerator, JobExecution, JobExecutionId,
    JobExecutionListener, JobInstance, JobInstanceKey, JobName, JobParameters, JobRepository,
    LifecycleEvent, LifecycleEventKind, LifecycleEventSink, LifecycleTransition, ListenerContext,
    ListenerFailure, ListenerFailureKind, ListenerPhase, RepositoryError, StepExecution,
    StepExecutionId, StepExecutionListener, StepName,
};

/// A dynamically dispatched, single-invocation asynchronous step body.
///
/// Implementations may borrow both themselves and the call-scoped
/// [`TaskletContext`] for the entire returned future. They must observe the
/// supplied [`StopToken`] when performing cancellable or repeated work.
///
/// ```
/// use oxide_batch::{
///     BoxFuture, Tasklet, TaskletContext, TaskletError, TaskletOutcome,
/// };
///
/// struct ImportTasklet;
///
/// impl Tasklet for ImportTasklet {
///     fn execute<'a>(
///         &'a self,
///         context: TaskletContext<'a>,
///     ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
///         Box::pin(async move {
///             if context.stop_token().is_stop_requested() {
///                 return Ok(TaskletOutcome::Stopped);
///             }
///             let _parameters = context.parameters();
///             Ok(TaskletOutcome::Completed)
///         })
///     }
/// }
/// ```
pub trait Tasklet: Send + Sync {
    /// Executes the step body once.
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>>;
}

/// A synchronous tasklet isolated by [`BlockingTaskletAdapter`].
///
/// Once this method starts it runs to completion even when stop is requested.
/// The adapter reports that request as a late stop after the method returns.
pub trait BlockingTasklet: Send + Sync + 'static {
    /// Executes owned call context on a blocking worker.
    ///
    /// # Errors
    ///
    /// Returns [`TaskletError`] when user work cannot complete.
    fn execute(&self, context: BlockingTaskletContext) -> Result<TaskletOutcome, TaskletError>;
}

/// A validated one-step tasklet definition.
pub struct TaskletStep {
    name: StepName,
    tasklet: Arc<dyn Tasklet>,
    listeners: Vec<Arc<dyn StepExecutionListener>>,
}

impl TaskletStep {
    /// Constructs a step from its validated name and async body.
    #[must_use]
    pub fn new(name: StepName, tasklet: Arc<dyn Tasklet>) -> Self {
        Self {
            name,
            tasklet,
            listeners: Vec::new(),
        }
    }

    /// Registers a step listener in deterministic before-order.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn StepExecutionListener>) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Borrows the step name.
    #[must_use]
    pub const fn name(&self) -> &StepName {
        &self.name
    }
}

impl fmt::Debug for TaskletStep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskletStep")
            .field("name", &self.name)
            .field("listener_count", &self.listeners.len())
            .finish_non_exhaustive()
    }
}

/// A validated single-step job definition.
pub struct TaskletJob {
    name: JobName,
    step: TaskletStep,
    listeners: Vec<Arc<dyn JobExecutionListener>>,
}

impl fmt::Debug for TaskletJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskletJob")
            .field("name", &self.name)
            .field("step", &self.step)
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

impl TaskletJob {
    /// Constructs a single-step job.
    #[must_use]
    pub const fn new(name: JobName, step: TaskletStep) -> Self {
        Self {
            name,
            step,
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

    /// Borrows the sole step.
    #[must_use]
    pub const fn step(&self) -> &TaskletStep {
        &self.step
    }
}

/// Borrowed execution data supplied to an asynchronous tasklet.
///
/// Its references are call-scoped and cannot be retained as static framework
/// or application state:
///
/// ```compile_fail
/// use oxide_batch::{JobParameters, TaskletContext};
///
/// fn escape(context: TaskletContext<'_>) -> &'static JobParameters {
///     context.parameters()
/// }
/// ```
#[derive(Clone, Copy)]
pub struct TaskletContext<'a> {
    parameters: &'a JobParameters,
    job_execution_id: JobExecutionId,
    step_execution_id: StepExecutionId,
    stop: &'a StopToken,
    correlation: &'a ExecutionCorrelation,
    event_sink: Option<&'a dyn LifecycleEventSink>,
}

impl<'a> TaskletContext<'a> {
    /// Borrows the launch parameters.
    #[must_use]
    pub const fn parameters(&self) -> &'a JobParameters {
        self.parameters
    }

    /// Returns the enclosing job-attempt identifier.
    #[must_use]
    pub const fn job_execution_id(self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns this step-attempt identifier.
    #[must_use]
    pub const fn step_execution_id(self) -> StepExecutionId {
        self.step_execution_id
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(&self) -> &'a StopToken {
        self.stop
    }

    /// Borrows the validated execution correlation.
    #[must_use]
    pub const fn correlation(&self) -> &'a ExecutionCorrelation {
        self.correlation
    }

    pub(crate) fn emit_chunk_event(&self, kind: LifecycleEventKind, sequence: crate::ChunkCount) {
        let Some(sink) = self.event_sink else {
            return;
        };
        let event = LifecycleEvent::chunk(kind, self.correlation.clone(), sequence);
        let _ = catch_unwind(AssertUnwindSafe(|| sink.emit(&event)));
    }

    fn into_blocking(self) -> BlockingTaskletContext {
        BlockingTaskletContext {
            parameters: self.parameters.clone(),
            job_execution_id: self.job_execution_id,
            step_execution_id: self.step_execution_id,
            stop: self.stop.clone(),
        }
    }
}

impl fmt::Debug for TaskletContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskletContext")
            .field("job_execution_id", &self.job_execution_id)
            .field("step_execution_id", &self.step_execution_id)
            .field("stop_requested", &self.stop.is_stop_requested())
            .field("correlation", &self.correlation)
            .field("event_sink", &self.event_sink.map(|_| "<attached>"))
            .finish_non_exhaustive()
    }
}

/// Owned execution data supplied to an isolated blocking tasklet.
#[derive(Clone, Debug)]
pub struct BlockingTaskletContext {
    parameters: JobParameters,
    job_execution_id: JobExecutionId,
    step_execution_id: StepExecutionId,
    stop: StopToken,
}

impl BlockingTaskletContext {
    /// Borrows the launch parameters.
    #[must_use]
    pub const fn parameters(&self) -> &JobParameters {
        &self.parameters
    }

    /// Returns the enclosing job-attempt identifier.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns this step-attempt identifier.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }

    /// Borrows the cooperative stop token.
    ///
    /// Blocking code cannot be interrupted after it starts. This token is
    /// useful only for application-specific polling inside the synchronous
    /// body; the adapter still awaits the body before returning.
    #[must_use]
    pub const fn stop_token(&self) -> &StopToken {
        &self.stop
    }
}

/// The user-controlled result of one tasklet invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TaskletOutcome {
    /// User work finished successfully.
    Completed,
    /// User work observed a cooperative stop.
    Stopped,
    /// A blocking adapter completed already-running synchronous work and then
    /// observed stop.
    ///
    /// Application tasklets should return [`Self::Stopped`]. This variant is
    /// emitted by [`BlockingTaskletAdapter`] to preserve the late-stop
    /// limitation in the launch report.
    StoppedAfterBlockingWork,
    /// An adapter-owned commit returned without a knowable durable outcome.
    ///
    /// Application tasklets should not return this variant. It exists so
    /// framework adapters can persist `UNKNOWN` without guessing.
    CommitOutcomeUnknown,
}

/// A value-redacted typed user-component failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskletError {
    kind: TaskletErrorKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskletErrorKind {
    Component,
    Panic,
}

impl TaskletError {
    /// Constructs a classified tasklet failure.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            kind: TaskletErrorKind::Component,
        }
    }

    /// Classifies an arbitrary user error without retaining its payload.
    #[must_use]
    pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
        drop(error);
        Self::new()
    }

    const fn panic() -> Self {
        Self {
            kind: TaskletErrorKind::Panic,
        }
    }
}

impl Default for TaskletError {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskletError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the tasklet failed")
    }
}

impl Error for TaskletError {}

#[derive(Debug)]
struct StopState {
    requested: AtomicBool,
    notify: Notify,
}

/// The owner used by application code or an operator adapter to request stop.
#[derive(Clone, Debug)]
pub struct StopSource {
    state: Arc<StopState>,
}

/// A cloneable cooperative stop token passed to user work.
#[derive(Clone, Debug)]
pub struct StopToken {
    state: Arc<StopState>,
}

impl StopSource {
    /// Creates a stop source and its corresponding tasklet token.
    #[must_use]
    pub fn new() -> (Self, StopToken) {
        let state = Arc::new(StopState {
            requested: AtomicBool::new(false),
            notify: Notify::new(),
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            StopToken { state },
        )
    }

    /// Requests a cooperative stop and wakes current waiters.
    pub fn request_stop(&self) {
        self.state.requested.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }
}

impl StopToken {
    /// Returns whether stop has been requested.
    #[must_use]
    pub fn is_stop_requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    /// Waits for a stop request without exposing an executor-specific type.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_stop_requested() {
                return;
            }
            notified.await;
        }
    }
}

/// Classifies when a cooperative stop was observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StopTiming {
    /// Stop was requested before user work started.
    BeforeStart,
    /// An asynchronous tasklet observed stop while it was running.
    DuringExecution,
    /// Stop arrived after synchronous work started and was reported after it
    /// completed.
    AfterBlockingWork,
}

/// Classifies a tasklet failure without exposing an error or panic payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TaskletFailure {
    /// The tasklet returned [`TaskletError`].
    Error,
    /// The tasklet panicked before or while its future was polled.
    Panic,
    /// A listener returned a classified error.
    ListenerError,
    /// A listener panicked at its framework boundary.
    ListenerPanic,
}

/// The stable execution result captured by a launch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TaskletExecutionOutcome {
    /// The job and step completed.
    Completed,
    /// The job and step failed at the tasklet boundary.
    Failed(TaskletFailure),
    /// The job and step stopped cooperatively.
    Stopped(StopTiming),
    /// A resource commit may or may not have reached durable storage.
    Unknown,
}

/// Final persisted execution snapshots returned by [`JobLauncher`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchReport {
    instance: JobInstance,
    job_execution: JobExecution,
    step_execution: StepExecution,
    outcome: TaskletExecutionOutcome,
    original_outcome: Option<TaskletExecutionOutcome>,
    original_failure: Option<FailureSummary>,
    listener_failures: Vec<ListenerFailure>,
}

impl LaunchReport {
    /// Borrows the selected logical job instance.
    #[must_use]
    pub const fn instance(&self) -> &JobInstance {
        &self.instance
    }

    /// Borrows the final job-execution snapshot.
    #[must_use]
    pub const fn job_execution(&self) -> &JobExecution {
        &self.job_execution
    }

    /// Borrows the final step-execution snapshot.
    #[must_use]
    pub const fn step_execution(&self) -> &StepExecution {
        &self.step_execution
    }

    /// Returns the classified user-work outcome.
    #[must_use]
    pub const fn outcome(&self) -> TaskletExecutionOutcome {
        self.outcome
    }

    /// Returns the provisional tasklet or nested-listener outcome retained
    /// when an after-listener changed the enclosing result.
    #[must_use]
    pub const fn original_outcome(&self) -> Option<TaskletExecutionOutcome> {
        self.original_outcome
    }

    /// Returns the original redacted tasklet failure retained when a listener
    /// changed the final outcome.
    #[must_use]
    pub const fn original_failure(&self) -> Option<FailureSummary> {
        self.original_failure
    }

    /// Borrows listener failures in callback execution order.
    #[must_use]
    pub fn listener_failures(&self) -> &[ListenerFailure] {
        &self.listener_failures
    }
}

/// A launch failure that prevented a final execution report.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LaunchError {
    /// Metadata creation, transition, or commit failed.
    Repository(RepositoryError),
}

impl fmt::Display for LaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => {
                write!(formatter, "job repository operation failed: {error}")
            }
        }
    }
}

impl Error for LaunchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
        }
    }
}

impl From<RepositoryError> for LaunchError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

/// Async-first launcher for one-step tasklet jobs.
///
/// The launcher borrows its repository, clock, and identifier source and never
/// creates or owns an async runtime.
pub struct JobLauncher<'a> {
    repository: &'a dyn JobRepository,
    clock: &'a dyn Clock,
    ids: &'a dyn IdGenerator,
    event_sink: Option<&'a dyn LifecycleEventSink>,
}

impl<'a> JobLauncher<'a> {
    /// Constructs a launcher from explicitly owned infrastructure ports.
    #[must_use]
    pub const fn new(
        repository: &'a dyn JobRepository,
        clock: &'a dyn Clock,
        ids: &'a dyn IdGenerator,
    ) -> Self {
        Self {
            repository,
            clock,
            ids,
            event_sink: None,
        }
    }

    /// Attaches a non-authoritative lifecycle-event sink.
    #[must_use]
    pub const fn with_event_sink(mut self, event_sink: &'a dyn LifecycleEventSink) -> Self {
        self.event_sink = Some(event_sink);
        self
    }

    /// Creates and executes one launch or restart attempt.
    ///
    /// Creation is committed before the `STARTED` transition, and `STARTED` is
    /// committed before user work. A final status is committed after the
    /// tasklet boundary returns. Dropping this future can therefore leave an
    /// accepted execution in `STARTING` or `STARTED`; recovery must inspect the
    /// repository rather than infer an outcome.
    ///
    /// # Errors
    ///
    /// Returns [`LaunchError`] when a repository operation, lifecycle update,
    /// or commit fails. A tasklet error or panic is instead persisted as a
    /// failed execution and returned in [`LaunchReport`].
    #[allow(
        clippy::too_many_lines,
        reason = "the launch method keeps the listener nesting and commit order visible"
    )]
    pub async fn launch(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<LaunchReport, LaunchError> {
        let key = JobInstanceKey::new(job.name.clone(), parameters);
        let graph = self.create_execution_graph(&key, job.step.name()).await?;
        self.emit_event(LifecycleEventKind::LaunchAccepted, &graph.correlation, None);
        self.emit_event(LifecycleEventKind::JobStarting, &graph.correlation, None);
        self.emit_event(LifecycleEventKind::StepStarting, &graph.correlation, None);

        if stop.is_stop_requested() {
            let (job_execution, step_execution) = self
                .stop_graph(
                    &graph.job_execution,
                    &graph.step_execution,
                    &graph.correlation,
                )
                .await?;
            return Ok(LaunchReport {
                instance: graph.instance,
                job_execution,
                step_execution,
                outcome: TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart),
                original_outcome: None,
                original_failure: None,
                listener_failures: Vec::new(),
            });
        }

        let context = ListenerContext::new(&graph.correlation, parameters, stop);
        if let Some(failure) = self.run_before_job(&job.listeners, context).await? {
            let outcome = listener_failure_outcome(failure.kind());
            let step_execution = self
                .finish_step(
                    &graph.step_execution,
                    outcome,
                    Some(failure.summary()),
                    &graph.correlation,
                )
                .await?;
            let job_execution = self
                .finish_job(
                    &graph.job_execution,
                    outcome,
                    Some(failure.summary()),
                    &graph.correlation,
                )
                .await?;
            return Ok(LaunchReport {
                instance: graph.instance,
                job_execution,
                step_execution,
                outcome,
                original_outcome: None,
                original_failure: None,
                listener_failures: vec![failure],
            });
        }

        let started_job = self
            .start_job(&graph.job_execution, &graph.correlation)
            .await?;
        if stop.is_stop_requested() {
            let (job_execution, step_execution) = self
                .stop_graph(&started_job, &graph.step_execution, &graph.correlation)
                .await?;
            return Ok(LaunchReport {
                instance: graph.instance,
                job_execution,
                step_execution,
                outcome: TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart),
                original_outcome: None,
                original_failure: None,
                listener_failures: Vec::new(),
            });
        }

        if let Some(failure) = self.run_before_step(&job.step.listeners, context).await? {
            let outcome = listener_failure_outcome(failure.kind());
            let step_execution = self
                .finish_step(
                    &graph.step_execution,
                    outcome,
                    Some(failure.summary()),
                    &graph.correlation,
                )
                .await?;
            let mut listener_failures = vec![failure];
            let mut original_outcome = None;
            let after_job_failures = self.run_after_job(&job.listeners, context, outcome).await?;
            if !after_job_failures.is_empty() {
                original_outcome = Some(outcome);
                listener_failures.extend(after_job_failures);
            }
            let final_outcome = listener_failure_outcome(listener_failures[0].kind());
            let job_execution = self
                .finish_job(
                    &started_job,
                    final_outcome,
                    Some(listener_failures[0].summary()),
                    &graph.correlation,
                )
                .await?;
            return Ok(LaunchReport {
                instance: graph.instance,
                job_execution,
                step_execution,
                outcome: final_outcome,
                original_outcome,
                original_failure: None,
                listener_failures,
            });
        }

        let started_step = self
            .start_step(&graph.step_execution, &graph.correlation)
            .await?;
        let tasklet_context = TaskletContext {
            parameters,
            job_execution_id: started_job.id(),
            step_execution_id: started_step.id(),
            stop,
            correlation: &graph.correlation,
            event_sink: self.event_sink,
        };
        let invocation = invoke_tasklet(job.step.tasklet.as_ref(), tasklet_context).await;
        let provisional_outcome = match invocation {
            Ok(TaskletOutcome::Completed) if !stop.is_stop_requested() => {
                TaskletExecutionOutcome::Completed
            }
            Ok(TaskletOutcome::Completed | TaskletOutcome::Stopped) => {
                TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution)
            }
            Ok(TaskletOutcome::StoppedAfterBlockingWork) => {
                TaskletExecutionOutcome::Stopped(StopTiming::AfterBlockingWork)
            }
            Ok(TaskletOutcome::CommitOutcomeUnknown) => TaskletExecutionOutcome::Unknown,
            Err(failure) => TaskletExecutionOutcome::Failed(failure),
        };
        let tasklet_failure = if matches!(
            provisional_outcome,
            TaskletExecutionOutcome::Failed(TaskletFailure::Error | TaskletFailure::Panic)
        ) {
            Some(self.next_failure_summary()?)
        } else {
            None
        };

        let mut outcome = provisional_outcome;
        let mut original_outcome = None;
        let mut listener_failures = self
            .run_after_step(&job.step.listeners, context, outcome)
            .await?;
        if let Some(failure) = listener_failures.first()
            && outcome != TaskletExecutionOutcome::Unknown
        {
            original_outcome = Some(outcome);
            outcome = listener_failure_outcome(failure.kind());
        }
        let step_failure = listener_failures
            .first()
            .map(|failure| failure.summary())
            .or(tasklet_failure);
        let step_execution = self
            .finish_step(&started_step, outcome, step_failure, &graph.correlation)
            .await?;

        let after_job_failures = self.run_after_job(&job.listeners, context, outcome).await?;
        if !after_job_failures.is_empty() {
            if original_outcome.is_none() && outcome != TaskletExecutionOutcome::Unknown {
                original_outcome = Some(outcome);
            }
            if listener_failures.is_empty() && outcome != TaskletExecutionOutcome::Unknown {
                outcome = listener_failure_outcome(after_job_failures[0].kind());
            }
            listener_failures.extend(after_job_failures);
        }
        let job_failure = listener_failures
            .first()
            .map(|failure| failure.summary())
            .or(tasklet_failure);
        let job_execution = self
            .finish_job(&started_job, outcome, job_failure, &graph.correlation)
            .await?;

        Ok(LaunchReport {
            instance: graph.instance,
            job_execution,
            step_execution,
            outcome,
            original_outcome,
            original_failure: original_outcome.and(tasklet_failure),
            listener_failures,
        })
    }

    async fn create_execution_graph(
        &self,
        key: &JobInstanceKey,
        step_name: &StepName,
    ) -> Result<CreatedExecutionGraph, LaunchError> {
        let mut unit = self.repository.begin().await?;
        let instance = unit
            .select_or_create_job_instance(key)
            .await?
            .instance()
            .clone();
        let job_execution = unit.create_job_execution(instance.id()).await?;
        let step_execution = unit
            .create_step_execution(job_execution.id(), step_name)
            .await?;
        let attempt_count = unit.job_executions(instance.id()).await?.len();
        let attempt = u64::try_from(attempt_count)
            .ok()
            .and_then(NonZeroU64::new)
            .map(ExecutionAttempt::new)
            .ok_or(RepositoryError::Unavailable)?;
        unit.commit().await?;
        let correlation = ExecutionCorrelation::new(
            key.job_name().clone(),
            instance.id(),
            job_execution.id(),
            attempt,
            step_name.clone(),
            step_execution.id(),
            attempt,
        );
        Ok(CreatedExecutionGraph {
            instance,
            job_execution,
            step_execution,
            correlation,
        })
    }

    async fn start_job(
        &self,
        job: &JobExecution,
        correlation: &ExecutionCorrelation,
    ) -> Result<JobExecution, LaunchError> {
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let started_job = unit
            .transition_job_execution(
                job.id(),
                job.version(),
                LifecycleTransition::new(BatchStatus::Started, now),
            )
            .await?;
        unit.commit().await?;
        self.emit_event(LifecycleEventKind::JobStarted, correlation, None);
        Ok(started_job)
    }

    async fn start_step(
        &self,
        step: &StepExecution,
        correlation: &ExecutionCorrelation,
    ) -> Result<StepExecution, LaunchError> {
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let started_step = unit
            .transition_step_execution(
                step.id(),
                step.version(),
                LifecycleTransition::new(BatchStatus::Started, now),
            )
            .await?;
        unit.commit().await?;
        self.emit_event(LifecycleEventKind::StepStarted, correlation, None);
        Ok(started_step)
    }

    async fn finish_job(
        &self,
        job: &JobExecution,
        outcome: TaskletExecutionOutcome,
        failure: Option<FailureSummary>,
        correlation: &ExecutionCorrelation,
    ) -> Result<JobExecution, LaunchError> {
        let (status, exit_status) = final_status(outcome);
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let job = unit
            .enrich_job_exit_status(job.id(), job.version(), &exit_status)
            .await?;
        let transition = transition_for_outcome(status, now, failure)?;
        let job = unit
            .transition_job_execution(job.id(), job.version(), transition)
            .await?;
        unit.commit().await?;
        self.emit_final_event(true, outcome, correlation, failure);
        Ok(job)
    }

    async fn finish_step(
        &self,
        step: &StepExecution,
        outcome: TaskletExecutionOutcome,
        failure: Option<FailureSummary>,
        correlation: &ExecutionCorrelation,
    ) -> Result<StepExecution, LaunchError> {
        let (status, exit_status) = final_status(outcome);
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let step = unit
            .enrich_step_exit_status(step.id(), step.version(), &exit_status)
            .await?;
        let transition = transition_for_outcome(status, now, failure)?;
        let step = unit
            .transition_step_execution(step.id(), step.version(), transition)
            .await?;
        unit.commit().await?;
        self.emit_final_event(false, outcome, correlation, failure);
        Ok(step)
    }

    async fn stop_graph(
        &self,
        job: &JobExecution,
        step: &StepExecution,
        correlation: &ExecutionCorrelation,
    ) -> Result<(JobExecution, StepExecution), LaunchError> {
        let stopping_job = self.mark_job_stopping(job, correlation).await?;
        let stopping_step = self.mark_step_stopping(step, correlation).await?;
        let outcome = TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart);
        let step = self
            .finish_step(&stopping_step, outcome, None, correlation)
            .await?;
        let job = self
            .finish_job(&stopping_job, outcome, None, correlation)
            .await?;
        Ok((job, step))
    }

    async fn mark_job_stopping(
        &self,
        job: &JobExecution,
        correlation: &ExecutionCorrelation,
    ) -> Result<JobExecution, LaunchError> {
        let mut unit = self.repository.begin().await?;
        let job = unit
            .transition_job_execution(
                job.id(),
                job.version(),
                LifecycleTransition::new(BatchStatus::Stopping, self.clock.now()),
            )
            .await?;
        unit.commit().await?;
        self.emit_event(LifecycleEventKind::JobStopping, correlation, None);
        Ok(job)
    }

    async fn mark_step_stopping(
        &self,
        step: &StepExecution,
        correlation: &ExecutionCorrelation,
    ) -> Result<StepExecution, LaunchError> {
        let mut unit = self.repository.begin().await?;
        let step = unit
            .transition_step_execution(
                step.id(),
                step.version(),
                LifecycleTransition::new(BatchStatus::Stopping, self.clock.now()),
            )
            .await?;
        unit.commit().await?;
        self.emit_event(LifecycleEventKind::StepStopping, correlation, None);
        Ok(step)
    }

    async fn run_before_job(
        &self,
        listeners: &[Arc<dyn JobExecutionListener>],
        context: ListenerContext<'_>,
    ) -> Result<Option<ListenerFailure>, LaunchError> {
        for (index, listener) in listeners.iter().enumerate() {
            if let Err(kind) = invoke_before_job(listener.as_ref(), context).await {
                return self
                    .listener_failure(ListenerPhase::BeforeJob, index, kind, context)
                    .map(Some);
            }
        }
        Ok(None)
    }

    async fn run_before_step(
        &self,
        listeners: &[Arc<dyn StepExecutionListener>],
        context: ListenerContext<'_>,
    ) -> Result<Option<ListenerFailure>, LaunchError> {
        for (index, listener) in listeners.iter().enumerate() {
            if let Err(kind) = invoke_before_step(listener.as_ref(), context).await {
                return self
                    .listener_failure(ListenerPhase::BeforeStep, index, kind, context)
                    .map(Some);
            }
        }
        Ok(None)
    }

    async fn run_after_job(
        &self,
        listeners: &[Arc<dyn JobExecutionListener>],
        context: ListenerContext<'_>,
        outcome: TaskletExecutionOutcome,
    ) -> Result<Vec<ListenerFailure>, LaunchError> {
        let mut failures = Vec::new();
        for (index, listener) in listeners.iter().enumerate().rev() {
            if let Err(kind) = invoke_after_job(listener.as_ref(), context, outcome).await {
                failures.push(self.listener_failure(
                    ListenerPhase::AfterJob,
                    index,
                    kind,
                    context,
                )?);
            }
        }
        Ok(failures)
    }

    async fn run_after_step(
        &self,
        listeners: &[Arc<dyn StepExecutionListener>],
        context: ListenerContext<'_>,
        outcome: TaskletExecutionOutcome,
    ) -> Result<Vec<ListenerFailure>, LaunchError> {
        let mut failures = Vec::new();
        for (index, listener) in listeners.iter().enumerate().rev() {
            if let Err(kind) = invoke_after_step(listener.as_ref(), context, outcome).await {
                failures.push(self.listener_failure(
                    ListenerPhase::AfterStep,
                    index,
                    kind,
                    context,
                )?);
            }
        }
        Ok(failures)
    }

    fn listener_failure(
        &self,
        phase: ListenerPhase,
        registration_index: usize,
        kind: ListenerFailureKind,
        context: ListenerContext<'_>,
    ) -> Result<ListenerFailure, LaunchError> {
        let summary = self.next_failure_summary()?;
        let event_kind = match phase {
            ListenerPhase::BeforeJob => LifecycleEventKind::JobBeforeListenerFailed,
            ListenerPhase::BeforeStep => LifecycleEventKind::StepBeforeListenerFailed,
            ListenerPhase::AfterStep => LifecycleEventKind::StepAfterListenerFailed,
            ListenerPhase::AfterJob => LifecycleEventKind::JobAfterListenerFailed,
        };
        self.emit_event(event_kind, context.correlation(), Some(summary));
        Ok(ListenerFailure::new(
            phase,
            registration_index,
            kind,
            summary,
        ))
    }

    fn next_failure_summary(&self) -> Result<FailureSummary, LaunchError> {
        Ok(FailureSummary::new(
            FailureCategory::UserComponent,
            self.ids
                .next_failure_id()
                .map_err(RepositoryError::Identifier)?,
        ))
    }

    fn emit_final_event(
        &self,
        job: bool,
        outcome: TaskletExecutionOutcome,
        correlation: &ExecutionCorrelation,
        failure: Option<FailureSummary>,
    ) {
        let kind = match (job, outcome) {
            (true, TaskletExecutionOutcome::Completed) => LifecycleEventKind::JobCompleted,
            (false, TaskletExecutionOutcome::Completed) => LifecycleEventKind::StepCompleted,
            (true, TaskletExecutionOutcome::Stopped(_)) => LifecycleEventKind::JobStopped,
            (false, TaskletExecutionOutcome::Stopped(_)) => LifecycleEventKind::StepStopped,
            (true, TaskletExecutionOutcome::Failed(_)) => LifecycleEventKind::JobFailed,
            (false, TaskletExecutionOutcome::Failed(_)) => LifecycleEventKind::StepFailed,
            (true, TaskletExecutionOutcome::Unknown) => LifecycleEventKind::JobUnknown,
            (false, TaskletExecutionOutcome::Unknown) => LifecycleEventKind::StepUnknown,
        };
        self.emit_event(kind, correlation, failure);
    }

    fn emit_event(
        &self,
        kind: LifecycleEventKind,
        correlation: &ExecutionCorrelation,
        failure: Option<FailureSummary>,
    ) {
        let Some(sink) = self.event_sink else {
            return;
        };
        let event = failure.map_or_else(
            || LifecycleEvent::new(kind, correlation.clone()),
            |summary| LifecycleEvent::failed(kind, correlation.clone(), summary),
        );
        let _ = catch_unwind(AssertUnwindSafe(|| sink.emit(&event)));
    }
}

struct CreatedExecutionGraph {
    instance: JobInstance,
    job_execution: JobExecution,
    step_execution: StepExecution,
    correlation: ExecutionCorrelation,
}

fn final_status(outcome: TaskletExecutionOutcome) -> (BatchStatus, ExitStatus) {
    match outcome {
        TaskletExecutionOutcome::Completed => (BatchStatus::Completed, ExitStatus::completed()),
        TaskletExecutionOutcome::Stopped(_) => (BatchStatus::Stopped, ExitStatus::stopped()),
        TaskletExecutionOutcome::Failed(_) => (BatchStatus::Failed, ExitStatus::failed()),
        TaskletExecutionOutcome::Unknown => (BatchStatus::Unknown, ExitStatus::unknown()),
    }
}

fn transition_for_outcome(
    status: BatchStatus,
    transitioned_at: std::time::SystemTime,
    failure: Option<FailureSummary>,
) -> Result<LifecycleTransition, LaunchError> {
    if matches!(status, BatchStatus::Failed) {
        let summary = failure.ok_or(RepositoryError::Unavailable)?;
        Ok(LifecycleTransition::failed(transitioned_at, summary))
    } else {
        Ok(LifecycleTransition::new(status, transitioned_at))
    }
}

const fn listener_failure_outcome(kind: ListenerFailureKind) -> TaskletExecutionOutcome {
    match kind {
        ListenerFailureKind::Error => {
            TaskletExecutionOutcome::Failed(TaskletFailure::ListenerError)
        }
        ListenerFailureKind::Panic => {
            TaskletExecutionOutcome::Failed(TaskletFailure::ListenerPanic)
        }
    }
}

async fn invoke_before_job(
    listener: &dyn JobExecutionListener,
    context: ListenerContext<'_>,
) -> Result<(), ListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.before_job(context)))
        .map_err(|_| ListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ListenerFailureKind::Error),
        Err(_) => Err(ListenerFailureKind::Panic),
    }
}

async fn invoke_after_job(
    listener: &dyn JobExecutionListener,
    context: ListenerContext<'_>,
    outcome: TaskletExecutionOutcome,
) -> Result<(), ListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.after_job(context, outcome)))
        .map_err(|_| ListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ListenerFailureKind::Error),
        Err(_) => Err(ListenerFailureKind::Panic),
    }
}

async fn invoke_before_step(
    listener: &dyn StepExecutionListener,
    context: ListenerContext<'_>,
) -> Result<(), ListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.before_step(context)))
        .map_err(|_| ListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ListenerFailureKind::Error),
        Err(_) => Err(ListenerFailureKind::Panic),
    }
}

async fn invoke_after_step(
    listener: &dyn StepExecutionListener,
    context: ListenerContext<'_>,
    outcome: TaskletExecutionOutcome,
) -> Result<(), ListenerFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| listener.after_step(context, outcome)))
        .map_err(|_| ListenerFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ListenerFailureKind::Error),
        Err(_) => Err(ListenerFailureKind::Panic),
    }
}

async fn invoke_tasklet(
    tasklet: &dyn Tasklet,
    context: TaskletContext<'_>,
) -> Result<TaskletOutcome, TaskletFailure> {
    let future = catch_unwind(AssertUnwindSafe(|| tasklet.execute(context)))
        .map_err(|_| TaskletFailure::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(error)) => match error.kind {
            TaskletErrorKind::Component => Err(TaskletFailure::Error),
            TaskletErrorKind::Panic => Err(TaskletFailure::Panic),
        },
        Err(_) => Err(TaskletFailure::Panic),
    }
}

/// Isolates synchronous tasklet work behind a bounded Tokio blocking pool.
///
/// The semaphore limits submitted blocking calls independently of Tokio's
/// process-wide blocking-thread limit. This adapter requires the launch future
/// to be polled inside a Tokio runtime, but does not create or own one.
pub struct BlockingTaskletAdapter<T> {
    tasklet: Arc<T>,
    permits: Arc<Semaphore>,
}

impl<T> BlockingTaskletAdapter<T>
where
    T: BlockingTasklet,
{
    /// Constructs an adapter with an explicit nonzero concurrency bound.
    #[must_use]
    pub fn new(tasklet: T, maximum_concurrency: NonZeroUsize) -> Self {
        Self {
            tasklet: Arc::new(tasklet),
            permits: Arc::new(Semaphore::new(maximum_concurrency.get())),
        }
    }
}

impl<T> fmt::Debug for BlockingTaskletAdapter<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingTaskletAdapter")
            .field("available_permits", &self.permits.available_permits())
            .finish_non_exhaustive()
    }
}

impl<T> Tasklet for BlockingTaskletAdapter<T>
where
    T: BlockingTasklet,
{
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            if context.stop.is_stop_requested() {
                return Ok(TaskletOutcome::Stopped);
            }

            let permit = tokio::select! {
                result = Arc::clone(&self.permits).acquire_owned() => {
                    match result {
                        Ok(permit) => permit,
                        Err(_) => return Err(TaskletError::new()),
                    }
                }
                () = context.stop.cancelled() => return Ok(TaskletOutcome::Stopped),
            };
            if context.stop.is_stop_requested() {
                return Ok(TaskletOutcome::Stopped);
            }

            let stop = context.stop.clone();
            let tasklet = Arc::clone(&self.tasklet);
            let owned_context = context.into_blocking();
            let joined = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                if owned_context.stop_token().is_stop_requested() {
                    (false, Ok(TaskletOutcome::Stopped))
                } else {
                    (true, tasklet.execute(owned_context))
                }
            })
            .await;

            let (started, result) = match joined {
                Ok(result) => result,
                Err(error) if error.is_panic() => return Err(TaskletError::panic()),
                Err(_) => return Err(TaskletError::new()),
            };
            let outcome = result?;
            if started && stop.is_stop_requested() {
                Ok(TaskletOutcome::StoppedAfterBlockingWork)
            } else {
                Ok(outcome)
            }
        })
    }
}
