//! Async tasklet execution and cooperative stopping.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::FutureExt;
use tokio::sync::{Notify, Semaphore};

use crate::{
    BatchStatus, BoxFuture, Clock, ExitStatus, FailureCategory, FailureSummary, IdGenerator,
    JobExecution, JobExecutionId, JobInstance, JobInstanceKey, JobName, JobParameters,
    JobRepository, LifecycleTransition, RepositoryError, StepExecution, StepExecutionId, StepName,
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
}

impl TaskletStep {
    /// Constructs a step from its validated name and async body.
    #[must_use]
    pub fn new(name: StepName, tasklet: Arc<dyn Tasklet>) -> Self {
        Self { name, tasklet }
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
            .finish_non_exhaustive()
    }
}

/// A validated single-step job definition.
#[derive(Debug)]
pub struct TaskletJob {
    name: JobName,
    step: TaskletStep,
}

impl TaskletJob {
    /// Constructs a single-step job.
    #[must_use]
    pub const fn new(name: JobName, step: TaskletStep) -> Self {
        Self { name, step }
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
#[derive(Clone, Copy, Debug)]
pub struct TaskletContext<'a> {
    parameters: &'a JobParameters,
    job_execution_id: JobExecutionId,
    step_execution_id: StepExecutionId,
    stop: &'a StopToken,
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

    fn into_blocking(self) -> BlockingTaskletContext {
        BlockingTaskletContext {
            parameters: self.parameters.clone(),
            job_execution_id: self.job_execution_id,
            step_execution_id: self.step_execution_id,
            stop: self.stop.clone(),
        }
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
}

/// Final persisted execution snapshots returned by [`JobLauncher`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchReport {
    instance: JobInstance,
    job_execution: JobExecution,
    step_execution: StepExecution,
    outcome: TaskletExecutionOutcome,
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
        }
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
    pub async fn launch(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<LaunchReport, LaunchError> {
        let key = JobInstanceKey::new(job.name.clone(), parameters);
        let (instance, job_execution, step_execution) =
            self.create_execution_graph(&key, job.step.name()).await?;

        if stop.is_stop_requested() {
            let (job_execution, step_execution) = self
                .finish_stopped(&job_execution, &step_execution, true)
                .await?;
            return Ok(LaunchReport {
                instance,
                job_execution,
                step_execution,
                outcome: TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart),
            });
        }

        let (started_job, started_step) = self.start(&job_execution, &step_execution).await?;
        if stop.is_stop_requested() {
            let (job_execution, step_execution) = self
                .finish_stopped(&started_job, &started_step, false)
                .await?;
            return Ok(LaunchReport {
                instance,
                job_execution,
                step_execution,
                outcome: TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart),
            });
        }
        let context = TaskletContext {
            parameters,
            job_execution_id: started_job.id(),
            step_execution_id: started_step.id(),
            stop,
        };
        let invocation = invoke_tasklet(job.step.tasklet.as_ref(), context).await;
        let outcome = match invocation {
            Ok(TaskletOutcome::Completed) if !stop.is_stop_requested() => {
                TaskletExecutionOutcome::Completed
            }
            Ok(TaskletOutcome::Completed | TaskletOutcome::Stopped) => {
                TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution)
            }
            Ok(TaskletOutcome::StoppedAfterBlockingWork) => {
                TaskletExecutionOutcome::Stopped(StopTiming::AfterBlockingWork)
            }
            Err(failure) => TaskletExecutionOutcome::Failed(failure),
        };
        let (job_execution, step_execution) =
            self.finish(&started_job, &started_step, outcome).await?;

        Ok(LaunchReport {
            instance,
            job_execution,
            step_execution,
            outcome,
        })
    }

    async fn create_execution_graph(
        &self,
        key: &JobInstanceKey,
        step_name: &StepName,
    ) -> Result<(JobInstance, JobExecution, StepExecution), LaunchError> {
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
        unit.commit().await?;
        Ok((instance, job_execution, step_execution))
    }

    async fn start(
        &self,
        job: &JobExecution,
        step: &StepExecution,
    ) -> Result<(JobExecution, StepExecution), LaunchError> {
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let started_job = unit
            .transition_job_execution(
                job.id(),
                job.version(),
                LifecycleTransition::new(BatchStatus::Started, now),
            )
            .await?;
        let started_step = unit
            .transition_step_execution(
                step.id(),
                step.version(),
                LifecycleTransition::new(BatchStatus::Started, now),
            )
            .await?;
        unit.commit().await?;
        Ok((started_job, started_step))
    }

    async fn finish(
        &self,
        job: &JobExecution,
        step: &StepExecution,
        outcome: TaskletExecutionOutcome,
    ) -> Result<(JobExecution, StepExecution), LaunchError> {
        match outcome {
            TaskletExecutionOutcome::Completed => {
                self.finish_known(
                    job,
                    step,
                    BatchStatus::Completed,
                    ExitStatus::completed(),
                    None,
                )
                .await
            }
            TaskletExecutionOutcome::Stopped(_) => self.finish_stopped(job, step, false).await,
            TaskletExecutionOutcome::Failed(_) => {
                let summary = FailureSummary::new(
                    FailureCategory::UserComponent,
                    self.ids
                        .next_failure_id()
                        .map_err(RepositoryError::Identifier)?,
                );
                self.finish_known(
                    job,
                    step,
                    BatchStatus::Failed,
                    ExitStatus::failed(),
                    Some(summary),
                )
                .await
            }
        }
    }

    async fn finish_stopped(
        &self,
        job: &JobExecution,
        step: &StepExecution,
        requires_stopping: bool,
    ) -> Result<(JobExecution, StepExecution), LaunchError> {
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let (job, step) = if requires_stopping {
            let stopping_job = unit
                .transition_job_execution(
                    job.id(),
                    job.version(),
                    LifecycleTransition::new(BatchStatus::Stopping, now),
                )
                .await?;
            let stopping_step = unit
                .transition_step_execution(
                    step.id(),
                    step.version(),
                    LifecycleTransition::new(BatchStatus::Stopping, now),
                )
                .await?;
            (stopping_job, stopping_step)
        } else {
            (job.clone(), step.clone())
        };
        let stopped_job = unit
            .enrich_job_exit_status(job.id(), job.version(), &ExitStatus::stopped())
            .await?;
        let stopped_step = unit
            .enrich_step_exit_status(step.id(), step.version(), &ExitStatus::stopped())
            .await?;
        let stopped_job = unit
            .transition_job_execution(
                stopped_job.id(),
                stopped_job.version(),
                LifecycleTransition::new(BatchStatus::Stopped, now),
            )
            .await?;
        let stopped_step = unit
            .transition_step_execution(
                stopped_step.id(),
                stopped_step.version(),
                LifecycleTransition::new(BatchStatus::Stopped, now),
            )
            .await?;
        unit.commit().await?;
        Ok((stopped_job, stopped_step))
    }

    async fn finish_known(
        &self,
        job: &JobExecution,
        step: &StepExecution,
        status: BatchStatus,
        exit_status: ExitStatus,
        failure: Option<FailureSummary>,
    ) -> Result<(JobExecution, StepExecution), LaunchError> {
        let now = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let job = unit
            .enrich_job_exit_status(job.id(), job.version(), &exit_status)
            .await?;
        let step = unit
            .enrich_step_exit_status(step.id(), step.version(), &exit_status)
            .await?;
        let job_transition = failure.map_or_else(
            || LifecycleTransition::new(status, now),
            |summary| LifecycleTransition::failed(now, summary),
        );
        let step_transition = failure.map_or_else(
            || LifecycleTransition::new(status, now),
            |summary| LifecycleTransition::failed(now, summary),
        );
        let job = unit
            .transition_job_execution(job.id(), job.version(), job_transition)
            .await?;
        let step = unit
            .transition_step_execution(step.id(), step.version(), step_transition)
            .await?;
        unit.commit().await?;
        Ok((job, step))
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
