//! Normalized execution traces used by wrapper/plan equivalence evidence.
//!
//! A trace is an ordered, human-readable list of lines that records every
//! repository command, every lifecycle event, and the final durable rows of one
//! launch. Comparing two traces line by line proves that two execution paths
//! issued the same commands in the same order and left the same durable state.
//!
//! Traces contain identifiers, statuses, exit codes, versions, and counts only.
//! Parameters, contexts, item values, error payloads, and manifests are never
//! recorded.

use std::error::Error;
use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use oxide_batch::{
    BoxFuture, DefinitionIdentity, DefinitionUpgrade, ExecutionMetadata, ExecutionVersion,
    ExitStatus, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey,
    JobInstanceSelection, JobName, JobRepository, LifecycleEvent, LifecycleEventSink,
    LifecycleTransition, RecoveryDecision, RecoveryRequest, RecoveryResult, RepositoryError,
    RepositoryUnitOfWork, StepExecution, StepExecutionId, StepName,
};

/// Environment variable that rewrites golden traces instead of comparing them.
pub const UPDATE_GOLDEN_VARIABLE: &str = "OXIDEBATCH_UPDATE_TRACE_GOLDEN";

/// A cloneable, order-preserving normalized trace.
#[derive(Clone, Debug, Default)]
pub struct ExecutionTrace {
    lines: Arc<Mutex<Vec<String>>>,
}

impl ExecutionTrace {
    /// Creates an empty trace.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one normalized line.
    pub fn record(&self, line: impl Into<String>) {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line.into());
    }

    /// Returns the recorded lines in order.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Renders the trace as newline-terminated text.
    #[must_use]
    pub fn rendered(&self) -> String {
        let mut rendered = String::new();
        for line in self.lines() {
            rendered.push_str(&line);
            rendered.push('\n');
        }
        rendered
    }

    /// Records the durable rows reachable from one launch.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when the repository cannot be inspected.
    pub async fn record_durable_state(
        &self,
        repository: &dyn JobRepository,
        instance_id: JobInstanceId,
    ) -> Result<(), RepositoryError> {
        let mut unit = repository.begin().await?;
        let executions = unit.job_executions(instance_id).await?;
        let mut steps = Vec::new();
        for execution in &executions {
            steps.push(unit.step_executions(execution.id()).await?);
        }
        unit.rollback().await?;
        self.record(format!("durable instance={}", instance_id.get()));
        for (execution, step_executions) in executions.iter().zip(steps) {
            self.record(format!(
                "durable job_execution={} {}",
                execution.id().get(),
                render_metadata(execution.metadata(), execution.version())
            ));
            for step in &step_executions {
                self.record(format!(
                    "durable step_execution={} step={} {}",
                    step.id().get(),
                    step.step_name().as_str(),
                    render_metadata(step.metadata(), step.version())
                ));
            }
        }
        Ok(())
    }

    /// Compares this trace with a committed golden file.
    ///
    /// Setting [`UPDATE_GOLDEN_VARIABLE`] rewrites the file instead, so a
    /// deliberate contract change is reviewed as a fixture diff.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionTraceError`] when the golden file cannot be read or
    /// written, or when the rendered trace differs from it.
    pub fn assert_matches_golden(&self, name: &str) -> Result<(), ExecutionTraceError> {
        let path = golden_path(name);
        let rendered = self.rendered();
        if std::env::var_os(UPDATE_GOLDEN_VARIABLE).is_some() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|error| ExecutionTraceError::Io {
                    path: path.clone(),
                    message: error.to_string(),
                })?;
            }
            fs::write(&path, rendered).map_err(|error| ExecutionTraceError::Io {
                path: path.clone(),
                message: error.to_string(),
            })?;
            return Ok(());
        }
        let expected = fs::read_to_string(&path).map_err(|error| ExecutionTraceError::Io {
            path: path.clone(),
            message: error.to_string(),
        })?;
        if expected == rendered {
            return Ok(());
        }
        Err(ExecutionTraceError::Mismatch {
            path,
            difference: first_difference(&expected, &rendered),
        })
    }
}

/// Compares two traces recorded in one test process.
///
/// # Errors
///
/// Returns [`ExecutionTraceError::Divergence`] with the first differing line.
pub fn assert_traces_match(
    left: &ExecutionTrace,
    right: &ExecutionTrace,
) -> Result<(), ExecutionTraceError> {
    let rendered_left = left.rendered();
    let rendered_right = right.rendered();
    if rendered_left == rendered_right {
        return Ok(());
    }
    Err(ExecutionTraceError::Divergence {
        difference: first_difference(&rendered_left, &rendered_right),
    })
}

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/LIFE-DEFINITION-001")
        .join(format!("{name}.trace"))
}

fn first_difference(expected: &str, actual: &str) -> String {
    let mut expected_lines = expected.lines();
    let mut actual_lines = actual.lines();
    let mut index = 0_usize;
    loop {
        match (expected_lines.next(), actual_lines.next()) {
            (None, None) => return "traces differ only in trailing whitespace".to_owned(),
            (expected_line, actual_line) if expected_line != actual_line => {
                return format!(
                    "line {index}: expected {:?}, found {:?}",
                    expected_line.unwrap_or("<end of trace>"),
                    actual_line.unwrap_or("<end of trace>")
                );
            }
            _ => index += 1,
        }
    }
}

fn render_metadata(metadata: &ExecutionMetadata, version: ExecutionVersion) -> String {
    let counts = metadata.counts();
    format!(
        "status={:?} exit={} version={} read={} processed={} written={} filtered={} committed={} rolled_back={} failure={}",
        metadata.status(),
        metadata.exit_status().code().as_str(),
        version.get(),
        counts.read(),
        counts.processed(),
        counts.written(),
        counts.filtered(),
        counts.committed(),
        counts.rolled_back(),
        metadata.failure().map_or_else(
            || "none".to_owned(),
            |failure| format!("{:?}", failure.category())
        )
    )
}

/// A recorded trace that did not match its reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionTraceError {
    /// A golden trace file could not be read or written.
    Io {
        /// Golden trace path.
        path: PathBuf,
        /// Operating-system message.
        message: String,
    },
    /// The rendered trace differed from its golden file.
    Mismatch {
        /// Golden trace path.
        path: PathBuf,
        /// First differing line.
        difference: String,
    },
    /// Two traces recorded in one process differed.
    Divergence {
        /// First differing line.
        difference: String,
    },
}

impl fmt::Display for ExecutionTraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, message } => {
                write!(
                    formatter,
                    "golden trace {} is unusable: {message}",
                    path.display()
                )
            }
            Self::Mismatch { path, difference } => write!(
                formatter,
                "trace differs from golden {}: {difference}; rerun with {UPDATE_GOLDEN_VARIABLE}=1 to review an intended change",
                path.display()
            ),
            Self::Divergence { difference } => {
                write!(formatter, "traces diverged: {difference}")
            }
        }
    }
}

impl Error for ExecutionTraceError {}

/// A [`LifecycleEventSink`] that appends normalized event lines.
#[derive(Clone, Debug)]
pub struct TracingEventSink {
    trace: ExecutionTrace,
}

impl TracingEventSink {
    /// Wraps a trace.
    #[must_use]
    pub const fn new(trace: ExecutionTrace) -> Self {
        Self { trace }
    }
}

impl LifecycleEventSink for TracingEventSink {
    fn emit(&self, event: &LifecycleEvent) {
        let correlation = event.correlation();
        let mut line = format!(
            "event {} job_execution={} step_execution={} job_attempt={} step_attempt={}",
            event.kind().as_str(),
            correlation.job_execution_id().get(),
            correlation.step_execution_id().get(),
            correlation.job_attempt().get(),
            correlation.step_attempt().get()
        );
        if let Some(failure) = event.failure() {
            let _ = write!(line, " failure={:?}", failure.category());
        }
        if let Some(sequence) = event.chunk_sequence() {
            let _ = write!(line, " chunk={}", sequence.get());
        }
        if let Some(phase) = event.fault_phase() {
            let _ = write!(line, " phase={phase:?}");
        }
        if let Some(ordinal) = event.retry_ordinal() {
            let _ = write!(line, " retry_ordinal={}", ordinal.get());
        }
        if let Some(backoff) = event.backoff() {
            let _ = write!(line, " backoff_ms={}", backoff.as_millis());
        }
        self.trace.record(line);
    }
}

/// A [`JobRepository`] decorator that records every command it forwards.
pub struct RecordingRepository<R> {
    inner: R,
    trace: ExecutionTrace,
}

impl<R> RecordingRepository<R> {
    /// Wraps a repository with a shared trace.
    #[must_use]
    pub const fn new(inner: R, trace: ExecutionTrace) -> Self {
        Self { inner, trace }
    }

    /// Borrows the recorded repository.
    #[must_use]
    pub const fn inner(&self) -> &R {
        &self.inner
    }
}

impl<R> fmt::Debug for RecordingRepository<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingRepository")
            .finish_non_exhaustive()
    }
}

impl<R> JobRepository for RecordingRepository<R>
where
    R: JobRepository,
{
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            self.trace.record("repository begin");
            let inner = self.inner.begin().await?;
            let unit: Box<dyn RepositoryUnitOfWork + 'a> = Box::new(RecordingUnitOfWork {
                inner,
                trace: self.trace.clone(),
            });
            Ok(unit)
        })
    }
}

struct RecordingUnitOfWork<'a> {
    inner: Box<dyn RepositoryUnitOfWork + 'a>,
    trace: ExecutionTrace,
}

impl RepositoryUnitOfWork for RecordingUnitOfWork<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        self.trace.record(format!(
            "unit register_definition_upgrade job={} key={}",
            job_name.as_str(),
            upgrade.key().as_str()
        ));
        self.inner.register_definition_upgrade(job_name, upgrade)
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        self.trace.record(format!(
            "unit select_or_create_job_instance job={} identifying={}",
            key.job_name().as_str(),
            key.identifying_parameter_count()
        ));
        self.inner.select_or_create_job_instance(key)
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit create_job_execution instance={}",
            job_instance_id.get()
        ));
        self.inner.create_job_execution(job_instance_id)
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit create_job_execution_with_definition instance={} manifest_format={} digest={}",
            job_instance_id.get(),
            definition.manifest_format(),
            digest_prefix(definition.manifest_digest())
        ));
        self.inner
            .create_job_execution_with_definition(job_instance_id, definition)
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit create_step_execution job_execution={} step={}",
            job_execution_id.get(),
            step_name.as_str()
        ));
        self.inner
            .create_step_execution(job_execution_id, step_name)
    }

    fn transition_job_execution(
        &mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit transition_job_execution job_execution={} expected_version={} target={:?}",
            id.get(),
            expected_version.get(),
            transition.target()
        ));
        self.inner
            .transition_job_execution(id, expected_version, transition)
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit enrich_job_exit_status job_execution={} expected_version={} exit={}",
            id.get(),
            expected_version.get(),
            exit_status.code().as_str()
        ));
        self.inner
            .enrich_job_exit_status(id, expected_version, exit_status)
    }

    fn transition_step_execution(
        &mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit transition_step_execution step_execution={} expected_version={} target={:?}",
            id.get(),
            expected_version.get(),
            transition.target()
        ));
        self.inner
            .transition_step_execution(id, expected_version, transition)
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        self.trace.record(format!(
            "unit enrich_step_exit_status step_execution={} expected_version={} exit={}",
            id.get(),
            expected_version.get(),
            exit_status.code().as_str()
        ));
        self.inner
            .enrich_step_exit_status(id, expected_version, exit_status)
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<Option<JobInstance>, RepositoryError>> {
        self.trace.record(format!(
            "unit find_job_instance job={}",
            key.job_name().as_str()
        ));
        self.inner.find_job_instance(key)
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        self.trace
            .record(format!("unit get_job_instance instance={}", id.get()));
        self.inner.get_job_instance(id)
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        self.trace
            .record(format!("unit get_job_execution job_execution={}", id.get()));
        self.inner.get_job_execution(id)
    }

    fn job_executions(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Vec<JobExecution>, RepositoryError>> {
        self.trace.record(format!(
            "unit job_executions instance={}",
            job_instance_id.get()
        ));
        self.inner.job_executions(job_instance_id)
    }

    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        self.trace.record(format!(
            "unit get_step_execution step_execution={}",
            id.get()
        ));
        self.inner.get_step_execution(id)
    }

    fn step_executions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepExecution>, RepositoryError>> {
        self.trace.record(format!(
            "unit step_executions job_execution={}",
            job_execution_id.get()
        ));
        self.inner.step_executions(job_execution_id)
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        id: JobExecutionId,
        request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>> {
        self.trace.record(format!(
            "unit recover_job_execution job_execution={} disposition={:?}",
            id.get(),
            request.disposition()
        ));
        self.inner.recover_job_execution(id, request)
    }

    fn recovery_decision(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>> {
        self.trace
            .record(format!("unit recovery_decision job_execution={}", id.get()));
        self.inner.recovery_decision(id)
    }

    fn commit<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        self.trace.record("unit commit");
        let this = *self;
        this.inner.commit()
    }

    fn rollback<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        self.trace.record("unit rollback");
        let this = *self;
        this.inner.rollback()
    }
}

fn digest_prefix(digest: &[u8; 32]) -> String {
    let mut rendered = String::with_capacity(8);
    for byte in &digest[..4] {
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}
