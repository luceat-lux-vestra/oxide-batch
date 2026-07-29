//! Facade-owned lifecycle events and value-redacted diagnostic projections.

use std::fmt;
use std::num::NonZeroU64;

use crate::{
    BatchStatus, ChunkCount, FailureSummary, JobExecutionId, JobInstanceId, JobName,
    StepExecutionId, StepName,
};

/// A nonzero, instance-scoped execution-attempt ordinal.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutionAttempt(NonZeroU64);

impl ExecutionAttempt {
    /// Constructs an attempt ordinal from a nonzero value.
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the numeric attempt ordinal.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for ExecutionAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// Stable, bounded identifiers shared by job and step diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionCorrelation {
    job_name: JobName,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    job_attempt: ExecutionAttempt,
    step_name: StepName,
    step_execution_id: StepExecutionId,
    step_attempt: ExecutionAttempt,
}

impl ExecutionCorrelation {
    /// Constructs complete correlation for a single-step execution graph.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        job_name: JobName,
        job_instance_id: JobInstanceId,
        job_execution_id: JobExecutionId,
        job_attempt: ExecutionAttempt,
        step_name: StepName,
        step_execution_id: StepExecutionId,
        step_attempt: ExecutionAttempt,
    ) -> Self {
        Self {
            job_name,
            job_instance_id,
            job_execution_id,
            job_attempt,
            step_name,
            step_execution_id,
            step_attempt,
        }
    }

    /// Borrows the job definition name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Returns the logical job-instance identifier.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Returns the job-attempt identifier.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the instance-scoped job attempt.
    #[must_use]
    pub const fn job_attempt(&self) -> ExecutionAttempt {
        self.job_attempt
    }

    /// Borrows the step definition name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Returns the step-attempt identifier.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }

    /// Returns the instance-scoped step attempt.
    #[must_use]
    pub const fn step_attempt(&self) -> ExecutionAttempt {
        self.step_attempt
    }
}

/// Stable severity for a lifecycle event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum EventSeverity {
    /// Normal lifecycle progress.
    Info,
    /// A cooperative stop or recoverable condition.
    Warn,
    /// A failed lifecycle or user-component boundary.
    Error,
}

impl EventSeverity {
    /// Returns the stable lowercase representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for EventSeverity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The framework component associated with an event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum EventComponent {
    /// The launch facade.
    Launcher,
    /// A job execution.
    Job,
    /// A step execution.
    Step,
    /// A bounded chunk transaction.
    Chunk,
    /// A job or step listener boundary.
    Listener,
}

impl EventComponent {
    /// Returns the stable lowercase representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Launcher => "launcher",
            Self::Job => "job",
            Self::Step => "step",
            Self::Chunk => "chunk",
            Self::Listener => "listener",
        }
    }
}

impl fmt::Display for EventComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable M1 lifecycle event names.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum LifecycleEventKind {
    /// The repository accepted a launch and created its execution graph.
    LaunchAccepted,
    /// Job metadata is durably `STARTING`.
    JobStarting,
    /// Step metadata is durably `STARTING`.
    StepStarting,
    /// Job metadata is durably `STARTED`.
    JobStarted,
    /// Step metadata is durably `STARTED`.
    StepStarted,
    /// Job metadata is durably `STOPPING`.
    JobStopping,
    /// Step metadata is durably `STOPPING`.
    StepStopping,
    /// Job metadata is durably `STOPPED`.
    JobStopped,
    /// Step metadata is durably `STOPPED`.
    StepStopped,
    /// Job metadata is durably `COMPLETED`.
    JobCompleted,
    /// Step metadata is durably `COMPLETED`.
    StepCompleted,
    /// Job metadata is durably `FAILED`.
    JobFailed,
    /// Step metadata is durably `FAILED`.
    StepFailed,
    /// Job metadata is durably `UNKNOWN`.
    JobUnknown,
    /// Step metadata is durably `UNKNOWN`.
    StepUnknown,
    /// A bounded chunk transaction is starting.
    ChunkStarted,
    /// A bounded chunk transaction committed.
    ChunkCommitted,
    /// A bounded chunk transaction rolled back.
    ChunkRolledBack,
    /// A chunk commit result is unknown.
    ChunkUnknown,
    /// A job before-listener returned an error or panicked.
    JobBeforeListenerFailed,
    /// A job after-listener returned an error or panicked.
    JobAfterListenerFailed,
    /// A step before-listener returned an error or panicked.
    StepBeforeListenerFailed,
    /// A step after-listener returned an error or panicked.
    StepAfterListenerFailed,
}

impl LifecycleEventKind {
    /// Returns the stable dotted event name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LaunchAccepted => "launch.accepted",
            Self::JobStarting => "job.starting",
            Self::StepStarting => "step.starting",
            Self::JobStarted => "job.started",
            Self::StepStarted => "step.started",
            Self::JobStopping => "job.stopping",
            Self::StepStopping => "step.stopping",
            Self::JobStopped => "job.stopped",
            Self::StepStopped => "step.stopped",
            Self::JobCompleted => "job.completed",
            Self::StepCompleted => "step.completed",
            Self::JobFailed => "job.failed",
            Self::StepFailed => "step.failed",
            Self::JobUnknown => "job.unknown",
            Self::StepUnknown => "step.unknown",
            Self::ChunkStarted => "chunk.started",
            Self::ChunkCommitted => "chunk.committed",
            Self::ChunkRolledBack => "chunk.rolled_back",
            Self::ChunkUnknown => "chunk.unknown",
            Self::JobBeforeListenerFailed => "job.before_listener.failed",
            Self::JobAfterListenerFailed => "job.after_listener.failed",
            Self::StepBeforeListenerFailed => "step.before_listener.failed",
            Self::StepAfterListenerFailed => "step.after_listener.failed",
        }
    }

    /// Returns the component associated with the event.
    #[must_use]
    pub const fn component(self) -> EventComponent {
        match self {
            Self::LaunchAccepted => EventComponent::Launcher,
            Self::JobStarting
            | Self::JobStarted
            | Self::JobStopping
            | Self::JobStopped
            | Self::JobCompleted
            | Self::JobFailed
            | Self::JobUnknown => EventComponent::Job,
            Self::StepStarting
            | Self::StepStarted
            | Self::StepStopping
            | Self::StepStopped
            | Self::StepCompleted
            | Self::StepFailed
            | Self::StepUnknown => EventComponent::Step,
            Self::ChunkStarted
            | Self::ChunkCommitted
            | Self::ChunkRolledBack
            | Self::ChunkUnknown => EventComponent::Chunk,
            Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed => EventComponent::Listener,
        }
    }

    /// Returns the lifecycle status represented by the event, when any.
    #[must_use]
    pub const fn status(self) -> Option<BatchStatus> {
        match self {
            Self::JobStarting | Self::StepStarting => Some(BatchStatus::Starting),
            Self::JobStarted | Self::StepStarted => Some(BatchStatus::Started),
            Self::JobStopping | Self::StepStopping => Some(BatchStatus::Stopping),
            Self::JobStopped | Self::StepStopped => Some(BatchStatus::Stopped),
            Self::JobCompleted | Self::StepCompleted => Some(BatchStatus::Completed),
            Self::JobFailed | Self::StepFailed => Some(BatchStatus::Failed),
            Self::JobUnknown | Self::StepUnknown => Some(BatchStatus::Unknown),
            Self::LaunchAccepted
            | Self::ChunkStarted
            | Self::ChunkCommitted
            | Self::ChunkRolledBack
            | Self::ChunkUnknown
            | Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed => None,
        }
    }

    /// Returns the stable event severity.
    #[must_use]
    pub const fn severity(self) -> EventSeverity {
        match self {
            Self::JobFailed
            | Self::StepFailed
            | Self::JobUnknown
            | Self::StepUnknown
            | Self::ChunkUnknown
            | Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed => EventSeverity::Error,
            Self::JobStopping
            | Self::StepStopping
            | Self::JobStopped
            | Self::StepStopped
            | Self::ChunkRolledBack => EventSeverity::Warn,
            Self::LaunchAccepted
            | Self::JobStarting
            | Self::StepStarting
            | Self::JobStarted
            | Self::StepStarted
            | Self::JobCompleted
            | Self::StepCompleted
            | Self::ChunkStarted
            | Self::ChunkCommitted => EventSeverity::Info,
        }
    }
}

impl fmt::Display for LifecycleEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A structured lifecycle event containing only reviewed, bounded fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleEvent {
    kind: LifecycleEventKind,
    correlation: ExecutionCorrelation,
    failure: Option<FailureSummary>,
    chunk_sequence: Option<ChunkCount>,
}

impl LifecycleEvent {
    pub(crate) const fn new(kind: LifecycleEventKind, correlation: ExecutionCorrelation) -> Self {
        Self {
            kind,
            correlation,
            failure: None,
            chunk_sequence: None,
        }
    }

    pub(crate) const fn failed(
        kind: LifecycleEventKind,
        correlation: ExecutionCorrelation,
        failure: FailureSummary,
    ) -> Self {
        Self {
            kind,
            correlation,
            failure: Some(failure),
            chunk_sequence: None,
        }
    }

    pub(crate) const fn chunk(
        kind: LifecycleEventKind,
        correlation: ExecutionCorrelation,
        sequence: ChunkCount,
    ) -> Self {
        Self {
            kind,
            correlation,
            failure: None,
            chunk_sequence: Some(sequence),
        }
    }

    /// Returns the stable event kind.
    #[must_use]
    pub const fn kind(&self) -> LifecycleEventKind {
        self.kind
    }

    /// Borrows the complete execution correlation.
    #[must_use]
    pub const fn correlation(&self) -> &ExecutionCorrelation {
        &self.correlation
    }

    /// Returns the redacted failure summary, when present.
    #[must_use]
    pub const fn failure(&self) -> Option<FailureSummary> {
        self.failure
    }

    /// Returns the chunk-attempt sequence for chunk events.
    #[must_use]
    pub const fn chunk_sequence(&self) -> Option<ChunkCount> {
        self.chunk_sequence
    }

    /// Produces the reviewed fields suitable for a tracing span or event.
    #[must_use]
    pub fn span_fields(&self) -> Vec<DiagnosticField> {
        let mut fields = vec![
            DiagnosticField::new("event.name", self.kind.as_str()),
            DiagnosticField::new("event.severity", self.kind.severity().as_str()),
            DiagnosticField::new("component", self.kind.component().as_str()),
            DiagnosticField::new("job.name", self.correlation.job_name().as_str()),
            DiagnosticField::new(
                "job.instance.id",
                self.correlation.job_instance_id().to_string(),
            ),
            DiagnosticField::new(
                "job.execution.id",
                self.correlation.job_execution_id().to_string(),
            ),
            DiagnosticField::new("job.attempt", self.correlation.job_attempt().to_string()),
            DiagnosticField::new("step.name", self.correlation.step_name().as_str()),
            DiagnosticField::new(
                "step.execution.id",
                self.correlation.step_execution_id().to_string(),
            ),
            DiagnosticField::new("step.attempt", self.correlation.step_attempt().to_string()),
        ];
        if let Some(status) = self.kind.status() {
            fields.push(DiagnosticField::new("batch.status", status.to_string()));
        }
        if let Some(sequence) = self.chunk_sequence {
            fields.push(DiagnosticField::new(
                "chunk.sequence",
                sequence.get().to_string(),
            ));
        }
        if let Some(failure) = self.failure {
            fields.push(DiagnosticField::new(
                "failure.category",
                format!("{:?}", failure.category()),
            ));
            fields.push(DiagnosticField::new(
                "failure.id",
                failure.failure_id().to_string(),
            ));
        }
        fields
    }

    /// Produces a bounded metric label set.
    ///
    /// Identifiers, names, parameters, contexts, records, and error text are
    /// intentionally absent.
    #[must_use]
    pub fn metric_labels(&self) -> Vec<MetricLabel> {
        let mut labels = vec![
            MetricLabel::new("event", self.kind.as_str()),
            MetricLabel::new("component", self.kind.component().as_str()),
        ];
        if let Some(status) = self.kind.status() {
            labels.push(MetricLabel::new("status", status.to_string()));
        }
        labels
    }
}

impl fmt::Display for LifecycleEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event={} severity={} job={} job_instance_id={} job_execution_id={} \
             job_attempt={} step={} step_execution_id={} step_attempt={}",
            self.kind,
            self.kind.severity(),
            self.correlation.job_name(),
            self.correlation.job_instance_id(),
            self.correlation.job_execution_id(),
            self.correlation.job_attempt(),
            self.correlation.step_name(),
            self.correlation.step_execution_id(),
            self.correlation.step_attempt(),
        )?;
        if let Some(status) = self.kind.status() {
            write!(formatter, " status={status}")?;
        }
        if let Some(sequence) = self.chunk_sequence {
            write!(formatter, " chunk_sequence={}", sequence.get())?;
        }
        if let Some(failure) = self.failure {
            write!(
                formatter,
                " failure_category={:?} failure_id={}",
                failure.category(),
                failure.failure_id()
            )?;
        }
        Ok(())
    }
}

/// A reviewed key/value field suitable for structured logs or spans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticField {
    key: &'static str,
    value: String,
}

impl DiagnosticField {
    fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }

    /// Returns the stable field key.
    #[must_use]
    pub const fn key(&self) -> &'static str {
        self.key
    }

    /// Returns the reviewed field value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for DiagnosticField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.key, self.value)
    }
}

/// A bounded, framework-owned metric label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricLabel {
    key: &'static str,
    value: String,
}

impl MetricLabel {
    fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }

    /// Returns the stable label key.
    #[must_use]
    pub const fn key(&self) -> &'static str {
        self.key
    }

    /// Returns the bounded framework-owned value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for MetricLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.key, self.value)
    }
}

/// Receives committed lifecycle observations.
///
/// Sink failures and panics are isolated by [`crate::JobLauncher`] and cannot
/// change execution correctness.
pub trait LifecycleEventSink: Send + Sync {
    /// Emits one event after the corresponding metadata commit.
    fn emit(&self, event: &LifecycleEvent);
}
