use std::fmt;
use std::time::SystemTime;

use super::{
    DomainError, ExitCode, FailureId, JobExecutionId, JobInstanceId, JobInstanceKey,
    StepExecutionId, StepName,
};

/// The framework lifecycle status of a job or step execution.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BatchStatus {
    /// Metadata exists and user work has not started.
    Starting,
    /// User work is running.
    Started,
    /// A cooperative stop is in progress.
    Stopping,
    /// The attempt stopped cooperatively and may be restarted.
    Stopped,
    /// The attempt failed and may be restartable.
    Failed,
    /// The attempt completed successfully.
    Completed,
    /// The instance is intentionally terminal and cannot restart.
    Abandoned,
    /// The durable outcome is ambiguous and requires recovery.
    Unknown,
}

impl BatchStatus {
    /// Returns whether the attempt is actively running or stopping.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Started | Self::Stopping)
    }

    /// Returns whether the attempt has a known finished outcome.
    #[must_use]
    pub const fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::Failed | Self::Completed | Self::Abandoned
        )
    }

    /// Returns whether the logical instance is terminal and not restartable.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Abandoned)
    }
}

impl fmt::Display for BatchStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Starting => "STARTING",
            Self::Started => "STARTED",
            Self::Stopping => "STOPPING",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
            Self::Completed => "COMPLETED",
            Self::Abandoned => "ABANDONED",
            Self::Unknown => "UNKNOWN",
        })
    }
}

/// A flow- and operator-facing result kept separate from [`BatchStatus`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExitStatus {
    code: ExitCode,
}

impl ExitStatus {
    /// Constructs an exit status from a validated code.
    #[must_use]
    pub const fn new(code: ExitCode) -> Self {
        Self { code }
    }

    /// Constructs the conventional `UNKNOWN` exit status.
    ///
    /// This cannot fail because the framework-owned code is valid.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            code: ExitCode::framework_owned("UNKNOWN"),
        }
    }

    /// Borrows the exit code.
    #[must_use]
    pub const fn code(&self) -> &ExitCode {
        &self.code
    }
}

impl fmt::Display for ExitStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.code.fmt(formatter)
    }
}

/// Durable item and transaction counters for an execution.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ExecutionCounts {
    read: u64,
    processed: u64,
    written: u64,
    filtered: u64,
    committed: u64,
    rolled_back: u64,
}

impl ExecutionCounts {
    /// Constructs a complete counter snapshot.
    #[must_use]
    pub const fn new(
        read: u64,
        processed: u64,
        written: u64,
        filtered: u64,
        committed: u64,
        rolled_back: u64,
    ) -> Self {
        Self {
            read,
            processed,
            written,
            filtered,
            committed,
            rolled_back,
        }
    }

    /// Returns the durable read count.
    #[must_use]
    pub const fn read(self) -> u64 {
        self.read
    }

    /// Returns the durable processed count.
    #[must_use]
    pub const fn processed(self) -> u64 {
        self.processed
    }

    /// Returns the durable written count.
    #[must_use]
    pub const fn written(self) -> u64 {
        self.written
    }

    /// Returns the durable filtered count.
    #[must_use]
    pub const fn filtered(self) -> u64 {
        self.filtered
    }

    /// Returns the committed chunk/transaction count.
    #[must_use]
    pub const fn committed(self) -> u64 {
        self.committed
    }

    /// Returns the rolled-back chunk/transaction count.
    #[must_use]
    pub const fn rolled_back(self) -> u64 {
        self.rolled_back
    }
}

/// Validated creation, start, and end instants for an execution attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct ExecutionTimestamps {
    created_at: SystemTime,
    started_at: Option<SystemTime>,
    ended_at: Option<SystemTime>,
}

impl ExecutionTimestamps {
    /// Validates and constructs an execution timestamp snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidTimestampOrder`] when start precedes
    /// creation or end precedes creation/start.
    pub fn new(
        created_at: SystemTime,
        started_at: Option<SystemTime>,
        ended_at: Option<SystemTime>,
    ) -> Result<Self, DomainError> {
        if started_at.is_some_and(|started| started < created_at)
            || ended_at.is_some_and(|ended| ended < started_at.unwrap_or(created_at))
        {
            return Err(DomainError::InvalidTimestampOrder);
        }
        Ok(Self {
            created_at,
            started_at,
            ended_at,
        })
    }

    /// Returns the metadata creation instant.
    #[must_use]
    pub const fn created_at(self) -> SystemTime {
        self.created_at
    }

    /// Returns when user work began, when known.
    #[must_use]
    pub const fn started_at(self) -> Option<SystemTime> {
        self.started_at
    }

    /// Returns when the attempt ended, when known.
    #[must_use]
    pub const fn ended_at(self) -> Option<SystemTime> {
        self.ended_at
    }
}

/// A stable framework category for a redacted failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FailureCategory {
    /// Invalid job definition or launch configuration.
    InvalidDefinition,
    /// Duplicate, completed, or non-restartable execution.
    DuplicateExecution,
    /// Illegal or conflicting lifecycle transition.
    IllegalTransition,
    /// A transient repository or infrastructure failure.
    TransientInfrastructure,
    /// A permanent repository or infrastructure failure.
    PermanentInfrastructure,
    /// A user reader, processor, writer, tasklet, or listener failure.
    UserComponent,
    /// Cancellation, stop, or deadline expiry.
    Cancelled,
    /// Serialization or version incompatibility.
    Serialization,
    /// A framework invariant was violated.
    Invariant,
}

/// A value-redacted failure summary suitable for execution inspection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FailureSummary {
    category: FailureCategory,
    failure_id: FailureId,
}

impl FailureSummary {
    /// Constructs a failure summary from a stable category and opaque ID.
    #[must_use]
    pub const fn new(category: FailureCategory, failure_id: FailureId) -> Self {
        Self {
            category,
            failure_id,
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.category
    }

    /// Returns the opaque diagnostic correlation ID.
    #[must_use]
    pub const fn failure_id(self) -> FailureId {
        self.failure_id
    }
}

/// Validated lifecycle, outcome, timestamps, counters, and failure metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionMetadata {
    status: BatchStatus,
    exit_status: ExitStatus,
    timestamps: ExecutionTimestamps,
    counts: ExecutionCounts,
    failure: Option<FailureSummary>,
}

impl ExecutionMetadata {
    /// Validates and constructs an execution metadata snapshot.
    ///
    /// # Errors
    ///
    /// Active executions cannot have an end time, known finished executions
    /// require one, and failed executions require a redacted failure summary.
    pub fn new(
        status: BatchStatus,
        exit_status: ExitStatus,
        timestamps: ExecutionTimestamps,
        counts: ExecutionCounts,
        failure: Option<FailureSummary>,
    ) -> Result<Self, DomainError> {
        if status.is_active() && timestamps.ended_at().is_some() {
            return Err(DomainError::ActiveExecutionHasEndTime);
        }
        if status.is_finished() && timestamps.ended_at().is_none() {
            return Err(DomainError::FinishedExecutionMissingEndTime);
        }
        if matches!(status, BatchStatus::Failed) && failure.is_none() {
            return Err(DomainError::FailedExecutionMissingFailure);
        }
        Ok(Self {
            status,
            exit_status,
            timestamps,
            counts,
            failure,
        })
    }

    /// Returns the framework lifecycle status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the flow/operator exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the validated timestamps.
    #[must_use]
    pub const fn timestamps(&self) -> ExecutionTimestamps {
        self.timestamps
    }

    /// Returns the durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Returns the redacted failure summary, when present.
    #[must_use]
    pub const fn failure(&self) -> Option<FailureSummary> {
        self.failure
    }
}

/// One logical occurrence of a named job and its identifying parameters.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobInstance {
    id: JobInstanceId,
    key: JobInstanceKey,
}

impl JobInstance {
    /// Constructs a logical job instance.
    #[must_use]
    pub const fn new(id: JobInstanceId, key: JobInstanceKey) -> Self {
        Self { id, key }
    }

    /// Returns the opaque instance identifier.
    #[must_use]
    pub const fn id(&self) -> JobInstanceId {
        self.id
    }

    /// Borrows the canonical logical key.
    #[must_use]
    pub const fn key(&self) -> &JobInstanceKey {
        &self.key
    }
}

/// One launch or restart attempt for a [`JobInstance`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobExecution {
    id: JobExecutionId,
    job_instance_id: JobInstanceId,
    metadata: ExecutionMetadata,
}

impl JobExecution {
    /// Constructs a job execution record from validated metadata.
    #[must_use]
    pub const fn new(
        id: JobExecutionId,
        job_instance_id: JobInstanceId,
        metadata: ExecutionMetadata,
    ) -> Self {
        Self {
            id,
            job_instance_id,
            metadata,
        }
    }

    /// Returns the attempt identifier.
    #[must_use]
    pub const fn id(&self) -> JobExecutionId {
        self.id
    }

    /// Returns the logical instance identifier.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Borrows the execution metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ExecutionMetadata {
        &self.metadata
    }
}

/// One attempt to execute a named step within a job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepExecution {
    id: StepExecutionId,
    job_execution_id: JobExecutionId,
    step_name: StepName,
    metadata: ExecutionMetadata,
}

impl StepExecution {
    /// Constructs a step execution record from validated metadata.
    #[must_use]
    pub const fn new(
        id: StepExecutionId,
        job_execution_id: JobExecutionId,
        step_name: StepName,
        metadata: ExecutionMetadata,
    ) -> Self {
        Self {
            id,
            job_execution_id,
            step_name,
            metadata,
        }
    }

    /// Returns the step-attempt identifier.
    #[must_use]
    pub const fn id(&self) -> StepExecutionId {
        self.id
    }

    /// Returns the enclosing job-execution identifier.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Borrows the logical step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Borrows the execution metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ExecutionMetadata {
        &self.metadata
    }
}
