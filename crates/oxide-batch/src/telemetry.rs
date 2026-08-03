//! Versioned, bounded, and non-authoritative telemetry contracts.
//!
//! Durable repository state remains the only correctness authority. Every
//! sink boundary in this module is panic-isolated, queues drop rather than
//! backpressure execution, and metric labels come from a closed catalog.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;

use crate::{
    ActorRef, AuthorizationClass, BatchStatus, BoxFuture, DiagnosticField, EventComponent,
    EventSeverity, JobExecutionId, JobName, MetricLabel, OperationId, OperatorAction,
    OperatorOutcomeClass, OperatorRejection, OperatorRequest, PurgeCounts, ReasonCode,
    RecoveryProposal, RetentionAction, RetentionOutcome, StepName,
};

/// The stable M4 telemetry schema version.
pub const TELEMETRY_SCHEMA_VERSION: u16 = 1;
/// Maximum distinct label combinations retained by one metric family.
pub const METRIC_CARDINALITY_BUDGET: usize = 200;
/// Maximum explicitly allowed job and step names.
pub const MAX_METRIC_NAME_ALLOWLIST: usize = 50;
/// Reserved value used for values outside an allowlist or cardinality budget.
pub const OTHER_LABEL_VALUE: &str = "__other__";
/// Minimum bounded exporter queue length.
pub const MIN_EXPORT_QUEUE_RECORDS: usize = 64;
/// Maximum bounded exporter queue length.
pub const MAX_EXPORT_QUEUE_RECORDS: usize = 65_536;
/// Default bounded exporter queue length.
pub const DEFAULT_EXPORT_QUEUE_RECORDS: usize = 1_024;
/// Minimum throttling window for drop notifications.
pub const MIN_DROP_REPORT_WINDOW: Duration = Duration::from_secs(1);
/// Maximum throttling window for drop notifications.
pub const MAX_DROP_REPORT_WINDOW: Duration = Duration::from_hours(1);
/// Default throttling window for drop notifications.
pub const DEFAULT_DROP_REPORT_WINDOW: Duration = Duration::from_mins(1);
/// Default retained events returned for one incident execution.
pub const DEFAULT_RETAINED_EVENTS_PER_EXECUTION: usize = 200;
/// Maximum retained events returned for one incident execution.
pub const MAX_RETAINED_EVENTS_PER_EXECUTION: usize = 200;
/// Default total retained events across executions.
pub const DEFAULT_RETAINED_EVENT_CAPACITY: usize = 4_096;

const JOB_SPAN_FIELDS: &[&str] = &[
    "job.name",
    "job.instance.id",
    "job.execution.id",
    "job.attempt",
    "status",
    "failure.category",
    "failure.id",
];
const STEP_SPAN_FIELDS: &[&str] = &[
    "job.name",
    "job.instance.id",
    "job.execution.id",
    "job.attempt",
    "step.name",
    "step.execution.id",
    "step.attempt",
    "status",
    "failure.category",
    "failure.id",
];
const CHUNK_SPAN_FIELDS: &[&str] = &[
    "job.execution.id",
    "step.execution.id",
    "chunk.sequence",
    "status",
    "failure.category",
    "failure.id",
];
const ITEM_SPAN_FIELDS: &[&str] = &[
    "job.execution.id",
    "step.execution.id",
    "chunk.sequence",
    "outcome",
    "failure.category",
    "failure.id",
];
const RETRY_SPAN_FIELDS: &[&str] = &[
    "job.execution.id",
    "step.execution.id",
    "retry.ordinal",
    "outcome",
    "failure.category",
    "failure.id",
];
const BACKOFF_SPAN_FIELDS: &[&str] = &[
    "job.execution.id",
    "step.execution.id",
    "retry.ordinal",
    "backoff.duration_class",
    "outcome",
];

/// One stable span in the telemetry schema version 1 hierarchy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TelemetrySpanKind {
    /// The root span for one job execution attempt.
    JobExecution,
    /// One step execution attempt below its job execution.
    StepExecution,
    /// One bounded chunk attempt below its step execution.
    ChunkAttempt,
    /// The read phase of a chunk attempt.
    ItemRead,
    /// The process phase of a chunk attempt.
    ItemProcess,
    /// The write phase of a chunk attempt.
    ItemWrite,
    /// The repository commit phase of a chunk attempt.
    RepositoryCommit,
    /// One retry attempt below its step execution.
    Retry,
    /// The bounded wait below one retry attempt.
    Backoff,
}

impl TelemetrySpanKind {
    /// Returns the stable schema-version-1 span name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobExecution => "job.execution",
            Self::StepExecution => "step.execution",
            Self::ChunkAttempt => "chunk.attempt",
            Self::ItemRead => "item.read",
            Self::ItemProcess => "item.process",
            Self::ItemWrite => "item.write",
            Self::RepositoryCommit => "repository.commit",
            Self::Retry => "retry",
            Self::Backoff => "backoff",
        }
    }

    /// Returns the required direct parent in the stable hierarchy.
    #[must_use]
    pub const fn parent(self) -> Option<Self> {
        match self {
            Self::JobExecution => None,
            Self::StepExecution => Some(Self::JobExecution),
            Self::ChunkAttempt | Self::Retry => Some(Self::StepExecution),
            Self::ItemRead | Self::ItemProcess | Self::ItemWrite | Self::RepositoryCommit => {
                Some(Self::ChunkAttempt)
            }
            Self::Backoff => Some(Self::Retry),
        }
    }

    /// Returns the framework component represented by this span.
    #[must_use]
    pub const fn component(self) -> EventComponent {
        match self {
            Self::JobExecution => EventComponent::Job,
            Self::StepExecution => EventComponent::Step,
            Self::ChunkAttempt => EventComponent::Chunk,
            Self::ItemRead | Self::ItemProcess | Self::ItemWrite => EventComponent::Item,
            Self::RepositoryCommit => EventComponent::Repository,
            Self::Retry | Self::Backoff => EventComponent::Retry,
        }
    }

    /// Returns the complete reviewed field-key set for this span.
    #[must_use]
    pub const fn safe_field_keys(self) -> &'static [&'static str] {
        match self {
            Self::JobExecution => JOB_SPAN_FIELDS,
            Self::StepExecution => STEP_SPAN_FIELDS,
            Self::ChunkAttempt | Self::RepositoryCommit => CHUNK_SPAN_FIELDS,
            Self::ItemRead | Self::ItemProcess | Self::ItemWrite => ITEM_SPAN_FIELDS,
            Self::Retry => RETRY_SPAN_FIELDS,
            Self::Backoff => BACKOFF_SPAN_FIELDS,
        }
    }
}

impl fmt::Display for TelemetrySpanKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable adapter-neutral span outcome classes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TelemetrySpanStatus {
    /// No terminal outcome has been assigned yet.
    Unset,
    /// The observed work completed successfully.
    Ok,
    /// The observed work failed with a known outcome.
    Error,
    /// The observed work stopped cooperatively.
    Cancelled,
    /// The commit or execution outcome is unknown.
    Unknown,
}

impl TelemetrySpanStatus {
    /// Maps a durable lifecycle status to its adapter-neutral span outcome.
    #[must_use]
    pub const fn from_batch_status(status: BatchStatus) -> Self {
        match status {
            BatchStatus::Starting | BatchStatus::Started | BatchStatus::Stopping => Self::Unset,
            BatchStatus::Stopped => Self::Cancelled,
            BatchStatus::Failed | BatchStatus::Abandoned => Self::Error,
            BatchStatus::Completed => Self::Ok,
            // `BatchStatus` is `#[non_exhaustive]`, so a status this build
            // does not know reports the outcome it has: unknown.
            // `BatchStatus::Unknown`, and any status this build does not know:
            // `BatchStatus` is `#[non_exhaustive]`, and an unrecognized status
            // reports the outcome it has.
            _ => Self::Unknown,
        }
    }

    /// Returns the stable lowercase representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }
}

impl From<BatchStatus> for TelemetrySpanStatus {
    fn from(status: BatchStatus) -> Self {
        Self::from_batch_status(status)
    }
}

impl fmt::Display for TelemetrySpanStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The complete telemetry schema version 1 span catalog.
pub const TELEMETRY_SPAN_CATALOG: &[TelemetrySpanKind] = &[
    TelemetrySpanKind::JobExecution,
    TelemetrySpanKind::StepExecution,
    TelemetrySpanKind::ChunkAttempt,
    TelemetrySpanKind::ItemRead,
    TelemetrySpanKind::ItemProcess,
    TelemetrySpanKind::ItemWrite,
    TelemetrySpanKind::RepositoryCommit,
    TelemetrySpanKind::Retry,
    TelemetrySpanKind::Backoff,
];

/// Stable timing of an event relative to the decision it observes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum EventTiming {
    /// Emitted after the governing durable commit returns successfully.
    AfterCommit,
    /// Emitted after a bounded read returns successfully.
    AfterRead,
    /// Emitted after durable evidence is gathered and before it is returned.
    AfterEvidence,
    /// Emitted when a non-durable runtime boundary is observed.
    RuntimeBoundary,
}

/// One stable event in telemetry schema version 1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TelemetryEventKind {
    /// A launch request entered the guarded operator boundary.
    LaunchRequested,
    /// A launch became durable.
    LaunchAccepted,
    /// A launch guard durably rejected the request.
    LaunchRejected,
    /// A job or step lifecycle event.
    JobStarting,
    /// A job lifecycle transition.
    JobStarted,
    /// A job lifecycle transition.
    JobStopping,
    /// A job lifecycle transition.
    JobStopped,
    /// A job lifecycle transition.
    JobFailed,
    /// A job lifecycle transition.
    JobCompleted,
    /// A job lifecycle transition.
    JobAbandoned,
    /// A job commit outcome is unknown.
    JobUnknown,
    /// A step lifecycle transition.
    StepStarting,
    /// A step lifecycle transition.
    StepStarted,
    /// A step lifecycle transition.
    StepStopping,
    /// A step lifecycle transition.
    StepStopped,
    /// A step lifecycle transition.
    StepFailed,
    /// A step lifecycle transition.
    StepCompleted,
    /// A step commit outcome is unknown.
    StepUnknown,
    /// Chunk work began.
    ChunkStarted,
    /// A chunk transaction committed.
    ChunkCommitted,
    /// A chunk transaction rolled back.
    ChunkRolledBack,
    /// A chunk commit outcome is unknown.
    ChunkUnknown,
    /// A job before-listener failed or panicked.
    JobBeforeListenerFailed,
    /// A job after-listener failed or panicked.
    JobAfterListenerFailed,
    /// A step before-listener failed or panicked.
    StepBeforeListenerFailed,
    /// A step after-listener failed or panicked.
    StepAfterListenerFailed,
    /// A retry reservation committed.
    RetryReserved,
    /// A retry backoff began.
    RetryBackoffStarted,
    /// A retry backoff was cancelled.
    RetryBackoffCancelled,
    /// A retry budget was exhausted.
    RetryExhausted,
    /// A skip became durable with its accepting chunk.
    ItemSkipped,
    /// A known rollback committed its classification.
    FaultRollbackCommitted,
    /// A commit-safe no-rollback classification committed.
    FaultNoRollbackCommitted,
    /// A checkpoint was loaded.
    CheckpointLoaded,
    /// A checkpoint committed.
    CheckpointCommitted,
    /// A repository optimistic conflict was observed.
    RepositoryConflict,
    /// A repository transient failure was observed.
    RepositoryTransientFailure,
    /// A flow step result committed.
    FlowStepResultCommitted,
    /// A flow decision committed.
    FlowDecisionCommitted,
    /// A completed flow step was reused.
    FlowCompletedStepReused,
    /// A step start limit rejected another start.
    StepStartLimitExceeded,
    /// An operator request and effect committed.
    OperatorRequestAccepted,
    /// An operator rejection audit committed.
    OperatorRequestRejected,
    /// An operator request completed or replayed.
    OperatorRequestCompleted,
    /// A bounded explorer page returned.
    ExplorerPageServed,
    /// Shutdown was requested.
    ShutdownRequested,
    /// New intake was stopped.
    ShutdownIntakeStopped,
    /// Every owned child joined.
    ShutdownDrainCompleted,
    /// A shutdown deadline elapsed.
    ShutdownDeadlineExceeded,
    /// Durable evidence identified a stale candidate.
    StaleDetected,
    /// A recovery proposal was produced.
    RecoveryProposed,
    /// A recovery decision and lifecycle change committed.
    RecoveryApplied,
    /// A recovery request was durably rejected.
    RecoveryRejected,
    /// A retention plan was produced.
    RetentionPlanned,
    /// A retention mutation committed.
    RetentionApplied,
    /// A retention request was durably rejected.
    RetentionRejected,
    /// A bounded split branch began.
    SplitBranchStarted,
    /// A bounded split branch ended.
    SplitBranchCompleted,
    /// A partition plan committed.
    PartitionPlanCommitted,
    /// A local partition assignment committed.
    PartitionAssigned,
    /// A local partition result committed.
    PartitionCompleted,
    /// Parent partition aggregation committed.
    PartitionAggregated,
    /// A bounded exporter dropped a record.
    TelemetryExportDropped,
    /// A metadata migration began.
    MigrationStarted,
    /// A metadata migration completed.
    MigrationCompleted,
    /// A metadata migration failed.
    MigrationFailed,
}

impl TelemetryEventKind {
    /// Returns the stable dotted catalog name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LaunchRequested => "launch.requested",
            Self::LaunchAccepted => "launch.accepted",
            Self::LaunchRejected => "launch.rejected",
            Self::JobStarting => "job.starting",
            Self::JobStarted => "job.started",
            Self::JobStopping => "job.stopping",
            Self::JobStopped => "job.stopped",
            Self::JobFailed => "job.failed",
            Self::JobCompleted => "job.completed",
            Self::JobAbandoned => "job.abandoned",
            Self::JobUnknown => "job.unknown",
            Self::StepStarting => "step.starting",
            Self::StepStarted => "step.started",
            Self::StepStopping => "step.stopping",
            Self::StepStopped => "step.stopped",
            Self::StepFailed => "step.failed",
            Self::StepCompleted => "step.completed",
            Self::StepUnknown => "step.unknown",
            Self::ChunkStarted => "chunk.started",
            Self::ChunkCommitted => "chunk.committed",
            Self::ChunkRolledBack => "chunk.rolled_back",
            Self::ChunkUnknown => "chunk.unknown",
            Self::JobBeforeListenerFailed => "job.before_listener.failed",
            Self::JobAfterListenerFailed => "job.after_listener.failed",
            Self::StepBeforeListenerFailed => "step.before_listener.failed",
            Self::StepAfterListenerFailed => "step.after_listener.failed",
            Self::RetryReserved => "retry.reserved",
            Self::RetryBackoffStarted => "retry.backoff_started",
            Self::RetryBackoffCancelled => "retry.backoff_cancelled",
            Self::RetryExhausted => "retry.exhausted",
            Self::ItemSkipped => "item.skipped",
            Self::FaultRollbackCommitted => "fault.rollback_committed",
            Self::FaultNoRollbackCommitted => "fault.no_rollback_committed",
            Self::CheckpointLoaded => "checkpoint.loaded",
            Self::CheckpointCommitted => "checkpoint.committed",
            Self::RepositoryConflict => "repository.conflict",
            Self::RepositoryTransientFailure => "repository.transient_failure",
            Self::FlowStepResultCommitted => "flow.step_result_committed",
            Self::FlowDecisionCommitted => "flow.decision_committed",
            Self::FlowCompletedStepReused => "flow.completed_step_reused",
            Self::StepStartLimitExceeded => "step.start_limit_exceeded",
            Self::OperatorRequestAccepted => "operator.request_accepted",
            Self::OperatorRequestRejected => "operator.request_rejected",
            Self::OperatorRequestCompleted => "operator.request_completed",
            Self::ExplorerPageServed => "explorer.page_served",
            Self::ShutdownRequested => "shutdown.requested",
            Self::ShutdownIntakeStopped => "shutdown.intake_stopped",
            Self::ShutdownDrainCompleted => "shutdown.drain_completed",
            Self::ShutdownDeadlineExceeded => "shutdown.deadline_exceeded",
            Self::StaleDetected => "stale.detected",
            Self::RecoveryProposed => "recovery.proposed",
            Self::RecoveryApplied => "recovery.applied",
            Self::RecoveryRejected => "recovery.rejected",
            Self::RetentionPlanned => "retention.planned",
            Self::RetentionApplied => "retention.applied",
            Self::RetentionRejected => "retention.rejected",
            Self::SplitBranchStarted => "split.branch_started",
            Self::SplitBranchCompleted => "split.branch_completed",
            Self::PartitionPlanCommitted => "partition.plan_committed",
            Self::PartitionAssigned => "partition.assigned",
            Self::PartitionCompleted => "partition.completed",
            Self::PartitionAggregated => "partition.aggregated",
            Self::TelemetryExportDropped => "telemetry.export_dropped",
            Self::MigrationStarted => "migration.started",
            Self::MigrationCompleted => "migration.completed",
            Self::MigrationFailed => "migration.failed",
        }
    }

    /// Returns the stable severity.
    #[must_use]
    pub const fn severity(self) -> EventSeverity {
        match self {
            Self::ExplorerPageServed => EventSeverity::Debug,
            Self::JobFailed
            | Self::StepFailed
            | Self::JobUnknown
            | Self::StepUnknown
            | Self::ChunkUnknown
            | Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed
            | Self::RetryExhausted
            | Self::ShutdownDeadlineExceeded
            | Self::MigrationFailed => EventSeverity::Error,
            Self::LaunchRejected
            | Self::JobStopping
            | Self::JobStopped
            | Self::StepStopping
            | Self::StepStopped
            | Self::ChunkRolledBack
            | Self::RetryBackoffCancelled
            | Self::ItemSkipped
            | Self::FaultRollbackCommitted
            | Self::FaultNoRollbackCommitted
            | Self::OperatorRequestRejected
            | Self::RecoveryRejected
            | Self::RetentionRejected
            | Self::TelemetryExportDropped => EventSeverity::Warn,
            _ => EventSeverity::Info,
        }
    }

    /// Returns the framework component.
    #[must_use]
    pub const fn component(self) -> EventComponent {
        match self {
            Self::LaunchRequested | Self::LaunchAccepted | Self::LaunchRejected => {
                EventComponent::Launcher
            }
            Self::JobStarting
            | Self::JobStarted
            | Self::JobStopping
            | Self::JobStopped
            | Self::JobFailed
            | Self::JobCompleted
            | Self::JobAbandoned
            | Self::JobUnknown => EventComponent::Job,
            Self::StepStarting
            | Self::StepStarted
            | Self::StepStopping
            | Self::StepStopped
            | Self::StepFailed
            | Self::StepCompleted
            | Self::StepUnknown
            | Self::StepStartLimitExceeded => EventComponent::Step,
            Self::ChunkStarted
            | Self::ChunkCommitted
            | Self::ChunkRolledBack
            | Self::ChunkUnknown => EventComponent::Chunk,
            Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed => EventComponent::Listener,
            Self::RetryReserved
            | Self::RetryBackoffStarted
            | Self::RetryBackoffCancelled
            | Self::RetryExhausted => EventComponent::Retry,
            Self::ItemSkipped => EventComponent::Item,
            Self::FaultRollbackCommitted | Self::FaultNoRollbackCommitted => EventComponent::Fault,
            Self::CheckpointLoaded | Self::CheckpointCommitted => EventComponent::Checkpoint,
            Self::RepositoryConflict | Self::RepositoryTransientFailure => {
                EventComponent::Repository
            }
            Self::FlowStepResultCommitted
            | Self::FlowDecisionCommitted
            | Self::FlowCompletedStepReused => EventComponent::Flow,
            Self::OperatorRequestAccepted
            | Self::OperatorRequestRejected
            | Self::OperatorRequestCompleted => EventComponent::Operator,
            Self::ExplorerPageServed => EventComponent::Explorer,
            Self::ShutdownRequested
            | Self::ShutdownIntakeStopped
            | Self::ShutdownDrainCompleted
            | Self::ShutdownDeadlineExceeded => EventComponent::Shutdown,
            Self::StaleDetected
            | Self::RecoveryProposed
            | Self::RecoveryApplied
            | Self::RecoveryRejected => EventComponent::Recovery,
            Self::RetentionPlanned | Self::RetentionApplied | Self::RetentionRejected => {
                EventComponent::Retention
            }
            Self::SplitBranchStarted | Self::SplitBranchCompleted => EventComponent::Split,
            Self::PartitionPlanCommitted
            | Self::PartitionAssigned
            | Self::PartitionCompleted
            | Self::PartitionAggregated => EventComponent::Partition,
            Self::TelemetryExportDropped => EventComponent::Telemetry,
            Self::MigrationStarted | Self::MigrationCompleted | Self::MigrationFailed => {
                EventComponent::Migration
            }
        }
    }

    /// Returns when the event is emitted relative to its observation.
    #[must_use]
    pub const fn timing(self) -> EventTiming {
        match self {
            Self::OperatorRequestAccepted
            | Self::OperatorRequestRejected
            | Self::OperatorRequestCompleted
            | Self::RecoveryApplied
            | Self::RecoveryRejected
            | Self::RetentionApplied
            | Self::RetentionRejected
            | Self::PartitionPlanCommitted
            | Self::PartitionAssigned
            | Self::PartitionCompleted
            | Self::PartitionAggregated
            | Self::LaunchAccepted
            | Self::LaunchRejected
            | Self::JobStarting
            | Self::JobStarted
            | Self::JobStopping
            | Self::JobStopped
            | Self::JobFailed
            | Self::JobCompleted
            | Self::JobAbandoned
            | Self::JobUnknown
            | Self::StepStarting
            | Self::StepStarted
            | Self::StepStopping
            | Self::StepStopped
            | Self::StepFailed
            | Self::StepCompleted
            | Self::StepUnknown
            | Self::ChunkCommitted
            | Self::ChunkRolledBack
            | Self::ChunkUnknown
            | Self::JobBeforeListenerFailed
            | Self::JobAfterListenerFailed
            | Self::StepBeforeListenerFailed
            | Self::StepAfterListenerFailed
            | Self::RetryReserved
            | Self::RetryExhausted
            | Self::ItemSkipped
            | Self::FaultRollbackCommitted
            | Self::FaultNoRollbackCommitted
            | Self::CheckpointCommitted
            | Self::FlowStepResultCommitted
            | Self::FlowDecisionCommitted => EventTiming::AfterCommit,
            Self::ExplorerPageServed => EventTiming::AfterRead,
            Self::StaleDetected | Self::RecoveryProposed | Self::RetentionPlanned => {
                EventTiming::AfterEvidence
            }
            _ => EventTiming::RuntimeBoundary,
        }
    }
}

impl fmt::Display for TelemetryEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The complete telemetry schema version 1 event catalog.
pub const TELEMETRY_EVENT_CATALOG: &[TelemetryEventKind] = &[
    TelemetryEventKind::LaunchRequested,
    TelemetryEventKind::LaunchAccepted,
    TelemetryEventKind::LaunchRejected,
    TelemetryEventKind::JobStarting,
    TelemetryEventKind::JobStarted,
    TelemetryEventKind::JobStopping,
    TelemetryEventKind::JobStopped,
    TelemetryEventKind::JobFailed,
    TelemetryEventKind::JobCompleted,
    TelemetryEventKind::JobAbandoned,
    TelemetryEventKind::JobUnknown,
    TelemetryEventKind::StepStarting,
    TelemetryEventKind::StepStarted,
    TelemetryEventKind::StepStopping,
    TelemetryEventKind::StepStopped,
    TelemetryEventKind::StepFailed,
    TelemetryEventKind::StepCompleted,
    TelemetryEventKind::StepUnknown,
    TelemetryEventKind::ChunkStarted,
    TelemetryEventKind::ChunkCommitted,
    TelemetryEventKind::ChunkRolledBack,
    TelemetryEventKind::ChunkUnknown,
    TelemetryEventKind::JobBeforeListenerFailed,
    TelemetryEventKind::JobAfterListenerFailed,
    TelemetryEventKind::StepBeforeListenerFailed,
    TelemetryEventKind::StepAfterListenerFailed,
    TelemetryEventKind::RetryReserved,
    TelemetryEventKind::RetryBackoffStarted,
    TelemetryEventKind::RetryBackoffCancelled,
    TelemetryEventKind::RetryExhausted,
    TelemetryEventKind::ItemSkipped,
    TelemetryEventKind::FaultRollbackCommitted,
    TelemetryEventKind::FaultNoRollbackCommitted,
    TelemetryEventKind::CheckpointLoaded,
    TelemetryEventKind::CheckpointCommitted,
    TelemetryEventKind::RepositoryConflict,
    TelemetryEventKind::RepositoryTransientFailure,
    TelemetryEventKind::FlowStepResultCommitted,
    TelemetryEventKind::FlowDecisionCommitted,
    TelemetryEventKind::FlowCompletedStepReused,
    TelemetryEventKind::StepStartLimitExceeded,
    TelemetryEventKind::OperatorRequestAccepted,
    TelemetryEventKind::OperatorRequestRejected,
    TelemetryEventKind::OperatorRequestCompleted,
    TelemetryEventKind::ExplorerPageServed,
    TelemetryEventKind::ShutdownRequested,
    TelemetryEventKind::ShutdownIntakeStopped,
    TelemetryEventKind::ShutdownDrainCompleted,
    TelemetryEventKind::ShutdownDeadlineExceeded,
    TelemetryEventKind::StaleDetected,
    TelemetryEventKind::RecoveryProposed,
    TelemetryEventKind::RecoveryApplied,
    TelemetryEventKind::RecoveryRejected,
    TelemetryEventKind::RetentionPlanned,
    TelemetryEventKind::RetentionApplied,
    TelemetryEventKind::RetentionRejected,
    TelemetryEventKind::SplitBranchStarted,
    TelemetryEventKind::SplitBranchCompleted,
    TelemetryEventKind::PartitionPlanCommitted,
    TelemetryEventKind::PartitionAssigned,
    TelemetryEventKind::PartitionCompleted,
    TelemetryEventKind::PartitionAggregated,
    TelemetryEventKind::TelemetryExportDropped,
    TelemetryEventKind::MigrationStarted,
    TelemetryEventKind::MigrationCompleted,
    TelemetryEventKind::MigrationFailed,
];

/// One versioned event containing only reviewed safe fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryRecord {
    kind: TelemetryEventKind,
    fields: Vec<DiagnosticField>,
    job_execution_id: Option<JobExecutionId>,
}

impl TelemetryRecord {
    /// Constructs a catalog event without application-supplied fields.
    #[must_use]
    pub const fn catalog(kind: TelemetryEventKind) -> Self {
        Self {
            kind,
            fields: Vec::new(),
            job_execution_id: None,
        }
    }

    pub(crate) fn operator(
        kind: TelemetryEventKind,
        request: &OperatorRequest,
        outcome: Option<OperatorOutcomeClass>,
        rejection: Option<OperatorRejection>,
    ) -> Self {
        let mut record = Self::catalog(kind);
        record.fields = vec![
            DiagnosticField::new("operator.action", request.action().as_str()),
            DiagnosticField::new(
                "authorization.class",
                request.authorization_class().as_str(),
            ),
            DiagnosticField::new("operation.id", request.operation_id().as_str()),
            DiagnosticField::new("actor.ref", request.actor().as_str()),
        ];
        if let Some(reason) = request.reason() {
            record
                .fields
                .push(DiagnosticField::new("reason.code", reason.as_str()));
        }
        if let Some(outcome) = outcome {
            record
                .fields
                .push(DiagnosticField::new("outcome.class", outcome.as_str()));
        }
        if let Some(rejection) = rejection {
            record
                .fields
                .push(DiagnosticField::new("rejection.class", rejection.as_str()));
        }
        record.job_execution_id = request.job_execution_id();
        record
    }

    pub(crate) fn recovery(kind: TelemetryEventKind, proposal: &RecoveryProposal) -> Self {
        let execution_id = proposal.evidence().execution_id();
        let mut record = Self::catalog(kind);
        record.job_execution_id = Some(execution_id);
        record.fields = vec![
            DiagnosticField::new("job.execution.id", execution_id.to_string()),
            DiagnosticField::new("evidence.digest_present", "true"),
            DiagnosticField::new(
                "inactivity.class",
                inactivity_class(proposal.evidence().inactivity()),
            ),
        ];
        record
    }

    pub(crate) fn shutdown(kind: TelemetryEventKind, drain: &'static str, unjoined: usize) -> Self {
        let mut record = Self::catalog(kind);
        record.fields = vec![
            DiagnosticField::new("drain.result", drain),
            DiagnosticField::new("unjoined.tasks", unjoined.to_string()),
        ];
        record
    }

    pub(crate) const fn explorer(job_execution_id: Option<JobExecutionId>) -> Self {
        Self {
            kind: TelemetryEventKind::ExplorerPageServed,
            fields: Vec::new(),
            job_execution_id,
        }
    }

    pub(crate) fn retention(
        kind: TelemetryEventKind,
        action: Option<RetentionAction>,
        outcome: Option<RetentionOutcome>,
        counts: PurgeCounts,
    ) -> Self {
        let mut fields = Vec::new();
        if let Some(action) = action {
            fields.push(DiagnosticField::new("retention.action", action.as_str()));
        }
        if let Some(outcome) = outcome {
            fields.push(DiagnosticField::new("outcome.class", outcome.as_str()));
        }
        fields.extend([
            DiagnosticField::new(
                "deleted.flow_decisions",
                counts.flow_decisions().to_string(),
            ),
            DiagnosticField::new(
                "deleted.recovery_decisions",
                counts.recovery_decisions().to_string(),
            ),
            DiagnosticField::new(
                "deleted.operator_requests",
                counts.operator_requests().to_string(),
            ),
            DiagnosticField::new(
                "deleted.step_partitions",
                counts.step_partitions().to_string(),
            ),
            DiagnosticField::new(
                "deleted.step_executions",
                counts.step_executions().to_string(),
            ),
            DiagnosticField::new(
                "deleted.job_executions",
                counts.job_executions().to_string(),
            ),
            DiagnosticField::new("deleted.job_instances", counts.job_instances().to_string()),
        ]);
        Self {
            kind,
            fields,
            job_execution_id: None,
        }
    }

    /// Returns the schema version carried by this event.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        TELEMETRY_SCHEMA_VERSION
    }

    /// Returns the stable catalog kind.
    #[must_use]
    pub const fn kind(&self) -> TelemetryEventKind {
        self.kind
    }

    /// Returns the named execution retained by an incident buffer, when any.
    #[must_use]
    pub const fn job_execution_id(&self) -> Option<JobExecutionId> {
        self.job_execution_id
    }

    /// Borrows reviewed structured fields.
    #[must_use]
    pub fn fields(&self) -> &[DiagnosticField] {
        &self.fields
    }
}

fn inactivity_class(duration: Duration) -> &'static str {
    match duration.as_secs() {
        0..=59 => "lt_1m",
        60..=899 => "1m_to_15m",
        900..=3_599 => "15m_to_1h",
        3_600..=86_399 => "1h_to_24h",
        _ => "gte_24h",
    }
}

/// Receives versioned observational events.
pub trait TelemetryEventSink: Send + Sync {
    /// Emits one reviewed record.
    fn emit(&self, event: &TelemetryRecord);
}

/// Invalid finite incident-buffer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncidentBufferConfigurationError;

impl fmt::Display for IncidentBufferConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("incident event bounds must be nonzero and per-execution at most 200")
    }
}

impl std::error::Error for IncidentBufferConfigurationError {}

#[derive(Debug)]
struct IncidentState {
    records: VecDeque<TelemetryRecord>,
}

/// A process-local, finite incident event buffer.
///
/// The buffer is diagnostic only and loses its contents on crash. Durable
/// metadata remains authoritative.
#[derive(Debug)]
pub struct IncidentEventBuffer {
    per_execution: usize,
    total: usize,
    state: Mutex<IncidentState>,
}

impl IncidentEventBuffer {
    /// Validates and constructs one finite buffer.
    ///
    /// # Errors
    ///
    /// Rejects zero bounds, a per-execution bound above 200, or a total bound
    /// smaller than the per-execution bound.
    pub fn new(
        per_execution: usize,
        total: usize,
    ) -> Result<Self, IncidentBufferConfigurationError> {
        if per_execution == 0
            || per_execution > MAX_RETAINED_EVENTS_PER_EXECUTION
            || total < per_execution
        {
            return Err(IncidentBufferConfigurationError);
        }
        Ok(Self {
            per_execution,
            total,
            state: Mutex::new(IncidentState {
                records: VecDeque::with_capacity(total),
            }),
        })
    }

    /// Returns at most the configured newest records for one execution.
    #[must_use]
    pub fn events_for(&self, execution_id: JobExecutionId) -> Vec<TelemetryRecord> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut selected = state
            .records
            .iter()
            .rev()
            .filter(|record| record.job_execution_id() == Some(execution_id))
            .take(self.per_execution)
            .cloned()
            .collect::<Vec<_>>();
        selected.reverse();
        selected
    }
}

impl Default for IncidentEventBuffer {
    fn default() -> Self {
        Self {
            per_execution: DEFAULT_RETAINED_EVENTS_PER_EXECUTION,
            total: DEFAULT_RETAINED_EVENT_CAPACITY,
            state: Mutex::new(IncidentState {
                records: VecDeque::with_capacity(DEFAULT_RETAINED_EVENT_CAPACITY),
            }),
        }
    }
}

impl TelemetryEventSink for IncidentEventBuffer {
    fn emit(&self, event: &TelemetryRecord) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.records.len() == self.total {
            state.records.pop_front();
        }
        state.records.push_back(event.clone());
    }
}

/// Emits an event while isolating a sink panic from execution correctness.
pub(crate) fn emit_safely(sink: Option<&Arc<dyn TelemetryEventSink>>, event: &TelemetryRecord) {
    if let Some(sink) = sink {
        let _ = catch_unwind(AssertUnwindSafe(|| sink.emit(event)));
    }
}

/// A complete set of typed dimensions accepted by the metric catalog.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricDimensions {
    event: Option<TelemetryEventKind>,
    status: Option<BatchStatus>,
    action: Option<OperatorAction>,
    authorization: Option<AuthorizationClass>,
    outcome: Option<OperatorOutcomeClass>,
    job_name: Option<JobName>,
    step_name: Option<StepName>,
}

impl MetricDimensions {
    /// Adds one bounded event-name dimension.
    #[must_use]
    pub const fn with_event(mut self, value: TelemetryEventKind) -> Self {
        self.event = Some(value);
        self
    }

    /// Adds one lifecycle-status dimension.
    #[must_use]
    pub const fn with_status(mut self, value: BatchStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Adds one operator-action dimension.
    #[must_use]
    pub const fn with_action(mut self, value: OperatorAction) -> Self {
        self.action = Some(value);
        self
    }

    /// Adds one authorization-class dimension.
    #[must_use]
    pub const fn with_authorization(mut self, value: AuthorizationClass) -> Self {
        self.authorization = Some(value);
        self
    }

    /// Adds one operator-outcome dimension.
    #[must_use]
    pub const fn with_outcome(mut self, value: OperatorOutcomeClass) -> Self {
        self.outcome = Some(value);
        self
    }

    /// Adds a validated job name, subject to the configured allowlist.
    #[must_use]
    pub fn with_job_name(mut self, value: JobName) -> Self {
        self.job_name = Some(value);
        self
    }

    /// Adds a validated step name, subject to the configured allowlist.
    #[must_use]
    pub fn with_step_name(mut self, value: StepName) -> Self {
        self.step_name = Some(value);
        self
    }
}

/// A stable metric family and its complete allowed label set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum MetricFamily {
    /// Currently active executions.
    ActiveExecutions,
    /// Completed execution outcomes.
    CompletedExecutions,
    /// Execution duration distribution.
    ExecutionDuration,
    /// Read, process, write, filter, skip, retry, commit, and rollback counts.
    ItemCount,
    /// Repository operation duration distribution.
    RepositoryOperationDuration,
    /// Repository optimistic conflicts.
    RepositoryConflicts,
    /// Repository failures.
    RepositoryErrors,
    /// Bounded queue depth.
    QueueDepth,
    /// Configured concurrency budget.
    ConfiguredConcurrency,
    /// Currently active concurrency.
    ActiveConcurrency,
    /// Guarded operator request outcomes.
    OperatorRequests,
    /// Versioned execution event counter.
    ExecutionEvents,
    /// Recovery outcomes.
    RecoveryOutcomes,
    /// Shutdown outcomes.
    ShutdownOutcomes,
    /// Bounded exporter drops.
    ExportDropped,
}

impl MetricFamily {
    /// Returns the stable family name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ActiveExecutions => "oxide_batch_active_executions",
            Self::CompletedExecutions => "oxide_batch_completed_executions_total",
            Self::ExecutionDuration => "oxide_batch_execution_duration_seconds",
            Self::ItemCount => "oxide_batch_item_operations_total",
            Self::RepositoryOperationDuration => {
                "oxide_batch_repository_operation_duration_seconds"
            }
            Self::RepositoryConflicts => "oxide_batch_repository_conflicts_total",
            Self::RepositoryErrors => "oxide_batch_repository_errors_total",
            Self::QueueDepth => "oxide_batch_queue_depth_records",
            Self::ConfiguredConcurrency => "oxide_batch_concurrency_configured_workers",
            Self::ActiveConcurrency => "oxide_batch_concurrency_active_workers",
            Self::OperatorRequests => "oxide_batch_operator_requests_total",
            Self::ExecutionEvents => "oxide_batch_execution_events_total",
            Self::RecoveryOutcomes => "oxide_batch_recovery_outcomes_total",
            Self::ShutdownOutcomes => "oxide_batch_shutdown_outcomes_total",
            Self::ExportDropped => "oxide_batch_telemetry_export_dropped_total",
        }
    }

    /// Returns the versioned unit.
    #[must_use]
    pub const fn unit(self) -> MetricUnit {
        match self {
            Self::ExecutionDuration | Self::RepositoryOperationDuration => MetricUnit::Seconds,
            Self::ItemCount => MetricUnit::Items,
            Self::QueueDepth => MetricUnit::Records,
            Self::ConfiguredConcurrency | Self::ActiveConcurrency => MetricUnit::Workers,
            Self::RepositoryConflicts
            | Self::RepositoryErrors
            | Self::OperatorRequests
            | Self::ExecutionEvents
            | Self::RecoveryOutcomes
            | Self::ShutdownOutcomes
            | Self::ExportDropped => MetricUnit::Events,
            Self::ActiveExecutions | Self::CompletedExecutions => MetricUnit::Executions,
        }
    }

    /// Returns the complete permitted label-key set for this family.
    #[must_use]
    pub const fn label_keys(self) -> &'static [&'static str] {
        match self {
            Self::ActiveExecutions | Self::CompletedExecutions | Self::ExecutionDuration => {
                &["status", "job", "step"]
            }
            Self::ItemCount
            | Self::RepositoryOperationDuration
            | Self::RepositoryConflicts
            | Self::RepositoryErrors
            | Self::QueueDepth
            | Self::ConfiguredConcurrency
            | Self::ActiveConcurrency => &["event"],
            Self::OperatorRequests => &["action", "authorization", "outcome"],
            Self::ExecutionEvents => &["event", "status", "job", "step"],
            Self::RecoveryOutcomes => &["outcome", "action"],
            Self::ShutdownOutcomes => &["status"],
            Self::ExportDropped => &["reason"],
        }
    }
}

/// Stable measurement unit of one metric family.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum MetricUnit {
    /// Execution count.
    Executions,
    /// Event or operation count.
    Events,
    /// Duration in seconds.
    Seconds,
    /// Item count.
    Items,
    /// Queue record count.
    Records,
    /// Worker count.
    Workers,
}

/// A metric observation after allowlist and cardinality enforcement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricObservation {
    family: MetricFamily,
    labels: Vec<MetricLabel>,
    overflowed: bool,
}

impl MetricObservation {
    /// Returns the stable family.
    #[must_use]
    pub const fn family(&self) -> MetricFamily {
        self.family
    }

    /// Borrows the complete bounded label set.
    #[must_use]
    pub fn labels(&self) -> &[MetricLabel] {
        &self.labels
    }

    /// Returns whether a new combination was mapped to the reserved series.
    #[must_use]
    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }
}

/// Invalid metric name-labelling configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricConfigurationError;

impl fmt::Display for MetricConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("metric name allowlists may contain at most 50 names")
    }
}

impl std::error::Error for MetricConfigurationError {}

/// Enforces the per-family label-cardinality and name-allowlist budgets.
#[derive(Debug, Default)]
pub struct MetricCardinalityGuard {
    job_names: BTreeSet<JobName>,
    step_names: BTreeSet<StepName>,
    observed: BTreeMap<MetricFamily, BTreeSet<String>>,
    dropped: BTreeMap<MetricFamily, u64>,
}

impl MetricCardinalityGuard {
    /// Configures explicit job and step name allowlists.
    ///
    /// # Errors
    ///
    /// Rejects either allowlist when it contains more than 50 names.
    pub fn new(
        job_names: impl IntoIterator<Item = JobName>,
        step_names: impl IntoIterator<Item = StepName>,
    ) -> Result<Self, MetricConfigurationError> {
        let job_names: BTreeSet<_> = job_names.into_iter().collect();
        let step_names: BTreeSet<_> = step_names.into_iter().collect();
        if job_names.len() > MAX_METRIC_NAME_ALLOWLIST
            || step_names.len() > MAX_METRIC_NAME_ALLOWLIST
        {
            return Err(MetricConfigurationError);
        }
        Ok(Self {
            job_names,
            step_names,
            observed: BTreeMap::new(),
            dropped: BTreeMap::new(),
        })
    }

    /// Applies the declared family label set and finite series budget.
    #[must_use]
    pub fn observe(
        &mut self,
        family: MetricFamily,
        dimensions: &MetricDimensions,
    ) -> MetricObservation {
        let mut labels = family_labels(family, dimensions, &self.job_names, &self.step_names);
        let key = label_key(&labels);
        let observed = self.observed.entry(family).or_default();
        let overflowed = !observed.contains(&key)
            && observed.len() >= METRIC_CARDINALITY_BUDGET.saturating_sub(1);
        if overflowed {
            for label in &mut labels {
                label.replace_value(OTHER_LABEL_VALUE);
            }
            *self.dropped.entry(family).or_default() += 1;
            observed.insert(label_key(&labels));
        } else {
            observed.insert(key);
        }
        MetricObservation {
            family,
            labels,
            overflowed,
        }
    }

    /// Returns the count of combinations mapped to the reserved series.
    #[must_use]
    pub fn dropped_cardinality(&self, family: MetricFamily) -> u64 {
        self.dropped.get(&family).copied().unwrap_or(0)
    }

    /// Returns the currently retained series count for one family.
    #[must_use]
    pub fn series_count(&self, family: MetricFamily) -> usize {
        self.observed.get(&family).map_or(0, BTreeSet::len)
    }
}

fn family_labels(
    family: MetricFamily,
    dimensions: &MetricDimensions,
    job_names: &BTreeSet<JobName>,
    step_names: &BTreeSet<StepName>,
) -> Vec<MetricLabel> {
    let mut labels = Vec::new();
    match family {
        MetricFamily::ActiveExecutions
        | MetricFamily::CompletedExecutions
        | MetricFamily::ExecutionDuration => {
            labels.push(MetricLabel::new(
                "status",
                dimensions
                    .status
                    .map_or_else(|| "none".to_owned(), |status| status.to_string()),
            ));
            labels.push(MetricLabel::new(
                "job",
                allowed_job_name(dimensions.job_name.as_ref(), job_names),
            ));
            labels.push(MetricLabel::new(
                "step",
                allowed_step_name(dimensions.step_name.as_ref(), step_names),
            ));
        }
        MetricFamily::ItemCount
        | MetricFamily::RepositoryOperationDuration
        | MetricFamily::RepositoryConflicts
        | MetricFamily::RepositoryErrors
        | MetricFamily::QueueDepth
        | MetricFamily::ConfiguredConcurrency
        | MetricFamily::ActiveConcurrency => labels.push(MetricLabel::new(
            "event",
            dimensions
                .event
                .map_or("unknown", TelemetryEventKind::as_str),
        )),
        MetricFamily::OperatorRequests => {
            labels.push(MetricLabel::new(
                "action",
                dimensions.action.map_or("unknown", OperatorAction::as_str),
            ));
            labels.push(MetricLabel::new(
                "authorization",
                dimensions
                    .authorization
                    .map_or("unknown", AuthorizationClass::as_str),
            ));
            labels.push(MetricLabel::new(
                "outcome",
                dimensions
                    .outcome
                    .map_or("unknown", OperatorOutcomeClass::as_str),
            ));
        }
        MetricFamily::ExecutionEvents => {
            labels.push(MetricLabel::new(
                "event",
                dimensions
                    .event
                    .map_or("unknown", TelemetryEventKind::as_str),
            ));
            labels.push(MetricLabel::new(
                "status",
                dimensions
                    .status
                    .map_or_else(|| "none".to_owned(), |status| status.to_string()),
            ));
            labels.push(MetricLabel::new(
                "job",
                allowed_job_name(dimensions.job_name.as_ref(), job_names),
            ));
            labels.push(MetricLabel::new(
                "step",
                allowed_step_name(dimensions.step_name.as_ref(), step_names),
            ));
        }
        MetricFamily::RecoveryOutcomes => {
            labels.push(MetricLabel::new(
                "outcome",
                dimensions
                    .outcome
                    .map_or("unknown", OperatorOutcomeClass::as_str),
            ));
            labels.push(MetricLabel::new(
                "action",
                dimensions.action.map_or("RECOVER", OperatorAction::as_str),
            ));
        }
        MetricFamily::ShutdownOutcomes => labels.push(MetricLabel::new(
            "status",
            dimensions
                .status
                .map_or_else(|| "none".to_owned(), |status| status.to_string()),
        )),
        MetricFamily::ExportDropped => labels.push(MetricLabel::new("reason", "queue_full")),
    }
    labels
}

fn allowed_job_name<'a>(name: Option<&'a JobName>, allowlist: &'a BTreeSet<JobName>) -> &'a str {
    match name {
        Some(name) if allowlist.contains(name) => name.as_str(),
        _ => OTHER_LABEL_VALUE,
    }
}

fn allowed_step_name<'a>(name: Option<&'a StepName>, allowlist: &'a BTreeSet<StepName>) -> &'a str {
    match name {
        Some(name) if allowlist.contains(name) => name.as_str(),
        _ => OTHER_LABEL_VALUE,
    }
}

fn label_key(labels: &[MetricLabel]) -> String {
    labels
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// A validated finite exporter queue bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportQueueBound(usize);

impl ExportQueueBound {
    /// Validates `64..=65536` records.
    ///
    /// # Errors
    ///
    /// Returns [`ExporterConfigurationError`] outside the accepted range.
    pub const fn new(value: usize) -> Result<Self, ExporterConfigurationError> {
        if value < MIN_EXPORT_QUEUE_RECORDS || value > MAX_EXPORT_QUEUE_RECORDS {
            return Err(ExporterConfigurationError::QueueBound);
        }
        Ok(Self(value))
    }

    /// Returns the accepted record count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for ExportQueueBound {
    fn default() -> Self {
        Self(DEFAULT_EXPORT_QUEUE_RECORDS)
    }
}

/// A validated throttling window for exporter-drop reporting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DropReportWindow(Duration);

impl DropReportWindow {
    /// Validates `1 s..=1 h`.
    ///
    /// # Errors
    ///
    /// Returns [`ExporterConfigurationError`] outside the accepted range.
    pub const fn new(value: Duration) -> Result<Self, ExporterConfigurationError> {
        if value.as_millis() < MIN_DROP_REPORT_WINDOW.as_millis()
            || value.as_millis() > MAX_DROP_REPORT_WINDOW.as_millis()
        {
            return Err(ExporterConfigurationError::DropReportWindow);
        }
        Ok(Self(value))
    }

    /// Returns the accepted window.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for DropReportWindow {
    fn default() -> Self {
        Self(DEFAULT_DROP_REPORT_WINDOW)
    }
}

/// Invalid bounded exporter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExporterConfigurationError {
    /// Queue count was outside `64..=65536`.
    QueueBound,
    /// Drop report window was outside `1 s..=1 h`.
    DropReportWindow,
}

impl fmt::Display for ExporterConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueBound => formatter.write_str("export queue must hold 64 to 65536 records"),
            Self::DropReportWindow => {
                formatter.write_str("drop report window must be between 1 second and 1 hour")
            }
        }
    }
}

impl std::error::Error for ExporterConfigurationError {}

#[derive(Debug)]
struct QueueState {
    records: VecDeque<TelemetryRecord>,
    dropped: u64,
    last_drop_report: Option<Duration>,
}

/// Result of one non-blocking enqueue attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EnqueueResult {
    /// The record entered the queue.
    Accepted,
    /// The newest record was dropped because the queue was full.
    Dropped {
        /// Whether a throttled `telemetry.export_dropped` observation is due.
        report_due: bool,
    },
}

/// Cloneable producer for one bounded exporter queue.
#[derive(Clone, Debug)]
pub struct TelemetryQueue {
    bound: ExportQueueBound,
    report_window: DropReportWindow,
    state: Arc<Mutex<QueueState>>,
}

impl TelemetryQueue {
    /// Constructs an empty bounded queue.
    #[must_use]
    pub fn new(bound: ExportQueueBound, report_window: DropReportWindow) -> Self {
        Self {
            bound,
            report_window,
            state: Arc::new(Mutex::new(QueueState {
                records: VecDeque::with_capacity(bound.get()),
                dropped: 0,
                last_drop_report: None,
            })),
        }
    }

    /// Enqueues without waiting; a full queue drops this newest record.
    #[must_use]
    pub fn enqueue(&self, record: TelemetryRecord, now: Duration) -> EnqueueResult {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.records.len() < self.bound.get() {
            state.records.push_back(record);
            return EnqueueResult::Accepted;
        }
        state.dropped = state.dropped.saturating_add(1);
        let report_due = state.last_drop_report.is_none_or(|last| {
            now.checked_sub(last)
                .is_some_and(|elapsed| elapsed >= self.report_window.get())
        });
        if report_due {
            state.last_drop_report = Some(now);
        }
        EnqueueResult::Dropped { report_due }
    }

    /// Returns the current queued count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .records
            .len()
    }

    /// Returns whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the cumulative dropped-newest count.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .dropped
    }

    fn pop(&self) -> Option<TelemetryRecord> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .records
            .pop_front()
    }
}

/// Adapter-owned asynchronous export boundary.
pub trait TelemetryExportSink: Send + Sync {
    /// Exports one reviewed record without exposing SDK types to the facade.
    fn export<'a>(&'a self, record: &'a TelemetryRecord) -> BoxFuture<'a, Result<(), ExportError>>;
}

/// A value-redacted exporter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportError;

impl fmt::Display for ExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("telemetry export failed")
    }
}

impl std::error::Error for ExportError {}

/// Flush result that never changes batch correctness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportFlushReport {
    exported: u64,
    failed: u64,
    dropped: u64,
}

impl ExportFlushReport {
    /// Returns successfully exported records.
    #[must_use]
    pub const fn exported(self) -> u64 {
        self.exported
    }

    /// Returns records isolated after an exporter error or panic.
    #[must_use]
    pub const fn failed(self) -> u64 {
        self.failed
    }

    /// Returns records dropped before export.
    #[must_use]
    pub const fn dropped(self) -> u64 {
        self.dropped
    }
}

/// Drains one queue from an application-owned task.
pub struct TelemetryExporter<S> {
    queue: TelemetryQueue,
    sink: S,
}

impl<S: TelemetryExportSink> TelemetryExporter<S> {
    /// Binds a queue to an adapter without spawning any task.
    #[must_use]
    pub const fn new(queue: TelemetryQueue, sink: S) -> Self {
        Self { queue, sink }
    }

    /// Drains every currently queued record and isolates adapter failures.
    pub async fn flush(&self) -> ExportFlushReport {
        let mut exported = 0_u64;
        let mut failed = 0_u64;
        while let Some(record) = self.queue.pop() {
            let result = AssertUnwindSafe(self.sink.export(&record))
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(())) => exported = exported.saturating_add(1),
                Ok(Err(_)) | Err(_) => failed = failed.saturating_add(1),
            }
        }
        ExportFlushReport {
            exported,
            failed,
            dropped: self.queue.dropped(),
        }
    }
}

// These imports are deliberately exercised in public field constructors. Keep
// them visible to rustdoc as the closed safe-field vocabulary grows.
const _: fn(&ActorRef, &OperationId, &ReasonCode, PurgeCounts) = |_, _, _, _| {};
