//! Facade-owned domain values and execution records.

mod error;
mod execution;
mod identity;
mod lifecycle;
mod parameter;

pub use error::{DomainError, IdentifierKind, NameKind};
pub use execution::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExitStatus,
    FailureCategory, FailureSummary, JobExecution, JobInstance, StepExecution,
};
pub use identity::{
    ExitCode, FailureId, JobExecutionId, JobInstanceId, JobName, OperatorRequestId, ParameterName,
    RecoveryDecisionId, RetentionActionId, StepExecutionId, StepName, StepPartitionId,
};
pub use lifecycle::{ExecutionVersion, LifecycleError, LifecycleTransition};
pub use parameter::{
    JobInstanceKey, JobParameter, JobParameters, ParameterRole, ParameterValue, ParameterValueKind,
};
