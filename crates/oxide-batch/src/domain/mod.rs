//! Facade-owned domain values and execution records.

mod error;
mod execution;
mod identity;
mod parameter;

pub use error::{DomainError, IdentifierKind, NameKind};
pub use execution::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExitStatus,
    FailureCategory, FailureSummary, JobExecution, JobInstance, StepExecution,
};
pub use identity::{
    ExitCode, FailureId, JobExecutionId, JobInstanceId, JobName, ParameterName, StepExecutionId,
    StepName,
};
pub use parameter::{
    JobInstanceKey, JobParameter, JobParameters, ParameterRole, ParameterValue, ParameterValueKind,
};
