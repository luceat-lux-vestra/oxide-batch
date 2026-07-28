//! The public facade for `OxideBatch`.
//!
//! `OxideBatch` models a batch job as a named definition launched with typed
//! parameters. Identifying parameters select a logical [`JobInstance`], while
//! each launch or restart receives a distinct [`JobExecutionId`].
//!
//! ```
//! use oxide_batch::{
//!     JobInstanceKey, JobName, JobParameter, JobParameters, ParameterName,
//!     ParameterRole, ParameterValue,
//! };
//!
//! let mut parameters = JobParameters::new();
//! parameters.insert(
//!     ParameterName::new("business_date")?,
//!     JobParameter::new(
//!         ParameterValue::string("2026-07-29")?,
//!         ParameterRole::Identifying,
//!     ),
//! )?;
//!
//! let key = JobInstanceKey::new(JobName::new("daily_import")?, &parameters);
//! assert_eq!(key.identifying_parameter_count(), 1);
//! # Ok::<(), oxide_batch::DomainError>(())
//! ```
//!
//! Parameter values are sensitive by default. Their [`Debug`](std::fmt::Debug)
//! representations expose the value kind, but not the underlying value.

#![forbid(unsafe_code)]

mod domain;

pub use domain::{
    BatchStatus, DomainError, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExitCode,
    ExitStatus, FailureCategory, FailureId, FailureSummary, IdentifierKind, JobExecution,
    JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobName, JobParameter,
    JobParameters, NameKind, ParameterName, ParameterRole, ParameterValue, ParameterValueKind,
    StepExecution, StepExecutionId, StepName,
};

/// The version of the `OxideBatch` facade crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
