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
//!
//! Repository operations run inside an explicit [`RepositoryUnitOfWork`].
//! Time and identifiers are injected into the reference
//! [`InMemoryJobRepository`], so tests do not depend on wall-clock time or
//! process-global identifier state. A unit of work publishes changes only
//! after [`RepositoryUnitOfWork::commit`] succeeds; dropping it rolls back its
//! staged metadata.
//!
//! [`JobLauncher`] runs one-step [`TaskletJob`] definitions on an
//! application-owned executor. Tasklets borrow their call-scoped
//! [`TaskletContext`], receive cooperative [`StopToken`] state, and persist
//! completed, failed, panicked, or stopped outcomes through the repository.
//! Synchronous bodies use [`BlockingTaskletAdapter`] with an explicit nonzero
//! concurrency bound.

#![forbid(unsafe_code)]

mod domain;
mod repository;
mod runtime;

pub use domain::{
    BatchStatus, DomainError, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps,
    ExecutionVersion, ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary,
    IdentifierKind, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey,
    JobName, JobParameter, JobParameters, LifecycleError, LifecycleTransition, NameKind,
    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, StepExecution,
    StepExecutionId, StepName,
};
pub use repository::{
    BoxFuture, Clock, IdGenerationError, IdGenerator, InMemoryJobRepository, JobInstanceSelection,
    JobRepository, RepositoryError, RepositoryUnitOfWork, SequentialIdGenerator, SystemClock,
};
pub use runtime::{
    BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext, JobLauncher, LaunchError,
    LaunchReport, StopSource, StopTiming, StopToken, Tasklet, TaskletContext, TaskletError,
    TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome, TaskletStep,
};

/// The version of the `OxideBatch` facade crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
