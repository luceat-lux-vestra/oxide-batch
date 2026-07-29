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
//!
//! [`JobExecutionListener`] and [`StepExecutionListener`] callbacks nest around
//! tasklet work with deterministic ordering. [`LifecycleEvent`] values are
//! emitted after corresponding metadata commits through a non-authoritative
//! [`LifecycleEventSink`]. Their structured fields exclude parameters,
//! contexts, records, credentials, and arbitrary user error payloads.
//!
//! M2 chunk definitions use [`ChunkStep`], [`ChunkJob`], [`ItemReader`],
//! [`ItemProcessor`], [`ItemWriter`], and [`ChunkCompletion`]. The
//! [`JobLauncher::launch_chunk`] path reuses job/step lifecycle metadata while
//! [`ChunkTransaction`] isolates adapter-owned commit and rollback. End of
//! input, filtering, cooperative stopping, failures, unknown commit, and
//! post-commit acknowledgement remain distinct typed outcomes. An enlisted
//! writer borrows [`BusinessTransaction`] for only its call; no database-driver
//! type crosses the facade.
//!
//! [`Checkpoint`] and [`ExecutionContext`] retain bounded versioned JSON through
//! application-owned [`VersionedStateCodec`] implementations. Codec signatures
//! exchange JSON object bytes, keeping serializer types out of the public
//! contract. Their `Debug` output never includes payloads.
//!
//! Run the complete in-memory example from the workspace root:
//!
//! ```text
//! cargo run -p oxide-batch --example first_job
//! ```
//!
//! The application supplies the async executor; public tasklet and repository
//! contracts use [`BoxFuture`] rather than executor- or database-driver types.

#![forbid(unsafe_code)]

mod chunk;
mod chunk_runtime;
mod diagnostics;
mod domain;
mod listener;
mod repository;
mod runtime;
mod state;

pub use chunk::{
    BusinessStatement, BusinessTransaction, BusinessTransactionError, BusinessValue,
    BusinessValueKind, BusinessWriteResult, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkCount, ChunkCounts,
    ChunkError, ChunkProgress, ChunkSize, ChunkTransaction, ChunkTransactionContext,
    ChunkTransactionError, ChunkTransactionManager, ItemProcessor, ItemReader, ItemWriter,
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    WriteContext, WriteOutcome, WriterError,
};
pub use chunk_runtime::{
    ChunkAttemptOutcome, ChunkExecutionOutcome, ChunkExecutionReport, ChunkFailure, ChunkJob,
    ChunkLaunchReport, ChunkListener, ChunkListenerContext, ChunkListenerError,
    ChunkListenerFailure, ChunkListenerFailureKind, ChunkListenerPhase, ChunkStep,
};
pub use diagnostics::{
    DiagnosticField, EventComponent, EventSeverity, ExecutionAttempt, ExecutionCorrelation,
    LifecycleEvent, LifecycleEventKind, LifecycleEventSink, MetricLabel,
};
pub use domain::{
    BatchStatus, DomainError, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps,
    ExecutionVersion, ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary,
    IdentifierKind, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey,
    JobName, JobParameter, JobParameters, LifecycleError, LifecycleTransition, NameKind,
    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, StepExecution,
    StepExecutionId, StepName,
};
pub use listener::{
    JobExecutionListener, ListenerContext, ListenerError, ListenerFailure, ListenerFailureKind,
    ListenerPhase, StepExecutionListener,
};
pub use repository::{
    BoxFuture, Clock, IdGenerationError, IdGenerator, InMemoryJobRepository, JobInstanceSelection,
    JobRepository, RepositoryError, RepositoryUnitOfWork, SequentialIdGenerator, SystemClock,
};
#[cfg(feature = "postgres")]
pub use repository::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresJobRepository, PostgresMigrator, TlsMode,
};
pub use runtime::{
    BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext, JobLauncher, LaunchError,
    LaunchReport, StopSource, StopTiming, StopToken, Tasklet, TaskletContext, TaskletError,
    TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome, TaskletStep,
};
pub use state::{
    Checkpoint, DurableStateKind, ExecutionContext, StateCodecError, StateError, StateLimits,
    StateSchemaId, StateSchemaVersion, VersionedStateCodec,
};

/// The version of the `OxideBatch` facade crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
