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
//! M3 adds runtime-neutral fault-tolerance values. [`FaultPolicy`] combines a
//! [`FaultClassifier`] over stable [`FaultPhase`] and [`FailureCategory`]
//! inputs with a bounded [`RetryLimit`], [`RetryStateLimit`], [`SkipLimit`],
//! and a deterministic [`BackoffPolicy`]. [`FaultPolicy::decide`] is a pure
//! function of the policy, a framework-owned [`FaultDescriptor`], and
//! [`FaultEvidence`], so a restart reproduces the same decision. Waiting uses
//! an injected [`BackoffSleeper`] rather than wall-clock time, and
//! [`RollbackDisposition::CommitSafeSkip`] still records a skip instead of
//! silently dropping an item.
//!
//! [`ItemListenerSet`] owns the M3 [`ReadListener`], [`ProcessListener`],
//! [`WriteListener`], [`RetryListener`], and [`SkipListener`] families. Before
//! callbacks run in registration order and stop at the first failure; the
//! matching completion callbacks run only the entered listeners in reverse
//! order and aggregate every failure. A panic is classified exactly like a
//! returned [`ListenerError`], and no callback receives an error payload.
//!
//! [`ChunkStep::with_fault_runtime`] installs a [`FaultRuntime`] and makes that
//! policy executable. A retryable fault rolls the chunk attempt back, reserves
//! its ordinal through a bounded [`FaultStateStore`], runs the retry scope,
//! waits the injected backoff, and replays the chunk from inputs it already
//! read, so a stateful reader never rewinds. An accepted skip is provisional
//! until the commit that records it, and a commit-safe skip additionally
//! requires [`ChunkDeliveryMode::AtomicSameResource`] and an enlisted
//! transaction. [`ChunkExecutionReport`] returns per-phase
//! [`RetryCounts`] and [`SkipCounts`], rollback and no-rollback counts, and
//! redacted [`ItemListenerFailure`] values.
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
mod definition;
mod diagnostics;
mod domain;
mod fault;
mod fault_state;
mod flow;
mod item_listener;
mod listener;
mod plan;
mod repository;
mod runtime;
mod state;

pub use chunk::{
    BusinessStatement, BusinessTransaction, BusinessTransactionError, BusinessValue,
    BusinessValueKind, BusinessWriteResult, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkCount, ChunkCounts,
    ChunkError, ChunkFaultProgress, ChunkProgress, ChunkSize, ChunkTransaction,
    ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager, InheritedStepProgress,
    ItemProcessor, ItemReader, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError,
    ReadContext, ReadOutcome, ReaderError, WriteContext, WriteOutcome, WriterError,
};
pub use chunk_runtime::{
    ChunkAttemptOutcome, ChunkExecutionOutcome, ChunkExecutionReport, ChunkFailure, ChunkJob,
    ChunkLaunchReport, ChunkListener, ChunkListenerContext, ChunkListenerError,
    ChunkListenerFailure, ChunkListenerFailureKind, ChunkListenerPhase, ChunkStep,
};
pub use definition::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ClassifierRevision,
    ComponentRevision, DefinitionError, DefinitionIdentity, DefinitionManifest, DefinitionRevision,
    DefinitionTokenKind, DefinitionUpgrade, DefinitionUpgradeKey, ManifestError,
    StepDefinitionUpgrade,
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
pub use fault::{
    BackoffKind, BackoffOutcome, BackoffPolicy, BackoffSleeper, FaultAction, FaultClassifier,
    FaultDecision, FaultDescriptor, FaultEvidence, FaultPhase, FaultPolicy, FaultPolicyError,
    FaultRule, RetryLimit, RetryOrdinal, RetryStateLimit, RollbackDisposition, SkipCounts,
    SkipLimit,
};
pub use fault_state::{
    FaultProgress, FaultRuntime, FaultStateEntry, FaultStateEnvelope, FaultStateError,
    FaultStateFormatError, FaultStateStore, InMemoryFaultState, RetryCounts, RetryKey,
    RetryReservation,
};
pub use flow::{
    DeciderError, DecisionInput, DecisionStepInput, FlowDecision, FlowDecisionId,
    FlowDecisionRequest, FlowDecisionSequence, FlowExecutionOutcome, FlowFailure, FlowJob,
    FlowJobError, FlowLaunchReport, FlowLauncher, FlowRuntimeError, FlowStepState,
    FlowTransitionKind, JobExecutionDecider,
};
pub use item_listener::{
    BeforeCallbackOutcome, ItemListenerContext, ItemListenerError, ItemListenerFailure,
    ItemListenerPhase, ItemListenerSet, ProcessListener, ReadListener, RetryListener, RetryOutcome,
    SkipListener, WriteListener,
};
pub use listener::{
    JobExecutionListener, ListenerContext, ListenerError, ListenerFailure, ListenerFailureKind,
    ListenerPhase, StepExecutionListener,
};
pub use plan::{
    CompiledExecutionPlan, DeciderRevision, DecisionInputVersion, DecisionNode, ExitPattern,
    FlowGraph, FlowNode, FlowSelectionError, FlowTarget, FlowTransition, MAX_NODES,
    MAX_OUTGOING_TRANSITIONS, MAX_PATTERN_BYTES, MAX_TRANSITIONS, NodeId, PatternSpecificity,
    PlanError, StartControls, StartLimit, StepComponents, StepNode, TerminalKind,
};
pub use repository::{
    BoxFuture, Clock, IdGenerationError, IdGenerator, InMemoryJobRepository, JobInstanceSelection,
    JobRepository, RecoveryDecision, RecoveryDisposition, RecoveryField, RecoveryRequest,
    RecoveryRequestError, RecoveryResult, RepositoryError, RepositoryUnitOfWork,
    SequentialIdGenerator, SystemClock,
};
#[cfg(feature = "postgres")]
pub use repository::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresFaultState, PostgresJobRepository, PostgresMigrator, TlsMode,
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
