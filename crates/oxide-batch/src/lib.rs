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
//! [`ItemReader`], [`ItemProcessor`], and [`ItemWriter`] are plain `async fn`
//! contracts ([ADR-0008](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/decisions/0008-item-component-contract.md)):
//! an implementor writes a natural `async fn`, with no `BoxFuture`, `Pin`,
//! `Box::pin`, or named future type in sight, and [`ChunkStep`] drives it
//! monomorphized with no per-item allocation. [`BoxedReader`],
//! [`BoxedProcessor`], and [`BoxedWriter`] are the explicit, opt-in erasure
//! boundary for dynamic composition.
//!
//! ```
//! use oxide_batch::{ItemReader, ReadContext, ReadOutcome, ReaderError, StopSource};
//!
//! struct Counter(u64);
//!
//! impl ItemReader<u64> for Counter {
//!     async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
//!         self.0 += 1;
//!         Ok(ReadOutcome::Item(self.0))
//!     }
//! }
//!
//! let mut counter = Counter(0);
//! let (_source, stop) = StopSource::new();
//! let outcome = futures_executor::block_on(counter.read(ReadContext::new(&stop)))?;
//! assert_eq!(outcome, ReadOutcome::Item(1));
//! # Ok::<(), ReaderError>(())
//! ```
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
//! A codec declares its current schema version and the [`StateSchemaUpgrade`]
//! edges it can apply. Decoding an older recorded version walks one bounded,
//! deterministic chain of those edges and only then calls
//! [`VersionedStateCodec::decode`], so a codec parses exactly one shape. A
//! recorded version newer than the codec, or one with no declared path to the
//! current version, fails closed rather than being truncated, defaulted, or
//! reinterpreted.
//!
//! [`ItemStream`] restores a namespaced [`ComponentStateEnvelope`] before
//! item work begins, prepares a candidate at the commit boundary, and closes
//! after the step attempt's terminal outcome is known -- the M6
//! open/update/close contract, in the same ADR-0008 shape as
//! [`ItemReader`]/[`ItemProcessor`]/[`ItemWriter`]. A
//! [`ComponentStateCodec`] adds a codec identity/version axis distinct from
//! the application schema identity/version axis; any existing
//! [`VersionedStateCodec`] plugs into [`DefaultComponentCodec`] unchanged.
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! use oxide_batch::{
//!     CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity,
//!     DefaultComponentCodec, ItemStream, RestartabilityDeclaration, StateCodecError, StateLimits,
//!     StateSchemaId, StateSchemaVersion, StopSource, StreamCloseContext, StreamCloseError,
//!     StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome,
//!     StreamRuntimeOutcome, StreamUpdateContext, StreamUpdateError, VersionedStateCodec,
//! };
//!
//! struct RowCountSchema {
//!     schema: StateSchemaId,
//!     version: StateSchemaVersion,
//! }
//!
//! impl VersionedStateCodec<u64> for RowCountSchema {
//!     fn schema_id(&self) -> &StateSchemaId {
//!         &self.schema
//!     }
//!     fn current_version(&self) -> StateSchemaVersion {
//!         self.version
//!     }
//!     fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
//!         serde_json::to_vec(&serde_json::json!({ "rows": value }))
//!             .map_err(|_| StateCodecError::InvalidPayload)
//!     }
//!     fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
//!         let value: serde_json::Value =
//!             serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
//!         value
//!             .get("rows")
//!             .and_then(serde_json::Value::as_u64)
//!             .ok_or(StateCodecError::InvalidPayload)
//!     }
//! }
//!
//! fn codec() -> Result<DefaultComponentCodec<RowCountSchema>, Box<dyn std::error::Error>> {
//!     Ok(DefaultComponentCodec::new(
//!         RowCountSchema {
//!             schema: StateSchemaId::new("example.row-count")?,
//!             version: StateSchemaVersion::new(1)?,
//!         },
//!         CodecId::new("example.row-count-codec")?,
//!         CodecVersion::new(1)?,
//!         RestartabilityDeclaration::Restartable,
//!     ))
//! }
//!
//! struct RowCount(AtomicU64);
//!
//! impl ItemStream for RowCount {
//!     async fn open(
//!         &self,
//!         context: StreamOpenContext<'_>,
//!     ) -> Result<StreamOpenOutcome, StreamOpenError> {
//!         let Some(inherited) = context.inherited_state() else {
//!             return Ok(StreamOpenOutcome::Initial);
//!         };
//!         let codec = codec().map_err(|_| StreamOpenError::new())?;
//!         let restored: u64 = inherited.decode(&codec).map_err(|_| StreamOpenError::new())?;
//!         self.0.store(restored, Ordering::SeqCst);
//!         Ok(StreamOpenOutcome::Restored)
//!     }
//!
//!     async fn update(
//!         &self,
//!         _context: StreamUpdateContext<'_>,
//!     ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
//!         let rows = self.0.fetch_add(1, Ordering::SeqCst) + 1;
//!         let codec = codec().map_err(|_| StreamUpdateError::new())?;
//!         let namespace = ComponentStreamIdentity::new("reader.row_count")
//!             .map_err(|_| StreamUpdateError::new())?;
//!         ComponentStateEnvelope::encode(namespace, &rows, &codec, StateLimits::default())
//!             .map_err(|_| StreamUpdateError::new())
//!     }
//!
//!     async fn close(
//!         &self,
//!         context: StreamCloseContext<'_>,
//!     ) -> Result<StreamCloseOutcome, StreamCloseError> {
//!         assert!(matches!(context.outcome(), StreamRuntimeOutcome::Committed));
//!         Ok(StreamCloseOutcome::Closed)
//!     }
//! }
//!
//! let stream = RowCount(AtomicU64::new(0));
//! let (_source, stop) = StopSource::new();
//!
//! let opened = futures_executor::block_on(stream.open(StreamOpenContext::new(None, &stop)))?;
//! assert_eq!(opened, StreamOpenOutcome::Initial);
//!
//! let candidate = futures_executor::block_on(stream.update(StreamUpdateContext::new(&stop)))?;
//! assert_eq!(candidate.decode(&codec()?)?, 1u64);
//!
//! let closed = futures_executor::block_on(stream.close(StreamCloseContext::new(
//!     &stop,
//!     StreamRuntimeOutcome::Committed,
//! )))?;
//! assert_eq!(closed, StreamCloseOutcome::Closed);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [`DefinitionManifest`] reads canonical definition bytes back without
//! guessing. A newer format, a non-canonical encoding, a floating-point value,
//! an out-of-bound graph, or a digest that does not match the supplied bytes
//! fails closed.
//!
//! ```
//! use oxide_batch::{
//!     ComponentRevision, DefinitionIdentity, DefinitionManifest, DefinitionRevision, JobName,
//!     StepName,
//! };
//!
//! let identity = DefinitionIdentity::tasklet(
//!     &JobName::new("daily_import")?,
//!     &StepName::new("import")?,
//!     DefinitionRevision::new("2026-07-31")?,
//!     &ComponentRevision::new("tasklet-v1")?,
//! )?;
//! let manifest = DefinitionManifest::read_verified(
//!     identity.canonical_manifest(),
//!     identity.manifest_digest(),
//! )?;
//! assert_eq!(manifest.format(), 1);
//! assert_eq!(manifest.node_count(), None);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A multi-step definition is declared as a [`FlowGraph`] of [`FlowNode`]
//! values joined by exit-pattern [`FlowTransition`] edges and compiled into an
//! immutable [`CompiledExecutionPlan`] that owns the canonical manifest and
//! fingerprint:
//!
//! ```
//! use oxide_batch::{
//!     ComponentRevision, DefinitionRevision, ExitPattern, FlowGraph, FlowNode, FlowTarget,
//!     FlowTransition, JobName, NodeId, StepComponents, StepNode, StepName, TerminalKind,
//! };
//!
//! let load = NodeId::new("load")?;
//! let report = NodeId::new("report")?;
//! let plan = FlowGraph::new(load.clone())
//!     .with_node(FlowNode::step(StepNode::new(
//!         load.clone(),
//!         StepName::new("load")?,
//!         StepComponents::Tasklet(ComponentRevision::new("load-v1")?),
//!     )))
//!     .with_node(FlowNode::step(StepNode::new(
//!         report.clone(),
//!         StepName::new("report")?,
//!         StepComponents::Tasklet(ComponentRevision::new("report-v1")?),
//!     )))
//!     .with_sequence(load, FlowTarget::Node(report.clone()))?
//!     .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?
//!     .compile(&JobName::new("daily_import")?, DefinitionRevision::new("v1")?)?;
//!
//! assert_eq!(plan.manifest_format(), 2);
//! assert_eq!(plan.node_count(), 2);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [`ExitPattern`] selects one of those transitions from the bounded
//! [`ExitCode`] a step reports, never from a [`BatchStatus`]:
//!
//! ```
//! use oxide_batch::{ExitCode, ExitPattern};
//!
//! let failed = ExitPattern::new("FAILED")?;
//! let any = ExitPattern::new("*")?;
//! assert!(failed.matches(&ExitCode::new("FAILED")?));
//! assert!(!failed.matches(&ExitCode::new("COMPLETED")?));
//! assert!(any.matches(&ExitCode::new("COMPLETED")?));
//! assert!(failed.specificity() > any.specificity());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [`FaultPolicy::decide`] is a pure function of the policy, a
//! framework-owned [`FaultDescriptor`], and [`FaultEvidence`]:
//!
//! ```
//! use std::time::Duration;
//!
//! use oxide_batch::{
//!     BackoffPolicy, ChunkDeliveryMode, ClassifierRevision, FailureCategory, FailureId,
//!     FailureSummary, FaultAction, FaultClassifier, FaultDecision, FaultDescriptor,
//!     FaultEvidence, FaultPhase, FaultPolicy, FaultRule, RetryLimit, RetryOrdinal,
//!     RetryStateLimit, SkipCounts, SkipLimit,
//! };
//!
//! let classifier = FaultClassifier::new(
//!     ClassifierRevision::new("import_v1")?,
//!     [FaultRule::new(
//!         FaultPhase::Write,
//!         FailureCategory::Timeout,
//!         FaultAction::retry(),
//!     )?],
//! )?;
//! let policy = FaultPolicy::new(
//!     classifier,
//!     RetryLimit::new(3)?,
//!     RetryStateLimit::new(64)?,
//!     SkipLimit::NONE,
//!     BackoffPolicy::exponential(Duration::from_millis(50), 2, Duration::from_secs(5))?,
//! )?;
//!
//! let fault = FaultDescriptor::new(
//!     FaultPhase::Write,
//!     FailureSummary::new(FailureCategory::Timeout, FailureId::new(1)?),
//!     RetryOrdinal::INITIAL,
//!     SkipCounts::ZERO,
//!     true,
//!     ChunkDeliveryMode::AtomicSameResource,
//! );
//! assert_eq!(
//!     policy.decide(&fault, FaultEvidence::NONE),
//!     FaultDecision::Retry {
//!         ordinal: RetryOrdinal::new(1)?,
//!         delay: Duration::from_millis(50),
//!     }
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
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
mod fault;
mod fault_state;
mod flow;
mod item_listener;
mod item_stream;
mod listener;
mod repository;
mod runtime;
mod service;
mod shutdown;
mod telemetry;

pub use chunk::{
    BoxedProcessor, BoxedReader, BoxedWriter, BusinessStatement, BusinessTransaction,
    BusinessTransactionError, BusinessValue, BusinessValueKind, BusinessWriteResult,
    ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
    ChunkCompletionOutcome, ChunkFaultProgress, ChunkTransaction, ChunkTransactionContext,
    ChunkTransactionError, ChunkTransactionManager, InheritedStepProgress, ItemProcessor,
    ItemReader, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, WriteContext, WriteOutcome, WriterError,
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
pub use fault::{BackoffOutcome, BackoffSleeper};
pub use fault_state::{
    FaultProgress, FaultRuntime, FaultStateEntry, FaultStateEnvelope, FaultStateError,
    FaultStateFormatError, FaultStateStore, InMemoryFaultState, RetryCounts, RetryKey,
    RetryReservation,
};
pub use flow::{
    DeciderError, DecisionInput, DecisionStepInput, FlowEvent, FlowEventKind, FlowEventSink,
    FlowExecutionOutcome, FlowFailure, FlowJob, FlowJobError, FlowLaunchReport, FlowLauncher,
    FlowRuntimeError, JobExecutionDecider, PartitionFactoryError, PartitionPlanFactory,
    PartitionPlanRequest, PartitionTaskletFactory, PartitionWorkerInput, TaskletStepFactory,
};
pub use item_listener::{
    BeforeCallbackOutcome, ItemListenerContext, ItemListenerError, ItemListenerFailure,
    ItemListenerPhase, ItemListenerSet, ProcessListener, ReadListener, RetryListener, RetryOutcome,
    SkipListener, WriteListener,
};
pub use item_stream::{
    BoxedStream, ItemStream, StreamCloseContext, StreamCloseError, StreamCloseOutcome,
    StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamRuntimeOutcome,
    StreamUpdateContext, StreamUpdateError,
};
pub use listener::{
    JobExecutionListener, ListenerContext, ListenerError, ListenerFailure, ListenerFailureKind,
    ListenerPhase, StepExecutionListener,
};
pub use oxide_batch_core::{
    BackoffKind, BackoffPolicy, BatchStatus, Checkpoint, ChecksumAlgorithm,
    ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkError, ChunkProgress,
    ChunkRestartContract, ChunkSize, ClassifierRevision, CodecId, CodecVersion,
    CodecVersionUpgrade, ComponentRevision, ComponentStateCodec, ComponentStateEnvelope,
    ComponentStateError, ComponentStatePayload, ComponentStreamIdentity, ContentIdentity,
    DefaultComponentCodec, DefinitionError, DefinitionIdentity, DefinitionManifest,
    DefinitionRevision, DefinitionTokenKind, DefinitionUpgrade, DefinitionUpgradeKey, DomainError,
    DurableStateKind, ExecutionContext, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps,
    ExecutionVersion, ExitCode, ExitStatus, ExternalStateError, ExternalStateReference,
    ExternalStateStore, FailureCategory, FailureId, FailureSummary, FaultAction, FaultClassifier,
    FaultDecision, FaultDescriptor, FaultEvidence, FaultPhase, FaultPolicy, FaultPolicyError,
    FaultRule, FlowTarget, IdentifierKind, InFlightPolicy, JobExecution, JobExecutionId,
    JobInstance, JobInstanceId, JobInstanceKey, JobName, JobParameter, JobParameters,
    LifecycleError, LifecycleTransition, MAX_NODES, MAX_PARTITIONS, MAX_TRANSITIONS, ManifestError,
    NameKind, NodeId, OperatorRequestId, ParameterName, ParameterRole, ParameterValue,
    ParameterValueKind, RecoveryDecisionId, RestartabilityDeclaration, RetentionActionId,
    RetryLimit, RetryOrdinal, RetryStateLimit, RollbackDisposition, SkipCounts, SkipLimit,
    StartControls, StartLimit, StateCodecError, StateError, StateLimits, StateSchemaId,
    StateSchemaUpgrade, StateSchemaVersion, StateSensitivity, StepDefinitionUpgrade, StepExecution,
    StepExecutionId, StepName, StepPartitionId, StreamStateContract, TerminalKind,
    VersionedStateCodec,
};
pub use oxide_batch_plan::{
    CompiledExecutionPlan, DeciderRevision, DecisionInputVersion, DecisionNode, ExitPattern,
    FlowGraph, FlowNode, FlowSelectionError, FlowTransition, JoinNode, LocalFailurePolicy,
    MAX_BRANCH_STEPS, MAX_OUTGOING_TRANSITIONS, MAX_PARTITION_WORKERS, MAX_PATTERN_BYTES,
    MAX_SPLIT_BRANCHES, PartitionBudget, PartitionCount, PartitionedStepNode, PatternSpecificity,
    PlanError, SplitBranch, SplitBudget, SplitNode, StepComponents, StepNode,
};
pub use oxide_batch_repository::{
    ActorRef, AuthorizationClass, BoxFuture, Clock, Cursor, CursorError, CursorKey,
    DEFAULT_MAX_CLOCK_SKEW, DEFAULT_PAGE_SIZE, DEFAULT_PURGE_AGE, DEFAULT_STALE_THRESHOLD,
    DefinitionDescriptor, ExecutionControl, ExplorerError, ExplorerQuery, ExplorerRepository,
    FlowDecision, FlowDecisionId, FlowDecisionRequest, FlowDecisionSequence, FlowStepState,
    FlowTransitionKind, IdGenerationError, IdGenerator, JobExecutionProjection,
    JobInstanceProjection, JobInstanceSelection, JobRepository, MAX_ACTOR_REF_BYTES,
    MAX_CLOCK_SKEW, MAX_CURSOR_BYTES, MAX_OPERATION_ID_BYTES, MAX_PAGE_SIZE,
    MAX_PARTITION_CONTEXT_BYTES, MAX_PARTITION_KEY_BYTES, MAX_PURGE_BATCH, MAX_REASON_CODE_BYTES,
    MAX_RESPONSE_BYTES, MAX_STALE_THRESHOLD, MIN_CLOCK_SKEW, MIN_PURGE_AGE, MIN_STALE_THRESHOLD,
    MIN_UNRESOLVED_AGE, MaxClockSkew, MonotonicClock, MonotonicInstant, OperationId,
    OperatorAction, OperatorOutcomeClass, OperatorRecord, OperatorRecordDraft, OperatorRejection,
    OperatorRequest, OwnerObservation, OwnerToken, Page, PageRequest, PageSize,
    ParameterDescriptor, PartitionAggregate, PartitionAggregationError, PartitionKey,
    PartitionPlanEntry, PartitionResult, PartitionValueError, PurgeBatchBound, PurgeCandidate,
    PurgeCounts, PurgePlan, PurgePlanRequest, PurgeSurvey, QueryWindow, ReasonCode,
    RecoveryDecision, RecoveryDirective, RecoveryDisposition, RecoveryError, RecoveryEvidence,
    RecoveryField, RecoveryMarkers, RecoveryProposal, RecoveryRepository, RecoveryRequest,
    RecoveryRequestError, RecoveryResult, RecoverySnapshot, RecoveryStepEvidence,
    RepositoryCapability, RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork,
    RequestDigest, RequestField, RequestFieldError, RetentionAction, RetentionError, RetentionHold,
    RetentionOutcome, RetentionRecord, RetentionRecordDraft, SequentialIdGenerator, StaleThreshold,
    StateEnvelopeDescriptor, StepExecutionProjection, StepPartition, StepPartitionProjection,
    SystemClock, SystemMonotonicClock, TerminalStatusSet, aggregate_step_partitions,
};
#[cfg(feature = "postgres")]
pub use repository::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresExplorer, PostgresFaultState, PostgresJobRepository, PostgresMigrator, TlsMode,
};
pub use repository::{InMemoryExplorer, InMemoryJobRepository};
pub use runtime::{
    BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext, JobLauncher, LaunchError,
    LaunchReport, StopPollInterval, StopSource, StopTiming, StopToken, Tasklet, TaskletContext,
    TaskletError, TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome, TaskletStep,
};
pub use service::{
    JobExplorer, JobOperator, OperatorError, OperatorOutcome, RecoveryProposer, RetentionReport,
    RetentionService,
};
pub use shutdown::{
    DEFAULT_SHUTDOWN_DEADLINE, DEFAULT_TELEMETRY_FLUSH_DEADLINE, DrainResult,
    MAX_SHUTDOWN_DEADLINE, MAX_TELEMETRY_FLUSH_DEADLINE, MIN_SHUTDOWN_DEADLINE,
    MIN_TELEMETRY_FLUSH_DEADLINE, ShutdownCoordinator, ShutdownDeadline, ShutdownError,
    ShutdownHookError, ShutdownHookStatus, ShutdownReport, ShutdownRequest, ShutdownSignal,
    ShutdownTaskPhase, TaskJoinDeadline, TelemetryFlushDeadline, TelemetryFlushStatus,
    UnjoinedPhase,
};
pub use telemetry::{
    DEFAULT_DROP_REPORT_WINDOW, DEFAULT_EXPORT_QUEUE_RECORDS, DEFAULT_RETAINED_EVENT_CAPACITY,
    DEFAULT_RETAINED_EVENTS_PER_EXECUTION, DropReportWindow, EnqueueResult, EventTiming,
    ExportError, ExportFlushReport, ExportQueueBound, ExporterConfigurationError,
    IncidentBufferConfigurationError, IncidentEventBuffer, MAX_DROP_REPORT_WINDOW,
    MAX_EXPORT_QUEUE_RECORDS, MAX_METRIC_NAME_ALLOWLIST, MAX_RETAINED_EVENTS_PER_EXECUTION,
    METRIC_CARDINALITY_BUDGET, MIN_DROP_REPORT_WINDOW, MIN_EXPORT_QUEUE_RECORDS,
    MetricCardinalityGuard, MetricConfigurationError, MetricDimensions, MetricFamily,
    MetricObservation, MetricUnit, OTHER_LABEL_VALUE, TELEMETRY_EVENT_CATALOG,
    TELEMETRY_SCHEMA_VERSION, TELEMETRY_SPAN_CATALOG, TelemetryEventKind, TelemetryEventSink,
    TelemetryExportSink, TelemetryExporter, TelemetryQueue, TelemetryRecord, TelemetrySpanKind,
    TelemetrySpanStatus,
};

/// The version of the `OxideBatch` facade crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
