//! Facade public-surface evidence for the staged crate extraction.
//!
//! Extraction is behavior-preserving repackaging, so the curated `oxide-batch`
//! surface must survive every stage unchanged. Two independent checks hold it:
//!
//! - the [`resolves`] module names every supported import path, so a path that
//!   stops resolving after a move fails to compile;
//! - [`public_api_snapshot_is_unchanged_by_extraction`] renders the facade's
//!   exports from `src/lib.rs` and compares them with a committed snapshot.
//!
//! The snapshot pins exported paths and their feature gate. Item signatures
//! are held by the rest of the suite, the rustdoc build, and the compile-fail
//! tests rather than by this file.
//!
//! Rewrite the snapshot deliberately with
//! `OXIDEBATCH_UPDATE_FACADE_SNAPSHOT=1 cargo test -p oxide-batch
//! --all-features --test facade_surface`. A rewrite is an accepted, separately
//! reviewed API change, never a way to make a stage pass.

#![allow(unused_imports)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

/// Environment variable that rewrites the committed snapshot.
const UPDATE_VARIABLE: &str = "OXIDEBATCH_UPDATE_FACADE_SNAPSHOT";

/// The feature that gates the optional `PostgreSQL` surface.
const OPTIONAL_FEATURE: &str = "postgres";

/// Every supported `oxide-batch` import path.
///
/// The list is the compile-time half of the evidence: after a stage moves an
/// item into an extracted crate, the facade must re-export it under the same
/// path or this module stops compiling.
mod resolves {
    pub use oxide_batch::{
        ActorRef, AdaptiveBounds, AdaptiveCompletionPolicy, AuthorizationClass, BackoffKind,
        BackoffOutcome, BackoffPolicy, BackoffSleeper, BatchStatus, BeforeCallbackOutcome,
        BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext, BoxFuture, BoxedProcessor,
        BoxedReader, BoxedStream, BoxedWriter, BusinessStatement, BusinessTransaction,
        BusinessTransactionError, BusinessValue, BusinessValueKind, BusinessWriteResult,
        Checkpoint, ChecksumAlgorithm, ChunkAttemptOutcome, ChunkCommitReceipt, ChunkCompletion,
        ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome,
        ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkError,
        ChunkExecutionOutcome, ChunkExecutionReport, ChunkFailure, ChunkFaultProgress, ChunkJob,
        ChunkLaunchReport, ChunkListener, ChunkListenerContext, ChunkListenerError,
        ChunkListenerFailure, ChunkListenerFailureKind, ChunkListenerPhase, ChunkProgress,
        ChunkRestartContract, ChunkSize, ChunkStep, ChunkTimeThreshold, ChunkTransaction,
        ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager,
        ClassifierRevision, Clock, CodecId, CodecVersion, CodecVersionUpgrade,
        CompiledExecutionPlan, CompletionPolicy, CompletionPolicyError, ComponentRevision,
        ComponentStateCodec, ComponentStateEnvelope, ComponentStateError, ComponentStatePayload,
        ComponentStreamIdentity, CompositeCompletionPolicy, CompositeMode, ContentIdentity, Cursor,
        CursorError, CursorKey, DEFAULT_DROP_REPORT_WINDOW, DEFAULT_EXPORT_QUEUE_RECORDS,
        DEFAULT_MAX_CLOCK_SKEW, DEFAULT_PAGE_SIZE, DEFAULT_PURGE_AGE,
        DEFAULT_RETAINED_EVENT_CAPACITY, DEFAULT_RETAINED_EVENTS_PER_EXECUTION,
        DEFAULT_SHUTDOWN_DEADLINE, DEFAULT_STALE_THRESHOLD, DEFAULT_TELEMETRY_FLUSH_DEADLINE,
        DeciderError, DeciderRevision, DecisionInput, DecisionInputVersion, DecisionNode,
        DecisionStepInput, DefaultComponentCodec, DefinitionDescriptor, DefinitionError,
        DefinitionIdentity, DefinitionManifest, DefinitionRevision, DefinitionTokenKind,
        DefinitionUpgrade, DefinitionUpgradeKey, DiagnosticField, DomainError, DrainResult,
        DropReportWindow, DurableStateKind, EnqueueResult, EventComponent, EventSeverity,
        EventTiming, ExecutionAttempt, ExecutionContext, ExecutionControl, ExecutionCorrelation,
        ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitCode,
        ExitPattern, ExitStatus, ExplorerError, ExplorerQuery, ExplorerRepository, ExportError,
        ExportFlushReport, ExportQueueBound, ExporterConfigurationError, ExternalStateError,
        ExternalStateReference, ExternalStateStore, FailureCategory, FailureId, FailureSummary,
        FaultAction, FaultClassifier, FaultDecision, FaultDescriptor, FaultEvidence, FaultPhase,
        FaultPolicy, FaultPolicyError, FaultProgress, FaultRule, FaultRuntime, FaultStateEntry,
        FaultStateEnvelope, FaultStateError, FaultStateFormatError, FaultStateStore, FlowDecision,
        FlowDecisionId, FlowDecisionRequest, FlowDecisionSequence, FlowEvent, FlowEventKind,
        FlowEventSink, FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowJobError,
        FlowLaunchReport, FlowLauncher, FlowNode, FlowRuntimeError, FlowSelectionError,
        FlowStepState, FlowTarget, FlowTransition, FlowTransitionKind, IdGenerationError,
        IdGenerator, IdentifierKind, InFlightPolicy, InMemoryExplorer, InMemoryFaultState,
        InMemoryJobRepository, IncidentBufferConfigurationError, IncidentEventBuffer,
        InheritedStepProgress, ItemCountCompletionPolicy, ItemListenerContext, ItemListenerError,
        ItemListenerFailure, ItemListenerPhase, ItemListenerSet, ItemProcessor, ItemReader,
        ItemStream, ItemWriter, JobExecution, JobExecutionDecider, JobExecutionId,
        JobExecutionListener, JobExecutionProjection, JobExplorer, JobInstance, JobInstanceId,
        JobInstanceKey, JobInstanceProjection, JobInstanceSelection, JobLauncher, JobName,
        JobOperator, JobParameter, JobParameters, JobRepository, JoinNode, LaunchError,
        LaunchReport, LifecycleError, LifecycleEvent, LifecycleEventKind, LifecycleEventSink,
        LifecycleTransition, ListenerContext, ListenerError, ListenerFailure, ListenerFailureKind,
        ListenerPhase, LocalFailurePolicy, MAX_ACTOR_REF_BYTES, MAX_BRANCH_STEPS, MAX_CLOCK_SKEW,
        MAX_COMPOSITE_MEMBERS, MAX_CURSOR_BYTES, MAX_DROP_REPORT_WINDOW, MAX_EXPORT_QUEUE_RECORDS,
        MAX_METRIC_NAME_ALLOWLIST, MAX_NODES, MAX_OPERATION_ID_BYTES, MAX_OUTGOING_TRANSITIONS,
        MAX_PAGE_SIZE, MAX_PARTITION_CONTEXT_BYTES, MAX_PARTITION_KEY_BYTES, MAX_PARTITION_WORKERS,
        MAX_PARTITIONS, MAX_PATTERN_BYTES, MAX_PURGE_BATCH, MAX_REASON_CODE_BYTES,
        MAX_RESPONSE_BYTES, MAX_RETAINED_EVENTS_PER_EXECUTION, MAX_SHUTDOWN_DEADLINE,
        MAX_SPLIT_BRANCHES, MAX_STALE_THRESHOLD, MAX_TELEMETRY_FLUSH_DEADLINE, MAX_TRANSITIONS,
        METRIC_CARDINALITY_BUDGET, MIN_CLOCK_SKEW, MIN_DROP_REPORT_WINDOW,
        MIN_EXPORT_QUEUE_RECORDS, MIN_PURGE_AGE, MIN_SHUTDOWN_DEADLINE, MIN_STALE_THRESHOLD,
        MIN_TELEMETRY_FLUSH_DEADLINE, MIN_UNRESOLVED_AGE, ManifestError, MaxClockSkew,
        MetricCardinalityGuard, MetricConfigurationError, MetricDimensions, MetricFamily,
        MetricLabel, MetricObservation, MetricUnit, MonotonicClock, MonotonicInstant, NameKind,
        NodeId, OTHER_LABEL_VALUE, OperationId, OperatorAction, OperatorError, OperatorOutcome,
        OperatorOutcomeClass, OperatorRecord, OperatorRecordDraft, OperatorRejection,
        OperatorRequest, OperatorRequestId, OwnerObservation, OwnerToken, Page, PageRequest,
        PageSize, ParameterDescriptor, ParameterName, ParameterRole, ParameterValue,
        ParameterValueKind, PartitionAggregate, PartitionAggregationError, PartitionBudget,
        PartitionCount, PartitionFactoryError, PartitionKey, PartitionPlanEntry,
        PartitionPlanFactory, PartitionPlanRequest, PartitionResult, PartitionTaskletFactory,
        PartitionValueError, PartitionWorkerInput, PartitionedStepNode, PatternSpecificity,
        PlanError, ProcessContext, ProcessListener, ProcessOutcome, ProcessorError,
        PurgeBatchBound, PurgeCandidate, PurgeCounts, PurgePlan, PurgePlanRequest, PurgeSurvey,
        QueryWindow, ReadContext, ReadListener, ReadOutcome, ReaderError, ReasonCode,
        RecoveryDecision, RecoveryDecisionId, RecoveryDirective, RecoveryDisposition,
        RecoveryError, RecoveryEvidence, RecoveryField, RecoveryMarkers, RecoveryProposal,
        RecoveryProposer, RecoveryRepository, RecoveryRequest, RecoveryRequestError,
        RecoveryResult, RecoverySnapshot, RecoveryStepEvidence, RepositoryCapability,
        RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork, RequestDigest, RequestField,
        RequestFieldError, RestartabilityDeclaration, RetentionAction, RetentionActionId,
        RetentionError, RetentionHold, RetentionOutcome, RetentionRecord, RetentionRecordDraft,
        RetentionReport, RetentionService, RetryCounts, RetryKey, RetryLimit, RetryListener,
        RetryOrdinal, RetryOutcome, RetryReservation, RetryStateLimit, RollbackDisposition,
        SequentialIdGenerator, ShutdownCoordinator, ShutdownDeadline, ShutdownError,
        ShutdownHookError, ShutdownHookStatus, ShutdownReport, ShutdownRequest, ShutdownSignal,
        ShutdownTaskPhase, SkipCounts, SkipLimit, SkipListener, SplitBranch, SplitBudget,
        SplitNode, StaleThreshold, StartControls, StartLimit, StateCodecError,
        StateEnvelopeDescriptor, StateError, StateLimits, StateSchemaId, StateSchemaUpgrade,
        StateSchemaVersion, StateSensitivity, StepComponents, StepDefinitionUpgrade, StepExecution,
        StepExecutionId, StepExecutionListener, StepExecutionProjection, StepName, StepNode,
        StepPartition, StepPartitionId, StepPartitionProjection, StopPollInterval, StopSource,
        StopTiming, StopToken, StreamCloseContext, StreamCloseError, StreamCloseOutcome,
        StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamRuntimeOutcome,
        StreamStateContract, StreamUpdateContext, StreamUpdateError, SystemClock,
        SystemMonotonicClock, TELEMETRY_EVENT_CATALOG, TELEMETRY_SCHEMA_VERSION,
        TELEMETRY_SPAN_CATALOG, TaskJoinDeadline, Tasklet, TaskletContext, TaskletError,
        TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome, TaskletStep,
        TaskletStepFactory, TelemetryEventKind, TelemetryEventSink, TelemetryExportSink,
        TelemetryExporter, TelemetryFlushDeadline, TelemetryFlushStatus, TelemetryQueue,
        TelemetryRecord, TelemetrySpanKind, TelemetrySpanStatus, TerminalKind, TerminalStatusSet,
        TimeCompletionPolicy, UnjoinedPhase, VERSION, VersionedStateCodec, WriteContext,
        WriteListener, WriteOutcome, WriterError, aggregate_step_partitions,
    };

    #[cfg(feature = "postgres")]
    pub use oxide_batch::{
        CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
        PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError,
        PostgresDurableStepState, PostgresExplorer, PostgresFaultState, PostgresJobRepository,
        PostgresMigrator, TlsMode,
    };
}

#[test]
fn public_api_snapshot_is_unchanged_by_extraction() -> Result<(), Box<dyn Error>> {
    let rendered = render(&exports(&read(&facade_source())?));
    let snapshot = snapshot_path();

    if std::env::var_os(UPDATE_VARIABLE).is_some() {
        fs::write(&snapshot, &rendered)?;
        return Ok(());
    }

    let committed = read(&snapshot)?;
    if committed == rendered {
        return Ok(());
    }

    Err(Box::from(format!(
        "the facade surface changed:\n{}\nrerun with {UPDATE_VARIABLE}=1 only \
         for a reviewed API change",
        difference(&committed, &rendered)
    )))
}

#[test]
fn facade_import_paths_resolve_unchanged_after_each_stage() -> Result<(), Box<dyn Error>> {
    let declared = exports(&read(&facade_source())?);
    let imported = exports(&read(&this_source())?);

    if declared == imported {
        return Ok(());
    }

    Err(Box::from(format!(
        "the resolves module and the facade disagree:\n{}",
        difference(&render(&declared), &render(&imported))
    )))
}

/// Parses every exported name and feature gate from one Rust source file.
///
/// Recognizes `pub use path::{..}` blocks, single-item `pub use path::Item;`
/// statements, and `pub const NAME`. A `#[cfg(feature = "..")]` attribute on
/// the preceding line gates the statement that follows it.
fn exports(source: &str) -> BTreeSet<String> {
    let gate = format!("#[cfg(feature = \"{OPTIONAL_FEATURE}\")]");
    let mut found = BTreeSet::new();
    let mut lines = source.lines().map(str::trim).peekable();
    let mut gated = false;

    while let Some(line) = lines.next() {
        if line == gate {
            gated = true;
            continue;
        }

        if let Some(rest) = line.strip_prefix("pub use ") {
            let mut statement = rest.to_owned();
            while !statement.ends_with(';') {
                let Some(next) = lines.next() else {
                    break;
                };
                statement.push(' ');
                statement.push_str(next);
            }
            for name in names(&statement) {
                found.insert(entry(&name, gated));
            }
        } else if let Some(rest) = line.strip_prefix("pub const ") {
            if let Some(name) = rest.split(':').next() {
                found.insert(entry(name.trim(), gated));
            }
        } else if line.is_empty() || line.starts_with("//") {
            continue;
        }

        gated = false;
    }

    found
}

/// Extracts the exported names from one `pub use` statement body.
fn names(statement: &str) -> Vec<String> {
    let body = statement.trim_end_matches(';');

    let listed = match (body.find('{'), body.rfind('}')) {
        (Some(open), Some(close)) if open < close => &body[open + 1..close],
        _ => match body.rsplit("::").next() {
            Some(last) => last,
            None => body,
        },
    };

    listed
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| match name.rsplit(" as ").next() {
            Some(alias) => alias.trim().to_owned(),
            None => name.to_owned(),
        })
        .collect()
}

/// Renders one snapshot entry.
fn entry(name: &str, gated: bool) -> String {
    if gated {
        format!("oxide_batch::{name} [{OPTIONAL_FEATURE}]")
    } else {
        format!("oxide_batch::{name}")
    }
}

/// Renders a sorted, newline-terminated snapshot.
fn render(entries: &BTreeSet<String>) -> String {
    let mut rendered = String::new();
    for line in entries {
        rendered.push_str(line);
        rendered.push('\n');
    }
    rendered
}

/// Renders the added and removed lines between two snapshots.
fn difference(committed: &str, rendered: &str) -> String {
    let before: BTreeSet<&str> = committed.lines().collect();
    let after: BTreeSet<&str> = rendered.lines().collect();

    let mut report = String::new();
    for removed in before.difference(&after) {
        report.push_str("  - ");
        report.push_str(removed);
        report.push('\n');
    }
    for added in after.difference(&before) {
        report.push_str("  + ");
        report.push_str(added);
        report.push('\n');
    }
    report
}

/// Reads one repository file.
fn read(path: &Path) -> Result<String, Box<dyn Error>> {
    fs::read_to_string(path).map_err(|error| {
        Box::<dyn Error>::from(format!("could not read {}: {error}", path.display()))
    })
}

/// Locates the facade crate root.
fn facade_source() -> PathBuf {
    crate_directory().join("src").join("lib.rs")
}

/// Locates this test's own source.
fn this_source() -> PathBuf {
    crate_directory().join("tests").join("facade_surface.rs")
}

/// Locates the committed snapshot.
fn snapshot_path() -> PathBuf {
    crate_directory()
        .join("tests")
        .join("fixtures")
        .join("facade")
        .join("public-api.txt")
}

/// Locates the facade crate directory.
fn crate_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
