//! Internal implementation crate for `OxideBatch`.
//!
//! **This crate is implementation detail. Use
//! [`oxide-batch`](https://crates.io/crates/oxide-batch) instead.**
//!
//! It exists on crates.io only because the published `oxide-batch` facade
//! depends on it. Its API carries no stability promise: items may be added,
//! changed, or removed in any release, without a deprecation period. It has no
//! supported-configuration matrix, no compatibility ledger row, and no
//! independent release cadence.
//!
//! Everything here that `OxideBatch` supports is re-exported from `oxide-batch`
//! under a stable path.
//!
//! The crate holds the metadata repository, unit-of-work, clock, identifier,
//! explorer, operator, retention, and recovery ports, the durable partition,
//! flow-decision, audit, retention, and recovery values those ports exchange,
//! the bounded operator request envelope, and the keyset pagination vocabulary
//! the explorer port pages with. It depends on no async runtime, database
//! driver, command-line framework, telemetry SDK, broker client, or web
//! framework, and on no `OxideBatch` crate other than `oxide-batch-core`.
//!
//! Metadata adapters, the services that drive these ports, the plan compiler,
//! and the execution engines live above this crate.
//!
//! # Items marked `#[doc(hidden)]`
//!
//! Some items exist as `#[doc(hidden)] pub` only because the facade's own code
//! was split from these types by the extraction boundary: private access that
//! one crate resolved by module privacy now crosses a crate boundary. They are
//! not part of any surface, supported or otherwise, and the facade never
//! re-exports one under its own name. The staged crate-extraction contract
//! records each one.

#![forbid(unsafe_code)]

mod explorer;
mod flow;
mod operator;
mod partition;
mod recovery;
mod repository;
mod request;
mod retention;

pub use explorer::{
    Cursor, CursorError, CursorKey, DEFAULT_PAGE_SIZE, DefinitionDescriptor, ExplorerError,
    ExplorerQuery, ExplorerRepository, JobExecutionProjection, JobInstanceProjection,
    MAX_CURSOR_BYTES, MAX_PAGE_SIZE, MAX_RESPONSE_BYTES, MIN_UNRESOLVED_AGE, Page, PageRequest,
    PageSize, ParameterDescriptor, QueryWindow, StateEnvelopeDescriptor, StepExecutionProjection,
    StepPartitionProjection,
};
pub use explorer::{ExplorerRow, page, resume_window, start_window};
pub use flow::{
    FlowDecision, FlowDecisionId, FlowDecisionRequest, FlowDecisionSequence, FlowStepState,
    FlowTransitionKind,
};
pub use operator::{
    OperatorOutcomeClass, OperatorRecord, OperatorRecordDraft, OperatorRejection, OperatorRequest,
    RecoveryDirective,
};
pub use partition::PartitionMutationError;
pub use partition::{
    MAX_PARTITION_CONTEXT_BYTES, MAX_PARTITION_KEY_BYTES, PartitionAggregate,
    PartitionAggregationError, PartitionKey, PartitionPlanEntry, PartitionResult,
    PartitionValueError, StepPartition, aggregate_step_partitions,
};
pub use recovery::{
    DEFAULT_MAX_CLOCK_SKEW, DEFAULT_STALE_THRESHOLD, MAX_CLOCK_SKEW, MAX_STALE_THRESHOLD,
    MIN_CLOCK_SKEW, MIN_STALE_THRESHOLD, MaxClockSkew, MonotonicClock, MonotonicInstant,
    OwnerObservation, OwnerToken, RecoveryError, RecoveryEvidence, RecoveryMarkers,
    RecoveryProposal, RecoveryRepository, RecoverySnapshot, RecoveryStepEvidence, StaleThreshold,
    SystemMonotonicClock,
};
pub use repository::{
    BoxFuture, Clock, ExecutionControl, IdGenerationError, IdGenerator, JobInstanceSelection,
    JobRepository, RecoveryDecision, RecoveryDisposition, RecoveryField, RecoveryRequest,
    RecoveryRequestError, RecoveryResult, RepositoryCapability, RepositoryError,
    RepositoryUnitOfWork, SequentialIdGenerator, SystemClock,
};
pub use repository::{aggregate_partition_parent, map_partition_aggregation, recovered_execution};
pub use request::hex_digest;
pub use request::{
    ActorRef, AuthorizationClass, MAX_ACTOR_REF_BYTES, MAX_OPERATION_ID_BYTES,
    MAX_REASON_CODE_BYTES, OperationId, OperatorAction, ReasonCode, RequestDigest, RequestField,
    RequestFieldError,
};
pub(crate) use request::{CanonicalWriter, RequestArguments, request_digest};
pub use retention::{
    DEFAULT_PURGE_AGE, MAX_PURGE_BATCH, MIN_PURGE_AGE, PurgeBatchBound, PurgeCandidate,
    PurgeCounts, PurgePlan, PurgePlanRequest, PurgeSurvey, RetentionAction, RetentionError,
    RetentionHold, RetentionOutcome, RetentionRecord, RetentionRecordDraft, TerminalStatusSet,
};
