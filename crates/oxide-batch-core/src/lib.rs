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
//! The crate holds domain identities, typed job parameters, statuses and exit
//! statuses, execution records and their lifecycle rules, bounded versioned
//! execution-context and checkpoint state, chunk sizing and counting values,
//! and restart-relevant definition identity with its canonical manifest
//! encoding. It depends on no async runtime, database driver, command-line
//! framework, telemetry SDK, broker client, or web framework, and on no other
//! `OxideBatch` crate.

#![forbid(unsafe_code)]

mod chunk;
mod definition;
mod domain;
mod fault;
mod flow;
mod state;

pub use chunk::{ChunkCount, ChunkCounts, ChunkError, ChunkProgress, ChunkSize};
pub use definition::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ClassifierRevision,
    ComponentRevision, DefinitionError, DefinitionIdentity, DefinitionManifest, DefinitionRevision,
    DefinitionTokenKind, DefinitionUpgrade, DefinitionUpgradeKey, InFlightPolicy, ManifestError,
    StepDefinitionUpgrade,
};
pub use definition::{
    MANIFEST_FORMAT_FLOW, MANIFEST_FORMAT_LOCAL_SCALE, MANIFEST_FORMAT_ONE_STEP, MAX_NODES,
    MAX_TRANSITIONS, check_manifest_format, validate_token,
};
pub use domain::{
    BatchStatus, DomainError, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps,
    ExecutionVersion, ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary,
    IdentifierKind, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey,
    JobName, JobParameter, JobParameters, LifecycleError, LifecycleTransition, NameKind,
    OperatorRequestId, ParameterName, ParameterRole, ParameterValue, ParameterValueKind,
    RecoveryDecisionId, RetentionActionId, StepExecution, StepExecutionId, StepName,
    StepPartitionId,
};
pub use fault::{
    BackoffKind, BackoffPolicy, FaultAction, FaultClassifier, FaultDecision, FaultDescriptor,
    FaultEvidence, FaultPhase, FaultPolicy, FaultPolicyError, FaultRule, RetryLimit, RetryOrdinal,
    RetryStateLimit, RollbackDisposition, SkipCounts, SkipLimit,
};
pub use flow::{FlowTarget, MAX_PARTITIONS, NodeId, StartControls, StartLimit, TerminalKind};
pub use state::{
    Checkpoint, DurableStateKind, ExecutionContext, StateCodecError, StateError, StateLimits,
    StateSchemaId, StateSchemaUpgrade, StateSchemaVersion, VersionedStateCodec,
};
