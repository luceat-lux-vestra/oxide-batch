//! Instance holds and the guarded two-phase purge of terminal history.
//!
//! This is the initial retention slice. It provides holds and a bounded,
//! target-guarded purge. Archive packages, export or import, checksum
//! verification of exported data, scheduled or automatic purge, retention
//! policy storage, cross-adapter portability, and partial-row redaction are
//! not part of it.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::{ActorRef, CanonicalWriter, OperationId, ReasonCode, hex_digest};
use crate::{
    BatchStatus, Clock, ExecutionVersion, JobExecutionId, JobInstanceId, JobName, JobRepository,
    RepositoryError, RetentionActionId, TelemetryEventKind, TelemetryEventSink, TelemetryRecord,
};

/// Maximum executions one purge batch may target.
pub const MAX_PURGE_BATCH: u32 = 1000;
/// Smallest accepted minimum age of a purge candidate.
pub const MIN_PURGE_AGE: Duration = Duration::from_hours(1);
/// Minimum age used when a caller does not choose one.
pub const DEFAULT_PURGE_AGE: Duration = Duration::from_hours(30 * 24);

/// A validated purge batch bound in `1..=1000`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PurgeBatchBound(u32);

impl PurgeBatchBound {
    /// Validates a caller-supplied batch bound.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::BatchBoundOutOfRange`] outside `1..=1000`.
    pub const fn new(value: u32) -> Result<Self, RetentionError> {
        if value == 0 || value > MAX_PURGE_BATCH {
            return Err(RetentionError::BatchBoundOutOfRange { requested: value });
        }
        Ok(Self(value))
    }

    /// Returns the validated bound.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for PurgeBatchBound {
    fn default() -> Self {
        Self(MAX_PURGE_BATCH)
    }
}

/// A non-empty set of terminal statuses a purge may target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalStatusSet(BTreeSet<BatchStatus>);

impl TerminalStatusSet {
    /// Validates a non-empty set of finished statuses.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::NonTerminalStatus`] for an active status and
    /// [`RetentionError::EmptyStatusSet`] for an empty set.
    pub fn new(statuses: impl IntoIterator<Item = BatchStatus>) -> Result<Self, RetentionError> {
        let mut set = BTreeSet::new();
        for status in statuses {
            if !status.is_finished() {
                return Err(RetentionError::NonTerminalStatus { status });
            }
            set.insert(status);
        }
        if set.is_empty() {
            return Err(RetentionError::EmptyStatusSet);
        }
        Ok(Self(set))
    }

    /// Returns every finished status.
    #[must_use]
    pub fn all() -> Self {
        Self(
            [
                BatchStatus::Completed,
                BatchStatus::Failed,
                BatchStatus::Stopped,
                BatchStatus::Abandoned,
            ]
            .into_iter()
            .collect(),
        )
    }

    /// Returns whether the set targets `status`.
    #[must_use]
    pub fn contains(&self, status: BatchStatus) -> bool {
        self.0.contains(&status)
    }

    /// Iterates the targeted statuses in stable order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = BatchStatus> + '_ {
        self.0.iter().copied()
    }
}

/// One bounded purge planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgePlanRequest {
    job_name: JobName,
    statuses: TerminalStatusSet,
    minimum_age: Duration,
    batch: PurgeBatchBound,
}

impl PurgePlanRequest {
    /// Validates one bounded purge planning request.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::AgeBoundTooSmall`] below [`MIN_PURGE_AGE`].
    pub fn new(
        job_name: JobName,
        statuses: TerminalStatusSet,
        minimum_age: Duration,
        batch: PurgeBatchBound,
    ) -> Result<Self, RetentionError> {
        if minimum_age.as_secs() < MIN_PURGE_AGE.as_secs() {
            return Err(RetentionError::AgeBoundTooSmall {
                minimum: MIN_PURGE_AGE,
            });
        }
        Ok(Self {
            job_name,
            statuses,
            minimum_age,
            batch,
        })
    }

    /// Borrows the targeted job name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Borrows the targeted terminal statuses.
    #[must_use]
    pub const fn statuses(&self) -> &TerminalStatusSet {
        &self.statuses
    }

    /// Returns the minimum durable age of a candidate.
    #[must_use]
    pub const fn minimum_age(&self) -> Duration {
        self.minimum_age
    }

    /// Returns the batch bound.
    #[must_use]
    pub const fn batch(&self) -> PurgeBatchBound {
        self.batch
    }
}

/// One purge candidate and the version observed while planning.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PurgeCandidate {
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    version: ExecutionVersion,
}

impl PurgeCandidate {
    /// Records one observed candidate.
    #[must_use]
    pub const fn new(
        job_instance_id: JobInstanceId,
        job_execution_id: JobExecutionId,
        version: ExecutionVersion,
    ) -> Self {
        Self {
            job_instance_id,
            job_execution_id,
            version,
        }
    }

    /// Returns the owning logical instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Returns the candidate execution.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the version observed while planning.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.version
    }
}

/// Per-table row counts of one purge plan or applied batch.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PurgeCounts {
    flow_decisions: u64,
    recovery_decisions: u64,
    operator_requests: u64,
    step_partitions: u64,
    step_executions: u64,
    job_executions: u64,
    job_instances: u64,
}

impl PurgeCounts {
    /// Records per-table counts in deletion order.
    #[must_use]
    pub const fn new(
        flow_decisions: u64,
        recovery_decisions: u64,
        operator_requests: u64,
        step_partitions: u64,
        step_executions: u64,
        job_executions: u64,
        job_instances: u64,
    ) -> Self {
        Self {
            flow_decisions,
            recovery_decisions,
            operator_requests,
            step_partitions,
            step_executions,
            job_executions,
            job_instances,
        }
    }

    /// Returns the flow-decision count.
    #[must_use]
    pub const fn flow_decisions(self) -> u64 {
        self.flow_decisions
    }

    /// Returns the recovery-decision count.
    #[must_use]
    pub const fn recovery_decisions(self) -> u64 {
        self.recovery_decisions
    }

    /// Returns the operator-request count.
    #[must_use]
    pub const fn operator_requests(self) -> u64 {
        self.operator_requests
    }

    /// Returns the step-partition count.
    #[must_use]
    pub const fn step_partitions(self) -> u64 {
        self.step_partitions
    }

    /// Returns the step-execution count.
    #[must_use]
    pub const fn step_executions(self) -> u64 {
        self.step_executions
    }

    /// Returns the job-execution count.
    #[must_use]
    pub const fn job_executions(self) -> u64 {
        self.job_executions
    }

    /// Returns the job-instance count.
    #[must_use]
    pub const fn job_instances(self) -> u64 {
        self.job_instances
    }
}

/// The bounded candidate survey one adapter produces while planning.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PurgeSurvey {
    candidates: Vec<PurgeCandidate>,
    counts: PurgeCounts,
}

impl PurgeSurvey {
    /// Records the observed candidates and their per-table counts.
    #[must_use]
    pub const fn new(candidates: Vec<PurgeCandidate>, counts: PurgeCounts) -> Self {
        Self { candidates, counts }
    }

    /// Borrows the observed candidates in identity order.
    #[must_use]
    pub fn candidates(&self) -> &[PurgeCandidate] {
        &self.candidates
    }

    /// Returns the per-table counts.
    #[must_use]
    pub const fn counts(&self) -> PurgeCounts {
        self.counts
    }
}

/// One bounded, digest-guarded purge plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgePlan {
    request: PurgePlanRequest,
    candidates: Vec<PurgeCandidate>,
    counts: PurgeCounts,
    digest: [u8; 32],
}

impl PurgePlan {
    fn new(request: PurgePlanRequest, survey: PurgeSurvey) -> Self {
        let digest = plan_digest(&request, survey.candidates());
        Self {
            request,
            candidates: survey.candidates,
            counts: survey.counts,
            digest,
        }
    }

    /// Borrows the planning request.
    #[must_use]
    pub const fn request(&self) -> &PurgePlanRequest {
        &self.request
    }

    /// Borrows the bounded candidate identities in identity order.
    #[must_use]
    pub fn candidates(&self) -> &[PurgeCandidate] {
        &self.candidates
    }

    /// Returns the per-table row counts observed while planning.
    #[must_use]
    pub const fn counts(&self) -> PurgeCounts {
        self.counts
    }

    /// Returns the digest computed over the candidates and their versions.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Returns the hexadecimal plan digest.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        hex_digest(&self.digest)
    }

    /// Returns whether the plan targets no candidate.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }
}

fn plan_digest(request: &PurgePlanRequest, candidates: &[PurgeCandidate]) -> [u8; 32] {
    let mut writer = CanonicalWriter::new("oxide-batch.retention-plan.v1");
    writer.push_str(request.job_name().as_str());
    for status in request.statuses().iter() {
        writer.push_str(status.as_str());
    }
    writer.push_u64(request.minimum_age().as_secs());
    writer.push_u64(u64::from(request.batch().get()));
    for candidate in candidates {
        writer.push_u64(candidate.job_instance_id().get());
        writer.push_u64(candidate.job_execution_id().get());
        writer.push_u64(candidate.version().get());
    }
    writer.digest()
}

/// One audited retention action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RetentionAction {
    /// Place one hold on a logical instance.
    Hold,
    /// Release the hold on a logical instance.
    ReleaseHold,
    /// Apply one bounded purge batch.
    ApplyPurge,
}

impl RetentionAction {
    /// Returns the stable durable code for this action.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hold => "HOLD",
            Self::ReleaseHold => "RELEASE_HOLD",
            Self::ApplyPurge => "APPLY_PURGE",
        }
    }
}

impl fmt::Display for RetentionAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One active retention hold on a logical instance.
///
/// A hold protects history from purge. It does not block launch, restart, or
/// any other lifecycle action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionHold {
    job_instance_id: JobInstanceId,
    actor: ActorRef,
    reason: ReasonCode,
    placed_at: SystemTime,
}

impl RetentionHold {
    /// Records one placed hold.
    #[must_use]
    pub const fn new(
        job_instance_id: JobInstanceId,
        actor: ActorRef,
        reason: ReasonCode,
        placed_at: SystemTime,
    ) -> Self {
        Self {
            job_instance_id,
            actor,
            reason,
            placed_at,
        }
    }

    /// Returns the held logical instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Borrows the opaque actor reference.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef {
        &self.actor
    }

    /// Borrows the closed-set reason code.
    #[must_use]
    pub const fn reason(&self) -> &ReasonCode {
        &self.reason
    }

    /// Returns the facade-clock instant the hold was placed.
    #[must_use]
    pub const fn placed_at(&self) -> SystemTime {
        self.placed_at
    }
}

/// The durable class of one recorded retention action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RetentionOutcome {
    /// The action was guarded, applied, and audited.
    Applied,
    /// A durable record for this operation identifier already existed.
    Replayed,
    /// A guard rejected the action; nothing was deleted or changed.
    Rejected,
}

impl RetentionOutcome {
    /// Returns the stable durable code for this class.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::Replayed => "REPLAYED",
            Self::Rejected => "REJECTED",
        }
    }
}

/// One append-only retention audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionRecord {
    id: RetentionActionId,
    action: RetentionAction,
    operation_id: OperationId,
    actor: ActorRef,
    reason: ReasonCode,
    job_instance_id: Option<JobInstanceId>,
    plan_digest: Option<[u8; 32]>,
    counts: PurgeCounts,
    batch_bound: Option<PurgeBatchBound>,
    outcome: RetentionOutcome,
    applied_at: SystemTime,
}

impl RetentionRecord {
    /// Rebuilds a record read from a durable adapter.
    #[must_use]
    pub fn from_parts(id: RetentionActionId, draft: RetentionRecordDraft) -> Self {
        Self {
            id,
            action: draft.action,
            operation_id: draft.operation_id,
            actor: draft.actor,
            reason: draft.reason,
            job_instance_id: draft.job_instance_id,
            plan_digest: draft.plan_digest,
            counts: draft.counts,
            batch_bound: draft.batch_bound,
            outcome: draft.outcome,
            applied_at: draft.applied_at,
        }
    }

    /// Returns the opaque record identifier.
    #[must_use]
    pub const fn id(&self) -> RetentionActionId {
        self.id
    }

    /// Returns the audited action.
    #[must_use]
    pub const fn action(&self) -> RetentionAction {
        self.action
    }

    /// Borrows the idempotency key.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    /// Borrows the opaque actor reference.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef {
        &self.actor
    }

    /// Borrows the closed-set reason code.
    #[must_use]
    pub const fn reason(&self) -> &ReasonCode {
        &self.reason
    }

    /// Returns the held instance, when the action targeted one.
    #[must_use]
    pub const fn job_instance_id(&self) -> Option<JobInstanceId> {
        self.job_instance_id
    }

    /// Returns the applied plan digest, when the action was a purge.
    #[must_use]
    pub const fn plan_digest(&self) -> Option<&[u8; 32]> {
        self.plan_digest.as_ref()
    }

    /// Returns the per-table deleted counts.
    #[must_use]
    pub const fn counts(&self) -> PurgeCounts {
        self.counts
    }

    /// Returns the batch bound, when the action was a purge.
    #[must_use]
    pub const fn batch_bound(&self) -> Option<PurgeBatchBound> {
        self.batch_bound
    }

    /// Returns the recorded outcome class.
    #[must_use]
    pub const fn outcome(&self) -> RetentionOutcome {
        self.outcome
    }

    /// Returns the facade-clock instant of the audited action.
    #[must_use]
    pub const fn applied_at(&self) -> SystemTime {
        self.applied_at
    }
}

/// The bounded retention audit row an adapter appends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionRecordDraft {
    action: RetentionAction,
    operation_id: OperationId,
    actor: ActorRef,
    reason: ReasonCode,
    job_instance_id: Option<JobInstanceId>,
    plan_digest: Option<[u8; 32]>,
    counts: PurgeCounts,
    batch_bound: Option<PurgeBatchBound>,
    outcome: RetentionOutcome,
    applied_at: SystemTime,
}

impl RetentionRecordDraft {
    /// Drafts the audit row for one applied instance-scoped action.
    ///
    /// A hold or hold release names an instance and deletes nothing, so the
    /// row carries no plan digest, no batch bound, and default counts.
    #[must_use]
    pub fn instance_action(
        action: RetentionAction,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_instance_id: JobInstanceId,
        applied_at: SystemTime,
    ) -> Self {
        Self {
            action,
            operation_id,
            actor,
            reason,
            job_instance_id: Some(job_instance_id),
            plan_digest: None,
            counts: PurgeCounts::default(),
            batch_bound: None,
            outcome: RetentionOutcome::Applied,
            applied_at,
        }
    }

    /// Drafts the audit row for one applied purge batch.
    ///
    /// The row is bound to the plan digest the batch was applied under and to
    /// the bound that limited it, so a replay can tell which plan produced the
    /// recorded counts.
    #[must_use]
    pub const fn purge(
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        plan_digest: [u8; 32],
        counts: PurgeCounts,
        batch_bound: PurgeBatchBound,
        applied_at: SystemTime,
    ) -> Self {
        Self {
            action: RetentionAction::ApplyPurge,
            operation_id,
            actor,
            reason,
            job_instance_id: None,
            plan_digest: Some(plan_digest),
            counts,
            batch_bound: Some(batch_bound),
            outcome: RetentionOutcome::Applied,
            applied_at,
        }
    }

    /// Rebuilds a draft from one durable audit row.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn from_durable(
        action: RetentionAction,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_instance_id: Option<JobInstanceId>,
        plan_digest: Option<[u8; 32]>,
        counts: PurgeCounts,
        batch_bound: Option<PurgeBatchBound>,
        outcome: RetentionOutcome,
        applied_at: SystemTime,
    ) -> Self {
        Self {
            action,
            operation_id,
            actor,
            reason,
            job_instance_id,
            plan_digest,
            counts,
            batch_bound,
            outcome,
            applied_at,
        }
    }

    /// Returns the audited action.
    #[must_use]
    pub const fn action(&self) -> RetentionAction {
        self.action
    }

    /// Borrows the idempotency key.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    /// Borrows the opaque actor reference.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef {
        &self.actor
    }

    /// Borrows the closed-set reason code.
    #[must_use]
    pub const fn reason(&self) -> &ReasonCode {
        &self.reason
    }

    /// Returns the held instance, when the action targets one.
    #[must_use]
    pub const fn job_instance_id(&self) -> Option<JobInstanceId> {
        self.job_instance_id
    }

    /// Returns the applied plan digest, when the action is a purge.
    #[must_use]
    pub const fn plan_digest(&self) -> Option<&[u8; 32]> {
        self.plan_digest.as_ref()
    }

    /// Returns the per-table deleted counts.
    #[must_use]
    pub const fn counts(&self) -> PurgeCounts {
        self.counts
    }

    /// Returns the batch bound, when the action is a purge.
    #[must_use]
    pub const fn batch_bound(&self) -> Option<PurgeBatchBound> {
        self.batch_bound
    }

    /// Returns the recorded outcome class.
    #[must_use]
    pub const fn outcome(&self) -> RetentionOutcome {
        self.outcome
    }

    /// Returns the facade-clock instant of the audited action.
    #[must_use]
    pub const fn applied_at(&self) -> SystemTime {
        self.applied_at
    }
}

/// The result of one audited retention call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionReport {
    outcome: RetentionOutcome,
    record: RetentionRecord,
    hold: Option<RetentionHold>,
}

impl RetentionReport {
    const fn new(
        outcome: RetentionOutcome,
        record: RetentionRecord,
        hold: Option<RetentionHold>,
    ) -> Self {
        Self {
            outcome,
            record,
            hold,
        }
    }

    /// Returns whether the action was applied, replayed, or rejected.
    #[must_use]
    pub const fn outcome(&self) -> RetentionOutcome {
        self.outcome
    }

    /// Borrows the durable audit record.
    #[must_use]
    pub const fn record(&self) -> &RetentionRecord {
        &self.record
    }

    /// Borrows the hold this call placed, when it placed one.
    #[must_use]
    pub const fn hold(&self) -> Option<&RetentionHold> {
        self.hold.as_ref()
    }

    /// Returns the per-table deleted counts.
    #[must_use]
    pub const fn counts(&self) -> PurgeCounts {
        self.record.counts()
    }
}

/// A typed retention failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetentionError {
    /// The requested batch bound is outside `1..=1000`.
    BatchBoundOutOfRange {
        /// Rejected bound.
        requested: u32,
    },
    /// The requested minimum age is below [`MIN_PURGE_AGE`].
    AgeBoundTooSmall {
        /// Smallest accepted age.
        minimum: Duration,
    },
    /// A purge may target only finished statuses.
    NonTerminalStatus {
        /// Rejected status.
        status: BatchStatus,
    },
    /// A purge must target at least one status.
    EmptyStatusSet,
    /// A candidate changed after the plan was produced; nothing was deleted.
    RetentionPlanStale,
    /// The instance is held, so it can be neither planned nor purged.
    InstanceHeld {
        /// Held logical instance.
        job_instance_id: JobInstanceId,
    },
    /// The operation identifier was reused with a different request.
    OperationIdConflict {
        /// Conflicting action.
        action: RetentionAction,
        /// Conflicting idempotency key.
        operation_id: OperationId,
    },
    /// The commit may or may not have become durable.
    OperationOutcomeUnknown,
    /// The repository failed.
    Repository(RepositoryError),
}

impl fmt::Display for RetentionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BatchBoundOutOfRange { requested } => write!(
                formatter,
                "purge batch bound {requested} is outside 1..={MAX_PURGE_BATCH}"
            ),
            Self::AgeBoundTooSmall { minimum } => write!(
                formatter,
                "the minimum age must be at least {} seconds",
                minimum.as_secs()
            ),
            Self::NonTerminalStatus { status } => {
                write!(formatter, "{status} is not a finished status")
            }
            Self::EmptyStatusSet => formatter.write_str("a purge must target at least one status"),
            Self::RetentionPlanStale => {
                formatter.write_str("the purge plan is stale and nothing was deleted")
            }
            Self::InstanceHeld { job_instance_id } => {
                write!(formatter, "job instance {job_instance_id} is held")
            }
            Self::OperationIdConflict {
                action,
                operation_id,
            } => write!(
                formatter,
                "operation identifier {operation_id} was already recorded for {action} with a different request"
            ),
            Self::OperationOutcomeUnknown => {
                formatter.write_str("the retention commit outcome is unknown")
            }
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for RetentionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RepositoryError> for RetentionError {
    fn from(value: RepositoryError) -> Self {
        match value {
            RepositoryError::CommitOutcomeUnknown => Self::OperationOutcomeUnknown,
            RepositoryError::RetentionPlanStale => Self::RetentionPlanStale,
            other => Self::Repository(other),
        }
    }
}

/// The portable retention service.
///
/// Purge requires the operator-writer role and its narrowly granted deletes.
/// The runtime role cannot purge, and the operator-reader role can plan but
/// not apply. Those privileges are enforced by the durable adapter's
/// deployment configuration, not by this type.
#[derive(Clone)]
pub struct RetentionService<R> {
    repository: R,
    clock: Arc<dyn Clock>,
    event_sinks: Vec<Arc<dyn TelemetryEventSink>>,
}

impl<R> fmt::Debug for RetentionService<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetentionService")
            .finish_non_exhaustive()
    }
}

impl<R: JobRepository> RetentionService<R> {
    /// Binds one repository and one injected facade clock.
    pub const fn new(repository: R, clock: Arc<dyn Clock>) -> Self {
        Self {
            repository,
            clock,
            event_sinks: Vec::new(),
        }
    }

    /// Attaches a non-authoritative, panic-isolated telemetry sink.
    #[must_use]
    pub fn with_event_sink(mut self, sink: Arc<dyn TelemetryEventSink>) -> Self {
        self.event_sinks.push(sink);
        self
    }

    /// Borrows the underlying repository.
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Reads the active hold of one logical instance.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::Repository`] when the read fails.
    pub async fn hold(
        &self,
        job_instance_id: JobInstanceId,
    ) -> Result<Option<RetentionHold>, RetentionError> {
        let mut unit = self.repository.begin().await?;
        let hold = unit.job_instance_hold(job_instance_id).await?;
        unit.rollback().await?;
        Ok(hold)
    }

    /// Places one audited hold on a logical instance.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::OperationIdConflict`] for a reused identifier
    /// and [`RetentionError::Repository`] for an infrastructure failure.
    pub async fn place_hold(
        &self,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_instance_id: JobInstanceId,
    ) -> Result<RetentionReport, RetentionError> {
        let result = self
            .hold_action(
                RetentionAction::Hold,
                operation_id,
                actor,
                reason,
                job_instance_id,
            )
            .await;
        if let Ok(report) = &result {
            self.emit_report(report);
        }
        result
    }

    /// Releases the hold on a logical instance.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::OperationIdConflict`] for a reused identifier
    /// and [`RetentionError::Repository`] for an infrastructure failure.
    pub async fn release_hold(
        &self,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_instance_id: JobInstanceId,
    ) -> Result<RetentionReport, RetentionError> {
        let result = self
            .hold_action(
                RetentionAction::ReleaseHold,
                operation_id,
                actor,
                reason,
                job_instance_id,
            )
            .await;
        if let Ok(report) = &result {
            self.emit_report(report);
        }
        result
    }

    async fn hold_action(
        &self,
        action: RetentionAction,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_instance_id: JobInstanceId,
    ) -> Result<RetentionReport, RetentionError> {
        if let Some(record) = self.replay(action, &operation_id).await? {
            return Ok(RetentionReport::new(
                RetentionOutcome::Replayed,
                record,
                None,
            ));
        }
        let applied_at = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let hold = match action {
            RetentionAction::Hold => Some(
                unit.place_instance_hold(job_instance_id, &actor, &reason, applied_at)
                    .await?,
            ),
            RetentionAction::ReleaseHold => {
                unit.release_instance_hold(job_instance_id).await?;
                None
            }
            RetentionAction::ApplyPurge => None,
        };
        let draft = RetentionRecordDraft::instance_action(
            action,
            operation_id,
            actor,
            reason,
            job_instance_id,
            applied_at,
        );
        let record = unit.append_retention_action(&draft).await?;
        unit.commit().await?;
        Ok(RetentionReport::new(
            RetentionOutcome::Applied,
            record,
            hold,
        ))
    }

    /// Produces one bounded, digest-guarded purge plan.
    ///
    /// Planning is a read-only action of the [`AuthorizationClass::Read`]
    /// class. It deletes nothing.
    ///
    /// [`AuthorizationClass::Read`]: super::AuthorizationClass::Read
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::Repository`] when the survey fails.
    pub async fn plan_purge(
        &self,
        request: &PurgePlanRequest,
    ) -> Result<PurgePlan, RetentionError> {
        let mut unit = self.repository.begin().await?;
        let survey = unit.purge_survey(request).await?;
        unit.rollback().await?;
        let plan = PurgePlan::new(request.clone(), survey);
        self.emit_record(&TelemetryRecord::retention(
            TelemetryEventKind::RetentionPlanned,
            None,
            None,
            PurgeCounts::default(),
        ));
        Ok(plan)
    }

    /// Applies one bounded purge batch under its plan digest.
    ///
    /// Application re-validates eligibility and observed versions inside one
    /// transaction. Any candidate that changed produces
    /// [`RetentionError::RetentionPlanStale`] and deletes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::RetentionPlanStale`] for a changed candidate,
    /// [`RetentionError::OperationIdConflict`] for a reused identifier,
    /// [`RetentionError::OperationOutcomeUnknown`] for an ambiguous commit, and
    /// [`RetentionError::Repository`] for an infrastructure failure.
    pub async fn apply_purge(
        &self,
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        plan: &PurgePlan,
    ) -> Result<RetentionReport, RetentionError> {
        if let Some(record) = self
            .replay(RetentionAction::ApplyPurge, &operation_id)
            .await?
        {
            let report = RetentionReport::new(RetentionOutcome::Replayed, record, None);
            self.emit_report(&report);
            return Ok(report);
        }
        let applied_at = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let counts = unit.apply_purge(plan).await?;
        let draft = RetentionRecordDraft::purge(
            operation_id,
            actor,
            reason,
            *plan.digest(),
            counts,
            plan.request().batch(),
            applied_at,
        );
        let record = unit.append_retention_action(&draft).await?;
        unit.commit().await?;
        let report = RetentionReport::new(RetentionOutcome::Applied, record, None);
        self.emit_report(&report);
        Ok(report)
    }

    async fn replay(
        &self,
        action: RetentionAction,
        operation_id: &OperationId,
    ) -> Result<Option<RetentionRecord>, RetentionError> {
        let mut unit = self.repository.begin().await?;
        let recorded = unit.find_retention_action(action, operation_id).await?;
        unit.rollback().await?;
        Ok(recorded)
    }

    fn emit_report(&self, report: &RetentionReport) {
        self.emit_record(&TelemetryRecord::retention(
            TelemetryEventKind::RetentionApplied,
            Some(report.record().action()),
            Some(report.outcome()),
            report.counts(),
        ));
    }

    fn emit_record(&self, record: &TelemetryRecord) {
        for sink in &self.event_sinks {
            crate::telemetry::emit_safely(Some(sink), record);
        }
    }
}
