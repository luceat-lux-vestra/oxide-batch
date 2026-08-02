//! Guarded, idempotent, audited operator actions.
//!
//! Every mutating action carries a bounded envelope, commits its append-only
//! audit row in the same transaction as its effect, and is replayable by its
//! operation identifier. The service performs no internal retry loop and never
//! guesses an ambiguous commit outcome.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use super::{
    ActorRef, AuthorizationClass, OperationId, OperatorAction, ReasonCode, RequestArguments,
    RequestDigest, request_digest,
};
use crate::{
    BatchStatus, Clock, DefinitionIdentity, ExecutionVersion, FailureSummary, JobExecution,
    JobExecutionId, JobInstanceId, JobInstanceKey, JobRepository, LifecycleError,
    LifecycleTransition, OperatorRequestId, RecoveryDisposition, RecoveryProposal, RecoveryRequest,
    RecoveryRequestError, RepositoryError, RepositoryUnitOfWork,
};

/// One validated mutating operator request.
///
/// The request digest covers the action, target identity, expected version,
/// and bounded arguments. Replaying an operation identifier with a different
/// digest is a conflict rather than a repeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorRequest {
    action: OperatorAction,
    operation_id: OperationId,
    actor: ActorRef,
    reason: Option<ReasonCode>,
    target: OperatorTarget,
    expected_version: Option<ExecutionVersion>,
    arguments: RequestArguments,
    digest: RequestDigest,
}

/// The disposition of one recovery decision together with the evidence that
/// disposition requires.
///
/// Pairing the two makes a `MarkFailed` decision without its stated failure
/// unrepresentable rather than a deferred validation error, and keeps an
/// `Abandon` decision from carrying a failure that its durable outcome ignores
/// but its request digest would still cover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryDirective {
    /// Make the observed attempt restart-eligible under a stated failure.
    MarkFailed(FailureSummary),
    /// Make the logical instance permanently non-restartable.
    Abandon,
}

impl RecoveryDirective {
    /// Returns the durable disposition this directive requests.
    #[must_use]
    pub const fn disposition(self) -> RecoveryDisposition {
        match self {
            Self::MarkFailed(_) => RecoveryDisposition::MarkFailed,
            Self::Abandon => RecoveryDisposition::Abandon,
        }
    }

    /// Returns the stated failure of a `MarkFailed` directive.
    #[must_use]
    pub const fn failure(self) -> Option<FailureSummary> {
        match self {
            Self::MarkFailed(failure) => Some(failure),
            Self::Abandon => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperatorTarget {
    InstanceKey(Box<JobInstanceKey>),
    Instance(JobInstanceId),
    Execution(JobExecutionId),
}

impl OperatorTarget {
    fn identity(&self) -> String {
        match self {
            Self::InstanceKey(key) => {
                format!("instance-key:{}", super::hex_digest(&key.digest()))
            }
            Self::Instance(id) => format!("instance:{id}"),
            Self::Execution(id) => format!("execution:{id}"),
        }
    }
}

impl OperatorRequest {
    /// Requests one launch of the instance selected by an identifying key.
    #[must_use]
    pub fn launch(
        operation_id: OperationId,
        actor: ActorRef,
        key: JobInstanceKey,
        definition: DefinitionIdentity,
    ) -> Self {
        Self::build(
            OperatorAction::Launch,
            operation_id,
            actor,
            None,
            OperatorTarget::InstanceKey(Box::new(key)),
            None,
            RequestArguments::Definition(Box::new(definition)),
        )
    }

    /// Requests one restart attempt for an existing logical instance.
    #[must_use]
    pub fn restart(
        operation_id: OperationId,
        actor: ActorRef,
        job_instance_id: JobInstanceId,
        definition: DefinitionIdentity,
    ) -> Self {
        Self::build(
            OperatorAction::Restart,
            operation_id,
            actor,
            None,
            OperatorTarget::Instance(job_instance_id),
            None,
            RequestArguments::Definition(Box::new(definition)),
        )
    }

    /// Requests one durable cooperative stop.
    #[must_use]
    pub fn stop(
        operation_id: OperationId,
        actor: ActorRef,
        job_execution_id: JobExecutionId,
        expected_version: ExecutionVersion,
    ) -> Self {
        Self::build(
            OperatorAction::Stop,
            operation_id,
            actor,
            None,
            OperatorTarget::Execution(job_execution_id),
            Some(expected_version),
            RequestArguments::None,
        )
    }

    /// Requests that one finished or recovered execution become `ABANDONED`.
    #[must_use]
    pub fn abandon(
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        job_execution_id: JobExecutionId,
        expected_version: ExecutionVersion,
    ) -> Self {
        Self::build(
            OperatorAction::Abandon,
            operation_id,
            actor,
            Some(reason),
            OperatorTarget::Execution(job_execution_id),
            Some(expected_version),
            RequestArguments::None,
        )
    }

    /// Requests one evidence-bound recovery decision.
    #[must_use]
    pub fn recover(
        operation_id: OperationId,
        actor: ActorRef,
        reason: ReasonCode,
        directive: RecoveryDirective,
        proposal: &RecoveryProposal,
    ) -> Self {
        let job_execution_id = proposal.evidence().execution_id();
        let expected_version = proposal.observed_version();
        let evidence_digest = *proposal.digest();
        Self::build(
            OperatorAction::Recover,
            operation_id,
            actor,
            Some(reason),
            OperatorTarget::Execution(job_execution_id),
            Some(expected_version),
            RequestArguments::Recovery {
                directive,
                evidence_digest,
                unknown_commit: proposal.evidence().unknown_commit(),
            },
        )
    }

    fn build(
        action: OperatorAction,
        operation_id: OperationId,
        actor: ActorRef,
        reason: Option<ReasonCode>,
        target: OperatorTarget,
        expected_version: Option<ExecutionVersion>,
        arguments: RequestArguments,
    ) -> Self {
        let digest = request_digest(
            action,
            &target.identity(),
            expected_version,
            reason.as_ref(),
            &arguments,
        );
        Self {
            action,
            operation_id,
            actor,
            reason,
            target,
            expected_version,
            arguments,
            digest,
        }
    }

    /// Returns the requested action.
    #[must_use]
    pub const fn action(&self) -> OperatorAction {
        self.action
    }

    /// Returns the class a deployment authorizes before this call.
    #[must_use]
    pub const fn authorization_class(&self) -> AuthorizationClass {
        self.action.authorization_class()
    }

    /// Borrows the caller-supplied idempotency key.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    /// Borrows the opaque actor reference.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef {
        &self.actor
    }

    /// Borrows the closed-set reason code, when the action requires one.
    #[must_use]
    pub const fn reason(&self) -> Option<&ReasonCode> {
        self.reason.as_ref()
    }

    /// Returns the observed optimistic version for a lifecycle mutation.
    #[must_use]
    pub const fn expected_version(&self) -> Option<ExecutionVersion> {
        self.expected_version
    }

    /// Returns the framework-computed canonical request digest.
    #[must_use]
    pub const fn digest(&self) -> &RequestDigest {
        &self.digest
    }

    /// Returns the targeted execution, when the action names one.
    #[must_use]
    pub const fn job_execution_id(&self) -> Option<JobExecutionId> {
        match self.target {
            OperatorTarget::Execution(id) => Some(id),
            _ => None,
        }
    }

    /// Returns the targeted logical instance, when the action names one.
    #[must_use]
    pub const fn job_instance_id(&self) -> Option<JobInstanceId> {
        match self.target {
            OperatorTarget::Instance(id) => Some(id),
            _ => None,
        }
    }

    fn definition(&self) -> Option<&DefinitionIdentity> {
        match &self.arguments {
            RequestArguments::Definition(definition) => Some(definition),
            _ => None,
        }
    }

    fn recovery_request(&self) -> Option<Result<RecoveryRequest, RecoveryRequestError>> {
        let RequestArguments::Recovery {
            directive,
            evidence_digest,
            ..
        } = &self.arguments
        else {
            return None;
        };
        let expected_version = self.expected_version?;
        let reason = self.reason.as_ref()?;
        Some(match directive {
            RecoveryDirective::Abandon => RecoveryRequest::abandon(
                expected_version,
                reason.as_str(),
                self.actor.as_str(),
                *evidence_digest,
            ),
            RecoveryDirective::MarkFailed(failure) => RecoveryRequest::mark_failed(
                expected_version,
                reason.as_str(),
                self.actor.as_str(),
                *evidence_digest,
                failure.category(),
                failure.failure_id(),
            ),
        })
    }

    fn recovery_guard(&self) -> Option<(RecoveryDirective, bool)> {
        match &self.arguments {
            RequestArguments::Recovery {
                directive,
                unknown_commit,
                ..
            } => Some((*directive, *unknown_commit)),
            _ => None,
        }
    }
}

/// The durable class of one recorded operator request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum OperatorOutcomeClass {
    /// The request was guarded, applied, and audited.
    Applied,
    /// A durable record for this operation identifier already existed.
    Replayed,
    /// A guard rejected the request; the audit row records the class.
    Rejected,
}

impl OperatorOutcomeClass {
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

/// The typed reason one guard rejected an operator action.
///
/// A rejection is durable and audited. It carries no user error text, SQL, or
/// credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperatorRejection {
    /// The supplied expected version lost its compare-and-swap.
    OptimisticConflict {
        /// The version observed under lock.
        current: ExecutionVersion,
    },
    /// The action is not legal from the observed status.
    InvalidState {
        /// The status observed under lock.
        status: BatchStatus,
    },
    /// The logical instance already completed.
    InstanceCompleted,
    /// The logical instance is permanently abandoned.
    InstanceAbandoned,
    /// Another attempt is active or requires explicit recovery.
    ExecutionAlreadyActive {
        /// The attempt preventing the action.
        execution_id: JobExecutionId,
        /// Its status observed under lock.
        status: BatchStatus,
    },
    /// The proposed definition cannot interpret the committed checkpoint.
    IncompatibleDefinition,
    /// A restart was requested for an instance with no prior attempt.
    RestartWithoutPriorAttempt,
    /// The instance-wide start limit for a logical step is exhausted.
    StartLimitExceeded,
    /// Abandoning an ambiguous execution requires an applied recovery decision.
    UnresolvedRecoveryRequired,
    /// The targeted execution does not exist.
    ExecutionNotFound,
    /// The targeted logical instance does not exist.
    InstanceNotFound,
}

impl OperatorRejection {
    /// Returns the stable durable code for this rejection.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OptimisticConflict { .. } => "OPTIMISTIC_CONFLICT",
            Self::InvalidState { .. } => "INVALID_STATE",
            Self::InstanceCompleted => "INSTANCE_COMPLETED",
            Self::InstanceAbandoned => "INSTANCE_ABANDONED",
            Self::ExecutionAlreadyActive { .. } => "EXECUTION_ALREADY_ACTIVE",
            Self::IncompatibleDefinition => "INCOMPATIBLE_DEFINITION",
            Self::RestartWithoutPriorAttempt => "RESTART_WITHOUT_PRIOR_ATTEMPT",
            Self::StartLimitExceeded => "START_LIMIT_EXCEEDED",
            Self::UnresolvedRecoveryRequired => "UNRESOLVED_RECOVERY_REQUIRED",
            Self::ExecutionNotFound => "EXECUTION_NOT_FOUND",
            Self::InstanceNotFound => "INSTANCE_NOT_FOUND",
        }
    }

    fn from_repository(error: &RepositoryError) -> Option<Self> {
        match error {
            RepositoryError::CompletedInstance { .. } => Some(Self::InstanceCompleted),
            RepositoryError::AbandonedInstance { .. } => Some(Self::InstanceAbandoned),
            RepositoryError::ExecutionAlreadyActive {
                execution_id,
                status,
                ..
            } => Some(Self::ExecutionAlreadyActive {
                execution_id: *execution_id,
                status: *status,
            }),
            RepositoryError::IncompatibleDefinition { .. }
            | RepositoryError::RestartStateNotFound { .. }
            | RepositoryError::InvalidDefinitionUpgrade { .. } => {
                Some(Self::IncompatibleDefinition)
            }
            RepositoryError::StartLimitExceeded { .. } => Some(Self::StartLimitExceeded),
            RepositoryError::JobExecutionNotFound { .. } => Some(Self::ExecutionNotFound),
            RepositoryError::JobInstanceNotFound { .. } => Some(Self::InstanceNotFound),

            RepositoryError::Lifecycle(LifecycleError::StaleVersion { actual, .. }) => {
                Some(Self::OptimisticConflict { current: *actual })
            }
            RepositoryError::Lifecycle(
                LifecycleError::IllegalTransition { from, .. }
                | LifecycleError::RestartRequiresNewAttempt { from },
            ) => Some(Self::InvalidState { status: *from }),
            RepositoryError::RecoveryNotAllowed { status, .. }
            | RepositoryError::Lifecycle(LifecycleError::NotRestartable { status }) => {
                Some(Self::InvalidState { status: *status })
            }
            _ => None,
        }
    }
}

impl fmt::Display for OperatorRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One append-only operator audit and idempotency record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorRecord {
    id: OperatorRequestId,
    action: OperatorAction,
    operation_id: OperationId,
    actor: ActorRef,
    reason: Option<ReasonCode>,
    digest: RequestDigest,
    job_instance_id: Option<JobInstanceId>,
    job_execution_id: Option<JobExecutionId>,
    observed_version: Option<ExecutionVersion>,
    prior_status: Option<BatchStatus>,
    result_status: Option<BatchStatus>,
    outcome: OperatorOutcomeClass,
    rejection: Option<OperatorRejection>,
    requested_at: SystemTime,
}

impl OperatorRecord {
    /// Rebuilds a record read from a durable adapter.
    #[must_use]
    pub fn from_parts(id: OperatorRequestId, draft: OperatorRecordDraft) -> Self {
        Self {
            id,
            action: draft.action,
            operation_id: draft.operation_id,
            actor: draft.actor,
            reason: draft.reason,
            digest: draft.digest,
            job_instance_id: draft.job_instance_id,
            job_execution_id: draft.job_execution_id,
            observed_version: draft.observed_version,
            prior_status: draft.prior_status,
            result_status: draft.result_status,
            outcome: draft.outcome,
            rejection: draft.rejection,
            requested_at: draft.requested_at,
        }
    }

    /// Returns the opaque record identifier.
    #[must_use]
    pub const fn id(&self) -> OperatorRequestId {
        self.id
    }

    /// Returns the audited action.
    #[must_use]
    pub const fn action(&self) -> OperatorAction {
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

    /// Borrows the closed-set reason code, when the action required one.
    #[must_use]
    pub const fn reason(&self) -> Option<&ReasonCode> {
        self.reason.as_ref()
    }

    /// Returns the recorded canonical request digest.
    #[must_use]
    pub const fn digest(&self) -> &RequestDigest {
        &self.digest
    }

    /// Returns the audited logical instance, when the action named one.
    #[must_use]
    pub const fn job_instance_id(&self) -> Option<JobInstanceId> {
        self.job_instance_id
    }

    /// Returns the audited execution, when the action produced or named one.
    #[must_use]
    pub const fn job_execution_id(&self) -> Option<JobExecutionId> {
        self.job_execution_id
    }

    /// Returns the version observed under lock.
    #[must_use]
    pub const fn observed_version(&self) -> Option<ExecutionVersion> {
        self.observed_version
    }

    /// Returns the status observed before the effect.
    #[must_use]
    pub const fn prior_status(&self) -> Option<BatchStatus> {
        self.prior_status
    }

    /// Returns the status the effect produced.
    #[must_use]
    pub const fn result_status(&self) -> Option<BatchStatus> {
        self.result_status
    }

    /// Returns the recorded outcome class.
    #[must_use]
    pub const fn outcome(&self) -> OperatorOutcomeClass {
        self.outcome
    }

    /// Returns the recorded rejection class, when the action was rejected.
    #[must_use]
    pub const fn rejection(&self) -> Option<OperatorRejection> {
        self.rejection
    }

    /// Returns the facade-clock instant recorded with the request.
    #[must_use]
    pub const fn requested_at(&self) -> SystemTime {
        self.requested_at
    }
}

/// The bounded audit row an adapter appends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorRecordDraft {
    action: OperatorAction,
    operation_id: OperationId,
    actor: ActorRef,
    reason: Option<ReasonCode>,
    digest: RequestDigest,
    job_instance_id: Option<JobInstanceId>,
    job_execution_id: Option<JobExecutionId>,
    observed_version: Option<ExecutionVersion>,
    prior_status: Option<BatchStatus>,
    result_status: Option<BatchStatus>,
    outcome: OperatorOutcomeClass,
    rejection: Option<OperatorRejection>,
    requested_at: SystemTime,
}

impl OperatorRecordDraft {
    /// Rebuilds a draft from one durable audit row.
    ///
    /// Durable adapters use this to return a recorded outcome without
    /// re-deriving it from a request that may no longer exist.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn from_durable(
        action: OperatorAction,
        operation_id: OperationId,
        actor: ActorRef,
        reason: Option<ReasonCode>,
        digest: RequestDigest,
        job_instance_id: Option<JobInstanceId>,
        job_execution_id: Option<JobExecutionId>,
        observed_version: Option<ExecutionVersion>,
        prior_status: Option<BatchStatus>,
        result_status: Option<BatchStatus>,
        outcome: OperatorOutcomeClass,
        rejection: Option<OperatorRejection>,
        requested_at: SystemTime,
    ) -> Self {
        Self {
            action,
            operation_id,
            actor,
            reason,
            digest,
            job_instance_id,
            job_execution_id,
            observed_version,
            prior_status,
            result_status,
            outcome,
            rejection,
            requested_at,
        }
    }

    /// Returns the audited action.
    #[must_use]
    pub const fn action(&self) -> OperatorAction {
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

    /// Borrows the closed-set reason code, when present.
    #[must_use]
    pub const fn reason(&self) -> Option<&ReasonCode> {
        self.reason.as_ref()
    }

    /// Returns the canonical request digest.
    #[must_use]
    pub const fn digest(&self) -> &RequestDigest {
        &self.digest
    }

    /// Returns the audited logical instance, when known.
    #[must_use]
    pub const fn job_instance_id(&self) -> Option<JobInstanceId> {
        self.job_instance_id
    }

    /// Returns the audited execution, when known.
    #[must_use]
    pub const fn job_execution_id(&self) -> Option<JobExecutionId> {
        self.job_execution_id
    }

    /// Returns the version observed under lock.
    #[must_use]
    pub const fn observed_version(&self) -> Option<ExecutionVersion> {
        self.observed_version
    }

    /// Returns the status observed before the effect.
    #[must_use]
    pub const fn prior_status(&self) -> Option<BatchStatus> {
        self.prior_status
    }

    /// Returns the status the effect produced.
    #[must_use]
    pub const fn result_status(&self) -> Option<BatchStatus> {
        self.result_status
    }

    /// Returns the recorded outcome class.
    #[must_use]
    pub const fn outcome(&self) -> OperatorOutcomeClass {
        self.outcome
    }

    /// Returns the recorded rejection class, when the action was rejected.
    #[must_use]
    pub const fn rejection(&self) -> Option<OperatorRejection> {
        self.rejection
    }

    /// Returns the facade-clock instant recorded with the request.
    #[must_use]
    pub const fn requested_at(&self) -> SystemTime {
        self.requested_at
    }
}

/// The result of one guarded operator call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorOutcome {
    class: OperatorOutcomeClass,
    record: OperatorRecord,
    execution: Option<JobExecution>,
    changed: bool,
}

impl OperatorOutcome {
    const fn new(
        class: OperatorOutcomeClass,
        record: OperatorRecord,
        execution: Option<JobExecution>,
        changed: bool,
    ) -> Self {
        Self {
            class,
            record,
            execution,
            changed,
        }
    }

    /// Returns whether the effect was applied, replayed, or rejected.
    #[must_use]
    pub const fn class(&self) -> OperatorOutcomeClass {
        self.class
    }

    /// Borrows the durable audit record of this operation identifier.
    #[must_use]
    pub const fn record(&self) -> &OperatorRecord {
        &self.record
    }

    /// Borrows the resulting execution snapshot, when the call produced one.
    ///
    /// A replay returns the recorded outcome without re-reading the execution.
    #[must_use]
    pub const fn execution(&self) -> Option<&JobExecution> {
        self.execution.as_ref()
    }

    /// Returns the rejection class, when a guard rejected the action.
    #[must_use]
    pub const fn rejection(&self) -> Option<OperatorRejection> {
        self.record.rejection
    }

    /// Returns whether this call changed durable state.
    ///
    /// A repeated stop or abandon succeeds and changes nothing.
    #[must_use]
    pub const fn changed_state(&self) -> bool {
        self.changed
    }
}

/// A typed operator-service failure that is not a guard rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperatorError {
    /// The operation identifier was reused with a different canonical request.
    OperationIdConflict {
        /// Conflicting action.
        action: OperatorAction,
        /// Conflicting idempotency key.
        operation_id: OperationId,
    },
    /// The commit may or may not have become durable.
    ///
    /// The caller resolves the ambiguity by replaying the same operation
    /// identifier, which either returns the recorded outcome or re-attempts the
    /// effect exactly once.
    OperationOutcomeUnknown,
    /// The recovery arguments could not produce a valid audited request.
    InvalidRecoveryRequest(RecoveryRequestError),
    /// The repository failed for a reason that is not a guard rejection.
    Repository(RepositoryError),
}

impl fmt::Display for OperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationIdConflict {
                action,
                operation_id,
            } => write!(
                formatter,
                "operation identifier {operation_id} was already recorded for {action} with a different request"
            ),
            Self::OperationOutcomeUnknown => {
                formatter.write_str("the operator commit outcome is unknown")
            }
            Self::InvalidRecoveryRequest(error) => error.fmt(formatter),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for OperatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidRecoveryRequest(error) => Some(error),
            Self::Repository(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RepositoryError> for OperatorError {
    fn from(value: RepositoryError) -> Self {
        match value {
            RepositoryError::CommitOutcomeUnknown => Self::OperationOutcomeUnknown,
            other => Self::Repository(other),
        }
    }
}

/// The portable guarded operator application service.
///
/// The service enforces lifecycle, version, definition, checkpoint,
/// idempotency, and bounds. A deployment authenticates the caller and
/// authorizes [`OperatorRequest::authorization_class`] before invoking it.
/// Removing deployment authorization does not weaken a core guard.
#[derive(Clone)]
pub struct JobOperator<R> {
    repository: R,
    clock: Arc<dyn Clock>,
}

impl<R> fmt::Debug for JobOperator<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobOperator")
            .finish_non_exhaustive()
    }
}

impl<R: JobRepository> JobOperator<R> {
    /// Binds one repository and one injected facade clock.
    pub const fn new(repository: R, clock: Arc<dyn Clock>) -> Self {
        Self { repository, clock }
    }

    /// Borrows the underlying repository.
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Applies one guarded, audited, idempotent operator action.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorError::OperationIdConflict`] for a reused identifier
    /// with a different canonical request,
    /// [`OperatorError::OperationOutcomeUnknown`] for an ambiguous commit, and
    /// [`OperatorError::Repository`] for an infrastructure failure. A guard
    /// rejection is an audited [`OperatorOutcomeClass::Rejected`] outcome
    /// rather than an error.
    pub async fn execute(
        &self,
        request: &OperatorRequest,
    ) -> Result<OperatorOutcome, OperatorError> {
        if let Some(recorded) = self.replay(request).await? {
            return Ok(recorded);
        }
        let requested_at = self.clock.now();
        let mut unit = self.repository.begin().await?;
        let effect = match self.apply(unit.as_mut(), request).await {
            Ok(effect) => effect,
            Err(EffectFailure::Rejected(rejection)) => {
                // A rejection must still be audited, so the rollback is
                // best-effort: an adapter that cannot roll back discards its
                // connection, and a genuine outage resurfaces when the audit
                // opens its own unit of work.
                let _ = unit.rollback().await;
                return self.audit_rejection(request, rejection, requested_at).await;
            }
            Err(EffectFailure::Failed(error)) => {
                // The applied effect is already lost; the failure that caused
                // it is the informative error, not a secondary rollback fault.
                let _ = unit.rollback().await;
                return Err(error);
            }
        };
        let draft = OperatorRecordDraft {
            action: request.action,
            operation_id: request.operation_id.clone(),
            actor: request.actor.clone(),
            reason: request.reason.clone(),
            digest: request.digest,
            job_instance_id: effect.job_instance_id,
            job_execution_id: effect.job_execution_id,
            observed_version: request.expected_version,
            prior_status: effect.prior_status,
            result_status: effect.result_status,
            outcome: OperatorOutcomeClass::Applied,
            rejection: None,
            requested_at,
        };
        let record = match unit.append_operator_request(&draft).await {
            Ok(record) => record,
            Err(RepositoryError::ConcurrentModification) => {
                // A concurrent caller may have durably recorded this operation
                // identifier between the replay probe and this append. That
                // transaction owns the effect; this one contributed nothing, so
                // a legitimate duplicate returns the recorded outcome rather
                // than an error that contradicts replay by operation
                // identifier. A conflict that is not this identifier finds no
                // record and keeps the original error.
                let _ = unit.rollback().await;
                return self.replay(request).await?.ok_or(OperatorError::Repository(
                    RepositoryError::ConcurrentModification,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        unit.commit().await?;
        Ok(OperatorOutcome::new(
            OperatorOutcomeClass::Applied,
            record,
            effect.execution,
            effect.changed,
        ))
    }

    async fn replay(
        &self,
        request: &OperatorRequest,
    ) -> Result<Option<OperatorOutcome>, OperatorError> {
        let mut unit = self.repository.begin().await?;
        let recorded = unit
            .find_operator_request(request.action, &request.operation_id)
            .await?;
        unit.rollback().await?;
        let Some(record) = recorded else {
            return Ok(None);
        };
        if record.digest() != &request.digest {
            return Err(OperatorError::OperationIdConflict {
                action: request.action,
                operation_id: request.operation_id.clone(),
            });
        }
        Ok(Some(OperatorOutcome::new(
            OperatorOutcomeClass::Replayed,
            record,
            None,
            false,
        )))
    }

    async fn audit_rejection(
        &self,
        request: &OperatorRequest,
        rejection: OperatorRejection,
        requested_at: SystemTime,
    ) -> Result<OperatorOutcome, OperatorError> {
        let draft = OperatorRecordDraft {
            action: request.action,
            operation_id: request.operation_id.clone(),
            actor: request.actor.clone(),
            reason: request.reason.clone(),
            digest: request.digest,
            job_instance_id: request.job_instance_id(),
            job_execution_id: request.job_execution_id(),
            observed_version: request.expected_version,
            prior_status: None,
            result_status: None,
            outcome: OperatorOutcomeClass::Rejected,
            rejection: Some(rejection),
            requested_at,
        };
        let mut unit = self.repository.begin().await?;
        let record = match unit.append_operator_request(&draft).await {
            Ok(record) => record,
            Err(RepositoryError::ConcurrentModification) => {
                // As in `execute`, a concurrent caller may have recorded this
                // operation identifier first. The rejection is already audited
                // by that transaction.
                let _ = unit.rollback().await;
                return self.replay(request).await?.ok_or(OperatorError::Repository(
                    RepositoryError::ConcurrentModification,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        unit.commit().await?;
        Ok(OperatorOutcome::new(
            OperatorOutcomeClass::Rejected,
            record,
            None,
            false,
        ))
    }

    async fn apply(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        match request.action {
            OperatorAction::Launch => self.launch(unit, request).await,
            OperatorAction::Restart => self.restart(unit, request).await,
            OperatorAction::Stop => self.stop(unit, request).await,
            OperatorAction::Abandon => self.abandon(unit, request).await,
            OperatorAction::Recover => self.recover(unit, request).await,
        }
    }

    async fn launch(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let OperatorTarget::InstanceKey(key) = &request.target else {
            return Err(EffectFailure::Rejected(OperatorRejection::InstanceNotFound));
        };
        let definition = request
            .definition()
            .ok_or(EffectFailure::Rejected(
                OperatorRejection::IncompatibleDefinition,
            ))?
            .clone();
        let selection = unit
            .select_or_create_job_instance(key)
            .await
            .map_err(EffectFailure::classify)?;
        let instance_id = selection.instance().id();
        let execution = unit
            .create_job_execution_with_definition(instance_id, &definition)
            .await
            .map_err(EffectFailure::classify)?;
        Ok(AppliedEffect::created(instance_id, execution))
    }

    async fn restart(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let OperatorTarget::Instance(instance_id) = request.target else {
            return Err(EffectFailure::Rejected(OperatorRejection::InstanceNotFound));
        };
        let definition = request
            .definition()
            .ok_or(EffectFailure::Rejected(
                OperatorRejection::IncompatibleDefinition,
            ))?
            .clone();
        let prior = unit
            .job_executions(instance_id)
            .await
            .map_err(EffectFailure::classify)?;
        let latest = prior.last().ok_or(EffectFailure::Rejected(
            OperatorRejection::RestartWithoutPriorAttempt,
        ))?;
        let prior_status = latest.metadata().status();
        if matches!(prior_status, BatchStatus::Unknown) {
            return Err(EffectFailure::Rejected(OperatorRejection::InvalidState {
                status: prior_status,
            }));
        }
        let execution = unit
            .create_job_execution_with_definition(instance_id, &definition)
            .await
            .map_err(EffectFailure::classify)?;
        Ok(AppliedEffect::created(instance_id, execution).with_prior(prior_status))
    }

    async fn stop(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let (id, expected_version) = execution_target(request)?;
        let observed = unit
            .get_job_execution(id)
            .await
            .map_err(EffectFailure::classify)?
            .ok_or(EffectFailure::Rejected(
                OperatorRejection::ExecutionNotFound,
            ))?;
        let status = observed.metadata().status();
        if !matches!(status, BatchStatus::Starting | BatchStatus::Started) {
            if matches!(status, BatchStatus::Stopping) || status.is_finished() {
                // A repeat request on a stopping or terminal execution succeeds
                // and changes nothing.
                return Ok(AppliedEffect::unchanged(&observed));
            }
            return Err(EffectFailure::Rejected(OperatorRejection::InvalidState {
                status,
            }));
        }
        let execution = unit
            .request_execution_stop(id, expected_version, &request.actor, self.clock.now())
            .await
            .map_err(EffectFailure::classify)?;
        Ok(AppliedEffect::updated(&execution, status))
    }

    async fn abandon(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let (id, expected_version) = execution_target(request)?;
        let observed = unit
            .get_job_execution(id)
            .await
            .map_err(EffectFailure::classify)?
            .ok_or(EffectFailure::Rejected(
                OperatorRejection::ExecutionNotFound,
            ))?;
        let status = observed.metadata().status();
        match status {
            BatchStatus::Abandoned => return Ok(AppliedEffect::unchanged(&observed)),
            BatchStatus::Stopped | BatchStatus::Failed => {}
            BatchStatus::Unknown => {
                let decision = unit
                    .recovery_decision(id)
                    .await
                    .map_err(EffectFailure::classify)?;
                if decision.is_none() {
                    return Err(EffectFailure::Rejected(
                        OperatorRejection::UnresolvedRecoveryRequired,
                    ));
                }
            }
            other => {
                return Err(EffectFailure::Rejected(OperatorRejection::InvalidState {
                    status: other,
                }));
            }
        }
        let transition = LifecycleTransition::new(BatchStatus::Abandoned, self.clock.now());
        let execution = unit
            .transition_job_execution(id, expected_version, transition)
            .await
            .map_err(EffectFailure::classify)?;
        Ok(AppliedEffect::updated(&execution, status))
    }

    async fn recover(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let (id, expected_version) = execution_target(request)?;
        let execution = unit
            .get_job_execution(id)
            .await
            .map_err(EffectFailure::classify)?
            .ok_or(EffectFailure::Rejected(
                OperatorRejection::ExecutionNotFound,
            ))?;
        if execution.version() != expected_version {
            return Err(EffectFailure::Rejected(
                OperatorRejection::OptimisticConflict {
                    current: execution.version(),
                },
            ));
        }
        let (directive, unknown_commit) = request.recovery_guard().ok_or(
            EffectFailure::Rejected(OperatorRejection::InvalidState {
                status: execution.metadata().status(),
            }),
        )?;
        if unknown_commit
            && matches!(directive, RecoveryDirective::MarkFailed(_))
            && request.reason().map(ReasonCode::as_str) != Some("UNKNOWN_EFFECT")
        {
            return Err(EffectFailure::Rejected(
                OperatorRejection::UnresolvedRecoveryRequired,
            ));
        }
        let recovery = request
            .recovery_request()
            .ok_or(EffectFailure::Rejected(OperatorRejection::InvalidState {
                status: BatchStatus::Unknown,
            }))?
            .map_err(|error| EffectFailure::Failed(OperatorError::InvalidRecoveryRequest(error)))?;
        let result = unit
            .recover_job_execution(id, &recovery)
            .await
            .map_err(EffectFailure::classify)?;
        Ok(AppliedEffect::updated(
            result.execution(),
            result.decision().prior_status(),
        ))
    }
}

fn execution_target(
    request: &OperatorRequest,
) -> Result<(JobExecutionId, ExecutionVersion), EffectFailure> {
    let OperatorTarget::Execution(id) = request.target else {
        return Err(EffectFailure::Rejected(
            OperatorRejection::ExecutionNotFound,
        ));
    };
    let expected_version = request.expected_version.ok_or(EffectFailure::Rejected(
        OperatorRejection::InvalidState {
            status: BatchStatus::Unknown,
        },
    ))?;
    Ok((id, expected_version))
}

struct AppliedEffect {
    job_instance_id: Option<JobInstanceId>,
    job_execution_id: Option<JobExecutionId>,
    prior_status: Option<BatchStatus>,
    result_status: Option<BatchStatus>,
    execution: Option<JobExecution>,
    changed: bool,
}

impl AppliedEffect {
    fn created(instance_id: JobInstanceId, execution: JobExecution) -> Self {
        Self {
            job_instance_id: Some(instance_id),
            job_execution_id: Some(execution.id()),
            prior_status: None,
            result_status: Some(execution.metadata().status()),
            execution: Some(execution),
            changed: true,
        }
    }

    fn updated(execution: &JobExecution, prior_status: BatchStatus) -> Self {
        Self {
            job_instance_id: Some(execution.job_instance_id()),
            job_execution_id: Some(execution.id()),
            prior_status: Some(prior_status),
            result_status: Some(execution.metadata().status()),
            execution: Some(execution.clone()),
            changed: true,
        }
    }

    fn unchanged(execution: &JobExecution) -> Self {
        let status = execution.metadata().status();
        Self {
            job_instance_id: Some(execution.job_instance_id()),
            job_execution_id: Some(execution.id()),
            prior_status: Some(status),
            result_status: Some(status),
            execution: Some(execution.clone()),
            changed: false,
        }
    }

    const fn with_prior(mut self, prior_status: BatchStatus) -> Self {
        self.prior_status = Some(prior_status);
        self
    }
}

enum EffectFailure {
    Rejected(OperatorRejection),
    Failed(OperatorError),
}

impl EffectFailure {
    fn classify(error: RepositoryError) -> Self {
        OperatorRejection::from_repository(&error)
            .map_or_else(|| Self::Failed(OperatorError::from(error)), Self::Rejected)
    }
}
