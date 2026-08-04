//! Guarded, idempotent, audited operator actions.
//!
//! Every mutating action carries a bounded envelope, commits its append-only
//! audit row in the same transaction as its effect, and is replayable by its
//! operation identifier. The service performs no internal retry loop and never
//! guesses an ambiguous commit outcome. The request, audit record, and guard
//! vocabulary it applies live in `oxide-batch-repository`.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use crate::{
    BatchStatus, Clock, ExecutionVersion, JobExecution, JobExecutionId, JobInstanceId,
    JobRepository, LifecycleTransition, OperationId, OperatorAction, OperatorOutcomeClass,
    OperatorRecord, OperatorRecordDraft, OperatorRejection, OperatorRequest, ReasonCode,
    RecoveryDirective, RecoveryRequestError, RepositoryError, RepositoryUnitOfWork,
    TelemetryEventKind, TelemetryEventSink, TelemetryRecord,
};

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
        self.record.rejection()
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
    event_sinks: Vec<Arc<dyn TelemetryEventSink>>,
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
            self.emit_outcome(request, &recorded);
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
                let outcome = self
                    .audit_rejection(request, rejection, requested_at)
                    .await?;
                self.emit_outcome(request, &outcome);
                return Ok(outcome);
            }
            Err(EffectFailure::Failed(error)) => {
                // The applied effect is already lost; the failure that caused
                // it is the informative error, not a secondary rollback fault.
                let _ = unit.rollback().await;
                return Err(error);
            }
        };
        let draft = OperatorRecordDraft::applied(
            request,
            effect.job_instance_id,
            effect.job_execution_id,
            effect.prior_status,
            effect.result_status,
            requested_at,
        );
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
        let outcome = OperatorOutcome::new(
            OperatorOutcomeClass::Applied,
            record,
            effect.execution,
            effect.changed,
        );
        self.emit_outcome(request, &outcome);
        Ok(outcome)
    }

    fn emit_outcome(&self, request: &OperatorRequest, outcome: &OperatorOutcome) {
        let primary = match outcome.class() {
            OperatorOutcomeClass::Applied | OperatorOutcomeClass::Replayed => {
                TelemetryEventKind::OperatorRequestAccepted
            }
            // The `Rejected` arm, and any outcome class added later. Telemetry
            // must never report an outcome this build does not recognize as
            // accepted, so it reports the non-accepting kind; the record still
            // carries the exact class.
            _ => TelemetryEventKind::OperatorRequestRejected,
        };
        self.emit_record(&TelemetryRecord::operator(
            primary,
            request,
            Some(outcome.class()),
            outcome.rejection(),
        ));
        if request.action() == OperatorAction::Recover {
            let recovery = if outcome.class() == OperatorOutcomeClass::Rejected {
                TelemetryEventKind::RecoveryRejected
            } else {
                TelemetryEventKind::RecoveryApplied
            };
            self.emit_record(&TelemetryRecord::operator(
                recovery,
                request,
                Some(outcome.class()),
                outcome.rejection(),
            ));
        }
        self.emit_record(&TelemetryRecord::operator(
            TelemetryEventKind::OperatorRequestCompleted,
            request,
            Some(outcome.class()),
            outcome.rejection(),
        ));
    }

    fn emit_record(&self, record: &TelemetryRecord) {
        for sink in &self.event_sinks {
            crate::telemetry::emit_safely(Some(sink), record);
        }
    }

    async fn replay(
        &self,
        request: &OperatorRequest,
    ) -> Result<Option<OperatorOutcome>, OperatorError> {
        let mut unit = self.repository.begin().await?;
        let recorded = unit
            .find_operator_request(request.action(), request.operation_id())
            .await?;
        unit.rollback().await?;
        let Some(record) = recorded else {
            return Ok(None);
        };
        if record.digest() != request.digest() {
            return Err(OperatorError::OperationIdConflict {
                action: request.action(),
                operation_id: request.operation_id().clone(),
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
        let draft = OperatorRecordDraft::rejected(request, rejection, requested_at);
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
        match request.action() {
            OperatorAction::Launch => self.launch(unit, request).await,
            OperatorAction::Restart => self.restart(unit, request).await,
            OperatorAction::Stop => self.stop(unit, request).await,
            OperatorAction::Abandon => self.abandon(unit, request).await,
            OperatorAction::Recover => self.recover(unit, request).await,
            // Absorbs any action added later: the build cannot apply it, so it
            // is an audited rejection rather than a silent success.
            _ => Err(EffectFailure::Rejected(
                OperatorRejection::UnsupportedAction,
            )),
        }
    }

    async fn launch(
        &self,
        unit: &mut dyn RepositoryUnitOfWork,
        request: &OperatorRequest,
    ) -> Result<AppliedEffect, EffectFailure> {
        let Some(key) = request.job_instance_key() else {
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
        let Some(instance_id) = request.job_instance_id() else {
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
            .request_execution_stop(id, expected_version, request.actor(), self.clock.now())
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
    let Some(id) = request.job_execution_id() else {
        return Err(EffectFailure::Rejected(
            OperatorRejection::ExecutionNotFound,
        ));
    };
    let expected_version = request.expected_version().ok_or(EffectFailure::Rejected(
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
