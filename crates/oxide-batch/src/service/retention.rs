//! The portable, audited retention service.
//!
//! Purge is planned and applied in two guarded phases. The durable holds, purge
//! plans, and audit records the service exchanges with a repository live in
//! `oxide-batch-repository`.

use std::fmt;
use std::sync::Arc;

use crate::{
    ActorRef, Clock, JobInstanceId, JobRepository, OperationId, PurgeCounts, PurgePlan,
    PurgePlanRequest, ReasonCode, RetentionAction, RetentionError, RetentionHold, RetentionOutcome,
    RetentionRecord, RetentionRecordDraft, TelemetryEventKind, TelemetryEventSink, TelemetryRecord,
};

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
            // Absorbs `ApplyPurge` and any action added later. The private
            // caller passes only `Hold` and `ReleaseHold`, and neither absorbed
            // action changes hold state.
            _ => None,
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
    /// [`AuthorizationClass::Read`]: crate::AuthorizationClass::Read
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
