//! Bounded operator, explorer, and retention service behavior.

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use contract::{ContractClock, ServiceBackend, run_service_contract};
use futures_executor::block_on;
use ids::DeterministicIds;
use oxide_batch::{
    ActorRef, BatchStatus, Clock, ComponentRevision, DefinitionIdentity, DefinitionRevision,
    FailureCategory, FailureId, FailureSummary, InMemoryExplorer, InMemoryJobRepository,
    JobInstanceKey, JobName, JobOperator, JobParameter, JobParameters, JobRepository,
    LifecycleTransition, MonotonicClock, MonotonicInstant, OperationId, OperatorAction,
    OperatorError, OperatorOutcomeClass, OperatorRecordDraft, OperatorRejection, OperatorRequest,
    OwnerToken, ParameterName, ParameterRole, ParameterValue, ReasonCode, RecoveryDirective,
    RecoveryProposer, RepositoryError, RequestField, RequestFieldError, StepName,
};

#[derive(Debug)]
struct FixedMonotonic;

impl MonotonicClock for FixedMonotonic {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_duration(Duration::ZERO)
    }
}

fn backend(
    clock: Arc<ContractClock>,
) -> Result<ServiceBackend<InMemoryJobRepository, InMemoryExplorer>, RepositoryError> {
    let first = NonZeroU64::new(1).ok_or(RepositoryError::Unavailable)?;
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock) as _,
        Arc::new(DeterministicIds::new(first)),
    );
    let explorer = InMemoryExplorer::new(&repository);
    Ok(ServiceBackend {
        repository,
        explorer,
        clock,
    })
}

#[test]
fn shared_service_contract_passes_in_memory() -> Result<(), Box<dyn Error>> {
    run_service_contract("in-memory", backend)?;
    Ok(())
}

#[test]
fn request_envelope_rejects_values_outside_its_closed_sets() {
    assert_eq!(
        ActorRef::new(""),
        Err(RequestFieldError::Empty {
            field: RequestField::ActorRef
        })
    );
    assert_eq!(
        ActorRef::new("operator space"),
        Err(RequestFieldError::InvalidCharacter {
            field: RequestField::ActorRef
        })
    );
    assert_eq!(
        ActorRef::new("a".repeat(129)),
        Err(RequestFieldError::TooLong {
            field: RequestField::ActorRef,
            max_bytes: 128
        })
    );
    assert_eq!(
        ReasonCode::new("lowercase"),
        Err(RequestFieldError::InvalidCharacter {
            field: RequestField::ReasonCode
        })
    );
    assert_eq!(
        OperationId::new("id/with/slash"),
        Err(RequestFieldError::InvalidCharacter {
            field: RequestField::OperationId
        })
    );
    assert!(ActorRef::new("svc:deploy-1@cluster.local").is_ok());
    assert!(ReasonCode::new("OPERATOR_DECISION_1").is_ok());
    assert!(OperationId::new("launch.2026-08-01:001").is_ok());
}

#[test]
fn request_digest_covers_the_target_and_arguments_but_not_the_actor() -> Result<(), Box<dyn Error>>
{
    let key = key("digest")?;
    let definition = definition()?;
    let first = OperatorRequest::launch(
        OperationId::new("digest-1")?,
        ActorRef::new("operator:one")?,
        key.clone(),
        definition.clone(),
    );
    let other_actor = OperatorRequest::launch(
        OperationId::new("digest-1")?,
        ActorRef::new("operator:two")?,
        key.clone(),
        definition.clone(),
    );
    let other_target = OperatorRequest::launch(
        OperationId::new("digest-1")?,
        ActorRef::new("operator:one")?,
        key_named("other", "digest")?,
        definition,
    );
    assert_eq!(first.digest(), other_actor.digest());
    assert_ne!(first.digest(), other_target.digest());
    assert_eq!(first.digest().to_hex().len(), 64);
    Ok(())
}

#[test]
fn ambiguous_operator_commit_reports_unknown_outcome() -> Result<(), Box<dyn Error>> {
    let clock = Arc::new(ContractClock::new(1_700_000_000_000));
    let first = NonZeroU64::new(1).ok_or(RepositoryError::Unavailable)?;
    let repository = FaultingRepository::new(
        InMemoryJobRepository::new(
            Arc::clone(&clock) as _,
            Arc::new(DeterministicIds::new(first)),
        ),
        Fault::AmbiguousCommit,
    );
    let operator = JobOperator::new(repository, clock as _);
    let request = OperatorRequest::launch(
        OperationId::new("ambiguous")?,
        ActorRef::new("operator:one")?,
        key("ambiguous")?,
        definition()?,
    );
    assert_eq!(
        block_on(operator.execute(&request)),
        Err(OperatorError::OperationOutcomeUnknown)
    );
    Ok(())
}

#[test]
fn a_duplicate_operation_identifier_replays_rather_than_reporting_a_conflict()
-> Result<(), Box<dyn Error>> {
    let clock = Arc::new(ContractClock::new(1_700_000_000_000));
    let first = NonZeroU64::new(1).ok_or(RepositoryError::Unavailable)?;
    let inner = InMemoryJobRepository::new(
        Arc::clone(&clock) as _,
        Arc::new(DeterministicIds::new(first)),
    );
    let request = OperatorRequest::launch(
        OperationId::new("racing-launch")?,
        ActorRef::new("operator:one")?,
        key("racing")?,
        definition()?,
    );

    // A concurrent caller commits the audit row for this operation identifier
    // while this caller sits between its replay probe and its own append. Only
    // the row is seeded: the winner's effect belongs to a transaction this test
    // does not need to reproduce.
    let seeded = OperatorRecordDraft::from_durable(
        OperatorAction::Launch,
        request.operation_id().clone(),
        ActorRef::new("operator:two")?,
        None,
        *request.digest(),
        None,
        None,
        None,
        None,
        None,
        OperatorOutcomeClass::Applied,
        None,
        clock.now(),
    );
    let mut seed = block_on(inner.begin())?;
    let winner = block_on(seed.append_operator_request(&seeded))?;
    block_on(seed.commit())?;

    // The probe misses the seeded row, so the effect applies and the audit
    // append collides on the operation identifier. The service must roll back
    // and return the recorded outcome rather than surfacing the collision.
    let loser = JobOperator::new(
        FaultingRepository::new(inner.clone(), Fault::MissedReplayProbe),
        Arc::clone(&clock) as _,
    );
    let replayed = block_on(loser.execute(&request))?;
    assert_eq!(replayed.class(), OperatorOutcomeClass::Replayed);
    assert_eq!(replayed.record().id(), winner.id());
    assert_eq!(replayed.record().digest(), request.digest());

    // The rolled-back effect left no instance behind.
    let mut audit = block_on(inner.begin())?;
    let instance = block_on(audit.find_job_instance(&key("racing")?))?;
    block_on(audit.rollback())?;
    assert!(instance.is_none());
    Ok(())
}

#[test]
fn a_recover_request_carries_the_failure_its_disposition_requires() -> Result<(), Box<dyn Error>> {
    let clock = Arc::new(ContractClock::new(1_700_000_000_000));
    let first = NonZeroU64::new(1).ok_or(RepositoryError::Unavailable)?;
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock) as _,
        Arc::new(DeterministicIds::new(first)),
    );

    let mut create = block_on(repository.begin())?;
    let instance = block_on(create.select_or_create_job_instance(&key("recover")?))?
        .instance()
        .clone();
    let execution = block_on(create.create_job_execution(instance.id()))?;
    block_on(create.commit())?;

    let mut stall = block_on(repository.begin())?;
    let _ambiguous = block_on(stall.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Unknown, clock.now()),
    ))?;
    block_on(stall.commit())?;

    let proposer = RecoveryProposer::new(
        InMemoryExplorer::new(&repository),
        Arc::clone(&clock) as _,
        Arc::new(FixedMonotonic),
        OwnerToken::from_bytes([7; 16]),
    );
    let proposal = block_on(proposer.propose(execution.id()))?;

    let failure = FailureSummary::new(FailureCategory::PermanentInfrastructure, FailureId::new(7)?);
    let rejected = OperatorRequest::recover(
        OperationId::new("recover-rejected")?,
        ActorRef::new("operator:one")?,
        ReasonCode::new("COMMIT_INSPECTED_NOT_DURABLE")?,
        RecoveryDirective::MarkFailed(failure),
        &proposal,
    );
    let operator = JobOperator::new(repository, Arc::clone(&clock) as _);
    let rejected = block_on(operator.execute(&rejected))?;
    assert_eq!(rejected.class(), OperatorOutcomeClass::Rejected);
    assert_eq!(
        rejected.record().rejection(),
        Some(OperatorRejection::UnresolvedRecoveryRequired)
    );

    let request = OperatorRequest::recover(
        OperationId::new("recover-1")?,
        ActorRef::new("operator:one")?,
        ReasonCode::new("UNKNOWN_EFFECT")?,
        RecoveryDirective::MarkFailed(failure),
        &proposal,
    );
    let outcome = block_on(operator.execute(&request))?;
    assert_eq!(outcome.class(), OperatorOutcomeClass::Applied);
    assert_eq!(outcome.record().result_status(), Some(BatchStatus::Failed));
    assert_eq!(outcome.record().prior_status(), Some(BatchStatus::Unknown));

    // The two dispositions are distinct requests, and an abandoning directive
    // carries no failure for the digest to cover.
    let abandoning = OperatorRequest::recover(
        OperationId::new("recover-1")?,
        ActorRef::new("operator:one")?,
        ReasonCode::new("COMMIT_INSPECTED_NOT_DURABLE")?,
        RecoveryDirective::Abandon,
        &proposal,
    );
    assert_ne!(request.digest(), abandoning.digest());
    assert_eq!(RecoveryDirective::Abandon.failure(), None);
    assert_eq!(
        RecoveryDirective::MarkFailed(failure).failure(),
        Some(failure)
    );
    Ok(())
}

fn key(discriminator: &str) -> Result<JobInstanceKey, Box<dyn Error>> {
    key_named("services_job", discriminator)
}

fn key_named(job: &str, discriminator: &str) -> Result<JobInstanceKey, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        JobParameter::new(
            ParameterValue::string(discriminator)?,
            ParameterRole::Identifying,
        ),
    )?;
    Ok(JobInstanceKey::new(JobName::new(job)?, &parameters))
}

fn definition() -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new("services_job")?,
        &StepName::new("only")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-1")?,
    )?)
}

/// The single deterministic fault a wrapped repository injects.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Fault {
    /// Every commit reports an unknown outcome.
    AmbiguousCommit,
    /// The first replay probe misses a record that is durably present.
    ///
    /// This reproduces the window between the probe and the append in which a
    /// concurrent caller records the same operation identifier first.
    MissedReplayProbe,
}

/// A repository that injects one deterministic fault into the in-memory one.
#[derive(Clone)]
struct FaultingRepository {
    inner: InMemoryJobRepository,
    fault: Fault,
    probes: Arc<AtomicUsize>,
}

impl FaultingRepository {
    fn new(inner: InMemoryJobRepository, fault: Fault) -> Self {
        Self {
            inner,
            fault,
            probes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl oxide_batch::JobRepository for FaultingRepository {
    fn begin<'a>(
        &'a self,
    ) -> oxide_batch::BoxFuture<
        'a,
        Result<Box<dyn oxide_batch::RepositoryUnitOfWork + 'a>, RepositoryError>,
    > {
        Box::pin(async move {
            let unit = self.inner.begin().await?;
            Ok(Box::new(FaultingUnit {
                inner: unit,
                fault: self.fault,
                probes: Arc::clone(&self.probes),
            }) as Box<dyn oxide_batch::RepositoryUnitOfWork>)
        })
    }
}

struct FaultingUnit<'a> {
    inner: Box<dyn oxide_batch::RepositoryUnitOfWork + 'a>,
    fault: Fault,
    probes: Arc<AtomicUsize>,
}

impl oxide_batch::RepositoryUnitOfWork for FaultingUnit<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a oxide_batch::DefinitionUpgrade,
    ) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>> {
        self.inner.register_definition_upgrade(job_name, upgrade)
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobInstanceSelection, RepositoryError>>
    {
        self.inner.select_or_create_job_instance(key)
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.inner.create_job_execution(job_instance_id)
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: oxide_batch::JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.inner
            .create_job_execution_with_definition(job_instance_id, definition)
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: oxide_batch::JobExecutionId,
        step_name: &'a StepName,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.inner
            .create_step_execution(job_execution_id, step_name)
    }

    fn transition_job_execution(
        &mut self,
        id: oxide_batch::JobExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        transition: oxide_batch::LifecycleTransition,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.inner
            .transition_job_execution(id, expected_version, transition)
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: oxide_batch::JobExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        exit_status: &'a oxide_batch::ExitStatus,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.inner
            .enrich_job_exit_status(id, expected_version, exit_status)
    }

    fn transition_step_execution(
        &mut self,
        id: oxide_batch::StepExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        transition: oxide_batch::LifecycleTransition,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.inner
            .transition_step_execution(id, expected_version, transition)
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: oxide_batch::StepExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        exit_status: &'a oxide_batch::ExitStatus,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.inner
            .enrich_step_exit_status(id, expected_version, exit_status)
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> oxide_batch::BoxFuture<'a, Result<Option<oxide_batch::JobInstance>, RepositoryError>> {
        self.inner.find_job_instance(key)
    }

    fn get_job_instance(
        &mut self,
        id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::JobInstance>, RepositoryError>> {
        self.inner.get_job_instance(id)
    }

    fn get_job_execution(
        &mut self,
        id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::JobExecution>, RepositoryError>>
    {
        self.inner.get_job_execution(id)
    }

    fn job_executions(
        &mut self,
        job_instance_id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<Vec<oxide_batch::JobExecution>, RepositoryError>> {
        self.inner.job_executions(job_instance_id)
    }

    fn get_step_execution(
        &mut self,
        id: oxide_batch::StepExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::StepExecution>, RepositoryError>>
    {
        self.inner.get_step_execution(id)
    }

    fn step_executions(
        &mut self,
        job_execution_id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Vec<oxide_batch::StepExecution>, RepositoryError>> {
        self.inner.step_executions(job_execution_id)
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        id: oxide_batch::JobExecutionId,
        request: &'a oxide_batch::RecoveryRequest,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::RecoveryResult, RepositoryError>> {
        self.inner.recover_job_execution(id, request)
    }

    fn recovery_decision(
        &mut self,
        id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::RecoveryDecision>, RepositoryError>>
    {
        self.inner.recovery_decision(id)
    }

    fn find_operator_request<'a>(
        &'a mut self,
        action: oxide_batch::OperatorAction,
        operation_id: &'a OperationId,
    ) -> oxide_batch::BoxFuture<'a, Result<Option<oxide_batch::OperatorRecord>, RepositoryError>>
    {
        if self.fault == Fault::MissedReplayProbe && self.probes.fetch_add(1, Ordering::SeqCst) == 0
        {
            return Box::pin(async { Ok(None) });
        }
        self.inner.find_operator_request(action, operation_id)
    }

    fn append_operator_request<'a>(
        &'a mut self,
        draft: &'a oxide_batch::OperatorRecordDraft,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::OperatorRecord, RepositoryError>> {
        self.inner.append_operator_request(draft)
    }

    fn commit<'a>(self: Box<Self>) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        if self.fault == Fault::AmbiguousCommit {
            return Box::pin(async { Err(RepositoryError::CommitOutcomeUnknown) });
        }
        self.inner.commit()
    }

    fn rollback<'a>(self: Box<Self>) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        self.inner.rollback()
    }
}
