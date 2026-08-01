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

use contract::{ContractClock, ServiceBackend, run_service_contract};
use futures_executor::block_on;
use ids::DeterministicIds;
use oxide_batch::{
    ActorRef, ComponentRevision, DefinitionIdentity, DefinitionRevision, InMemoryExplorer,
    InMemoryJobRepository, JobInstanceKey, JobName, JobOperator, JobParameter, JobParameters,
    OperationId, OperatorError, OperatorRequest, ParameterName, ParameterRole, ParameterValue,
    ReasonCode, RepositoryError, RequestField, RequestFieldError, StepName,
};

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
    let repository = AmbiguousCommitRepository(InMemoryJobRepository::new(
        Arc::clone(&clock) as _,
        Arc::new(DeterministicIds::new(first)),
    ));
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

/// A repository whose commits never report a known outcome.
#[derive(Clone)]
struct AmbiguousCommitRepository(InMemoryJobRepository);

impl oxide_batch::JobRepository for AmbiguousCommitRepository {
    fn begin<'a>(
        &'a self,
    ) -> oxide_batch::BoxFuture<
        'a,
        Result<Box<dyn oxide_batch::RepositoryUnitOfWork + 'a>, RepositoryError>,
    > {
        Box::pin(async move {
            let unit = self.0.begin().await?;
            Ok(Box::new(AmbiguousCommitUnit(unit)) as Box<dyn oxide_batch::RepositoryUnitOfWork>)
        })
    }
}

struct AmbiguousCommitUnit<'a>(Box<dyn oxide_batch::RepositoryUnitOfWork + 'a>);

impl oxide_batch::RepositoryUnitOfWork for AmbiguousCommitUnit<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a oxide_batch::DefinitionUpgrade,
    ) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>> {
        self.0.register_definition_upgrade(job_name, upgrade)
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobInstanceSelection, RepositoryError>>
    {
        self.0.select_or_create_job_instance(key)
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.0.create_job_execution(job_instance_id)
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: oxide_batch::JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.0
            .create_job_execution_with_definition(job_instance_id, definition)
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: oxide_batch::JobExecutionId,
        step_name: &'a StepName,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.0.create_step_execution(job_execution_id, step_name)
    }

    fn transition_job_execution(
        &mut self,
        id: oxide_batch::JobExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        transition: oxide_batch::LifecycleTransition,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.0
            .transition_job_execution(id, expected_version, transition)
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: oxide_batch::JobExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        exit_status: &'a oxide_batch::ExitStatus,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::JobExecution, RepositoryError>> {
        self.0
            .enrich_job_exit_status(id, expected_version, exit_status)
    }

    fn transition_step_execution(
        &mut self,
        id: oxide_batch::StepExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        transition: oxide_batch::LifecycleTransition,
    ) -> oxide_batch::BoxFuture<'_, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.0
            .transition_step_execution(id, expected_version, transition)
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: oxide_batch::StepExecutionId,
        expected_version: oxide_batch::ExecutionVersion,
        exit_status: &'a oxide_batch::ExitStatus,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::StepExecution, RepositoryError>> {
        self.0
            .enrich_step_exit_status(id, expected_version, exit_status)
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> oxide_batch::BoxFuture<'a, Result<Option<oxide_batch::JobInstance>, RepositoryError>> {
        self.0.find_job_instance(key)
    }

    fn get_job_instance(
        &mut self,
        id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::JobInstance>, RepositoryError>> {
        self.0.get_job_instance(id)
    }

    fn get_job_execution(
        &mut self,
        id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::JobExecution>, RepositoryError>>
    {
        self.0.get_job_execution(id)
    }

    fn job_executions(
        &mut self,
        job_instance_id: oxide_batch::JobInstanceId,
    ) -> oxide_batch::BoxFuture<'_, Result<Vec<oxide_batch::JobExecution>, RepositoryError>> {
        self.0.job_executions(job_instance_id)
    }

    fn get_step_execution(
        &mut self,
        id: oxide_batch::StepExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::StepExecution>, RepositoryError>>
    {
        self.0.get_step_execution(id)
    }

    fn step_executions(
        &mut self,
        job_execution_id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Vec<oxide_batch::StepExecution>, RepositoryError>> {
        self.0.step_executions(job_execution_id)
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        id: oxide_batch::JobExecutionId,
        request: &'a oxide_batch::RecoveryRequest,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::RecoveryResult, RepositoryError>> {
        self.0.recover_job_execution(id, request)
    }

    fn recovery_decision(
        &mut self,
        id: oxide_batch::JobExecutionId,
    ) -> oxide_batch::BoxFuture<'_, Result<Option<oxide_batch::RecoveryDecision>, RepositoryError>>
    {
        self.0.recovery_decision(id)
    }

    fn find_operator_request<'a>(
        &'a mut self,
        action: oxide_batch::OperatorAction,
        operation_id: &'a OperationId,
    ) -> oxide_batch::BoxFuture<'a, Result<Option<oxide_batch::OperatorRecord>, RepositoryError>>
    {
        self.0.find_operator_request(action, operation_id)
    }

    fn append_operator_request<'a>(
        &'a mut self,
        draft: &'a oxide_batch::OperatorRecordDraft,
    ) -> oxide_batch::BoxFuture<'a, Result<oxide_batch::OperatorRecord, RepositoryError>> {
        self.0.append_operator_request(draft)
    }

    fn commit<'a>(self: Box<Self>) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async { Err(RepositoryError::CommitOutcomeUnknown) })
    }

    fn rollback<'a>(self: Box<Self>) -> oxide_batch::BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        self.0.rollback()
    }
}
