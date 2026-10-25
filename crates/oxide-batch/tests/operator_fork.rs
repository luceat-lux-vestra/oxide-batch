//! M7/#615: explicit fork-as-operator audit and request-identity evidence.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_executor::block_on;
use oxide_batch::{
    ActorRef, BatchStatus, Clock, ComponentRevision, DefinitionArtifactId, DefinitionIdentity,
    DefinitionRegistration, DefinitionRegistry, DefinitionRevision, ExecutionVersion, ForkRequest,
    InMemoryJobRepository, JobInstanceKey, JobName, JobOperator, JobParameter, JobParameters,
    JobRepository, LifecycleTransition, OperationId, OperatorAction, OperatorOutcomeClass,
    OperatorRequest, ParameterName, ParameterRole, ParameterValue, RepositoryError,
    SequentialIdGenerator, StepName,
};

#[derive(Clone, Copy)]
struct ForkOperatorClock;

impl Clock for ForkOperatorClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_mins(2)
    }
}

fn registration(job_name: &str, artifact: &str) -> Result<DefinitionRegistration, Box<dyn Error>> {
    let job = JobName::new(job_name)?;
    Ok(DefinitionRegistration::new(
        DefinitionIdentity::tasklet(
            &job,
            &StepName::new("only")?,
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new("component-v1")?,
        )?,
        DefinitionArtifactId::new(artifact)?,
    )?)
}

fn parameters(value: &str) -> Result<JobParameters, Box<dyn Error>> {
    Ok(JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(ParameterValue::string(value)?, ParameterRole::Identifying),
    )])?)
}

fn setup() -> Result<
    (
        InMemoryJobRepository,
        oxide_batch::JobExecution,
        DefinitionRegistration,
    ),
    Box<dyn Error>,
> {
    block_on(async {
        let repository = InMemoryJobRepository::new(
            Arc::new(ForkOperatorClock),
            Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
        );
        let source = registration("operator-fork-source", "source-artifact-v1")?;
        let target = registration("operator-fork-target", "target-artifact-v1")?;
        let registry = DefinitionRegistry::new(repository.clone());
        registry.register(&source).await?;
        registry.register(&target).await?;

        let source_parameters = JobParameters::new();
        let key = JobInstanceKey::new(source.job_name().clone(), &source_parameters);
        let mut unit = repository.begin().await?;
        let instance = unit
            .select_or_create_job_instance(&key)
            .await?
            .instance()
            .clone();
        let starting = unit
            .create_job_execution_with_definition_and_parameters(
                instance.id(),
                source.definition(),
                &source_parameters,
            )
            .await?;
        let started = unit
            .transition_job_execution(
                starting.id(),
                starting.version(),
                LifecycleTransition::new(
                    BatchStatus::Started,
                    UNIX_EPOCH + Duration::from_secs(100),
                ),
            )
            .await?;
        let stopped = unit
            .transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(
                    BatchStatus::Stopped,
                    UNIX_EPOCH + Duration::from_secs(101),
                ),
            )
            .await?;
        unit.commit().await?;
        Ok::<_, Box<dyn Error>>((repository, stopped, target))
    })
}

#[test]
fn operator_fork_applies_once_and_replays_as_fork() -> Result<(), Box<dyn Error>> {
    let (repository, source, target) = setup()?;
    let fork = ForkRequest::new(
        OperationId::new("operator-fork-001")?,
        source.id(),
        source.version(),
        target,
        parameters("2026-10-25")?,
    );
    let request = OperatorRequest::fork(ActorRef::new("operator:fork")?, fork.clone());
    assert_eq!(request.action(), OperatorAction::Fork);
    assert_eq!(request.expected_version(), Some(source.version()));

    block_on(async {
        let operator = JobOperator::new(repository.clone(), Arc::new(ForkOperatorClock));
        let applied = operator.execute(&request).await?;
        assert_eq!(applied.class(), OperatorOutcomeClass::Applied);
        assert!(applied.changed_state());
        assert_eq!(applied.record().action(), OperatorAction::Fork);
        assert_eq!(applied.record().observed_version(), Some(source.version()));
        let target_execution = applied
            .execution()
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(target_execution.metadata().status(), BatchStatus::Starting);

        let mut inspect = repository.begin().await?;
        let lineage = inspect
            .fork_lineage(fork.operation_id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(lineage.source_execution_id(), source.id());
        assert_eq!(lineage.target_execution_id(), target_execution.id());
        assert_eq!(
            inspect
                .find_operator_request(OperatorAction::Fork, request.operation_id())
                .await?
                .map(|record| record.action()),
            Some(OperatorAction::Fork)
        );
        assert_eq!(
            inspect
                .job_executions(lineage.target_instance_id())
                .await?
                .len(),
            1
        );
        inspect.rollback().await?;

        let replayed = operator.execute(&request).await?;
        assert_eq!(replayed.class(), OperatorOutcomeClass::Replayed);
        assert!(!replayed.changed_state());
        assert!(replayed.execution().is_none());

        let mut final_read = repository.begin().await?;
        assert_eq!(
            final_read
                .job_executions(lineage.target_instance_id())
                .await?
                .len(),
            1
        );
        final_read.rollback().await?;
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn operator_fork_digest_binds_source_target_and_parameter_identity() -> Result<(), Box<dyn Error>> {
    let (_repository, source, target) = setup()?;
    let actor = ActorRef::new("operator:fork")?;
    let base = OperatorRequest::fork(
        actor.clone(),
        ForkRequest::new(
            OperationId::new("operator-fork-digest")?,
            source.id(),
            source.version(),
            target.clone(),
            parameters("2026-10-25")?,
        ),
    );

    let stale_source = OperatorRequest::fork(
        actor.clone(),
        ForkRequest::new(
            OperationId::new("operator-fork-digest")?,
            source.id(),
            ExecutionVersion::INITIAL,
            target.clone(),
            parameters("2026-10-25")?,
        ),
    );
    assert_ne!(base.digest(), stale_source.digest());

    let changed_artifact = DefinitionRegistration::new(
        target.definition().clone(),
        DefinitionArtifactId::new("target-artifact-v2")?,
    )?;
    let artifact_request = OperatorRequest::fork(
        actor.clone(),
        ForkRequest::new(
            OperationId::new("operator-fork-digest")?,
            source.id(),
            source.version(),
            changed_artifact,
            parameters("2026-10-25")?,
        ),
    );
    assert_ne!(base.digest(), artifact_request.digest());

    let parameter_request = OperatorRequest::fork(
        actor,
        ForkRequest::new(
            OperationId::new("operator-fork-digest")?,
            source.id(),
            source.version(),
            target,
            parameters("2026-10-26")?,
        ),
    );
    assert_ne!(base.digest(), parameter_request.digest());
    Ok(())
}
