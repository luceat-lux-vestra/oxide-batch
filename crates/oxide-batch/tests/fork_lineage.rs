//! M7/#552: explicit new-instance fork identity and memory atomicity evidence.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures_executor::block_on;
use oxide_batch::{
    BatchStatus, ComponentRevision, DefinitionArtifactId, DefinitionIdentity,
    DefinitionRegistration, DefinitionRegistry, DefinitionRevision, ExecutionVersion, ForkRequest,
    InMemoryJobRepository, JobFork, JobInstanceKey, JobName, JobParameters, JobRepository,
    LifecycleTransition, OperationId, RepositoryError, SequentialIdGenerator, StepName,
    SystemClock,
};

fn registration(job_name: &str) -> Result<DefinitionRegistration, Box<dyn Error>> {
    let job = JobName::new(job_name)?;
    Ok(DefinitionRegistration::new(
        DefinitionIdentity::tasklet(
            &job,
            &StepName::new("only")?,
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new("component-v1")?,
        )?,
        DefinitionArtifactId::new("artifact-v1")?,
    )?)
}

fn setup() -> Result<
    (
        InMemoryJobRepository,
        ForkRequest,
        oxide_batch::JobExecution,
    ),
    Box<dyn Error>,
> {
    block_on(async {
        let repository = InMemoryJobRepository::new(
            Arc::new(SystemClock),
            Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
        );
        let source = registration("fork-source")?;
        let target = registration("fork-target")?;
        let registry = DefinitionRegistry::new(repository.clone());
        registry.register(&source).await?;
        registry.register(&target).await?;

        let params = JobParameters::new();
        let key = JobInstanceKey::new(source.job_name().clone(), &params);
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
                &params,
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
        let request = ForkRequest::new(
            OperationId::new("fork-op-001")?,
            stopped.id(),
            stopped.version(),
            target,
            params,
        );
        Ok::<_, Box<dyn Error>>((repository, request, stopped))
    })
}

#[test]
fn plain_fork_creates_only_new_lineage_with_no_inherited_attempt() -> Result<(), Box<dyn Error>> {
    let (repository, request, source) = setup()?;
    block_on(async {
        let service = JobFork::new(repository.clone());
        let lineage = service.fork(&request).await?;
        assert_ne!(lineage.target_instance_id(), source.job_instance_id());
        assert_ne!(lineage.target_execution_id(), source.id());
        assert_eq!(lineage.source_execution_id(), source.id());
        assert_eq!(lineage.source_version(), source.version());
        assert_eq!(lineage.target(), request.target());

        let mut inspect = repository.begin().await?;
        let target = inspect
            .get_job_execution(lineage.target_execution_id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(target.metadata().status(), BatchStatus::Starting);
        assert_eq!(target.version(), ExecutionVersion::INITIAL);
        assert!(inspect.step_executions(target.id()).await?.is_empty());
        assert_eq!(
            inspect
                .job_executions(lineage.target_instance_id())
                .await?
                .len(),
            1
        );
        assert_eq!(inspect.scope_execution_attempt(target.id()).await?.get(), 1);
        assert_eq!(
            inspect.job_execution_parameters(target.id()).await?,
            *request.parameters()
        );
        let historical = inspect
            .get_job_execution(source.id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(historical, source);
        inspect.rollback().await?;

        assert_eq!(service.fork(&request).await?, lineage);
        assert_eq!(
            service.lineage(request.operation_id()).await?,
            Some(lineage.clone())
        );

        let second = ForkRequest::new(
            OperationId::new("fork-op-002")?,
            source.id(),
            source.version(),
            request.target().clone(),
            request.parameters().clone(),
        );
        assert_eq!(
            service.fork(&second).await,
            Err(RepositoryError::ForkInstanceCollision {
                id: lineage.target_instance_id()
            },)
        );
        let changed = ForkRequest::new(
            request.operation_id().clone(),
            source.id(),
            ExecutionVersion::INITIAL,
            request.target().clone(),
            request.parameters().clone(),
        );
        assert_eq!(
            service.fork(&changed).await,
            Err(RepositoryError::ForkOperationConflict)
        );
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn stale_and_nonterminal_sources_leave_no_target_or_lineage() -> Result<(), Box<dyn Error>> {
    let (repository, request, source) = setup()?;
    block_on(async {
        let service = JobFork::new(repository.clone());
        let stale = ForkRequest::new(
            OperationId::new("fork-op-stale")?,
            source.id(),
            ExecutionVersion::INITIAL,
            request.target().clone(),
            request.parameters().clone(),
        );
        assert_eq!(
            service.fork(&stale).await,
            Err(RepositoryError::ForkSourceStale)
        );
        assert!(service.lineage(stale.operation_id()).await?.is_none());
        let mut inspect = repository.begin().await?;
        assert!(
            inspect
                .find_job_instance(&stale.target_key())
                .await?
                .is_none()
        );
        inspect.rollback().await?;
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn ambiguous_fork_commit_uses_exact_operation_readback() -> Result<(), Box<dyn Error>> {
    let (repository, request, source) = setup()?;
    repository.inject_next_fork_commit_unknown();
    block_on(async {
        let service = JobFork::new(repository.clone());
        let lineage = service.fork(&request).await?;
        assert_eq!(lineage.source_execution_id(), source.id());
        assert_eq!(service.lineage(request.operation_id()).await?, Some(lineage.clone()));
        assert_eq!(service.fork(&request).await?, lineage);
        let mut check = repository.begin().await?;
        assert_eq!(check.job_executions(lineage.target_instance_id()).await?.len(), 1);
        check.rollback().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn abort_after_lineage_append_rolls_back_the_entire_target() -> Result<(), Box<dyn Error>> {
    let (repository, request, source) = setup()?;
    block_on(async {
        let mut transaction = repository.begin().await?;
        let instance = transaction
            .select_or_create_job_instance(&request.target_key()).await?
            .instance().clone();
        let execution = transaction
            .create_job_execution_with_definition_and_parameters(
                instance.id(), request.target().definition(), request.parameters()
            ).await?;
        let source_definition = transaction.scope_execution_definition(source.id()).await?;
        transaction.append_fork_lineage(&oxide_batch::ForkLineage::new(
            &request, &source_definition, [7_u8; 32], instance.id(), execution.id()
        )).await?;
        transaction.rollback().await?;

        let mut observe = repository.begin().await?;
        assert!(observe.find_job_instance(&request.target_key()).await?.is_none());
        assert_eq!(observe.get_job_execution(source.id()).await?, Some(source));
        observe.rollback().await?;
        assert!(JobFork::new(repository).lineage(request.operation_id()).await?.is_none());
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn unknown_target_registration_is_rejected_before_target_creation() -> Result<(), Box<dyn Error>> {
    let (repository, request, source) = setup()?;
    block_on(async {
        let unregistered = registration("fork-unregistered")?;
        let invalid = ForkRequest::new(
            OperationId::new("fork-unregistered-target")?,
            source.id(), source.version(), unregistered.clone(), JobParameters::new(),
        );
        assert_eq!(
            JobFork::new(repository.clone()).fork(&invalid).await,
            Err(RepositoryError::DefinitionNotRegistered {
                job_name: unregistered.job_name().clone(),
                revision: unregistered.revision().clone()
            })
        );
        let mut unit = repository.begin().await?;
        assert!(unit.find_job_instance(&invalid.target_key()).await?.is_none());
        unit.rollback().await?;
        assert!(JobFork::new(repository.clone()).lineage(invalid.operation_id()).await?.is_none());
        assert!(JobFork::new(repository).lineage(request.operation_id()).await?.is_none());
        Ok::<(), Box<dyn Error>>(())
    })
}
