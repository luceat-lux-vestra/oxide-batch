//! M7/#552: explicit source step context transformation, no restart lineage.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures_executor::block_on;
use oxide_batch::{
    BatchStatus, ComponentRevision, DefinitionArtifactId, DefinitionIdentity,
    DefinitionRegistration, DefinitionRegistry, DefinitionRevision, DefinitionStateKind,
    DefinitionStateTransform, DefinitionStateTransformError, DefinitionStateTransformId,
    DefinitionStateTransformer, DefinitionUpgradeRevision, ExecutionContext, ForkContextTransform,
    ForkRequest, InMemoryJobRepository, JobFork, JobInstanceKey, JobName, JobParameters,
    JobRepository, LifecycleTransition, NodeId, OperationId, RepositoryError,
    SequentialIdGenerator, StateLimits, StateSchemaId, StateSchemaVersion, StepName, SystemClock,
};

fn registration(name: &str) -> Result<DefinitionRegistration, Box<dyn Error>> {
    let job = JobName::new(name)?;
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

fn increment(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let count = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(count + 1);
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

#[test]
fn explicitly_transformed_fork_creates_fresh_target_and_context() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let repository = InMemoryJobRepository::new(
            Arc::new(SystemClock),
            Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
        );
        let source = registration("fork-transform-source")?;
        let target = registration("fork-transform-target")?;
        let registry = DefinitionRegistry::new(repository.clone());
        registry.register(&source).await?;
        registry.register(&target).await?;
        let params = JobParameters::new();
        let mut uow = repository.begin().await?;
        let instance = uow
            .select_or_create_job_instance(&JobInstanceKey::new(source.job_name().clone(), &params))
            .await?
            .instance()
            .clone();
        let starting = uow
            .create_job_execution_with_definition_and_parameters(
                instance.id(),
                source.definition(),
                &params,
            )
            .await?;
        let started = uow
            .transition_job_execution(
                starting.id(),
                starting.version(),
                LifecycleTransition::new(
                    BatchStatus::Started,
                    UNIX_EPOCH + Duration::from_secs(100),
                ),
            )
            .await?;
        let step = uow
            .create_step_execution(started.id(), &StepName::new("only")?)
            .await?;
        let active_step = uow
            .transition_step_execution(
                step.id(),
                step.version(),
                LifecycleTransition::new(
                    BatchStatus::Started,
                    UNIX_EPOCH + Duration::from_secs(101),
                ),
            )
            .await?;
        let context = ExecutionContext::from_transformed_payload(
            StateSchemaId::new("fork-context-v1")?,
            StateSchemaVersion::new(1)?,
            br#"{"value":7}"#,
            StateLimits::default(),
        )?;
        let committed_step = uow
            .commit_step_execution_context(active_step.id(), active_step.version(), &context)
            .await?;
        let finished_step = uow
            .transition_step_execution(
                committed_step.id(),
                committed_step.version(),
                LifecycleTransition::new(
                    BatchStatus::Completed,
                    UNIX_EPOCH + Duration::from_secs(102),
                ),
            )
            .await?;
        let terminal_source = uow
            .transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(
                    BatchStatus::Stopped,
                    UNIX_EPOCH + Duration::from_secs(103),
                ),
            )
            .await?;
        uow.commit().await?;

        let descriptor = DefinitionStateTransform::new(
            NodeId::new("only")?,
            NodeId::new("only")?,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("fork-context")?,
            StateSchemaId::new("fork-context-v1")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("fork-context-v2")?,
            StateSchemaVersion::new(2)?,
            DefinitionUpgradeRevision::new("fork-context-transform-v1")?,
        );
        let request = ForkRequest::new(
            OperationId::new("fork-transform-op-1")?,
            terminal_source.id(),
            terminal_source.version(),
            target,
            params,
        )
        .with_context_transform(ForkContextTransform::new(
            finished_step.id(),
            finished_step.version(),
            descriptor,
        ));
        let service = JobFork::new(repository.clone());
        assert_eq!(
            service.fork(&request).await,
            Err(RepositoryError::ForkTransformRejected)
        );
        assert!(service.lineage(request.operation_id()).await?.is_none());

        let transformer = DefinitionStateTransformer::new(
            DefinitionUpgradeRevision::new("fork-context-transform-v1")?,
            increment,
        );
        let lineage = service.fork_transformed(&request, &transformer).await?;
        assert_eq!(lineage.context_transform(), request.context_transform());
        assert_ne!(
            lineage.target_instance_id(),
            terminal_source.job_instance_id()
        );
        let mut observe = repository.begin().await?;
        let target_execution = observe
            .get_job_execution(lineage.target_execution_id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(target_execution.metadata().status(), BatchStatus::Starting);
        assert!(
            observe
                .step_executions(target_execution.id())
                .await?
                .is_empty()
        );
        let target_context = observe
            .scope_job_execution_context(target_execution.id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        assert_eq!(
            target_context.schema_id(),
            &StateSchemaId::new("fork-context-v2")?
        );
        assert_eq!(target_context.schema_version(), StateSchemaVersion::new(2)?);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&target_context.payload_json()?)?,
            serde_json::json!({"value":8}),
        );
        assert_eq!(
            observe.get_job_execution(terminal_source.id()).await?,
            Some(terminal_source)
        );
        observe.rollback().await?;
        assert_eq!(
            service.fork_transformed(&request, &transformer).await?,
            lineage
        );
        Ok::<(), Box<dyn Error>>(())
    })
}
