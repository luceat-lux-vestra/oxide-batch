//! M7/#552: durable transformed-fork integration on real PostgreSQL 15/18.
#![cfg(feature = "postgres")]

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, Clock, ComponentRevision, DefinitionArtifactId, DefinitionIdentity,
    DefinitionRegistration, DefinitionRegistry, DefinitionRevision, DefinitionStateKind,
    DefinitionStateTransform, DefinitionStateTransformError, DefinitionStateTransformId,
    DefinitionStateTransformer, DefinitionUpgradeRevision, ExecutionContext, ForkContextTransform,
    ForkRequest, JobFork, JobInstanceKey, JobName, JobParameters, JobRepository,
    LifecycleTransition, NodeId, OperationId, PostgresConfig, PostgresJobRepository,
    RepositoryError, StateLimits, StateSchemaId, StateSchemaVersion, StepName, TlsMode,
};

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn registration(name: &str) -> Result<DefinitionRegistration, Box<dyn Error>> {
    let job = JobName::new(name)?;
    Ok(DefinitionRegistration::new(
        DefinitionIdentity::tasklet(
            &job,
            &StepName::new("only")?,
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new("component-v1")?,
        )?,
        DefinitionArtifactId::new("fork-artifact-v1")?,
    )?)
}

fn increment(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let original = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(original + 1);
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

#[test]
fn transformed_fork_persists_across_reconnect_without_restart_inference()
-> Result<(), Box<dyn Error>> {
    let Ok(url) = std::env::var("OXIDEBATCH_POSTGRES_TEST_URL") else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL not set");
        return Ok(());
    };
    // Runs against the disposable conformance database. A unique identity
    // prevents cross-shard/test contamination without deleting durable lineage.
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let source_name = format!("fork_pg_source_{stamp}");
    let target_name = format!("fork_pg_target_{stamp}");
    let operation = format!("fork-pg-{stamp}");
    let config = PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let clock = Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(90)));
            let repository = PostgresJobRepository::connect(config.clone(), clock.clone()).await?;
            let source = registration(&source_name)?;
            let target = registration(&target_name)?;
            let registry = DefinitionRegistry::new(repository.clone());
            registry.register(&source).await?;
            registry.register(&target).await?;
            let parameters = JobParameters::new();

            let mut uow = repository.begin().await?;
            let instance = uow
                .select_or_create_job_instance(&JobInstanceKey::new(
                    source.job_name().clone(),
                    &parameters,
                ))
                .await?
                .instance()
                .clone();
            let started_job = uow
                .create_job_execution_with_definition_and_parameters(
                    instance.id(),
                    source.definition(),
                    &parameters,
                )
                .await?;
            let started_job = uow
                .transition_job_execution(
                    started_job.id(),
                    started_job.version(),
                    LifecycleTransition::new(
                        BatchStatus::Started,
                        UNIX_EPOCH + Duration::from_secs(91),
                    ),
                )
                .await?;
            let step = uow
                .create_step_execution(started_job.id(), &StepName::new("only")?)
                .await?;
            let step = uow
                .transition_step_execution(
                    step.id(),
                    step.version(),
                    LifecycleTransition::new(
                        BatchStatus::Started,
                        UNIX_EPOCH + Duration::from_secs(92),
                    ),
                )
                .await?;
            let context = ExecutionContext::from_transformed_payload(
                StateSchemaId::new("fork-pg-v1")?,
                StateSchemaVersion::new(1)?,
                br#"{"value":11}"#,
                StateLimits::default(),
            )?;
            let step = uow
                .commit_step_execution_context(step.id(), step.version(), &context)
                .await?;
            let step = uow
                .transition_step_execution(
                    step.id(),
                    step.version(),
                    LifecycleTransition::new(
                        BatchStatus::Completed,
                        UNIX_EPOCH + Duration::from_secs(93),
                    ),
                )
                .await?;
            let stopped = uow
                .transition_job_execution(
                    started_job.id(),
                    started_job.version(),
                    LifecycleTransition::new(
                        BatchStatus::Stopped,
                        UNIX_EPOCH + Duration::from_secs(94),
                    ),
                )
                .await?;
            uow.commit().await?;

            let descriptor = DefinitionStateTransform::new(
                NodeId::new("only")?,
                NodeId::new("only")?,
                DefinitionStateKind::ExecutionContext,
                DefinitionStateTransformId::new("pg-fork-job-context")?,
                StateSchemaId::new("fork-pg-v1")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("fork-pg-v2")?,
                StateSchemaVersion::new(2)?,
                DefinitionUpgradeRevision::new("pg-fork-transform-v1")?,
            );
            let request = ForkRequest::new(
                OperationId::new(operation)?,
                stopped.id(),
                stopped.version(),
                target,
                parameters,
            )
            .with_context_transform(ForkContextTransform::new(
                step.id(),
                step.version(),
                descriptor,
            ));
            let transformer = DefinitionStateTransformer::new(
                DefinitionUpgradeRevision::new("pg-fork-transform-v1")?,
                increment,
            );
            let lineage = JobFork::new(repository.clone())
                .fork_transformed(&request, &transformer)
                .await?;
            assert_ne!(lineage.target_instance_id(), stopped.job_instance_id());
            assert_ne!(lineage.target_execution_id(), stopped.id());
            drop(repository);

            // A new connection sees the exact immutable descriptor and the new
            // context; replay must not create an extra execution or invent restart.
            let reopened = PostgresJobRepository::connect(config, clock).await?;
            let service = JobFork::new(reopened.clone());
            assert_eq!(
                service.lineage(request.operation_id()).await?,
                Some(lineage.clone())
            );
            assert_eq!(
                service.fork_transformed(&request, &transformer).await?,
                lineage
            );
            let mut check = reopened.begin().await?;
            let target_execution = check
                .get_job_execution(lineage.target_execution_id())
                .await?
                .ok_or(RepositoryError::ForkStateCorrupt)?;
            assert_eq!(target_execution.metadata().status(), BatchStatus::Starting);
            assert_eq!(
                check
                    .job_executions(lineage.target_instance_id())
                    .await?
                    .len(),
                1
            );
            assert!(
                check
                    .step_executions(lineage.target_execution_id())
                    .await?
                    .is_empty()
            );
            let actual = check
                .scope_job_execution_context(lineage.target_execution_id())
                .await?
                .ok_or(RepositoryError::ForkStateCorrupt)?;
            assert_eq!(actual.schema_id(), &StateSchemaId::new("fork-pg-v2")?);
            assert_eq!(actual.schema_version(), StateSchemaVersion::new(2)?);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&actual.payload_json()?)?,
                serde_json::json!({"value":12}),
            );
            assert_eq!(check.get_job_execution(stopped.id()).await?, Some(stopped));
            check.rollback().await?;
            Ok::<(), Box<dyn Error>>(())
        })
}
