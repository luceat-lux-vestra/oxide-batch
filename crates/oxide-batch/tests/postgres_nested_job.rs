//! `PostgreSQL` contract evidence for M7 nested-job durable linkage.

#![cfg(feature = "postgres")]

mod security;

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    ActorRef, BatchStatus, Clock, ComponentRevision, DefinitionIdentity, DefinitionRevision,
    JobExecution, JobInstance, JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository,
    LifecycleTransition, NestedJobLinkRequest, NodeId, OperationId, ParameterName, ParameterRole,
    ParameterValue, PostgresConfig, PostgresJobRepository, PurgeBatchBound, PurgePlanRequest,
    ReasonCode, RepositoryError, RetentionService, StepName, TerminalStatusSet, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

async fn repository(url: &str, seconds: u64) -> Result<PostgresJobRepository, Box<dyn Error>> {
    Ok(PostgresJobRepository::connect(
        PostgresConfig::new(url.to_owned())?.with_tls_mode(TlsMode::Plaintext),
        Arc::new(FixedClock(at(seconds))),
    )
    .await?)
}

fn child_definition(job: &str) -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new(job)?,
        &StepName::new("child_step")?,
        DefinitionRevision::new("nested-child-v1")?,
        &ComponentRevision::new("nested-child-tasklet-v1")?,
    )?)
}

fn child_parameters(trace: &str) -> Result<JobParameters, Box<dyn Error>> {
    Ok(JobParameters::try_from_iter([
        (
            ParameterName::new("account")?,
            JobParameter::new(
                ParameterValue::string("acct-42")?,
                ParameterRole::Identifying,
            ),
        ),
        (
            ParameterName::new("trace")?,
            JobParameter::new(
                ParameterValue::string(trace)?,
                ParameterRole::NonIdentifying,
            ),
        ),
        (
            ParameterName::new("limit")?,
            JobParameter::new(ParameterValue::from(7_u64), ParameterRole::NonIdentifying),
        ),
        (
            ParameterName::new("enabled")?,
            JobParameter::new(ParameterValue::from(true), ParameterRole::NonIdentifying),
        ),
    ])?)
}

async fn create_parent_attempt(
    repository: &PostgresJobRepository,
    job: &str,
) -> Result<(JobInstance, JobExecution), Box<dyn Error>> {
    let parameters = JobParameters::new();
    let key = JobInstanceKey::new(JobName::new(job)?, &parameters);
    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let execution = unit.create_job_execution(instance.id()).await?;
    unit.commit().await?;
    Ok((instance, execution))
}

async fn stop_execution(
    repository: &PostgresJobRepository,
    execution: &JobExecution,
    started_at: SystemTime,
    stopped_at: SystemTime,
) -> Result<JobExecution, Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let started = unit
        .transition_job_execution(
            execution.id(),
            execution.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await?;
    let stopped = unit
        .transition_job_execution(
            started.id(),
            started.version(),
            LifecycleTransition::new(BatchStatus::Stopped, stopped_at),
        )
        .await?;
    unit.commit().await?;
    Ok(stopped)
}

async fn job_execution_exists(url: &str, id: u64) -> Result<bool, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
    )
    .bind(i64::try_from(id)?)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(exists)
}

async fn nested_link_exists(url: &str, parent_execution_id: u64) -> Result<bool, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_nested_job_link \
         WHERE parent_job_execution_id = $1)",
    )
    .bind(i64::try_from(parent_execution_id)?)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(exists)
}

#[test]
fn postgres_nested_job_completed_child_reuses_exact_committed_link() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repository = repository(&url, 1_000).await?;
        let (parent_instance, parent_execution) =
            create_parent_attempt(&repository, "pg_nested_parent_completed").await?;
        let node = NodeId::new("child")?;
        let definition = child_definition("pg_nested_child_completed")?;
        let parameters = child_parameters("trace-original")?;

        let mut create = repository.begin().await?;
        let first = create
            .create_nested_job_link(&NestedJobLinkRequest::new(
                parent_instance.id(),
                parent_execution.id(),
                node.clone(),
                definition,
                parameters.clone(),
                at(1_001),
            ))
            .await?;
        create.commit().await?;

        let mut complete = repository.begin().await?;
        let child = complete
            .get_job_execution(first.child_job_execution_id())
            .await?
            .ok_or("linked child execution missing")?;
        let started = complete
            .transition_job_execution(
                child.id(),
                child.version(),
                LifecycleTransition::new(BatchStatus::Started, at(1_002)),
            )
            .await?;
        complete
            .transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(BatchStatus::Completed, at(1_003)),
            )
            .await?;
        let observed = complete
            .observe_nested_job_terminal(parent_execution.id(), &node, at(1_004))
            .await?;
        assert_eq!(
            observed
                .terminal()
                .map(oxide_batch::NestedJobTerminalObservation::status),
            Some(BatchStatus::Completed)
        );
        complete.commit().await?;

        stop_execution(&repository, &parent_execution, at(1_005), at(1_006)).await?;
        let (_, restarted_parent) =
            create_parent_attempt(&repository, "pg_nested_parent_completed").await?;
        let mut continuation = repository.begin().await?;
        let reused = continuation
            .continue_nested_job_link(
                restarted_parent.id(),
                &node,
                parent_execution.id(),
                at(1_007),
            )
            .await?;
        assert_eq!(
            reused.child_job_execution_id(),
            first.child_job_execution_id()
        );
        assert_eq!(
            reused.child_job_instance_id(),
            first.child_job_instance_id()
        );
        assert_eq!(
            reused
                .terminal()
                .map(oxide_batch::NestedJobTerminalObservation::status),
            Some(BatchStatus::Completed)
        );
        assert_eq!(
            continuation
                .nested_job_parameters(restarted_parent.id(), &node)
                .await?,
            parameters
        );
        assert_eq!(
            continuation
                .job_executions(first.child_job_instance_id())
                .await?
                .len(),
            1
        );
        continuation.commit().await?;
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_nested_job_stopped_child_restarts_same_instance_with_complete_parameters()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repository = repository(&url, 2_000).await?;
        let (parent_instance, parent_execution) =
            create_parent_attempt(&repository, "pg_nested_parent_stopped").await?;
        let node = NodeId::new("child")?;
        let parameters = child_parameters("trace-restart")?;

        let mut create = repository.begin().await?;
        let first = create
            .create_nested_job_link(&NestedJobLinkRequest::new(
                parent_instance.id(),
                parent_execution.id(),
                node.clone(),
                child_definition("pg_nested_child_stopped")?,
                parameters.clone(),
                at(2_001),
            ))
            .await?;
        create.commit().await?;

        let mut read = repository.begin().await?;
        let child = read
            .get_job_execution(first.child_job_execution_id())
            .await?
            .ok_or("linked child execution missing")?;
        read.rollback().await?;
        stop_execution(&repository, &child, at(2_002), at(2_003)).await?;
        stop_execution(&repository, &parent_execution, at(2_004), at(2_005)).await?;

        let (_, restarted_parent) =
            create_parent_attempt(&repository, "pg_nested_parent_stopped").await?;
        let mut continuation = repository.begin().await?;
        let restarted = continuation
            .continue_nested_job_link(
                restarted_parent.id(),
                &node,
                parent_execution.id(),
                at(2_006),
            )
            .await?;
        assert_eq!(
            restarted.child_job_instance_id(),
            first.child_job_instance_id()
        );
        assert_ne!(
            restarted.child_job_execution_id(),
            first.child_job_execution_id()
        );
        assert!(restarted.terminal().is_none());
        assert_eq!(
            continuation
                .nested_job_parameters(restarted_parent.id(), &node)
                .await?,
            parameters
        );
        assert_eq!(
            continuation
                .job_executions(first.child_job_instance_id())
                .await?
                .len(),
            2
        );
        continuation.commit().await?;
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_nested_job_unknown_child_stays_unresolved_without_terminal_or_duplicate()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repository = repository(&url, 3_000).await?;
        let (parent_instance, parent_execution) =
            create_parent_attempt(&repository, "pg_nested_parent_unknown").await?;
        let node = NodeId::new("child")?;

        let mut create = repository.begin().await?;
        let first = create
            .create_nested_job_link(&NestedJobLinkRequest::new(
                parent_instance.id(),
                parent_execution.id(),
                node.clone(),
                child_definition("pg_nested_child_unknown")?,
                child_parameters("trace-unknown")?,
                at(3_001),
            ))
            .await?;
        create.commit().await?;
        stop_execution(&repository, &parent_execution, at(3_002), at(3_003)).await?;
        let (_, restarted_parent) =
            create_parent_attempt(&repository, "pg_nested_parent_unknown").await?;

        let mut mark_unknown = repository.begin().await?;
        let child = mark_unknown
            .get_job_execution(first.child_job_execution_id())
            .await?
            .ok_or("linked child execution missing")?;
        mark_unknown
            .transition_job_execution(
                child.id(),
                child.version(),
                LifecycleTransition::new(BatchStatus::Unknown, at(3_004)),
            )
            .await?;
        mark_unknown.commit().await?;

        let mut ambiguous = repository.begin().await?;
        assert!(matches!(
            ambiguous
                .continue_nested_job_link(
                    restarted_parent.id(),
                    &node,
                    parent_execution.id(),
                    at(3_005),
                )
                .await,
            Err(RepositoryError::NestedJobChildUnresolved {
                child_execution_id,
                status: BatchStatus::Unknown,
            }) if child_execution_id == first.child_job_execution_id()
        ));
        assert!(matches!(
            ambiguous
                .observe_nested_job_terminal(parent_execution.id(), &node, at(3_006))
                .await,
            Err(RepositoryError::NestedJobChildUnresolved {
                child_execution_id,
                status: BatchStatus::Unknown,
            }) if child_execution_id == first.child_job_execution_id()
        ));
        assert_eq!(
            ambiguous
                .job_executions(first.child_job_instance_id())
                .await?
                .len(),
            1
        );
        let durable = ambiguous
            .nested_job_link(parent_execution.id(), &node)
            .await?
            .ok_or("nested link missing")?;
        assert!(durable.terminal().is_none());
        ambiguous.rollback().await?;
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_nested_job_existing_link_rejects_nonidentifying_parameter_drift()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repository = repository(&url, 4_000).await?;
        let (parent_instance, parent_execution) =
            create_parent_attempt(&repository, "pg_nested_parent_drift").await?;
        let node = NodeId::new("child")?;
        let definition = child_definition("pg_nested_child_drift")?;

        let mut first = repository.begin().await?;
        let link = first
            .create_nested_job_link(&NestedJobLinkRequest::new(
                parent_instance.id(),
                parent_execution.id(),
                node.clone(),
                definition.clone(),
                child_parameters("trace-original")?,
                at(4_001),
            ))
            .await?;
        first.commit().await?;

        let mut duplicate = repository.begin().await?;
        assert_eq!(
            duplicate
                .create_nested_job_link(&NestedJobLinkRequest::new(
                    parent_instance.id(),
                    parent_execution.id(),
                    node,
                    definition,
                    child_parameters("trace-drifted")?,
                    at(4_002),
                ))
                .await,
            Err(RepositoryError::NestedJobStateCorrupt)
        );
        assert_eq!(
            duplicate
                .job_executions(link.child_job_instance_id())
                .await?
                .len(),
            1
        );
        duplicate.rollback().await?;
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_nested_job_parent_purge_cascades_link_without_deleting_child()
-> Result<(), Box<dyn Error>> {
    const PARENT_JOB: &str = "pg_nested_parent_retention";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repository = repository(&url, 5_000).await?;
        let (parent_instance, parent_execution) =
            create_parent_attempt(&repository, PARENT_JOB).await?;
        let node = NodeId::new("child")?;

        let mut create = repository.begin().await?;
        let link = create
            .create_nested_job_link(&NestedJobLinkRequest::new(
                parent_instance.id(),
                parent_execution.id(),
                node,
                child_definition("pg_nested_child_retention")?,
                child_parameters("trace-retention")?,
                at(5_001),
            ))
            .await?;
        create.commit().await?;

        assert!(nested_link_exists(&url, parent_execution.id().get()).await?);
        assert!(job_execution_exists(&url, link.child_job_execution_id().get()).await?);

        let mut complete = repository.begin().await?;
        let started = complete
            .transition_job_execution(
                parent_execution.id(),
                parent_execution.version(),
                LifecycleTransition::new(BatchStatus::Started, at(5_001)),
            )
            .await?;
        complete
            .transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(BatchStatus::Completed, at(5_002)),
            )
            .await?;
        complete.commit().await?;
        repository.close().await?;

        let later = FixedClock(at(5_002) + Duration::from_hours(30 * 24));
        let purging = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            Arc::new(later),
        )
        .await?;
        let retention = RetentionService::new(purging, Arc::new(later));
        let request = PurgePlanRequest::new(
            JobName::new(PARENT_JOB)?,
            TerminalStatusSet::new([BatchStatus::Completed])?,
            Duration::from_hours(24),
            PurgeBatchBound::new(10)?,
        )?;
        let plan = retention.plan_purge(&request).await?;
        assert_eq!(plan.candidates().len(), 1);
        retention
            .apply_purge(
                OperationId::new("m7-nested-job-retention")?,
                ActorRef::new("operator:m7-retention")?,
                ReasonCode::new("SCHEDULED_PURGE")?,
                &plan,
            )
            .await?;

        assert!(!nested_link_exists(&url, parent_execution.id().get()).await?);
        assert!(!job_execution_exists(&url, parent_execution.id().get()).await?);
        assert!(job_execution_exists(&url, link.child_job_execution_id().get()).await?);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_nested_job_security_policy_separates_link_writers() -> Result<(), Box<dyn Error>> {
    const DATABASE: &str = "oxide_batch_m7_nested_security";
    let Some(admin) = security::admin_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_ADMIN_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        security::recreate_database(&admin, DATABASE).await?;
        let database = security::with_database(&admin, DATABASE)?;
        security::apply_script(&database, &security::fixtures().join("roles.sql")).await?;
        oxide_batch::PostgresMigrator::migrate(&security::fixture_config(database.clone())?)
            .await?;
        security::apply_script(&database, &security::fixtures().join("grants.sql")).await?;

        let password = format!("m7_nested_{}", std::process::id());
        let roles = [
            "oxide_batch_m5_runtime",
            "oxide_batch_m5_explorer",
            "oxide_batch_m5_operator",
            "oxide_batch_m5_retention",
        ];
        for role in roles {
            security::run_statement(
                &database,
                format!("ALTER ROLE {role} PASSWORD '{password}'"),
            )
            .await?;
        }

        let insert = "INSERT INTO oxide_batch.ob_nested_job_link \
                      SELECT * FROM oxide_batch.ob_nested_job_link WHERE false";
        let update = "UPDATE oxide_batch.ob_nested_job_link \
                      SET terminal_status = terminal_status WHERE false";
        let runtime_url = security::with_role(&database, roles[0], &password)?;
        assert_eq!(
            security::attempt_statement(&runtime_url, insert).await?,
            security::StatementOutcome::Succeeded
        );
        assert_eq!(
            security::attempt_statement(&runtime_url, update).await?,
            security::StatementOutcome::Succeeded
        );

        for role in &roles[1..] {
            let url = security::with_role(&database, role, &password)?;
            for statement in [insert, update] {
                let outcome = security::attempt_statement(&url, statement).await?;
                assert_eq!(outcome.code(), Some(security::INSUFFICIENT_PRIVILEGE));
            }
        }

        security::drop_database(&admin, DATABASE).await?;
        for role in roles {
            security::run_statement(&admin, format!("DROP ROLE IF EXISTS {role}")).await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })
}
