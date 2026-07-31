//! Process-kill evidence for the M2 `PostgreSQL` restart boundary.

#![cfg(feature = "postgres")]

use std::error::Error;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, BusinessStatement, BusinessValue, Checkpoint, ChunkCommitReceipt,
    ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkFaultProgress,
    ChunkRestartContract, ChunkSize, ChunkTransactionContext, ChunkTransactionManager, Clock,
    ComponentRevision, DefinitionIdentity, DefinitionRevision, ExecutionContext, FailureCategory,
    FailureId, JobInstanceKey, JobName, JobParameters, JobRepository, LifecycleTransition,
    PostgresChunkStateError, PostgresChunkStateProvider, PostgresChunkTransactionManager,
    PostgresConfig, PostgresMigrator, RecoveryRequest, StateLimits, StateSchemaId,
    StateSchemaVersion, StepName, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const CRASH_MODE_ENV: &str = "OXIDEBATCH_M2_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 86;
const STEP_NAME: &str = "import";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeCommit,
    AfterCommit,
}

impl CrashPoint {
    const fn job_name(self) -> &'static str {
        match self {
            Self::BeforeCommit => "m2_crash_before_commit",
            Self::AfterCommit => "m2_crash_after_commit",
        }
    }

    const fn environment_value(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before-commit",
            Self::AfterCommit => "after-commit",
        }
    }

    const fn committed_position_after_crash(self) -> u64 {
        match self {
            Self::BeforeCommit => 2,
            Self::AfterCommit => 4,
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-commit" => Ok(Self::BeforeCommit),
            "after-commit" => Ok(Self::AfterCommit),
            _ => Err("unknown M2 crash mode".into()),
        }
    }
}

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

fn plaintext_config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn restart_contract() -> Result<ChunkRestartContract, Box<dyn Error>> {
    Ok(ChunkRestartContract::new(
        StateSchemaId::new("m2.crash.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("m2.crash.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

fn definition(point: CrashPoint) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let job_name = JobName::new(point.job_name())?;
    let step_name = StepName::new(STEP_NAME)?;
    Ok(DefinitionIdentity::chunk(
        &job_name,
        &step_name,
        ChunkSize::new(2)?,
        DefinitionRevision::new("m2-crash-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            restart_contract()?,
        ),
    )?)
}

fn checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": "m2.crash.position",
        "schema_version": 1,
        "payload": {"position": position},
    }))?;
    Ok(Checkpoint::from_json(&bytes, StateLimits::default())?)
}

fn execution_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"m2.crash.context","schema_version":1,"payload":{"fixture":"m2-process-kill"}}"#,
        StateLimits::default(),
    )?)
}

fn transaction_manager(
    repository: &oxide_batch::PostgresJobRepository,
) -> PostgresChunkTransactionManager {
    let provider: Arc<dyn PostgresChunkStateProvider> = Arc::new(
        |committed: oxide_batch::ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint = checkpoint(position).map_err(|_| PostgresChunkStateError::new())?;
            let context = execution_context().map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    );
    PostgresChunkTransactionManager::new(repository.clone(), provider)
}

async fn prepare_fixture(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.m2_crash_output (\
         job_name text NOT NULL, item bigint NOT NULL, \
         PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
    )
    .bind(job_name)
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
    )
    .bind(job_name)
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
    )
    .bind(job_name)
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    sqlx::query(
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
    )
    .bind(job_name)
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM oxide_batch_business.m2_crash_output WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn business_items(url: &str, job_name: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let items = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.m2_crash_output \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(items)
}

async fn commit_items(
    manager: &PostgresChunkTransactionManager,
    scope: ChunkTransactionContext,
    job_name: &str,
    items: &[i64],
) -> Result<(), Box<dyn Error>> {
    let mut transaction = manager.begin_for(scope).await?;
    let business = transaction
        .business_transaction()
        .ok_or("PostgreSQL crash fixture was not enlisted")?;
    for item in items {
        let values = [BusinessValue::text(job_name), BusinessValue::i64(*item)];
        business
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.m2_crash_output \
                 (job_name, item) VALUES ($1, $2)",
                &values,
            ))
            .await?;
    }
    let count = ChunkCount::new(u64::try_from(items.len())?);
    transaction
        .commit(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
        )
        .await?;
    Ok(())
}

async fn run_crash_worker(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let repository = oxide_batch::PostgresJobRepository::connect(
        plaintext_config(url)?,
        Arc::new(FixedClock(UNIX_EPOCH + Duration::from_mins(15))),
    )
    .await?;
    let job_name = JobName::new(point.job_name())?;
    let step_name = StepName::new(STEP_NAME)?;
    let definition = definition(point)?;
    let key = JobInstanceKey::new(job_name, &JobParameters::new());

    let mut create = repository.begin().await?;
    let instance = create
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let job = create
        .create_job_execution_with_definition(instance.id(), &definition)
        .await?;
    let step = create.create_step_execution(job.id(), &step_name).await?;
    create.commit().await?;

    let started_at = UNIX_EPOCH + Duration::from_secs(901);
    let mut start = repository.begin().await?;
    start
        .transition_job_execution(
            job.id(),
            job.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await?;
    start
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await?;
    start.commit().await?;

    let scope = ChunkTransactionContext::new(job.id(), step.id());
    let manager = transaction_manager(&repository);
    commit_items(&manager, scope, point.job_name(), &[10, 20]).await?;

    let mut interrupted = manager.begin_for(scope).await?;
    let business = interrupted
        .business_transaction()
        .ok_or("PostgreSQL crash fixture was not enlisted")?;
    for item in [30, 40] {
        let values = [
            BusinessValue::text(point.job_name()),
            BusinessValue::i64(item),
        ];
        business
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.m2_crash_output \
                 (job_name, item) VALUES ($1, $2)",
                &values,
            ))
            .await?;
    }

    if point == CrashPoint::BeforeCommit {
        std::process::exit(CRASH_EXIT_CODE);
    }

    let count = ChunkCount::new(2);
    interrupted
        .commit(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
        )
        .await?;
    std::process::exit(CRASH_EXIT_CODE);
}

fn spawn_crash_worker(point: CrashPoint) -> Result<ExitStatus, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    Ok(Command::new(executable)
        .arg("--exact")
        .arg("crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, point.environment_value())
        .status()?)
}

fn checkpoint_position(value: &Checkpoint) -> Result<u64, Box<dyn Error>> {
    let envelope: serde_json::Value = serde_json::from_slice(&value.to_json()?)?;
    envelope
        .pointer("/payload/position")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "checkpoint position is missing".into())
}

#[allow(
    clippy::too_many_lines,
    reason = "the crash, durable inspection, audited recovery, and restart assertions form one evidence chain"
)]
async fn inspect_recover_and_restart(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let repository = oxide_batch::PostgresJobRepository::connect(
        plaintext_config(url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(902))),
    )
    .await?;
    let key = JobInstanceKey::new(JobName::new(point.job_name())?, &JobParameters::new());
    let definition = definition(point)?;
    let step_name = StepName::new(STEP_NAME)?;

    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("crashed worker did not durably create the job instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("crashed worker did not durably create the job execution")?;
    let original_step = inspect
        .step_executions(original.id())
        .await?
        .into_iter()
        .next()
        .ok_or("crashed worker did not durably create the step execution")?;
    inspect.rollback().await?;
    assert_eq!(original.metadata().status(), BatchStatus::Started);
    assert_eq!(original_step.metadata().status(), BatchStatus::Started);

    let manager = transaction_manager(&repository);
    let original_scope = ChunkTransactionContext::new(original.id(), original_step.id());
    let durable_after_crash = manager.load_committed_state(original_scope).await?;
    assert_eq!(
        checkpoint_position(durable_after_crash.checkpoint())?,
        point.committed_position_after_crash()
    );
    assert_eq!(
        durable_after_crash
            .step_execution()
            .metadata()
            .counts()
            .read(),
        point.committed_position_after_crash()
    );

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "PROCESS_EXIT_INSPECTED",
        "m2-crash-harness",
        [42; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(945)?,
    )?;
    let mut recover = repository.begin().await?;
    let recovered = recover
        .recover_job_execution(original.id(), &request)
        .await?;
    recover.commit().await?;
    assert_eq!(recovered.decision().prior_status(), BatchStatus::Started);
    assert_eq!(recovered.decision().resulting_status(), BatchStatus::Failed);
    assert!(!format!("{:?}", recovered.decision()).contains("[42"));

    let mut restart = repository.begin().await?;
    let restarted = restart
        .create_job_execution_with_definition(instance.id(), &definition)
        .await?;
    let restarted_step = restart
        .create_step_execution(restarted.id(), &step_name)
        .await?;
    restart.commit().await?;
    assert_ne!(restarted.id(), original.id());
    assert_ne!(restarted_step.id(), original_step.id());

    let resumed_scope = ChunkTransactionContext::new(restarted.id(), restarted_step.id());
    let inherited = manager.load_committed_state(resumed_scope).await?;
    assert_eq!(
        checkpoint_position(inherited.checkpoint())?,
        point.committed_position_after_crash()
    );

    let resumed_at = UNIX_EPOCH + Duration::from_secs(903);
    let mut start = repository.begin().await?;
    let started_job = start
        .transition_job_execution(
            restarted.id(),
            restarted.version(),
            LifecycleTransition::new(BatchStatus::Started, resumed_at),
        )
        .await?;
    start
        .transition_step_execution(
            restarted_step.id(),
            restarted_step.version(),
            LifecycleTransition::new(BatchStatus::Started, resumed_at),
        )
        .await?;
    start.commit().await?;

    if point == CrashPoint::BeforeCommit {
        commit_items(&manager, resumed_scope, point.job_name(), &[30, 40]).await?;
    }

    let completed_state = manager.load_committed_state(resumed_scope).await?;
    assert_eq!(checkpoint_position(completed_state.checkpoint())?, 4);
    assert_eq!(
        completed_state.step_execution().metadata().counts(),
        oxide_batch::ExecutionCounts::new(4, 4, 4, 0, 2, 0)
    );
    assert_eq!(
        business_items(&url, point.job_name()).await?,
        [10, 20, 30, 40]
    );

    let completed_at = UNIX_EPOCH + Duration::from_secs(904);
    let mut complete = repository.begin().await?;
    complete
        .transition_step_execution(
            restarted_step.id(),
            completed_state.step_execution().version(),
            LifecycleTransition::new(BatchStatus::Completed, completed_at),
        )
        .await?;
    complete
        .transition_job_execution(
            restarted.id(),
            started_job.version(),
            LifecycleTransition::new(BatchStatus::Completed, completed_at),
        )
        .await?;
    complete.commit().await?;

    let mut final_inspection = repository.begin().await?;
    let attempts = final_inspection.job_executions(instance.id()).await?;
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts
            .last()
            .ok_or("restart attempt was not inspectable")?
            .metadata()
            .status(),
        BatchStatus::Completed
    );
    assert_eq!(
        final_inspection
            .recovery_decision(original.id())
            .await?
            .ok_or("recovery audit was not inspectable")?
            .reason_code(),
        "PROCESS_EXIT_INSPECTED"
    );
    final_inspection.rollback().await?;
    repository.close().await?;
    Ok(())
}

fn run_parent_scenario(point: CrashPoint) -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        PostgresMigrator::migrate(&plaintext_config(migrator_url.clone())?).await?;
        prepare_fixture(&migrator_url, point.job_name()).await
    })?;

    let status = spawn_crash_worker(point)?;
    assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

    runtime.block_on(inspect_recover_and_restart(point, runtime_url.clone()))?;
    runtime.block_on(prepare_fixture(&migrator_url, point.job_name()))?;
    Ok(())
}

#[test]
fn crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(CRASH_MODE_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let url = runtime_url().ok_or("crash worker database URL is missing")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_crash_worker(point, url))?;
    Err("crash worker returned without terminating".into())
}

#[test]
fn crash_before_commit_replays_chunk() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::BeforeCommit)
}

#[test]
fn crash_after_commit_does_not_replay_chunk() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::AfterCommit)
}
