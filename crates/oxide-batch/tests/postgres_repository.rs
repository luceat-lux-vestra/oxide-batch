//! `PostgreSQL` repository contracts.

#![cfg(feature = "postgres")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contract::run_repository_contract;
use oxide_batch::{
    BatchStatus, BoxFuture, BusinessStatement, BusinessValue, CaCertificate, Checkpoint,
    ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
    ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkDeliveryMode,
    ChunkExecutionOutcome, ChunkJob, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager, Clock,
    ComponentRevision, DefinitionIdentity, DefinitionRevision, DefinitionUpgrade,
    DefinitionUpgradeKey, ExecutionContext, FailureCategory, FailureId, FailureSummary,
    ItemProcessor, ItemReader, ItemWriter, JobInstanceKey, JobLauncher, JobName, JobParameters,
    JobRepository, LifecycleTransition, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresJobRepository,
    PostgresMigrator, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
    ReaderError, RecoveryRequest, RepositoryError, SequentialIdGenerator, StateLimits,
    StateSchemaId, StateSchemaVersion, StepDefinitionUpgrade, StepName, StopSource, TlsMode,
    WriteContext, WriteOutcome, WriterError,
};
use sqlx::postgres::PgPoolOptions;

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

fn admin_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_ADMIN_TEST_URL")
        .ok()
        .or_else(migrator_url)
}

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

async fn remove_contract_rows(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance \
         WHERE job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_job_instance \
         WHERE job_name = 'repository_contract_job'",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition \
         WHERE job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_job_definition \
         WHERE job_name = 'repository_contract_job'",
    ] {
        sqlx::query(statement).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

async fn remove_job_rows(url: &str, job_name: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
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
    pool.close().await;
    Ok(())
}

async fn prepare_business_fixture(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let schema_exists: bool =
        sqlx::query_scalar("SELECT to_regnamespace('oxide_batch_business') IS NOT NULL")
            .fetch_one(&pool)
            .await?;
    if !schema_exists {
        sqlx::query("CREATE SCHEMA oxide_batch_business")
            .execute(&pool)
            .await?;
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.chunk_output (\
         job_name text NOT NULL, item bigint NOT NULL, \
         PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM oxide_batch_business.chunk_output \
         WHERE job_name LIKE 'postgres_chunk_%'",
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}

#[test]
fn configuration_bounds_and_diagnostics_are_safe() -> Result<(), Box<dyn Error>> {
    let secret_url = "postgres://runtime:do-not-disclose@db.internal/metadata";
    let secret_ca = b"private-ca-contents".to_vec();
    let config = PostgresConfig::new(secret_url)?.with_tls_mode(TlsMode::VerifyFull {
        ca_certificate: Some(CaCertificate::new(secret_ca.clone())?),
    });
    let diagnostic = format!("{config:?}");
    assert!(!diagnostic.contains(secret_url));
    assert!(!diagnostic.contains("do-not-disclose"));
    assert!(!diagnostic.contains("private-ca-contents"));

    assert_eq!(
        PostgresConfig::new(secret_url)?.with_pool_size(0).err(),
        Some(PostgresConfigError::PoolSize)
    );
    assert_eq!(
        PostgresConfig::new(secret_url)?
            .with_lock_timeout(Duration::from_secs(31))
            .err(),
        Some(PostgresConfigError::LockExceedsStatement)
    );
    assert_eq!(
        PostgresConfig::new("postgres://runtime@localhost/db?sslmode=disable").err(),
        Some(PostgresConfigError::TlsOptionInConnectionString)
    );
    assert_eq!(
        CaCertificate::new(Vec::new()).err(),
        Some(PostgresConfigError::EmptyCaCertificate)
    );
    assert_eq!(PostgresMigrator::supported_schema_version(), 1);
    Ok(())
}

#[test]
fn shared_repository_contract_passes_on_postgres() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _runtime_guard = runtime.enter();
    run_repository_contract("postgres", || {
        runtime
            .block_on(async {
                remove_contract_rows(&url)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                PostgresJobRepository::connect(
                    plaintext_config(url.clone()).map_err(|_| RepositoryError::Unavailable)?,
                    Arc::new(FixedClock(UNIX_EPOCH)),
                )
                .await
            })
            .map_err(|_| RepositoryError::Unavailable)
    })?;
    runtime.block_on(remove_contract_rows(&url))?;
    Ok(())
}

#[test]
fn concurrent_identical_launches_create_one_active_execution() -> Result<(), Box<dyn Error>> {
    const CONTENDERS: usize = 8;

    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_contract_rows(&url).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(100))),
        )
        .await?;
        let barrier = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
        let key = JobInstanceKey::new(
            JobName::new("repository_contract_job")?,
            &JobParameters::new(),
        );
        let mut handles = Vec::with_capacity(CONTENDERS);
        for _ in 0..CONTENDERS {
            let repository = repository.clone();
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut unit = repository.begin().await?;
                barrier.wait().await;
                let instance = unit
                    .select_or_create_job_instance(&key)
                    .await?
                    .instance()
                    .clone();
                unit.create_job_execution(instance.id()).await?;
                unit.commit().await
            }));
        }
        let mut committed = 0;
        let mut active_rejections = 0;
        for handle in handles {
            match handle.await? {
                Ok(()) => committed += 1,
                Err(RepositoryError::ExecutionAlreadyActive { .. }) => active_rejections += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!(committed, 1);
        assert_eq!(active_rejections, CONTENDERS - 1);

        let mut inspection = repository.begin().await?;
        let instance = inspection
            .find_job_instance(&key)
            .await?
            .ok_or("canonical instance was not committed")?;
        assert_eq!(inspection.job_executions(instance.id()).await?.len(), 1);
        inspection.rollback().await?;
        repository.close().await?;
        remove_contract_rows(&url).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn migration_is_idempotent_when_migrator_fixture_is_available() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let config = plaintext_config(url)?;
    runtime.block_on(PostgresMigrator::migrate(&config))?;
    runtime.block_on(PostgresMigrator::migrate(&config))?;
    Ok(())
}

#[test]
fn newer_schema_is_rejected_without_guessing_compatibility() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query("UPDATE oxide_batch.ob_schema_version SET version = 2")
            .execute(&pool)
            .await?;
        let result = PostgresJobRepository::connect(
            plaintext_config(url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH)),
        )
        .await;
        sqlx::query("UPDATE oxide_batch.ob_schema_version SET version = 1")
            .execute(&pool)
            .await?;
        pool.close().await;
        let Err(error) = result else {
            return Err("newer schema was accepted".into());
        };
        assert_eq!(
            error,
            RepositoryError::NewerSchema {
                current: 2,
                supported: 1,
            }
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn disconnected_transaction_has_unknown_commit_and_pool_recovers() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(admin_url) = admin_url() else {
        eprintln!(
            "skipped: neither OXIDEBATCH_POSTGRES_ADMIN_TEST_URL nor \
             OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is set"
        );
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_contract_rows(&runtime_url).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(100))),
        )
        .await?;
        let mut unit = repository.begin().await?;
        let key = JobInstanceKey::new(
            JobName::new("repository_contract_job")?,
            &JobParameters::new(),
        );
        unit.select_or_create_job_instance(&key).await?;

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        let terminated: bool = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'oxide-batch' \
             AND state = 'idle in transaction' \
             ORDER BY backend_start LIMIT 1",
        )
        .fetch_one(&admin)
        .await?;
        assert!(terminated);
        assert_eq!(
            unit.commit().await,
            Err(RepositoryError::CommitOutcomeUnknown)
        );

        let inspection = repository.begin().await?;
        inspection.rollback().await?;
        repository.close().await?;
        admin.close().await;
        remove_contract_rows(&runtime_url).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

struct ChunkReader {
    items: VecDeque<i64>,
}

impl ItemReader<i64> for ChunkReader {
    fn read<'a>(
        &'a mut self,
        _context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<i64>, ReaderError>> {
        let item = self.items.pop_front();
        Box::pin(async move { Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)) })
    }
}

struct IdentityProcessor;

impl ItemProcessor<i64, i64> for IdentityProcessor {
    fn process<'a>(
        &'a self,
        item: &'a i64,
        _context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<i64>, ProcessorError>> {
        Box::pin(async move { Ok(ProcessOutcome::Item(*item)) })
    }
}

struct EnlistedWriter {
    job_name: &'static str,
    fail_after_write: bool,
}

impl ItemWriter<i64> for EnlistedWriter {
    fn write<'a>(
        &'a self,
        items: &'a [i64],
        mut context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        Box::pin(async move {
            let transaction = context.transaction().ok_or_else(WriterError::new)?;
            for item in items {
                let values = [
                    BusinessValue::text(self.job_name),
                    BusinessValue::i64(*item),
                ];
                transaction
                    .execute(BusinessStatement::new(
                        "INSERT INTO oxide_batch_business.chunk_output \
                         (job_name, item) VALUES ($1, $2)",
                        &values,
                    ))
                    .await
                    .map_err(WriterError::from_error)?;
            }
            if self.fail_after_write {
                return Err(WriterError::new());
            }
            Ok(WriteOutcome::Written)
        })
    }
}

struct TestCompletion {
    fail: bool,
}

impl ChunkCompletion for TestCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        if self.fail {
            Box::pin(async { Err(ChunkCompletionError::new()) })
        } else {
            Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
        }
    }
}

fn chunk_checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": "postgres.chunk.position",
        "schema_version": 1,
        "payload": {"position": position},
    }))?;
    Ok(Checkpoint::from_json(&bytes, StateLimits::default())?)
}

fn chunk_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"postgres.chunk.context","schema_version":1,"payload":{"source":"fixture"}}"#,
        StateLimits::default(),
    )?)
}

fn postgres_chunk_transactions(
    repository: &PostgresJobRepository,
) -> PostgresChunkTransactionManager {
    let provider: Arc<dyn PostgresChunkStateProvider> = Arc::new(
        |committed: oxide_batch::ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint =
                chunk_checkpoint(position).map_err(|_| PostgresChunkStateError::new())?;
            let context = chunk_context().map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    );
    PostgresChunkTransactionManager::new(repository.clone(), provider)
}

async fn business_items(url: &str, job_name: &str) -> Result<Vec<i64>, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let items = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.chunk_output \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(items)
}

async fn launch_postgres_chunk(
    repository: &PostgresJobRepository,
    job_name: &'static str,
    fail_after_write: bool,
    fail_completion: bool,
) -> Result<
    (
        oxide_batch::ChunkLaunchReport,
        PostgresChunkTransactionManager,
    ),
    Box<dyn Error>,
> {
    let transactions = postgres_chunk_transactions(repository);
    let step = ChunkStep::new(
        StepName::new("import")?,
        ChunkSize::new(2)?,
        Box::new(ChunkReader {
            items: VecDeque::from([10, 20, 30]),
        }),
        Arc::new(IdentityProcessor),
        Arc::new(EnlistedWriter {
            job_name,
            fail_after_write,
        }),
        Arc::new(transactions.clone()),
        Arc::new(TestCompletion {
            fail: fail_completion,
        }),
    );
    let mut job = ChunkJob::new(
        JobName::new(job_name)?,
        step,
        DefinitionRevision::new("postgres-fixture-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            ChunkRestartContract::new(
                StateSchemaId::new("postgres.chunk.position")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("postgres.chunk.context")?,
                StateSchemaVersion::new(1)?,
                ChunkDeliveryMode::AtomicSameResource,
            ),
        ),
    )?;
    let clock = FixedClock(UNIX_EPOCH + Duration::from_secs(500));
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let launcher = JobLauncher::new(repository, &clock, &ids);
    let (_source, token) = StopSource::new();
    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &token)
        .await?;
    Ok((report, transactions))
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end restart scenario keeps transactional phases and evidence contiguous"
)]
fn durable_restart_requires_compatible_definition_and_inherits_checkpoint()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_durable_restart";
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(700))),
        )
        .await?;
        let job_name = JobName::new(JOB)?;
        let source_step = StepName::new("import-v1")?;
        let target_step = StepName::new("import-v2")?;
        let restart_contract = || -> Result<ChunkRestartContract, Box<dyn Error>> {
            Ok(ChunkRestartContract::new(
                StateSchemaId::new("postgres.chunk.position")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("postgres.chunk.context")?,
                StateSchemaVersion::new(1)?,
                ChunkDeliveryMode::AtomicSameResource,
            ))
        };
        let v1 = DefinitionIdentity::chunk(
            &job_name,
            &source_step,
            ChunkSize::new(2)?,
            DefinitionRevision::new("v1")?,
            &ChunkComponentRevisions::new(
                ComponentRevision::new("reader-v1")?,
                ComponentRevision::new("processor-v1")?,
                ComponentRevision::new("writer-v1")?,
                ComponentRevision::new("checkpoint-codec-v1")?,
                restart_contract()?,
            ),
        )?;
        let v2 = DefinitionIdentity::chunk(
            &job_name,
            &target_step,
            ChunkSize::new(2)?,
            DefinitionRevision::new("v2")?,
            &ChunkComponentRevisions::new(
                ComponentRevision::new("reader-v2-compatible")?,
                ComponentRevision::new("processor-v2-compatible")?,
                ComponentRevision::new("writer-v2-compatible")?,
                ComponentRevision::new("checkpoint-codec-v1")?,
                restart_contract()?,
            ),
        )?;
        let drifted_v1 = DefinitionIdentity::chunk(
            &job_name,
            &source_step,
            ChunkSize::new(2)?,
            DefinitionRevision::new("v1")?,
            &ChunkComponentRevisions::new(
                ComponentRevision::new("reader-v1-drifted")?,
                ComponentRevision::new("processor-v1")?,
                ComponentRevision::new("writer-v1")?,
                ComponentRevision::new("checkpoint-codec-v1")?,
                restart_contract()?,
            ),
        )?;
        let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());

        let mut create = repository.begin().await?;
        let instance = create
            .select_or_create_job_instance(&key)
            .await?
            .instance()
            .clone();
        let first = create
            .create_job_execution_with_definition(instance.id(), &v1)
            .await?;
        let first_step = create
            .create_step_execution(first.id(), &source_step)
            .await?;
        create.commit().await?;

        let started_at = UNIX_EPOCH + Duration::from_secs(701);
        let mut start = repository.begin().await?;
        let started_job = start
            .transition_job_execution(
                first.id(),
                first.version(),
                LifecycleTransition::new(BatchStatus::Started, started_at),
            )
            .await?;
        start
            .transition_step_execution(
                first_step.id(),
                first_step.version(),
                LifecycleTransition::new(BatchStatus::Started, started_at),
            )
            .await?;
        start.commit().await?;

        let manager = postgres_chunk_transactions(&repository);
        let scope = ChunkTransactionContext::new(first.id(), first_step.id());
        let mut chunk = manager.begin_for(scope).await?;
        chunk
            .commit(ChunkCounts::new(
                ChunkCount::new(2),
                ChunkCount::new(2),
                ChunkCount::new(2),
                ChunkCount::ZERO,
            )?)
            .await?;
        let committed = manager.load_committed_state(scope).await?;
        assert_eq!(committed.checkpoint(), &chunk_checkpoint(2)?);

        let failed_at = UNIX_EPOCH + Duration::from_secs(702);
        let mut fail = repository.begin().await?;
        fail.transition_step_execution(
            first_step.id(),
            committed.step_execution().version(),
            LifecycleTransition::failed(
                failed_at,
                FailureSummary::new(FailureCategory::UserComponent, FailureId::new(901)?),
            ),
        )
        .await?;
        fail.transition_job_execution(
            first.id(),
            started_job.version(),
            LifecycleTransition::failed(
                failed_at,
                FailureSummary::new(FailureCategory::UserComponent, FailureId::new(902)?),
            ),
        )
        .await?;
        fail.commit().await?;

        let mut incompatible = repository.begin().await?;
        assert_eq!(
            incompatible
                .create_job_execution_with_definition(instance.id(), &drifted_v1)
                .await,
            Err(RepositoryError::DefinitionDrift {
                job_name: job_name.clone(),
                revision: DefinitionRevision::new("v1")?,
            })
        );
        incompatible.rollback().await?;

        let mut incompatible = repository.begin().await?;
        assert_eq!(
            incompatible
                .create_job_execution_with_definition(instance.id(), &v2)
                .await,
            Err(RepositoryError::IncompatibleDefinition {
                instance_id: instance.id(),
            })
        );
        incompatible.rollback().await?;

        let upgrade = DefinitionUpgrade::new(
            DefinitionUpgradeKey::new("v1-to-v2")?,
            v1,
            v2.clone(),
            [StepDefinitionUpgrade::new(
                source_step.clone(),
                target_step.clone(),
            )],
        )?;
        let mut register = repository.begin().await?;
        register
            .register_definition_upgrade(&job_name, &upgrade)
            .await?;
        register.commit().await?;

        let mut restart = repository.begin().await?;
        let second = restart
            .create_job_execution_with_definition(instance.id(), &v2)
            .await?;
        let second_step = restart
            .create_step_execution(second.id(), &target_step)
            .await?;
        restart.commit().await?;
        assert_ne!(first.id(), second.id());
        assert_ne!(first_step.id(), second_step.id());

        let resumed = manager
            .load_committed_state(ChunkTransactionContext::new(second.id(), second_step.id()))
            .await?;
        assert_eq!(resumed.checkpoint(), &chunk_checkpoint(2)?);
        assert_eq!(resumed.execution_context(), &chunk_context()?);
        assert_eq!(resumed.step_execution().metadata().counts().committed(), 1);

        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn unknown_execution_requires_audited_postgres_recovery() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_explicit_recovery";
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(800))),
        )
        .await?;
        let key = JobInstanceKey::new(JobName::new(JOB)?, &JobParameters::new());
        let mut create = repository.begin().await?;
        let instance = create
            .select_or_create_job_instance(&key)
            .await?
            .instance()
            .clone();
        let execution = create.create_job_execution(instance.id()).await?;
        create.commit().await?;
        let mut mark_unknown = repository.begin().await?;
        let unknown = mark_unknown
            .transition_job_execution(
                execution.id(),
                execution.version(),
                LifecycleTransition::new(
                    BatchStatus::Unknown,
                    UNIX_EPOCH + Duration::from_secs(800),
                ),
            )
            .await?;
        mark_unknown.commit().await?;

        let request = RecoveryRequest::mark_failed(
            unknown.version(),
            "DURABLE_INSPECTION_COMPLETE",
            "operator-correlation-7",
            [7; 32],
            FailureCategory::PermanentInfrastructure,
            FailureId::new(903)?,
        )?;
        let mut recover = repository.begin().await?;
        let recovered = recover
            .recover_job_execution(execution.id(), &request)
            .await?;
        recover.commit().await?;
        assert_eq!(
            recovered.execution().metadata().status(),
            BatchStatus::Failed
        );

        let mut inspect = repository.begin().await?;
        assert_eq!(
            inspect.recovery_decision(execution.id()).await?,
            Some(recovered.decision().clone())
        );
        let restarted = inspect.create_job_execution(instance.id()).await?;
        inspect.commit().await?;
        assert_ne!(restarted.id(), execution.id());

        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_chunk_commit_and_rollback_are_atomic() -> Result<(), Box<dyn Error>> {
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
        prepare_business_fixture(&migrator_url).await?;
        for job_name in [
            "postgres_chunk_commit",
            "postgres_chunk_rollback",
            "postgres_chunk_ack_failure",
        ] {
            remove_job_rows(&runtime_url, job_name).await?;
        }
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(500))),
        )
        .await?;

        let (committed, manager) =
            launch_postgres_chunk(&repository, "postgres_chunk_commit", false, false).await?;
        assert_eq!(
            committed.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Completed
        );
        let committed_scope = ChunkTransactionContext::new(
            committed.launch().job_execution().id(),
            committed.launch().step_execution().id(),
        );
        let durable = manager.load_committed_state(committed_scope).await?;
        assert_eq!(
            durable.step_execution().metadata().counts(),
            oxide_batch::ExecutionCounts::new(3, 3, 3, 0, 2, 0)
        );
        assert_eq!(durable.checkpoint(), &chunk_checkpoint(3)?);
        assert_eq!(
            business_items(&runtime_url, "postgres_chunk_commit").await?,
            [10, 20, 30]
        );

        let (rolled_back, manager) =
            launch_postgres_chunk(&repository, "postgres_chunk_rollback", true, false).await?;
        assert!(matches!(
            rolled_back.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Failed(_)
        ));
        let rolled_back_scope = ChunkTransactionContext::new(
            rolled_back.launch().job_execution().id(),
            rolled_back.launch().step_execution().id(),
        );
        let durable = manager.load_committed_state(rolled_back_scope).await?;
        assert_eq!(
            durable.step_execution().metadata().counts(),
            oxide_batch::ExecutionCounts::default()
        );
        assert_ne!(durable.checkpoint(), &chunk_checkpoint(2)?);
        assert!(
            business_items(&runtime_url, "postgres_chunk_rollback")
                .await?
                .is_empty()
        );

        let (ack_failed, manager) =
            launch_postgres_chunk(&repository, "postgres_chunk_ack_failure", false, true).await?;
        assert!(matches!(
            ack_failed.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Failed(_)
        ));
        let ack_scope = ChunkTransactionContext::new(
            ack_failed.launch().job_execution().id(),
            ack_failed.launch().step_execution().id(),
        );
        let durable = manager.load_committed_state(ack_scope).await?;
        assert_eq!(durable.checkpoint(), &chunk_checkpoint(2)?);
        assert_eq!(durable.step_execution().metadata().counts().committed(), 1);
        assert_eq!(
            business_items(&runtime_url, "postgres_chunk_ack_failure").await?,
            [10, 20]
        );

        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

async fn create_started_chunk_scope(
    repository: &PostgresJobRepository,
    job_name: &str,
) -> Result<ChunkTransactionContext, Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new(job_name)?, &JobParameters::new());
    let mut create = repository.begin().await?;
    let instance = create
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let job = create.create_job_execution(instance.id()).await?;
    let step = create
        .create_step_execution(job.id(), &StepName::new("import")?)
        .await?;
    create.commit().await?;

    let now = UNIX_EPOCH + Duration::from_secs(700);
    let mut start = repository.begin().await?;
    start
        .transition_job_execution(
            job.id(),
            job.version(),
            LifecycleTransition::new(BatchStatus::Started, now),
        )
        .await?;
    start
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, now),
        )
        .await?;
    start.commit().await?;
    Ok(ChunkTransactionContext::new(job.id(), step.id()))
}

#[test]
fn postgres_chunk_conflict_rolls_back_losing_business_write() -> Result<(), Box<dyn Error>> {
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
        const JOB: &str = "postgres_chunk_conflict";
        prepare_business_fixture(&migrator_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(700))),
        )
        .await?;
        let scope = create_started_chunk_scope(&repository, JOB).await?;
        let manager = postgres_chunk_transactions(&repository);
        let mut winner = manager.begin_for(scope).await?;
        let mut loser = manager.begin_for(scope).await?;
        let winner_values = [BusinessValue::text(JOB), BusinessValue::i64(1)];
        winner
            .business_transaction()
            .ok_or("winner was not enlisted")?
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.chunk_output \
                 (job_name, item) VALUES ($1, $2)",
                &winner_values,
            ))
            .await?;
        let loser_values = [BusinessValue::text(JOB), BusinessValue::i64(2)];
        loser
            .business_transaction()
            .ok_or("loser was not enlisted")?
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.chunk_output \
                 (job_name, item) VALUES ($1, $2)",
                &loser_values,
            ))
            .await?;
        let counts = ChunkCounts::new(
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::ZERO,
        )?;
        winner.commit(counts).await?;
        assert_eq!(
            loser.commit(counts).await,
            Err(ChunkTransactionError::NotCommitted)
        );
        assert_eq!(business_items(&runtime_url, JOB).await?, [1]);
        let durable = manager.load_committed_state(scope).await?;
        assert_eq!(durable.step_execution().metadata().counts().committed(), 1);
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_chunk_disconnect_is_known_not_committed_before_commit() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let Some(admin_url) = admin_url() else {
        eprintln!(
            "skipped: neither OXIDEBATCH_POSTGRES_ADMIN_TEST_URL nor \
             OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is set"
        );
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_chunk_disconnect";
        prepare_business_fixture(&migrator_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(700))),
        )
        .await?;
        let scope = create_started_chunk_scope(&repository, JOB).await?;
        let manager = postgres_chunk_transactions(&repository);
        let mut transaction = manager.begin_for(scope).await?;
        let values = [BusinessValue::text(JOB), BusinessValue::i64(1)];
        transaction
            .business_transaction()
            .ok_or("transaction was not enlisted")?
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.chunk_output \
                 (job_name, item) VALUES ($1, $2)",
                &values,
            ))
            .await?;

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        let terminated: bool = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'oxide-batch' \
             AND state = 'idle in transaction' \
             ORDER BY backend_start DESC LIMIT 1",
        )
        .fetch_one(&admin)
        .await?;
        assert!(terminated);
        let counts = ChunkCounts::new(
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::new(1),
            ChunkCount::ZERO,
        )?;
        assert_eq!(
            transaction.commit(counts).await,
            Err(ChunkTransactionError::NotCommitted)
        );
        assert!(business_items(&runtime_url, JOB).await?.is_empty());
        let durable = manager.load_committed_state(scope).await?;
        assert_eq!(
            durable.step_execution().metadata().counts(),
            oxide_batch::ExecutionCounts::default()
        );
        admin.close().await;
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}
