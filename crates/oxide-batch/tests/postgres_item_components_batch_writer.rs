//! #149 evidence: `PostgresBatchWriter` enlistment, bounded batching,
//! rollback, and disconnect-before-commit behavior against a real
//! `PostgreSQL` chunk transaction.
//!
//! Mirrors `postgres_repository.rs`'s `launch_postgres_chunk` pattern (full
//! `ChunkJob`/`JobLauncher` path) so this writer is exercised the same way
//! `EnlistedWriter` already is there, substituting [`PostgresBatchWriter`]
//! for that test-local writer. Genuine commit-response-ambiguity
//! (`ChunkTransactionError::CommitOutcomeUnknown`) is a property of the
//! shared `commit_postgres_connection` helper this writer never touches, and
//! is already proven by
//! `postgres_repository.rs::disconnect_during_commit_never_guesses_outcome`;
//! this file proves this writer composes correctly with that machinery, not
//! that the machinery itself is correct.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise.

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::item_components::{PostgresBatchMode, PostgresBatchWriter, postgres_batch_writer};
use oxide_batch::{
    BoxFuture, BusinessValue, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCounts,
    ChunkDeliveryMode, ChunkExecutionOutcome, ChunkJob, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager, Clock,
    ComponentRevision, DefinitionRevision, ExecutionContext, FailureCategory, ItemProcessor,
    ItemReader, JobLauncher, JobName, JobParameters, JobRepository, PostgresChunkStateError,
    PostgresChunkStateProvider, PostgresChunkTransactionManager, PostgresConfig,
    PostgresConfigError, PostgresJobRepository, ProcessContext, ProcessOutcome, ProcessorError,
    ReadContext, ReadOutcome, ReaderError, SequentialIdGenerator, StateLimits, StateSchemaId,
    StateSchemaVersion, StepName, StopSource, TlsMode, WriteContext, WriteOutcome,
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
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

fn admin_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_ADMIN_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(runtime_url)
}

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
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
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.batch_writer_output (\
         job_name text NOT NULL, item bigint NOT NULL, PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}

async fn remove_job_rows(url: &str, job_name: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
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
    sqlx::query("DELETE FROM oxide_batch_business.batch_writer_output WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn business_items(url: &str, job_name: &str) -> Result<Vec<i64>, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let items = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.batch_writer_output \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(items)
}

struct ChunkReader {
    items: VecDeque<i64>,
}

impl ItemReader<i64> for ChunkReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        let item = self.items.pop_front();
        Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

struct IdentityProcessor;

impl ItemProcessor<i64, i64> for IdentityProcessor {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

struct NoCompletion;

impl ChunkCompletion for NoCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

fn chunk_checkpoint(position: u64) -> Result<oxide_batch::Checkpoint, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": "postgres.batch-writer.chunk.position",
        "schema_version": 1,
        "payload": {"position": position},
    }))?;
    Ok(oxide_batch::Checkpoint::from_json(
        &bytes,
        StateLimits::default(),
    )?)
}

fn chunk_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"postgres.batch-writer.chunk.context","schema_version":1,"payload":{}}"#,
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

fn business_writer_for(
    job_name: &'static str,
    mode: PostgresBatchMode,
) -> PostgresBatchWriter<i64> {
    postgres_batch_writer(
        "INSERT INTO oxide_batch_business.batch_writer_output (job_name, item) VALUES",
        None::<&str>,
        2,
        mode,
        move |item: &i64| vec![BusinessValue::text(job_name), BusinessValue::i64(*item)],
    )
    .unwrap()
}

async fn launch_postgres_chunk(
    repository: &PostgresJobRepository,
    job_name: &'static str,
    items: Vec<i64>,
    mode: PostgresBatchMode,
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
        ChunkReader {
            items: VecDeque::from(items),
        },
        IdentityProcessor,
        business_writer_for(job_name, mode),
        Arc::new(transactions.clone()),
        Arc::new(NoCompletion),
    );
    let mut job = ChunkJob::new(
        JobName::new(job_name)?,
        step,
        DefinitionRevision::new("postgres-149-fixture-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            ChunkRestartContract::new(
                StateSchemaId::new("postgres.batch-writer.chunk.position")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("postgres.batch-writer.chunk.context")?,
                StateSchemaVersion::new(1)?,
                ChunkDeliveryMode::AtomicSameResource,
            ),
        ),
    )?;
    let clock = FixedClock(UNIX_EPOCH + Duration::from_mins(15));
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let launcher = JobLauncher::new(repository, &clock, &ids);
    let (_source, token) = StopSource::new();
    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &token)
        .await?;
    Ok((report, transactions))
}

#[test]
fn multi_row_values_writes_every_item_across_chunk_boundaries() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_149_multi_row";
        prepare_business_fixture(&runtime_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_mins(15))),
        )
        .await?;

        let (report, _manager) = launch_postgres_chunk(
            &repository,
            JOB,
            vec![10, 20, 30, 40, 50],
            PostgresBatchMode::MultiRowValues {
                max_parameters_per_statement: 4, // 2 columns/row => 2 rows/statement
            },
        )
        .await?;
        assert_eq!(
            report.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Completed
        );
        assert_eq!(
            business_items(&runtime_url, JOB).await?,
            [10, 20, 30, 40, 50]
        );

        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn per_row_statements_write_every_item() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_149_per_row";
        prepare_business_fixture(&runtime_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_mins(15))),
        )
        .await?;

        let (report, _manager) = launch_postgres_chunk(
            &repository,
            JOB,
            vec![1, 2, 3],
            PostgresBatchMode::PerRowStatements,
        )
        .await?;
        assert_eq!(
            report.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Completed
        );
        assert_eq!(business_items(&runtime_url, JOB).await?, [1, 2, 3]);

        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn constraint_violation_rolls_back_the_whole_chunk_with_no_partial_write()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_149_constraint_violation";
        prepare_business_fixture(&runtime_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_mins(15))),
        )
        .await?;

        // The chunk size is 2 and the two items in the first (and only)
        // chunk share the same `item` value, so their unique
        // `(job_name, item)` primary key collides mid-batch.
        let (report, manager) = launch_postgres_chunk(
            &repository,
            JOB,
            vec![7, 7],
            PostgresBatchMode::PerRowStatements,
        )
        .await?;
        assert!(matches!(
            report.chunk().ok_or("chunk report missing")?.outcome(),
            ChunkExecutionOutcome::Failed(_)
        ));
        assert!(business_items(&runtime_url, JOB).await?.is_empty());
        let scope = ChunkTransactionContext::new(
            report.launch().job_execution().id(),
            report.launch().step_execution().id(),
        );
        let durable = manager.load_committed_state(scope).await?;
        assert_eq!(
            durable.step_execution().metadata().counts(),
            oxide_batch::ExecutionCounts::new(0, 0, 0, 0, 0, 1)
        );

        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn non_enlisted_write_context_is_rejected_without_a_second_connection() -> Result<(), Box<dyn Error>>
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let writer = business_writer_for(
            "postgres_149_unit_level_writer",
            PostgresBatchMode::multi_row_values(),
        );
        let (_source, token) = StopSource::new();
        let result = oxide_batch::ItemWriter::write(
            &writer,
            &[1_i64, 2, 3],
            WriteContext::non_transactional(&token),
        )
        .await;
        let Err(error) = result else {
            return Err("non-enlisted write unexpectedly succeeded".into());
        };
        assert_eq!(error.category(), FailureCategory::UnsupportedCapability);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn empty_batch_and_stop_are_handled_without_touching_the_transaction() -> Result<(), Box<dyn Error>>
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let writer = business_writer_for(
            "postgres_149_unit_level_writer",
            PostgresBatchMode::multi_row_values(),
        );

        let (_source, token) = StopSource::new();
        let outcome =
            oxide_batch::ItemWriter::write(&writer, &[], WriteContext::non_transactional(&token))
                .await?;
        assert_eq!(outcome, WriteOutcome::Written);

        let (stop_source, stop_token) = StopSource::new();
        stop_source.request_stop();
        let outcome = oxide_batch::ItemWriter::write(
            &writer,
            &[1_i64],
            WriteContext::non_transactional(&stop_token),
        )
        .await?;
        assert_eq!(outcome, WriteOutcome::Stopped);
        Ok::<(), Box<dyn Error>>(())
    })
}

/// A connection lost after this writer's statements executed but before any
/// part of `commit()`'s own sequence (the durable step-execution `UPDATE`,
/// then the literal `COMMIT`) ever runs is a known not-committed outcome,
/// never guessed as success: this writer's statements, though sent
/// successfully to the now-dead connection, never became durable because the
/// enclosing transaction was never committed. This is a different, earlier
/// fault window than genuine commit-ambiguity (the response to `COMMIT`
/// itself being lost) -- that boundary is `commit_postgres_connection`'s,
/// already proven by
/// `postgres_repository.rs::disconnect_during_commit_never_guesses_outcome`,
/// and is unchanged by this writer: it never touches commit/rollback itself,
/// only the borrowed enlisted transaction's `execute`.
#[test]
fn disconnect_before_commit_leaves_writer_statements_uncommitted() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(admin_url) = admin_url() else {
        eprintln!("skipped: no admin URL available");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        const JOB: &str = "postgres_149_unknown_commit";
        prepare_business_fixture(&runtime_url).await?;
        remove_job_rows(&runtime_url, JOB).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_mins(15))),
        )
        .await?;
        let scope = create_started_chunk_scope(&repository, JOB).await?;
        let manager = postgres_chunk_transactions(&repository);
        let mut transaction = manager.begin_for(scope).await?;
        {
            let business = transaction
                .business_transaction()
                .ok_or("expected an enlisted business transaction")?;
            let writer = business_writer_for(JOB, PostgresBatchMode::PerRowStatements);
            let (_source, token) = StopSource::new();
            let context = WriteContext::enlisted(&token, business);
            let outcome = oxide_batch::ItemWriter::write(&writer, &[1_i64, 2], context).await?;
            assert_eq!(outcome, WriteOutcome::Written);
        }

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        let terminated: bool = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'oxide-batch' AND state = 'idle in transaction' \
             ORDER BY backend_start DESC LIMIT 1",
        )
        .fetch_one(&admin)
        .await?;
        assert!(
            terminated,
            "expected exactly one idle-in-transaction oxide-batch backend"
        );

        let counts = ChunkCounts::new(
            oxide_batch::ChunkCount::new(2),
            oxide_batch::ChunkCount::new(2),
            oxide_batch::ChunkCount::new(2),
            oxide_batch::ChunkCount::ZERO,
        )?;
        let result = transaction
            .commit(counts, oxide_batch::ChunkFaultProgress::NONE)
            .await;
        assert_eq!(result.err(), Some(ChunkTransactionError::NotCommitted));

        // No partial write survives: the writer's statements executed
        // against the (now-dead) connection but never became durable.
        assert!(business_items(&runtime_url, JOB).await?.is_empty());

        admin.close().await;
        repository.close().await?;
        remove_job_rows(&runtime_url, JOB).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

async fn create_started_chunk_scope(
    repository: &PostgresJobRepository,
    job_name: &str,
) -> Result<ChunkTransactionContext, Box<dyn Error>> {
    use oxide_batch::{BatchStatus, JobInstanceKey, LifecycleTransition};

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

    let now = UNIX_EPOCH + Duration::from_mins(15);
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
