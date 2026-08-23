//! #149 real process-kill crash/restart evidence for `PostgresCursorReader`
//! and `PostgresPagingReader`, mirroring `postgres_crash_recovery.rs`'s M2
//! pattern exactly: the crashing work runs in a genuinely separate OS
//! process (this same test binary, re-invoked via `--exact
//! crash_worker_process` with an environment variable selecting the
//! scenario), which calls `std::process::exit` rather than unwinding --  no
//! Rust `Drop` runs, no clean session termination is sent, so `PostgreSQL`
//! observes exactly the abrupt connection loss a `kill -9`'d process would
//! produce. The parent test process asserts the child's exit code, then
//! inspects durable state and restarts with a fresh reader/stream pair in
//! its own (uncrashed) process.
//!
//! Deliberately lower-level than the full `ChunkJob`/`JobLauncher` path (see
//! `crates/oxide-batch-test/tests/postgres_item_components_db_restart.rs`
//! for that evidence, using in-process cooperative-stop injection instead):
//! `std::process::exit` cannot be triggered mid-chunk from inside a running
//! `ChunkStep` without a new production hook, so this file drives
//! `PostgresChunkTransactionManager`/`ItemStream` directly, exactly as
//! `postgres_crash_recovery.rs` already does for the same reason.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL` and
//! `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`; skips (not fails) otherwise.

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::too_many_lines
)]

use std::error::Error;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::item_components::{
    KeysetColumn, PostgresCursorFormat, PostgresPagingFormat, PostgresRow, postgres_cursor_reader,
    postgres_paging_reader,
};
use oxide_batch::{
    BatchStatus, BusinessStatement, BusinessValue, ChunkCommitReceipt, ChunkCount, ChunkCounts,
    ChunkFaultProgress, ChunkTransactionContext, ChunkTransactionManager, Clock,
    ComponentStreamIdentity, ItemReader, ItemStream, JobInstanceKey, JobName, JobParameters,
    JobRepository, LifecycleTransition, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresJobRepository,
    PostgresMigrator, ReadContext, ReadOutcome, ReaderError, StateLimits, StepName,
    StreamOpenContext, StreamUpdateContext, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const CRASH_MODE_ENV: &str = "OXIDEBATCH_149_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 87;

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReaderKind {
    Cursor,
    Paging,
}

impl ReaderKind {
    const fn job_name(self) -> &'static str {
        match self {
            Self::Cursor => "oxide_batch_149_cursor_crash",
            Self::Paging => "oxide_batch_149_paging_crash",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "cursor" => Ok(Self::Cursor),
            "paging" => Ok(Self::Paging),
            _ => Err("unknown #149 crash mode".into()),
        }
    }

    const fn environment_value(self) -> &'static str {
        match self {
            Self::Cursor => "cursor",
            Self::Paging => "paging",
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

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn namespace(kind: ReaderKind) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(format!("oxide-batch-test.postgres-149-crash-{kind:?}"))
        .expect("static identity is valid")
}

impl std::fmt::Debug for ReaderKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cursor => "cursor",
            Self::Paging => "paging",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BusinessRow {
    id: i64,
}

fn map_row(row: &PostgresRow<'_>) -> Result<BusinessRow, ReaderError> {
    Ok(BusinessRow { id: row.i64("id")? })
}

fn key_columns() -> Vec<KeysetColumn> {
    vec![KeysetColumn::text("sort_key"), KeysetColumn::i64("id")]
}

fn reader_base_query(job_name: &str) -> String {
    format!(
        "SELECT sort_key, id FROM oxide_batch_business.postgres_149_crash_input \
         WHERE job_name = '{job_name}'"
    )
}

fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
    Arc::new(
        |committed: oxide_batch::ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint_bytes = format!(
                r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.postgres-149-crash","schema_version":1,"payload":{{"position":{position}}}}}"#
            );
            let checkpoint = oxide_batch::Checkpoint::from_json(
                checkpoint_bytes.as_bytes(),
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
            let context = oxide_batch::ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.postgres-149-crash","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    )
}

async fn prepare_fixture(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.postgres_149_crash_input (\
         job_name text NOT NULL, sort_key text NOT NULL, id bigint NOT NULL, \
         PRIMARY KEY (job_name, id))",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.postgres_149_crash_output (\
         job_name text NOT NULL, item bigint NOT NULL, PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM oxide_batch.ob_component_state WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
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
    sqlx::query("DELETE FROM oxide_batch_business.postgres_149_crash_input WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM oxide_batch_business.postgres_149_crash_output WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    for id in 1..=20_i64 {
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_149_crash_input \
             (job_name, sort_key, id) VALUES ($1, 'k', $2)",
        )
        .bind(job_name)
        .bind(id)
        .execute(&pool)
        .await?;
    }
    pool.close().await;
    Ok(())
}

async fn business_items(url: &str, job_name: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let items = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.postgres_149_crash_output \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(items)
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

/// Writes `items` for `job_name` through a real enlisted business
/// transaction and commits it together with `envelope` (the reader's own
/// candidate checkpoint) -- deliberately a raw `BusinessStatement` writer,
/// not `PostgresBatchWriter`, so this fixture's evidence about the
/// *readers* does not depend on the batch writer's own correctness (mirrors
/// `postgres_crash_recovery.rs::commit_items`'s same reasoning).
async fn commit_chunk(
    manager: &PostgresChunkTransactionManager,
    scope: ChunkTransactionContext,
    job_name: &str,
    items: &[i64],
    envelope: oxide_batch::ComponentStateEnvelope,
) -> Result<(), Box<dyn Error>> {
    let mut transaction = manager.begin_for(scope).await?;
    let business = transaction
        .business_transaction()
        .ok_or("#149 crash fixture was not enlisted")?;
    for item in items {
        let values = [BusinessValue::text(job_name), BusinessValue::i64(*item)];
        business
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.postgres_149_crash_output \
                 (job_name, item) VALUES ($1, $2)",
                &values,
            ))
            .await?;
    }
    let count = ChunkCount::new(u64::try_from(items.len())?);
    transaction
        .commit_with_component_state(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
            &[envelope],
        )
        .await?;
    Ok(())
}

fn inherited_envelope(
    all: Vec<oxide_batch::ComponentStateEnvelope>,
    namespace: &ComponentStreamIdentity,
) -> Option<oxide_batch::ComponentStateEnvelope> {
    all.into_iter()
        .find(|envelope| envelope.namespace() == namespace)
}

/// Runs entirely inside the crashing child process: reads and durably
/// commits the first 7 rows (business output + reader checkpoint in one
/// transaction), then reads 5 more rows into memory *without* committing
/// them, then calls `std::process::exit` -- no graceful shutdown, no
/// `ItemStream::close`, no connection close, exactly what a killed process
/// would produce.
async fn run_crash_worker(kind: ReaderKind, url: String) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        plaintext_config(url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH)),
    )
    .await?;
    let job_name = kind.job_name();
    let scope = create_started_chunk_scope(&repository, job_name).await?;
    let manager = PostgresChunkTransactionManager::new(repository.clone(), state_provider());
    let namespace = namespace(kind);

    let config = plaintext_config(url)?;
    let mut delivered = Vec::new();
    match kind {
        ReaderKind::Cursor => {
            let (mut reader, stream, _contract) = postgres_cursor_reader(
                config,
                reader_base_query(job_name),
                key_columns(),
                PostgresCursorFormat::new().with_fetch_size(3),
                map_row,
                namespace.clone(),
            )?;
            let (_source, token) = oxide_batch::StopSource::new();
            stream.open(StreamOpenContext::new(None, &token)).await?;
            for _ in 0..7 {
                if let ReadOutcome::Item(item) = reader.read(ReadContext::new(&token)).await? {
                    delivered.push(item.id);
                }
            }
            let envelope = stream.update(StreamUpdateContext::new(&token)).await?;
            commit_chunk(&manager, scope, job_name, &delivered, envelope).await?;
            for _ in 0..5 {
                reader.read(ReadContext::new(&token)).await?;
            }
        }
        ReaderKind::Paging => {
            let (mut reader, stream, _contract) = postgres_paging_reader(
                config,
                reader_base_query(job_name),
                key_columns(),
                PostgresPagingFormat::new().with_page_size(3),
                map_row,
                namespace.clone(),
            )?;
            let (_source, token) = oxide_batch::StopSource::new();
            stream.open(StreamOpenContext::new(None, &token)).await?;
            for _ in 0..7 {
                if let ReadOutcome::Item(item) = reader.read(ReadContext::new(&token)).await? {
                    delivered.push(item.id);
                }
            }
            let envelope = stream.update(StreamUpdateContext::new(&token)).await?;
            commit_chunk(&manager, scope, job_name, &delivered, envelope).await?;
            for _ in 0..5 {
                reader.read(ReadContext::new(&token)).await?;
            }
        }
    }

    std::process::exit(CRASH_EXIT_CODE);
}

fn spawn_crash_worker(kind: ReaderKind) -> Result<ExitStatus, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    Ok(Command::new(executable)
        .arg("--exact")
        .arg("crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, kind.environment_value())
        .status()?)
}

/// Runs in the parent (uncrashed) process: connects fresh, restores the
/// reader's last *committed* checkpoint, reads the remainder with a brand
/// new reader/stream pair, and asserts no gap and no duplicate across the
/// real process kill.
async fn inspect_and_restart(kind: ReaderKind, url: String) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        plaintext_config(url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH)),
    )
    .await?;
    let job_name = kind.job_name();
    let key = JobInstanceKey::new(JobName::new(job_name)?, &JobParameters::new());
    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("crashed worker did not durably create the job instance")?;
    let execution = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("crashed worker did not durably create the job execution")?;
    let step = inspect
        .step_executions(execution.id())
        .await?
        .into_iter()
        .next()
        .ok_or("crashed worker did not durably create the step execution")?;
    inspect.rollback().await?;
    assert_eq!(step.metadata().status(), BatchStatus::Started);

    let manager = PostgresChunkTransactionManager::new(repository.clone(), state_provider());
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    let namespace = namespace(kind);
    let committed_envelope =
        inherited_envelope(manager.inherited_component_state(scope).await?, &namespace)
            .ok_or("crashed worker never durably committed a reader checkpoint")?;

    // Exactly the first chunk's items are durable: nothing from the
    // in-memory-only second batch survived the crash.
    assert_eq!(
        business_items(&url, job_name).await?,
        (1..=7).collect::<Vec<_>>()
    );

    let config = plaintext_config(url.clone())?;
    let mut delivered_after_restart = Vec::new();
    match kind {
        ReaderKind::Cursor => {
            let (mut reader, stream, _contract) = postgres_cursor_reader(
                config,
                reader_base_query(job_name),
                key_columns(),
                PostgresCursorFormat::new().with_fetch_size(3),
                map_row,
                namespace.clone(),
            )?;
            let (_source, token) = oxide_batch::StopSource::new();
            stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await?;
            loop {
                match reader.read(ReadContext::new(&token)).await? {
                    ReadOutcome::Item(item) => delivered_after_restart.push(item.id),
                    ReadOutcome::EndOfInput => break,
                    ReadOutcome::Stopped => return Err("stop was never requested".into()),
                    other => return Err(format!("unexpected read outcome: {other:?}").into()),
                }
            }
            let envelope = stream.update(StreamUpdateContext::new(&token)).await?;
            commit_chunk(
                &manager,
                scope,
                job_name,
                &delivered_after_restart,
                envelope,
            )
            .await?;
        }
        ReaderKind::Paging => {
            let (mut reader, stream, _contract) = postgres_paging_reader(
                config,
                reader_base_query(job_name),
                key_columns(),
                PostgresPagingFormat::new().with_page_size(3),
                map_row,
                namespace.clone(),
            )?;
            let (_source, token) = oxide_batch::StopSource::new();
            stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await?;
            loop {
                match reader.read(ReadContext::new(&token)).await? {
                    ReadOutcome::Item(item) => delivered_after_restart.push(item.id),
                    ReadOutcome::EndOfInput => break,
                    ReadOutcome::Stopped => return Err("stop was never requested".into()),
                    other => return Err(format!("unexpected read outcome: {other:?}").into()),
                }
            }
            let envelope = stream.update(StreamUpdateContext::new(&token)).await?;
            commit_chunk(
                &manager,
                scope,
                job_name,
                &delivered_after_restart,
                envelope,
            )
            .await?;
        }
    }

    assert_eq!(delivered_after_restart, (8..=20).collect::<Vec<_>>());
    assert_eq!(
        business_items(&url, job_name).await?,
        (1..=20).collect::<Vec<_>>(),
        "every item committed exactly once across the crash: no omission, no duplication",
    );

    repository.close().await?;
    Ok(())
}

fn run_parent_scenario(kind: ReaderKind) -> Result<(), Box<dyn Error>> {
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
        prepare_fixture(&migrator_url, kind.job_name()).await
    })?;

    let status = spawn_crash_worker(kind)?;
    assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

    runtime.block_on(inspect_and_restart(kind, runtime_url.clone()))?;
    runtime.block_on(prepare_fixture(&migrator_url, kind.job_name()))?;
    Ok(())
}

#[test]
fn crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(CRASH_MODE_ENV) else {
        return Ok(());
    };
    let kind = ReaderKind::parse(&value)?;
    let url = runtime_url().ok_or("crash worker database URL is missing")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_crash_worker(kind, url))?;
    Err("crash worker returned without terminating".into())
}

#[test]
fn cursor_reader_survives_a_real_process_kill_mid_chunk() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(ReaderKind::Cursor)
}

#[test]
fn paging_reader_survives_a_real_process_kill_mid_chunk() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(ReaderKind::Paging)
}
