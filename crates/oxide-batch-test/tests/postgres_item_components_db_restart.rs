//! #149 crash/restart evidence for the `PostgreSQL` cursor reader, keyset
//! paging reader, and same-resource enlisted SQL batch writer, through the
//! real production restart path (`TestJob`/`JobLauncher`) and real durable
//! committed state.
//!
//! Mirrors `postgres_json_restart.rs`'s pattern exactly: `PostgresFixture`
//! for durable committed state, `TestJob` for the real launch path, and
//! `oxide_batch_test::inject` for distinguishable stop/commit-failure
//! injection. Unlike the file-based #148 components that file evidences,
//! these components' actual business data lives in `PostgreSQL` too, so this
//! file also owns a small dedicated business schema/table set up directly
//! through `sqlx` (kept to this test file only -- `PostgresFixture`'s public
//! surface stays free of any raw database-driver type).
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::similar_names,
    clippy::too_many_lines
)]

use std::error::Error;

use oxide_batch::item_components::{
    KeysetColumn, PostgresBatchMode, PostgresCursorFormat, PostgresPagingFormat, PostgresRow,
    postgres_batch_writer, postgres_cursor_reader, postgres_paging_reader,
};
use oxide_batch::{
    BatchStatus, BusinessValue, Checkpoint, ChunkCommitReceipt, ChunkCounts, ChunkDeliveryMode,
    ChunkJob, ChunkSize, ChunkStep, ChunkTransactionManager, ComponentRevision,
    ComponentStreamIdentity, DefinitionRevision, ExecutionContext, ExecutionCounts, ItemProcessor,
    JobName, JobParameters, PostgresChunkStateError, PostgresChunkStateProvider, PostgresConfig,
    PostgresConfigError, ProcessContext, ProcessOutcome, ProcessorError, ReaderError, StateLimits,
    StopSource, TlsMode, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectedTransactions, InjectionId, InjectionLog,
    PreCommitAction, Trigger,
};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};
use sqlx::postgres::PgPoolOptions;

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos()
}

fn fixture_stop_source() -> StopSource {
    let (source, _token) = StopSource::new();
    source
}

fn state_provider() -> std::sync::Arc<dyn PostgresChunkStateProvider> {
    std::sync::Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
        let position = committed
            .read()
            .checked_add(chunk.read().get())
            .ok_or_else(PostgresChunkStateError::new)?;
        let checkpoint_bytes = format!(
            r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.postgres-149-restart","schema_version":1,"payload":{{"position":{position}}}}}"#
        );
        let checkpoint = Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
            .map_err(|_| PostgresChunkStateError::new())?;
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.postgres-149-restart","schema_version":1,"payload":{}}"#,
            StateLimits::default(),
        )
        .map_err(|_| PostgresChunkStateError::new())?;
        Ok(ChunkCommitReceipt::new(checkpoint, context))
    })
}

struct Identity;

impl ItemProcessor<BusinessRow, BusinessRow> for Identity {
    async fn process(
        &self,
        item: &BusinessRow,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<BusinessRow>, ProcessorError> {
        Ok(ProcessOutcome::Item(item.clone()))
    }
}

struct RecordingWriter(std::sync::Arc<std::sync::Mutex<Vec<i64>>>);

impl oxide_batch::ItemWriter<BusinessRow> for RecordingWriter {
    async fn write(
        &self,
        items: &[BusinessRow],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(items.iter().map(|item| item.id));
        Ok(WriteOutcome::Written)
    }
}

fn recorded(writer: &std::sync::Arc<std::sync::Mutex<Vec<i64>>>) -> Vec<i64> {
    writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
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

async fn prepare_reader_rows(url: &str, job_name: &str, row_count: i64) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
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
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.restart_reader_rows (\
         job_name text NOT NULL, sort_key text NOT NULL, id bigint NOT NULL, \
         PRIMARY KEY (job_name, id))",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch_business.restart_reader_rows WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    for id in 1..=row_count {
        sqlx::query(
            "INSERT INTO oxide_batch_business.restart_reader_rows \
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

fn reader_base_query(job_name: &str) -> String {
    format!(
        "SELECT sort_key, id FROM oxide_batch_business.restart_reader_rows \
         WHERE job_name = '{job_name}'"
    )
}

async fn prepare_writer_table(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
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
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.restart_writer_rows (\
         job_name text NOT NULL, item bigint NOT NULL, PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}

async fn writer_rows(url: &str, job_name: &str) -> Result<Vec<i64>, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let rows = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.restart_writer_rows \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(rows)
}

fn business_writer(job_name: String) -> impl oxide_batch::ItemWriter<i64> {
    // Leaked to `&'static str`: `postgres_batch_writer`'s `bind` closure may
    // only borrow from its own per-call `&'a i64` argument (see
    // `postgres_batch.rs`'s HRTB-bound `bind` parameter), never from
    // captured closure state -- a per-test, dynamically generated job name
    // therefore needs a `'static` home, and a short-lived test binary
    // leaking a handful of small strings is an accepted, well-understood
    // pattern, not a production concern.
    let job_name: &'static str = Box::leak(job_name.into_boxed_str());
    postgres_batch_writer(
        "INSERT INTO oxide_batch_business.restart_writer_rows (job_name, item) VALUES",
        None::<&str>,
        2,
        PostgresBatchMode::PerRowStatements,
        move |item: &i64| vec![BusinessValue::text(job_name), BusinessValue::i64(*item)],
    )
    .unwrap()
}

/// Proves the cursor reader restarts by re-declaring a fresh server-side
/// cursor filtered by the last *committed* key, through the real launch
/// path: a stop is injected mid-chunk (after a genuine read, before the
/// chunk it belongs to ever commits), and a second, uninjected attempt
/// resumes from exactly the prior committed boundary -- no gap, no
/// duplicate.
#[tokio::test]
async fn postgres_cursor_reader_restart_through_the_real_launch_path() -> Result<(), Box<dyn Error>>
{
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url.clone()).await?;

    let job_name = JobName::new(format!("oxide_batch_149_cursor_restart_{}", nonce()))?;
    prepare_reader_rows(&url, job_name.as_str(), 5).await?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.postgres-149-cursor-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("cursor-reader-v1")?,
            );

    // Attempt A: chunk size 2. Rows 1+2 commit as chunk 1. Row 3 is
    // genuinely read (advancing the in-memory, not-yet-durable position)
    // into chunk 2, then a stop is injected on row 4's read call, so chunk
    // 2 never commits.
    let config_a = plaintext_config(url.clone())?;
    let (reader_a, stream_a, contract_a) = postgres_cursor_reader(
        config_a,
        reader_base_query(job_name.as_str()),
        key_columns(),
        PostgresCursorFormat::new().with_fetch_size(2),
        map_row,
        namespace.clone(),
    )?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let recorded_a = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("postgres_cursor_restart")?,
        ChunkSize::new(2)?,
        injected_reader_a,
        Identity,
        RecordingWriter(std::sync::Arc::clone(&recorded_a)),
        std::sync::Arc::new(fixture.transaction_manager(state_provider())),
        std::sync::Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("postgres-149-cursor-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(chunk_report_a.committed_counts().read().get(), 2);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(recorded(&recorded_a), vec![1, 2]);

    // Attempt B: a fresh reader/stream pair, restored from attempt A's
    // committed checkpoint.
    let config_b = plaintext_config(url.clone())?;
    let (reader_b, stream_b, contract_b) = postgres_cursor_reader(
        config_b,
        reader_base_query(job_name.as_str()),
        key_columns(),
        PostgresCursorFormat::new().with_fetch_size(2),
        map_row,
        namespace.clone(),
    )?;
    let recorded_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("postgres_cursor_restart")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(std::sync::Arc::clone(&recorded_b)),
        std::sync::Arc::new(fixture.transaction_manager(state_provider()))
            as std::sync::Arc<dyn ChunkTransactionManager>,
        std::sync::Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name.clone(),
        step_b,
        DefinitionRevision::new("postgres-149-cursor-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    assert_eq!(
        recorded(&recorded_b),
        vec![3, 4, 5],
        "attempt B resumed at row 3 -- not re-reading rows 1/2, not skipping row 3",
    );

    let mut combined = recorded(&recorded_a);
    combined.extend(recorded(&recorded_b));
    assert_eq!(
        combined,
        vec![1, 2, 3, 4, 5],
        "committed exactly once each across both attempts: no omission, no duplication",
    );

    Ok(())
}

/// Same restart proof as the cursor test, over the keyset paging reader.
#[tokio::test]
async fn postgres_paging_reader_restart_through_the_real_launch_path() -> Result<(), Box<dyn Error>>
{
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url.clone()).await?;

    let job_name = JobName::new(format!("oxide_batch_149_paging_restart_{}", nonce()))?;
    prepare_reader_rows(&url, job_name.as_str(), 5).await?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.postgres-149-paging-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("paging-reader-v1")?,
            );

    let config_a = plaintext_config(url.clone())?;
    let (reader_a, stream_a, contract_a) = postgres_paging_reader(
        config_a,
        reader_base_query(job_name.as_str()),
        key_columns(),
        PostgresPagingFormat::new().with_page_size(2),
        map_row,
        namespace.clone(),
    )?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let recorded_a = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("postgres_paging_restart")?,
        ChunkSize::new(2)?,
        injected_reader_a,
        Identity,
        RecordingWriter(std::sync::Arc::clone(&recorded_a)),
        std::sync::Arc::new(fixture.transaction_manager(state_provider())),
        std::sync::Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("postgres-149-paging-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    assert_eq!(recorded(&recorded_a), vec![1, 2]);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );

    let config_b = plaintext_config(url.clone())?;
    let (reader_b, stream_b, contract_b) = postgres_paging_reader(
        config_b,
        reader_base_query(job_name.as_str()),
        key_columns(),
        PostgresPagingFormat::new().with_page_size(2),
        map_row,
        namespace.clone(),
    )?;
    let recorded_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("postgres_paging_restart")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(std::sync::Arc::clone(&recorded_b)),
        std::sync::Arc::new(fixture.transaction_manager(state_provider()))
            as std::sync::Arc<dyn ChunkTransactionManager>,
        std::sync::Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name.clone(),
        step_b,
        DefinitionRevision::new("postgres-149-paging-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    assert_eq!(recorded(&recorded_b), vec![3, 4, 5]);

    let mut combined = recorded(&recorded_a);
    combined.extend(recorded(&recorded_b));
    assert_eq!(combined, vec![1, 2, 3, 4, 5]);

    Ok(())
}

/// Proves the same-resource enlisted batch writer's statements never
/// survive a pre-commit failure -- the injected chunk transaction's commit
/// is intercepted and fails closed before `PostgresChunkTransaction`'s own
/// `commit_with_component_state` ever runs, so the writer's statements
/// (already sent to the still-open, now-abandoned transaction) never become
/// durable -- and that a second, uninjected attempt commits the resumed
/// items exactly once.
#[tokio::test]
async fn postgres_batch_writer_restart_after_precommit_failure() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url.clone()).await?;
    prepare_writer_table(&url).await?;

    let job_name = JobName::new(format!("oxide_batch_149_batch_writer_restart_{}", nonce()))?;

    // Attempt A: chunk size 1, three items. The first chunk's commit is
    // intercepted and fails; the writer's statement for item 1 was already
    // sent to that (now-abandoned) transaction.
    let log = InjectionLog::new();
    let injected_transactions_a = InjectedTransactions::new(
        fixture.transaction_manager(state_provider()),
        1,
        PreCommitAction::Fail,
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("postgres_batch_writer_restart")?,
        ChunkSize::new(1)?,
        oxide_batch::item_components::IterReader::new(vec![1_i64, 2, 3]),
        oxide_batch::item_components::IdentityProcessor,
        business_writer(job_name.as_str().to_owned()),
        std::sync::Arc::new(injected_transactions_a),
        std::sync::Arc::new(NoCompletion),
    );
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("postgres-149-batch-writer-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    assert!(
        writer_rows(&url, job_name.as_str()).await?.is_empty(),
        "the injected pre-commit failure means item 1's statement never became durable",
    );

    // Attempt B: a fresh, uninjected transaction manager. The reader starts
    // over (this writer keeps no restart position of its own -- the
    // reader/checkpoint pairing owns that), so all three items commit
    // cleanly this time.
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("postgres_batch_writer_restart")?,
        ChunkSize::new(1)?,
        oxide_batch::item_components::IterReader::new(vec![1_i64, 2, 3]),
        oxide_batch::item_components::IdentityProcessor,
        business_writer(job_name.as_str().to_owned()),
        std::sync::Arc::new(fixture.transaction_manager(state_provider())),
        std::sync::Arc::new(NoCompletion),
    );
    let chunk_job_b = ChunkJob::new(
        job_name.clone(),
        step_b,
        DefinitionRevision::new("postgres-149-batch-writer-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    assert_eq!(
        writer_rows(&url, job_name.as_str()).await?,
        vec![1, 2, 3],
        "every item committed exactly once on the uninjected attempt",
    );

    let _ = report_a;
    Ok(())
}
