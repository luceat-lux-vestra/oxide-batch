//! #149 strict-review regression: a `FETCH`-level transient failure on
//! [`PostgresCursorReader`](oxide_batch::item_components::PostgresCursorReader)
//! must be recoverable through the real M3 fault-tolerance surface
//! (`FaultRuntime`/`FaultPolicy`), not a permanent, unretryable failure.
//!
//! Before this regression's fix, `fetch_more()`'s error branch dropped the
//! broken transaction but never reset `started`, so a retried `read()`
//! re-entered `fetch_more()` with no transaction to fetch from and failed
//! closed forever with `FailureCategory::Invariant` -- a fault a `Read`/
//! `TransientInfrastructure` retry rule could never actually recover from.
//! This file proves the fixed behavior against a real `PostgreSQL` server: a
//! genuine connection loss mid-`FETCH` (`pg_terminate_backend` against the
//! backend actually executing it, not an in-process simulation) is retried
//! by the real `FaultRuntime`, and the retried attempt re-`DECLARE`s a fresh
//! cursor filtered by the last row this reader actually delivered, so every
//! row is still delivered exactly once -- no skip, no duplicate.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{
    KeysetColumn, PostgresCursorFormat, PostgresRow, postgres_cursor_reader,
};
use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, BusinessTransaction,
    ChunkCommitReceipt, ChunkCounts, ChunkExecutionOutcome, ChunkFaultProgress, ChunkSize,
    ChunkStep, ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
    ClassifierRevision, ComponentStreamIdentity, ExecutionAttempt, ExecutionCorrelation,
    FailureCategory, FaultAction, FaultClassifier, FaultPhase, FaultPolicy, FaultRule,
    FaultRuntime, InMemoryFaultState, JobExecutionId, JobInstanceId, JobName, PostgresConfig,
    PostgresConfigError, RetryLimit, RetryStateLimit, SkipLimit, StepExecutionId, StepName,
    StopSource, StopToken, TlsMode, WriteContext, WriteOutcome, WriterError,
};
use sqlx::postgres::PgPoolOptions;

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct BusinessRow {
    id: i64,
}

fn map_row(row: &PostgresRow<'_>) -> Result<BusinessRow, oxide_batch::ReaderError> {
    Ok(BusinessRow { id: row.i64("id")? })
}

fn key_columns() -> Vec<KeysetColumn> {
    vec![KeysetColumn::i64("id")]
}

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(format!("oxide-batch-test.postgres-149-cursor-fault-{name}"))
        .expect("static identity is valid")
}

/// A `base_query` whose second `FETCH` batch (rows with `id >= 5`) stalls
/// for 300ms per row, giving a concurrent watcher a wide, deterministic
/// window to find and terminate the backend actually executing it. The
/// first batch (`id < 5`) is instant, so this scenario always reaches the
/// framework's normal `read()` path for five rows before the induced
/// failure.
async fn prepare_fixture(url: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.postgres_149_cursor_fault_rows (\
         id bigint PRIMARY KEY)",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch_business.postgres_149_cursor_fault_rows")
        .execute(&pool)
        .await?;
    for id in 0_i64..10 {
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_149_cursor_fault_rows (id) VALUES ($1)",
        )
        .bind(id)
        .execute(&pool)
        .await?;
    }
    pool.close().await;
    Ok(())
}

const SLOW_BASE_QUERY: &str = "SELECT (CASE WHEN id >= 5 THEN pg_sleep(0.3) ELSE pg_sleep(0) END), id \
     FROM oxide_batch_business.postgres_149_cursor_fault_rows";

fn correlation() -> ExecutionCorrelation {
    let attempt =
        |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("attempt is nonzero"));
    ExecutionCorrelation::new(
        JobName::new("postgres_149_cursor_fault").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("postgres_149_cursor_fault_step").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}

struct ImmediateSleeper;

impl BackoffSleeper for ImmediateSleeper {
    fn sleep<'a>(&'a self, _delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome> {
        let stopped = stop.is_stop_requested();
        Box::pin(async move {
            if stopped {
                BackoffOutcome::Stopped
            } else {
                BackoffOutcome::Elapsed
            }
        })
    }
}

/// A `FaultRuntime` that retries (once) every `Read`/`TransientInfrastructure`
/// fault -- exactly the classification `classify_pg_error` gives a
/// connection-severed `sqlx::Error` that carries no database error code.
fn retry_transient_infrastructure_runtime() -> FaultRuntime {
    let policy = FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("postgres-149-cursor-fault-retry-v1").unwrap(),
            [FaultRule::new(
                FaultPhase::Read,
                FailureCategory::TransientInfrastructure,
                FaultAction::retry(),
            )
            .unwrap()],
        )
        .unwrap(),
        RetryLimit::new(1).unwrap(),
        RetryStateLimit::new(4).unwrap(),
        SkipLimit::NONE,
        BackoffPolicy::none(),
    )
    .unwrap();
    let state = Arc::new(InMemoryFaultState::new(policy.retry_state_limit()));
    FaultRuntime::new(
        policy,
        Arc::new(ImmediateSleeper),
        state,
        oxide_batch::ChunkDeliveryMode::AtLeastOnce,
    )
    .unwrap()
}

/// A minimal, non-enlisted, always-succeeding transaction manager: this
/// scenario is about the reader's own retry recovery, not enlisted-write
/// semantics, so no real business transaction is needed.
struct Transactions;

impl ChunkTransactionManager for Transactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async { Ok(Box::new(NoopTransaction) as Box<dyn ChunkTransaction>) })
    }
}

struct NoopTransaction;

impl ChunkTransaction for NoopTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { Ok(receipt()) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

fn receipt() -> ChunkCommitReceipt {
    let checkpoint = oxide_batch::Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"postgres-149-cursor-fault.position","schema_version":1,"payload":{"position":0}}"#,
        oxide_batch::StateLimits::default(),
    )
    .expect("checkpoint fixture must be valid");
    let context = oxide_batch::ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"postgres-149-cursor-fault.context","schema_version":1,"payload":{}}"#,
        oxide_batch::StateLimits::default(),
    )
    .expect("context fixture must be valid");
    ChunkCommitReceipt::new(checkpoint, context)
}

struct Completion;

impl oxide_batch::ChunkCompletion for Completion {
    fn after_commit<'a>(
        &'a self,
        _context: oxide_batch::ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<oxide_batch::ChunkCompletionOutcome, oxide_batch::ChunkCompletionError>>
    {
        Box::pin(async { Ok(oxide_batch::ChunkCompletionOutcome::Acknowledged) })
    }
}

/// Records every item id this writer ever received, in delivery order --
/// direct proof of no skip and no duplicate across the induced failure and
/// its retry.
struct RecordingWriter(Arc<Mutex<Vec<i64>>>);

impl oxide_batch::ItemWriter<BusinessRow> for RecordingWriter {
    async fn write(
        &self,
        items: &[BusinessRow],
        context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(items.iter().map(|item| item.id));
        Ok(WriteOutcome::Written)
    }
}

/// Polls `pg_stat_activity` for the backend actually executing this
/// reader's slow second `FETCH` and terminates it exactly once -- a genuine
/// connection loss, not an in-process simulation. Returns whether it found
/// and terminated a match before giving up.
async fn terminate_the_slow_fetch_once(admin_url: String) -> Result<bool, sqlx::Error> {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;
    let mut terminated = false;
    for _ in 0..300 {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE application_name = 'oxide-batch' \
             AND state = 'active' AND query LIKE 'FETCH FORWARD%' LIMIT 1",
        )
        .fetch_optional(&admin)
        .await?;
        if let Some(pid) = pid {
            sqlx::query("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .execute(&admin)
                .await?;
            terminated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    admin.close().await;
    Ok(terminated)
}

#[test]
fn fetch_level_transient_failure_recovers_without_skip_or_duplicate_through_fault_runtime()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
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
        prepare_fixture(&url).await?;

        let config = plaintext_config(url.clone())?;
        let (reader, _stream, _contract) = postgres_cursor_reader(
            config,
            SLOW_BASE_QUERY.to_owned(),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(5),
            map_row,
            identity("retry"),
        )?;

        let recorded = Arc::new(Mutex::new(Vec::new()));
        let mut step: ChunkStep<BusinessRow, BusinessRow, _, _, _> = ChunkStep::new(
            StepName::new("postgres_149_cursor_fault_step").unwrap(),
            ChunkSize::new(20).unwrap(),
            reader,
            IdentityProcessor,
            RecordingWriter(Arc::clone(&recorded)),
            Arc::new(Transactions),
            Arc::new(Completion),
        )
        .with_fault_runtime(retry_transient_infrastructure_runtime());
        let (_source, stop) = StopSource::new();

        let correlation = correlation();
        let step_future = step.execute(&correlation, &stop);
        let terminate_future = terminate_the_slow_fetch_once(admin_url);
        let (report, terminate_result) = tokio::join!(step_future, terminate_future);

        assert!(
            terminate_result?,
            "never observed the backend executing the slow second FETCH within the window"
        );
        assert_eq!(
            report.outcome(),
            ChunkExecutionOutcome::Completed,
            "the real FaultRuntime must retry the connection-loss failure and complete, not \
             fail closed with FailureCategory::Invariant"
        );
        assert_eq!(
            report.committed_counts().read().get(),
            10,
            "all ten rows must be committed despite the induced mid-attempt FETCH failure"
        );
        assert!(
            report.retry_counts().read() >= 1,
            "the FaultRuntime must have actually reserved a read retry, not merely happened to \
             succeed some other way"
        );
        assert_eq!(
            *recorded.lock().unwrap_or_else(PoisonError::into_inner),
            (0_i64..10).collect::<Vec<_>>(),
            "every row must be delivered exactly once, in order -- no skip from the failed \
             FETCH's abandoned rows, no duplicate from the retried re-DECLARE"
        );
        Ok::<(), Box<dyn Error>>(())
    })
}
