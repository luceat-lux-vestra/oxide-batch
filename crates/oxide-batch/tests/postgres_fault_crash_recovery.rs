//! Process-kill evidence for the M3 durable retry-reservation boundary.

#![cfg(feature = "postgres")]

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, BoxedProcessor, BoxedWriter,
    Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
    ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCounts, ChunkDeliveryMode,
    ChunkExecutionOutcome, ChunkJob, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTransactionContext, Clock, ComponentRevision, DefinitionRevision, ExecutionContext,
    FailureCategory, FailureId, FaultAction, FaultClassifier, FaultDescriptor, FaultPhase,
    FaultPolicy, FaultRule, FaultRuntime, FaultStateError, FaultStateStore, ItemListenerContext,
    ItemListenerSet, ItemProcessor, ItemReader, ItemWriter, JobInstanceKey, JobName, JobParameters,
    JobRepository, ListenerError, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresFaultState, PostgresJobRepository,
    PostgresMigrator, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
    ReaderError, RecoveryRequest, RetryLimit, RetryOrdinal, RetryReservation, RetryStateLimit,
    RollbackDisposition, SequentialIdGenerator, SkipLimit, SkipListener, StateLimits,
    StateSchemaId, StateSchemaVersion, StepName, StopSource, StopToken, TlsMode, WriteContext,
    WriteOutcome, WriterError,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const CRASH_MODE_ENV: &str = "OXIDEBATCH_M3_FAULT_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 88;
const STEP: &str = "import";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeReservationCommit,
    AfterReservationCommit,
    DuringSkipCallback,
}

impl CrashPoint {
    const fn job_name(self) -> &'static str {
        match self {
            Self::BeforeReservationCommit => "m3_fault_crash_before_reservation",
            Self::AfterReservationCommit => "m3_fault_crash_after_reservation",
            Self::DuringSkipCallback => "m3_fault_crash_during_skip_callback",
        }
    }

    const fn environment_value(self) -> &'static str {
        match self {
            Self::BeforeReservationCommit => "before-reservation-commit",
            Self::AfterReservationCommit => "after-reservation-commit",
            Self::DuringSkipCallback => "during-skip-callback",
        }
    }

    const fn expected_reservations(self) -> u64 {
        match self {
            Self::AfterReservationCommit => 1,
            Self::BeforeReservationCommit | Self::DuringSkipCallback => 0,
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-reservation-commit" => Ok(Self::BeforeReservationCommit),
            "after-reservation-commit" => Ok(Self::AfterReservationCommit),
            "during-skip-callback" => Ok(Self::DuringSkipCallback),
            _ => Err("unknown M3 fault crash mode".into()),
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

struct OneItemReader(VecDeque<i64>);

impl ItemReader<i64> for OneItemReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
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

struct FailingProcessor;

impl ItemProcessor<i64, i64> for FailingProcessor {
    async fn process(
        &self,
        _item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Err(ProcessorError::with_category(
            FailureCategory::UserComponent,
        ))
    }
}

struct NoopWriter;

impl ItemWriter<i64> for NoopWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        Ok(WriteOutcome::Written)
    }
}

struct WitnessWriter {
    pool: PgPool,
    job_name: &'static str,
    fail: bool,
}

impl ItemWriter<i64> for WitnessWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        sqlx::query("INSERT INTO oxide_batch_business.m3_fault_crash_call (job_name) VALUES ($1)")
            .bind(self.job_name)
            .execute(&self.pool)
            .await
            .map_err(|_| WriterError::with_category(FailureCategory::PermanentInfrastructure))?;
        if self.fail {
            Err(WriterError::with_category(FailureCategory::Timeout))
        } else {
            Ok(WriteOutcome::Written)
        }
    }
}

struct SkipWitnessListener {
    pool: PgPool,
    job_name: &'static str,
    crash: bool,
}

impl SkipListener<i64, i64> for SkipWitnessListener {
    fn on_skip_in_process<'a>(
        &'a self,
        _input: &'a i64,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO oxide_batch_business.m3_fault_crash_call (job_name) VALUES ($1)",
            )
            .bind(self.job_name)
            .execute(&self.pool)
            .await
            .map_err(ListenerError::from_error)?;
            if self.crash {
                std::process::exit(CRASH_EXIT_CODE);
            }
            Ok(())
        })
    }
}

struct Completion;

impl ChunkCompletion for Completion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

struct ImmediateSleeper;

impl BackoffSleeper for ImmediateSleeper {
    fn sleep<'a>(
        &'a self,
        _delay: Duration,
        _stop: &'a StopToken,
    ) -> BoxFuture<'a, BackoffOutcome> {
        Box::pin(async { BackoffOutcome::Elapsed })
    }
}

struct CrashReservationStore {
    point: CrashPoint,
    inner: Arc<dyn FaultStateStore>,
}

impl FaultStateStore for CrashReservationStore {
    fn bind(&self, context: ChunkTransactionContext) -> BoxFuture<'_, Result<(), FaultStateError>> {
        self.inner.bind(context)
    }

    fn reserved_ordinal(
        &self,
        key: oxide_batch::RetryKey,
    ) -> BoxFuture<'_, Result<Option<RetryOrdinal>, FaultStateError>> {
        self.inner.reserved_ordinal(key)
    }

    fn reserve(&self, reservation: RetryReservation) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(async move {
            if self.point == CrashPoint::BeforeReservationCommit {
                std::process::exit(CRASH_EXIT_CODE);
            }
            let result = self.inner.reserve(reservation).await;
            if result.is_ok() {
                std::process::exit(CRASH_EXIT_CODE);
            }
            result
        })
    }

    fn resolve(&self, key: oxide_batch::RetryKey) -> BoxFuture<'_, Result<(), FaultStateError>> {
        self.inner.resolve(key)
    }

    fn clear_resolved(&self) -> BoxFuture<'_, Result<(), FaultStateError>> {
        self.inner.clear_resolved()
    }

    fn unresolved(&self) -> BoxFuture<'_, Result<u32, FaultStateError>> {
        self.inner.unresolved()
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

fn policy(point: CrashPoint) -> Result<FaultPolicy, Box<dyn Error>> {
    let (rule, retry_limit, skip_limit) = if point == CrashPoint::DuringSkipCallback {
        (
            FaultRule::new(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::CommitSafeSkip),
            )?,
            RetryLimit::NONE,
            SkipLimit::new(1),
        )
    } else {
        (
            FaultRule::new(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )?,
            RetryLimit::new(1)?,
            SkipLimit::NONE,
        )
    };
    Ok(FaultPolicy::new(
        FaultClassifier::new(
            oxide_batch::ClassifierRevision::new("m3-fault-crash-v1")?,
            [rule],
        )?,
        retry_limit,
        RetryStateLimit::new(4)?,
        skip_limit,
        BackoffPolicy::none(),
    )?)
}

fn restart_contract() -> Result<ChunkRestartContract, Box<dyn Error>> {
    Ok(ChunkRestartContract::new(
        StateSchemaId::new("m3.fault.crash.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("m3.fault.crash.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

fn checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    Ok(Checkpoint::from_json(
        &serde_json::to_vec(&serde_json::json!({
            "format": "oxide-batch.checkpoint",
            "format_version": 1,
            "schema": "m3.fault.crash.position",
            "schema_version": 1,
            "payload": {"position": position},
        }))?,
        StateLimits::default(),
    )?)
}

fn execution_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"m3.fault.crash.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )?)
}

fn transaction_manager(repository: &PostgresJobRepository) -> PostgresChunkTransactionManager {
    let provider: Arc<dyn PostgresChunkStateProvider> = Arc::new(
        |committed: oxide_batch::ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            Ok(ChunkCommitReceipt::new(
                checkpoint(position).map_err(|_| PostgresChunkStateError::new())?,
                execution_context().map_err(|_| PostgresChunkStateError::new())?,
            ))
        },
    );
    PostgresChunkTransactionManager::new(repository.clone(), provider)
}

type CrashChunkJob = ChunkJob<i64, i64, OneItemReader, BoxedProcessor<i64, i64>, BoxedWriter<i64>>;

fn chunk_job(
    point: CrashPoint,
    pool: PgPool,
    transactions: PostgresChunkTransactionManager,
    state: Arc<dyn FaultStateStore>,
    fail: bool,
) -> Result<CrashChunkJob, Box<dyn Error>> {
    let runtime = FaultRuntime::new(
        policy(point)?,
        Arc::new(ImmediateSleeper),
        state,
        ChunkDeliveryMode::AtomicSameResource,
    )?;
    // The crash point decides which concrete processor and writer run, so the
    // choice is only known at runtime: `BoxedProcessor`/`BoxedWriter` are the
    // explicit ADR-0008 erasure boundary for exactly this case.
    let processor = if point == CrashPoint::DuringSkipCallback {
        BoxedProcessor::new(FailingProcessor)
    } else {
        BoxedProcessor::new(IdentityProcessor)
    };
    let writer = if point == CrashPoint::DuringSkipCallback {
        BoxedWriter::new(NoopWriter)
    } else {
        BoxedWriter::new(WitnessWriter {
            pool: pool.clone(),
            job_name: point.job_name(),
            fail,
        })
    };
    let mut step = ChunkStep::new(
        StepName::new(STEP)?,
        ChunkSize::new(1)?,
        OneItemReader(VecDeque::from([1])),
        processor,
        writer,
        Arc::new(transactions),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime);
    if point == CrashPoint::DuringSkipCallback {
        let listeners =
            ItemListenerSet::new().with_skip_listener(Arc::new(SkipWitnessListener {
                pool,
                job_name: point.job_name(),
                crash: fail,
            }))?;
        step = step.with_item_listeners(listeners);
    }
    Ok(ChunkJob::new(
        JobName::new(point.job_name())?,
        step,
        DefinitionRevision::new("m3-fault-crash-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            restart_contract()?,
        ),
    )?)
}

async fn prepare_fixture(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.m3_fault_crash_call (\
         id bigserial PRIMARY KEY, job_name text NOT NULL)",
    )
    .execute(&pool)
    .await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
        "DELETE FROM oxide_batch_business.m3_fault_crash_call WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(job_name).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

async fn witness_count(url: &str, job_name: &str) -> Result<i64, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let count = sqlx::query_scalar(
        "SELECT count(*) FROM oxide_batch_business.m3_fault_crash_call WHERE job_name = $1",
    )
    .bind(job_name)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(count)
}

async fn run_crash_worker(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(5_000));
    let repository =
        PostgresJobRepository::connect(plaintext_config(url.clone())?, Arc::new(clock)).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let durable: Arc<dyn FaultStateStore> =
        Arc::new(PostgresFaultState::new(repository.clone(), &policy(point)?));
    let state: Arc<dyn FaultStateStore> = Arc::new(CrashReservationStore {
        point,
        inner: durable,
    });
    let mut job = chunk_job(point, pool, transaction_manager(&repository), state, true)?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let launcher = oxide_batch::JobLauncher::new(&repository, &clock, &ids);
    let _ = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await?;
    Err("fault crash worker crossed the selected process-exit boundary".into())
}

fn spawn_crash_worker(point: CrashPoint) -> Result<ExitStatus, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("fault_crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, point.environment_value())
        .status()?)
}

#[allow(
    clippy::too_many_lines,
    reason = "crash inspection, audited recovery, and retry restart form one evidence chain"
)]
async fn inspect_recover_and_restart(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(5_001));
    let repository =
        PostgresJobRepository::connect(plaintext_config(url.clone())?, Arc::new(clock)).await?;
    let key = JobInstanceKey::new(JobName::new(point.job_name())?, &JobParameters::new());
    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("fault crash worker did not create an instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("fault crash worker did not create an execution")?;
    let original_step = inspect
        .step_executions(original.id())
        .await?
        .into_iter()
        .next()
        .ok_or("fault crash worker did not create a step")?;
    inspect.rollback().await?;
    assert_eq!(witness_count(&url, point.job_name()).await?, 1);
    assert_eq!(
        original.metadata().status(),
        oxide_batch::BatchStatus::Started
    );
    assert_eq!(
        original_step.metadata().counts().rolled_back(),
        point.expected_reservations()
    );

    let scope = ChunkTransactionContext::new(original.id(), original_step.id());
    let manager = transaction_manager(&repository);
    let durable = manager.load_committed_state(scope).await?;
    assert_eq!(
        durable.fault_progress().retries().write(),
        point.expected_reservations()
    );
    assert_eq!(durable.fault_progress().skips().process(), 0);
    assert_eq!(
        u64::from(!durable.fault_state().is_empty()),
        point.expected_reservations()
    );

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "FAULT_PROCESS_EXIT_INSPECTED",
        "m3-fault-crash-harness",
        [44; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(5_001)?,
    )?;
    let mut recover = repository.begin().await?;
    recover
        .recover_job_execution(original.id(), &request)
        .await?;
    recover.commit().await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let state: Arc<dyn FaultStateStore> =
        Arc::new(PostgresFaultState::new(repository.clone(), &policy(point)?));
    let mut job = chunk_job(point, pool, manager, state, false)?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let report = oxide_batch::JobLauncher::new(&repository, &clock, &ids)
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await?;
    let chunk = report.chunk().ok_or("restart chunk report missing")?;
    assert_eq!(chunk.outcome(), ChunkExecutionOutcome::Completed);
    if point == CrashPoint::DuringSkipCallback {
        assert_eq!(chunk.skip_counts().process(), 1);
        assert_eq!(chunk.no_rollback_count(), 1);
    }
    assert_eq!(witness_count(&url, point.job_name()).await?, 2);
    assert_eq!(
        report
            .launch()
            .step_execution()
            .metadata()
            .counts()
            .rolled_back(),
        point.expected_reservations()
    );
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
        prepare_fixture(&migrator_url, point.job_name()).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;

    assert_eq!(spawn_crash_worker(point)?.code(), Some(CRASH_EXIT_CODE));
    runtime.block_on(inspect_recover_and_restart(point, runtime_url.clone()))?;
    runtime.block_on(prepare_fixture(&migrator_url, point.job_name()))?;
    Ok(())
}

#[test]
fn fault_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(CRASH_MODE_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let url = runtime_url().ok_or("fault crash worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_crash_worker(point, url))
}

#[test]
fn crash_before_reservation_replays_initial_call() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::BeforeReservationCommit)
}

#[test]
fn retry_reservation_survives_process_restart() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::AfterReservationCommit)
}

#[test]
fn crash_during_skip_callback_replays_before_atomic_skip_commit() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::DuringSkipCallback)
}
