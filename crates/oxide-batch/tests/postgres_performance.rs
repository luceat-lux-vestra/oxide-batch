//! P-001, P-003, and P-010 for the M5 performance and reference-workload
//! campaigns.
//!
//! Three reports, one binary target, because the accepted denominator shares
//! one P-003 report between the performance and reference-workload rows:
//! running the same fixed workload twice would produce two samples of the
//! same thing, not two different obligations. `cargo xtask performance`
//! resolves that P-003 report once, per matrix point, and reads it for both
//! rows.
//!
//! - [`p001_fixed_tasklet_lifecycle_overhead`] never opens a `PostgreSQL`
//!   connection. The accepted workload table defines P-001 as the in-memory
//!   no-op tasklet lifecycle, independent of the database major, and this
//!   report keeps that meaning even though it is retained once inside each
//!   matrix job for environmental consistency.
//! - [`p003_csv_to_postgres_reference_workload`] reads a deterministic,
//!   seeded, in-memory CSV and writes it to `PostgreSQL` through a chunk step
//!   whose business rows and checkpoint commit in the same transaction —
//!   `ChunkDeliveryMode::AtomicSameResource`.
//! - [`p010_postgres_local_partition_scaling`] runs a locally-partitioned step
//!   at 1, 10, and `MAX_PARTITION_WORKERS` concurrent workers, each partition
//!   committing one deterministic business row.
//!
//! No duration, rate, or efficiency figure here is compared against a limit.
//! No accepted document states one, and the committed scope records that as
//! `numeric_status: observational`. What each report gates is correctness and
//! the finite resource ceilings the workload declares.

#![cfg(feature = "postgres")]
// Reported ratios and per-item rates convert bounded counters into floating
// point for the report; every comparison remains against another float.
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::too_many_lines)]

#[path = "performance/mod.rs"]
mod performance;

use std::collections::BTreeSet;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxide_batch::{
    BatchStatus, BoxFuture, BusinessStatement, BusinessValue, Checkpoint, ChunkCommitReceipt,
    ChunkCompletion, ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome,
    ChunkComponentRevisions, ChunkCounts, ChunkDeliveryMode, ChunkJob, ChunkRestartContract,
    ChunkSize, ChunkTransactionContext, ComponentRevision, DefinitionRevision, ExecutionContext,
    ExecutionCounts, ExitStatus, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter, JobLauncher, JobName,
    JobParameters, JobRepository, MAX_PARTITION_WORKERS, NodeId, PartitionBudget, PartitionCount,
    PartitionFactoryError, PartitionKey, PartitionPlanEntry, PartitionPlanFactory,
    PartitionTaskletFactory, PartitionedStepNode, PostgresChunkStateError,
    PostgresChunkStateProvider, PostgresChunkTransactionManager, PostgresJobRepository,
    PostgresMigrator, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
    ReaderError, RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork,
    SequentialIdGenerator, StateLimits, StateSchemaId, StateSchemaVersion, StepComponents,
    StepName, StepNode, StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome,
    TaskletStep, TerminalKind, WriteContext, WriteOutcome, WriterError,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

use performance::{
    ConnectionPeakObserver, Failure, FixedClock, RssPeakSampler, config, execution_manifest,
    major_version, measurement_environment, migrator_url, remove_job, resident_kib,
    retain_observation, runtime_url,
};

/// The bounded interval every peak sampler in this file polls at.
///
/// Not a guarantee of catching the true instantaneous peak — a
/// bounded-interval sample never is — but it replaces a configured-ceiling
/// value copied into a field named "peak" with an actual observation of the
/// measured window, which is what `peak-resident-memory` and
/// `peak-connections` are declared to be.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(2);

// ---------------------------------------------------------------------
// P-001: in-memory no-op tasklet lifecycle overhead.
// ---------------------------------------------------------------------

const P001_WARMUP_ATTEMPTS: usize = 16;
const P001_MEASURED_ATTEMPTS: usize = 256;

/// A tasklet that does nothing, and records when it was entered.
struct NoOpTasklet {
    entered_at: Mutex<Option<Instant>>,
}

impl Tasklet for NoOpTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        if let Ok(mut slot) = self.entered_at.lock() {
            *slot = Some(Instant::now());
        }
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

/// Counts `begin()` calls through an inner in-memory repository, without
/// changing what it does.
struct CountingMemoryRepository<'a> {
    inner: &'a InMemoryJobRepository,
    begins: AtomicUsize,
}

impl<'a> CountingMemoryRepository<'a> {
    const fn new(inner: &'a InMemoryJobRepository) -> Self {
        Self {
            inner,
            begins: AtomicUsize::new(0),
        }
    }

    fn begins(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }
}

impl JobRepository for CountingMemoryRepository<'_> {
    fn connection_capacity(&self) -> u32 {
        self.inner.connection_capacity()
    }

    fn descriptor(&self) -> RepositoryDescriptor {
        self.inner.descriptor()
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        self.inner.begin()
    }
}

/// P-001: the fixed overhead of one no-op tasklet lifecycle, in memory.
///
/// Every one of the [`P001_WARMUP_ATTEMPTS`] `+` [`P001_MEASURED_ATTEMPTS`]
/// attempts launches under a fresh [`JobName`], so each is a new job instance
/// rather than a restart of a previous one — `no-attempt-is-reused` below
/// checks that every returned job-execution identifier was actually distinct,
/// rather than trusting the fresh name to have implied it. This scenario
/// opens no `PostgreSQL` connection: the accepted workload table defines
/// P-001 as in-memory, independent of the database major.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p001_fixed_tasklet_lifecycle_overhead() -> Result<(), Box<dyn Error>> {
    let clock = Arc::new(FixedClock::default());
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let counting = CountingMemoryRepository::new(&repository);

    let total_attempts = P001_WARMUP_ATTEMPTS + P001_MEASURED_ATTEMPTS;
    let mut end_to_end_micros = Vec::with_capacity(total_attempts);
    let mut job_overhead_micros = Vec::with_capacity(total_attempts);
    let mut step_overhead_micros = Vec::with_capacity(total_attempts);
    let mut execution_ids = BTreeSet::new();
    let mut every_attempt_completed = true;
    let mut every_status_completed = true;
    let mut rss_sampler: Option<RssPeakSampler> = None;

    for attempt in 0..total_attempts {
        if attempt == P001_WARMUP_ATTEMPTS {
            rss_sampler = Some(RssPeakSampler::start(SAMPLE_INTERVAL));
        }
        let name = JobName::new(format!("p001-fixed-overhead-{attempt}"))?;
        let tasklet = Arc::new(NoOpTasklet {
            entered_at: Mutex::new(None),
        });
        let job = oxide_batch::TaskletJob::new(
            name,
            TaskletStep::new(StepName::new("only")?, tasklet.clone()),
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new("p001-noop-v1")?,
        )?;
        let (_source, stop) = StopSource::new();
        let runner = JobLauncher::new(&counting, clock.as_ref(), ids.as_ref());

        let started = Instant::now();
        let launched = runner.launch(&job, &JobParameters::new(), &stop).await?;
        let finished = Instant::now();
        let entered = tasklet
            .entered_at
            .lock()
            .ok()
            .and_then(|slot| *slot)
            .unwrap_or(started);

        let job_execution = launched.job_execution();
        let is_completed = job_execution.metadata().status() == BatchStatus::Completed
            && *job_execution.metadata().exit_status() == ExitStatus::completed();
        let step_completed =
            launched.step_execution().metadata().status() == BatchStatus::Completed;

        every_attempt_completed &= is_completed;
        every_status_completed &= step_completed;
        execution_ids.insert(job_execution.id().get());

        end_to_end_micros.push(finished.saturating_duration_since(started).as_micros());
        job_overhead_micros.push(entered.saturating_duration_since(started).as_micros());
        step_overhead_micros.push(finished.saturating_duration_since(entered).as_micros());

        if attempt < P001_WARMUP_ATTEMPTS {
            // Warmup is excluded from the recorded series below but still
            // runs the full lifecycle, so the pool, allocator, and executor
            // are warm before the measured window starts.
        }
    }
    let observed_peak_resident_kib = match rss_sampler {
        Some(sampler) => sampler.stop().await,
        None => resident_kib(),
    };

    let measured_end_to_end = &end_to_end_micros[P001_WARMUP_ATTEMPTS..];
    let measured_job = &job_overhead_micros[P001_WARMUP_ATTEMPTS..];
    let measured_step = &step_overhead_micros[P001_WARMUP_ATTEMPTS..];

    let no_attempt_reused = execution_ids.len() == total_attempts;

    let document = json!({
        "report": "p001-fixed-overhead",
        "workload": "P-001",
        "postgresql_major_version": Value::Null,
        "against_database": false,
        "environment": measurement_environment(4),
        "declared": {
            "tasklet": "no-op",
            "warmup_attempts": P001_WARMUP_ATTEMPTS,
            "measured_attempts": P001_MEASURED_ATTEMPTS,
            "fresh_job_parameters_per_attempt": true,
        },
        "measurement_protocol": {
            "job_overhead_note": "Wall time from the launch call starting to the tasklet's own \
                                  execute() being entered: everything the framework does before \
                                  user work runs.",
            "step_overhead_note": "Wall time from the tasklet's execute() being entered to the \
                                   launch call returning: everything the framework does after \
                                   user work returns, including committing the terminal status.",
            "warmup_excluded": true,
        },
        "observation": {
            "end_to_end_duration_micros": summary(measured_end_to_end),
            "job_overhead_micros": summary(measured_job),
            "step_overhead_micros": summary(measured_step),
            "repository_round_trips_per_attempt": counting.begins() as f64 / total_attempts as f64,
            "metadata_writes_per_attempt": counting.begins() as f64 / total_attempts as f64,
            "metadata_writes_note": "Counted identically to repository round trips: an in-memory \
                                     repository does not distinguish a metadata write from the \
                                     unit of work that carried it the way a network round trip \
                                     to PostgreSQL would.",
            "peak_resident_memory_kib": observed_peak_resident_kib,
            "peak_resident_memory_note": format!(
                "A process-level RSS value sampled every {} ms for the duration of the measured \
                 attempts, retaining the maximum observed value — not a single end-of-run \
                 snapshot.",
                SAMPLE_INTERVAL.as_millis(),
            ),
            "peak_connections": 0,
            "peak_connections_note": "P-001 opens no PostgreSQL connection at all: this is a \
                                      structural zero, not a live sample, because there is \
                                      nothing to sample.",
        },
        "execution_manifest": execution_manifest()?,
        "measurements": {
            "end-to-end-duration": summary(measured_end_to_end)["mean"],
            "job-overhead": summary(measured_job)["mean"],
            "step-overhead": summary(measured_step)["mean"],
            "repository-round-trips": counting.begins() as f64 / total_attempts as f64,
            "metadata-writes": counting.begins() as f64 / total_attempts as f64,
            "peak-resident-memory": observed_peak_resident_kib,
            "peak-connections": 0,
        },
        "correctness": {
            "every_attempt_completes": every_attempt_completed,
            "durable_job_and_step_statuses_are_completed": every_status_completed,
            "no_attempt_is_reused": no_attempt_reused,
            "distinct_execution_ids": execution_ids.len(),
            "total_attempts": total_attempts,
        },
        "violations": correctness_violations([
            (every_attempt_completed, "an attempt did not durably complete"),
            (
                every_status_completed,
                "a step's durable status was not COMPLETED",
            ),
            (
                no_attempt_reused,
                "two attempts returned the same job-execution identifier",
            ),
        ]),
    });
    let passed = document["violations"].as_array().is_some_and(Vec::is_empty);
    let document = with_passed(document, passed);

    retain_observation("p001-fixed-overhead", &document)?;
    assert!(passed, "{document:#}");
    Ok(())
}

/// Summarizes a series of microsecond durations without judging any of them.
fn summary(values: &[u128]) -> Value {
    let count = values.len() as u128;
    let total: u128 = values.iter().sum();
    let min = values.iter().copied().min().unwrap_or_default();
    let max = values.iter().copied().max().unwrap_or_default();
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let median = sorted.get(sorted.len() / 2).copied().unwrap_or_default();
    json!({
        "samples": count,
        "total": total,
        "min": min,
        "median": median,
        "max": max,
        "mean": if count == 0 { 0.0 } else { total as f64 / count as f64 },
    })
}

/// Builds the `violations` array from a list of (holds, message) pairs.
fn correctness_violations(checks: impl IntoIterator<Item = (bool, &'static str)>) -> Value {
    Value::Array(
        checks
            .into_iter()
            .filter(|(holds, _)| !holds)
            .map(|(_, message)| Value::String(message.to_owned()))
            .collect(),
    )
}

/// Sets the top-level `passed` field on an observation document.
fn with_passed(mut document: Value, passed: bool) -> Value {
    document["passed"] = Value::Bool(passed);
    document
}

/// Renders a sha256 digest as lowercase hex.
fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

// ---------------------------------------------------------------------
// P-003: CSV to PostgreSQL, the shared reference workload.
// ---------------------------------------------------------------------

const P003_DATASET_ROWS: u64 = 10_000;
const P003_CHUNK_SIZE: u64 = 100;
const P003_SOURCE_SEED: u64 = 102;
const P003_JOB: &str = "p003-reference-workload";
/// The configured pool capacity for this sequential one-step job: the
/// connection ceiling `observed_peak_connections` is checked against, not a
/// claim about how many connections were actually live at once.
const P003_CONFIGURED_CONNECTION_CEILING: u32 = 2;

/// One row of the fixed reference dataset: an identity plus two scalar
/// columns derived from the seeded generator.
#[derive(Clone, Copy, Eq, PartialEq)]
struct ReferenceRow {
    id: u64,
    quantity: i64,
    amount_cents: i64,
}

impl ReferenceRow {
    /// Formats one row exactly as it appears in the source CSV, so the same
    /// function can render the source and reconstruct the written side for a
    /// byte-comparable digest.
    fn csv_line(self) -> String {
        format!("{},{},{}", self.id, self.quantity, self.amount_cents)
    }
}

/// A small, deterministic, dependency-free generator (`splitmix64`).
///
/// Not cryptographic and not meant to be: the only property this campaign
/// needs is that the same seed always produces the same 10,000 rows, on any
/// host, forever. `splitmix64` is a public-domain, single-file algorithm
/// exactly for that reason — no crate, no version, nothing that could someday
/// resolve differently.
struct SplitMix64(u64);

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Generates the fixed reference dataset deterministically from
/// [`P003_SOURCE_SEED`].
fn generate_reference_rows() -> Vec<ReferenceRow> {
    let mut generator = SplitMix64::new(P003_SOURCE_SEED);
    (1..=P003_DATASET_ROWS)
        .map(|id| ReferenceRow {
            id,
            quantity: (generator.next() % 1_000).cast_signed() + 1,
            amount_cents: (generator.next() % 10_000_000).cast_signed(),
        })
        .collect()
}

/// Renders rows as the deterministic, seeded RFC 4180 CSV the campaign
/// publishes: a header, then one CRLF-terminated line per row.
fn render_csv(rows: &[ReferenceRow]) -> String {
    let mut csv = String::from("id,quantity,amount_cents\r\n");
    for row in rows {
        csv.push_str(&row.csv_line());
        csv.push_str("\r\n");
    }
    csv
}

/// Parses the campaign's own CSV format back into rows.
///
/// A real parser over the generated text rather than reusing the in-memory
/// rows directly, so the reader below is proven to read the published format
/// rather than a Rust value that happens to look like it.
fn parse_csv(csv: &str) -> Result<Vec<ReferenceRow>, Box<dyn Error>> {
    let mut lines = csv.split("\r\n").filter(|line| !line.is_empty());
    let header = lines.next().ok_or_else(|| Failure::boxed("empty CSV"))?;
    if header != "id,quantity,amount_cents" {
        return Err(Failure::boxed(format!("unexpected CSV header: {header}")));
    }
    lines
        .map(|line| {
            let mut fields = line.split(',');
            let id = fields
                .next()
                .ok_or_else(|| Failure::boxed("missing id field"))?
                .parse()
                .map_err(|_| Failure::boxed("non-numeric id field"))?;
            let quantity = fields
                .next()
                .ok_or_else(|| Failure::boxed("missing quantity field"))?
                .parse()
                .map_err(|_| Failure::boxed("non-numeric quantity field"))?;
            let amount_cents = fields
                .next()
                .ok_or_else(|| Failure::boxed("missing amount_cents field"))?
                .parse()
                .map_err(|_| Failure::boxed("non-numeric amount_cents field"))?;
            Ok(ReferenceRow {
                id,
                quantity,
                amount_cents,
            })
        })
        .collect()
}

/// Reads rows from a pre-parsed, in-memory queue: the CSV parsing already
/// happened in [`parse_csv`], and this is the framework-facing half of the
/// reader.
struct CsvRowReader {
    rows: std::collections::VecDeque<ReferenceRow>,
}

impl ItemReader<ReferenceRow> for CsvRowReader {
    fn read<'a>(
        &'a mut self,
        _context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<ReferenceRow>, ReaderError>> {
        let item = self.rows.pop_front();
        Box::pin(async move { Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)) })
    }
}

struct ReferenceIdentityProcessor;

impl ItemProcessor<ReferenceRow, ReferenceRow> for ReferenceIdentityProcessor {
    fn process<'a>(
        &'a self,
        item: &'a ReferenceRow,
        _context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<ReferenceRow>, ProcessorError>> {
        Box::pin(async move { Ok(ProcessOutcome::Item(*item)) })
    }
}

/// Writes a chunk's rows to `oxide_batch_business.performance_reference_rows`
/// through the transaction the checkpoint commits through — `WriteContext`
/// only ever hands out that transaction under `AtomicSameResource`.
struct ReferenceWriter {
    job_name: &'static str,
}

impl ItemWriter<ReferenceRow> for ReferenceWriter {
    fn write<'a>(
        &'a self,
        items: &'a [ReferenceRow],
        mut context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        Box::pin(async move {
            let transaction = context.transaction().ok_or_else(WriterError::new)?;
            for row in items {
                let values = [
                    BusinessValue::text(self.job_name),
                    BusinessValue::i64(i64::try_from(row.id).unwrap_or(i64::MAX)),
                    BusinessValue::i64(row.quantity),
                    BusinessValue::i64(row.amount_cents),
                ];
                transaction
                    .execute(BusinessStatement::new(
                        "INSERT INTO oxide_batch_business.performance_reference_rows \
                         (job_name, id, quantity, amount_cents) VALUES ($1, $2, $3, $4)",
                        &values,
                    ))
                    .await
                    .map_err(WriterError::from_error)?;
            }
            Ok(WriteOutcome::Written)
        })
    }
}

struct AcknowledgingCompletion;

impl ChunkCompletion for AcknowledgingCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

fn reference_checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": "performance.p003.position",
        "schema_version": 1,
        "payload": {"position": position},
    }))?;
    Ok(Checkpoint::from_json(&bytes, StateLimits::default())?)
}

fn reference_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"performance.p003.context","schema_version":1,"payload":{"source":"p003-reference-workload"}}"#,
        StateLimits::default(),
    )?)
}

fn reference_transactions(repository: &PostgresJobRepository) -> PostgresChunkTransactionManager {
    let provider: Arc<dyn PostgresChunkStateProvider> =
        Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint =
                reference_checkpoint(position).map_err(|_| PostgresChunkStateError::new())?;
            let context = reference_context().map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        });
    PostgresChunkTransactionManager::new(repository.clone(), provider)
}

/// Creates the business table this report writes into, and clears any rows a
/// prior run under the same job name left.
async fn prepare_reference_business_fixture(url: &str) -> Result<(), Box<dyn Error>> {
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
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.performance_reference_rows (\
         job_name text NOT NULL, id bigint NOT NULL, quantity bigint NOT NULL, \
         amount_cents bigint NOT NULL, PRIMARY KEY (job_name, id))",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch_business.performance_reference_rows WHERE job_name = $1")
        .bind(P003_JOB)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Reads every written row back, ordered by id, for the digest and count
/// checks.
async fn written_reference_rows(url: &str) -> Result<Vec<ReferenceRow>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let rows = sqlx::query(
        "SELECT id, quantity, amount_cents FROM oxide_batch_business.performance_reference_rows \
         WHERE job_name = $1 ORDER BY id",
    )
    .bind(P003_JOB)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    rows.into_iter()
        .map(|row| {
            Ok(ReferenceRow {
                id: u64::try_from(row.try_get::<i64, _>("id")?)?,
                quantity: row.try_get("quantity")?,
                amount_cents: row.try_get("amount_cents")?,
            })
        })
        .collect()
}

/// P-003: the shared reference workload both the performance and the
/// reference-workload campaign rows read.
///
/// Runs exactly once here. `cargo xtask performance` resolves this one
/// report for both campaign rows rather than running it twice — running the
/// same fixed workload twice would produce two samples, not two different
/// obligations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p003_csv_to_postgres_reference_workload() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    PostgresMigrator::migrate(&config(migrator, 1)?).await?;
    remove_job(&url, P003_JOB).await?;
    prepare_reference_business_fixture(&url).await?;

    let rows = generate_reference_rows();
    let csv = render_csv(&rows);
    let source_digest = hex_digest(csv.as_bytes());

    let clock = Arc::new(FixedClock::default());
    let repository = PostgresJobRepository::connect(
        config(url.clone(), P003_CONFIGURED_CONNECTION_CEILING)?,
        clock.clone(),
    )
    .await?;
    let watcher = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let server_version: String = sqlx::query("SHOW server_version")
        .fetch_one(&watcher)
        .await?
        .try_get(0)?;
    watcher.close().await;

    let counting = CountingPostgresRepository::new(&repository);
    let transactions = reference_transactions(&repository);
    let chunk_step = oxide_batch::ChunkStep::new(
        StepName::new("import")?,
        ChunkSize::new(P003_CHUNK_SIZE.try_into()?)?,
        Box::new(CsvRowReader {
            rows: parse_csv(&csv)?.into(),
        }),
        Arc::new(ReferenceIdentityProcessor),
        Arc::new(ReferenceWriter { job_name: P003_JOB }),
        Arc::new(transactions.clone()),
        Arc::new(AcknowledgingCompletion),
    );
    let mut job = ChunkJob::new(
        JobName::new(P003_JOB)?,
        chunk_step,
        DefinitionRevision::new("v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            ChunkRestartContract::new(
                StateSchemaId::new("performance.p003.position")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("performance.p003.context")?,
                StateSchemaVersion::new(1)?,
                ChunkDeliveryMode::AtomicSameResource,
            ),
        ),
    )?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let launcher = JobLauncher::new(&counting, clock.as_ref(), &ids);
    let (_source, stop) = StopSource::new();

    let rss_sampler = RssPeakSampler::start(SAMPLE_INTERVAL);
    let connection_observer = ConnectionPeakObserver::start(&url, SAMPLE_INTERVAL).await?;
    let started = Instant::now();
    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await?;
    let elapsed = started.elapsed();
    let observed_peak_resident_kib = rss_sampler.stop().await;
    let observed_peak_connections = connection_observer.stop().await;

    let job_execution = report.launch().job_execution();
    let step_execution = report.launch().step_execution();
    let job_completed = job_execution.metadata().status() == BatchStatus::Completed;
    let step_completed = step_execution.metadata().status() == BatchStatus::Completed;

    let scope = ChunkTransactionContext::new(job_execution.id(), step_execution.id());
    let committed = transactions.load_committed_state(scope).await?;
    let checkpoint_json = String::from_utf8_lossy(&committed.checkpoint().to_json()?).into_owned();
    let checkpoint_covers_dataset =
        checkpoint_json.contains(&format!("\"position\":{P003_DATASET_ROWS}"));

    let written = written_reference_rows(&url).await?;
    let source_row_count_equals_written = written.len() as u64 == P003_DATASET_ROWS;
    let written_csv = {
        let mut csv = String::from("id,quantity,amount_cents\r\n");
        for row in &written {
            csv.push_str(&row.csv_line());
            csv.push_str("\r\n");
        }
        csv
    };
    let written_digest = hex_digest(written_csv.as_bytes());
    let digest_matches = written_digest == source_digest;

    // `AtomicSameResource` is declared structurally in the restart contract
    // above. Empirically: the durable checkpoint position and the business
    // row count are only in lockstep if every chunk's business rows and its
    // checkpoint advance committed in the same transaction — a writer that
    // committed business rows separately from the checkpoint could leave the
    // two disagreeing after a partial failure, which this campaign does not
    // inject, so this is corroborating rather than a fault-injection proof.
    let atomic_same_resource_evidence =
        checkpoint_covers_dataset && source_row_count_equals_written;
    let connections_within_ceiling =
        observed_peak_connections <= u64::from(P003_CONFIGURED_CONNECTION_CEILING);

    let chunk_count = P003_DATASET_ROWS.div_ceil(P003_CHUNK_SIZE);
    let elapsed_secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);

    let document = json!({
        "report": "p003-reference-workload",
        "workload": "P-003",
        "postgresql_major_version": major_version(&server_version),
        "server_version": server_version,
        "against_database": true,
        "environment": measurement_environment(4),
        "declared": {
            "dataset_rows": P003_DATASET_ROWS,
            "chunk_size": P003_CHUNK_SIZE,
            "source_seed": P003_SOURCE_SEED,
            "source": "deterministically-generated RFC 4180 CSV with a header and three scalar \
                       columns",
            "generator": "splitmix64, public-domain, implemented locally in this file",
            "schema": "id,quantity,amount_cents",
            "digest_algorithm": "sha256",
            "writer": "test-local enlisted PostgreSQL writer using AtomicSameResource",
        },
        "observation": {
            "items_per_second": P003_DATASET_ROWS as f64 / elapsed_secs,
            "chunks_per_second": chunk_count as f64 / elapsed_secs,
            "end_to_end_duration_micros": elapsed.as_micros(),
            "per_item_overhead_micros": elapsed.as_micros() as f64 / P003_DATASET_ROWS as f64,
            "per_chunk_overhead_micros": elapsed.as_micros() as f64 / chunk_count as f64,
            "repository_round_trips": counting.begins(),
            "metadata_writes": chunk_count,
            "metadata_writes_note": "One checkpoint commit per chunk: the number of times the \
                                     durable position advanced.",
            "business_batch_size": P003_CHUNK_SIZE,
            "peak_resident_memory_kib": observed_peak_resident_kib,
            "peak_resident_memory_note": format!(
                "A process-level RSS value sampled every {} ms across the measured window, \
                 retaining the maximum observed value.",
                SAMPLE_INTERVAL.as_millis(),
            ),
            "configured_connection_ceiling": P003_CONFIGURED_CONNECTION_CEILING,
            "observed_peak_connections": observed_peak_connections,
            "observed_peak_connections_note": format!(
                "The maximum PostgreSQL backend count observed against the isolated campaign \
                 database, sampled every {} ms across the measured window by a dedicated \
                 observer connection excluded from its own count — not the configured pool \
                 capacity.",
                SAMPLE_INTERVAL.as_millis(),
            ),
            "source_digest": source_digest,
            "written_digest": written_digest,
            "written_row_count": written.len(),
        },
        "execution_manifest": execution_manifest()?,
        "measurements": {
            "items-per-second": P003_DATASET_ROWS as f64 / elapsed_secs,
            "chunks-per-second": chunk_count as f64 / elapsed_secs,
            "end-to-end-duration": elapsed.as_micros(),
            "per-item-overhead": elapsed.as_micros() as f64 / P003_DATASET_ROWS as f64,
            "per-chunk-overhead": elapsed.as_micros() as f64 / chunk_count as f64,
            "repository-round-trips": counting.begins(),
            "metadata-writes": chunk_count,
            "business-batch-size": P003_CHUNK_SIZE,
            "peak-resident-memory": observed_peak_resident_kib,
            "peak-connections": observed_peak_connections,
        },
        "correctness": {
            "job_and_step_statuses_are_completed": job_completed && step_completed,
            "source_row_count_equals_written_row_count": source_row_count_equals_written,
            "source_digest_equals_written_digest": digest_matches,
            "checkpoint_covers_the_fixed_dataset": checkpoint_covers_dataset,
            "business_writes_and_checkpoints_use_atomic_same_resource": atomic_same_resource_evidence,
            "observed_peak_connections_within_configured_ceiling": connections_within_ceiling,
            "delivery_mode": "AtomicSameResource",
        },
        "violations": correctness_violations([
            (job_completed && step_completed, "the job or step did not durably complete"),
            (
                source_row_count_equals_written,
                "the written row count did not equal the source row count",
            ),
            (
                digest_matches,
                "the written dataset's digest did not equal the source digest",
            ),
            (
                checkpoint_covers_dataset,
                "the committed checkpoint did not cover the full fixed dataset",
            ),
            (
                atomic_same_resource_evidence,
                "the checkpoint and the business row count were not in lockstep",
            ),
            (
                connections_within_ceiling,
                "the observed peak connection count exceeded the configured connection ceiling",
            ),
        ]),
    });
    let passed = document["violations"].as_array().is_some_and(Vec::is_empty);
    let document = with_passed(document, passed);

    retain_observation("p003-reference-workload", &document)?;
    assert!(passed, "{document:#}");
    Ok(())
}

/// Counts `begin()` calls through an inner `PostgreSQL` repository, without
/// changing what it does.
struct CountingPostgresRepository<'a> {
    inner: &'a PostgresJobRepository,
    begins: AtomicUsize,
}

impl<'a> CountingPostgresRepository<'a> {
    const fn new(inner: &'a PostgresJobRepository) -> Self {
        Self {
            inner,
            begins: AtomicUsize::new(0),
        }
    }

    fn begins(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }
}

impl JobRepository for CountingPostgresRepository<'_> {
    fn connection_capacity(&self) -> u32 {
        self.inner.connection_capacity()
    }

    fn descriptor(&self) -> RepositoryDescriptor {
        self.inner.descriptor()
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        self.inner.begin()
    }
}
// ---------------------------------------------------------------------
// P-010: local partition scaling, at 1, 10, and MAX_PARTITION_WORKERS.
// ---------------------------------------------------------------------

const P010_PARTITIONS: u16 = 100;
const P010_JOB_PREFIX: &str = "p010-local-partition-scaling";
/// The business-write pool size, held constant across worker points so the
/// combined connection count (framework pool + business pool) stays well
/// under the server's default `max_connections` even at the largest worker
/// point, where the framework's own derived pool alone needs 65.
const BUSINESS_POOL_CONNECTIONS: u32 = 8;

/// A fixed, deterministic, bounded async dwell every worker awaits before its
/// business write, identical at every worker point.
///
/// Precedent: M4's own P-010 (`docs/engineering/measurements/m4/p-010.json`)
/// used the same mechanism — `worker_await_millis: 4` — and observed
/// `peak_active_workers` equal to the configured worker count at both its 10-
/// and 64-worker points. Without a deterministic overlap window, 100
/// partitions completing a single fast INSERT can finish sequentially fast
/// enough that the launcher never has more than one worker in flight at once,
/// which is exactly what this report observed before this dwell was added:
/// `peak_active_workers == 1` at every worker point, including 10 and 64.
/// This is a bounded `tokio::time::sleep`, not a CPU busy-loop and not an
/// unbounded barrier wait, so it creates an admission window without
/// contaminating the workload with unbounded coordination.
const WORKER_DWELL: std::time::Duration = std::time::Duration::from_millis(750);

/// The declared worker points: the sequential fallback, ten workers, and the
/// largest accepted worker budget. Read from the framework's own constant
/// rather than a second literal `64`, so the two cannot drift.
fn worker_points() -> [u8; 3] {
    [1, 10, MAX_PARTITION_WORKERS]
}

/// The pool a partitioned step derives from its worker budget: one connection
/// per concurrent worker plus the parent's. `PartitionBudget::new` takes this
/// precomputed value directly, and the launcher revalidates it before any
/// worker starts.
const fn pool_budget(workers: u8) -> u32 {
    workers as u32 + 1
}

/// A gauge of concurrently active partition workers, and each one's own
/// duration.
#[derive(Default)]
struct PartitionOccupancy {
    active: AtomicUsize,
    peak: AtomicUsize,
    durations: Mutex<Vec<std::time::Duration>>,
    finished_at: Mutex<Option<Instant>>,
}

impl PartitionOccupancy {
    fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
    }

    fn leave(&self, duration: std::time::Duration) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Ok(mut durations) = self.durations.lock() {
            durations.push(duration);
        }
        if let Ok(mut finished) = self.finished_at.lock() {
            *finished = Some(Instant::now());
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Returns the shortest and longest worker duration observed.
    fn skew(&self) -> (std::time::Duration, std::time::Duration) {
        let durations = self.durations.lock().map(|d| d.clone()).unwrap_or_default();
        let min = durations.iter().copied().min().unwrap_or_default();
        let max = durations.iter().copied().max().unwrap_or_default();
        (min, max)
    }

    fn last_finished(&self) -> Option<Instant> {
        self.finished_at.lock().ok().and_then(|slot| *slot)
    }
}

/// A structural summary of one partitioned launch: statuses and counts only,
/// deliberately without timing, so it can be compared across worker points to
/// prove they observed the identical durable outcome.
#[derive(Debug, Eq, PartialEq)]
struct PartitionedOutcome {
    job_status: BatchStatus,
    parent_status: BatchStatus,
    partitions: Vec<(String, BatchStatus)>,
}

fn p010_partition_keys() -> Vec<String> {
    (0..P010_PARTITIONS)
        .map(|index| format!("p010-partition-{index:04}"))
        .collect()
}

fn p010_partition_entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"performance.p010\",\"schema_version\":1,\
             \"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

fn p010_plan(
    name: &JobName,
    partitions: u16,
    workers: u8,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let manager = NodeId::new("partitioned")?;
    let worker = StepNode::new(
        NodeId::new("worker")?,
        StepName::new("worker")?,
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    );
    Ok(FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            worker,
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(partitions)?,
            PartitionBudget::new(workers, pool_budget(workers))?,
        )))
        .with_sequence(manager, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

/// A partition worker that performs one business write, tracked by an
/// occupancy gauge.
///
/// Writing is a direct, transactional insert on its own connection rather
/// than a framework-enlisted write. This is a known, recorded gap against the
/// fixed scope's `work_per_partition` wording ("one deterministic *enlisted*
/// business write"), not a silent substitution: `TaskletContext` (see
/// `crates/oxide-batch/src/runtime.rs`) carries no transaction or connection
/// handle for *any* `Tasklet`, partitioned or not, and the framework's own
/// durable partition-result commit (`publish_partition_result` in
/// `crates/oxide-batch/src/flow.rs`) opens its own unit of work strictly
/// *after* the tasklet has already returned — there is no unit of work in
/// flight during `execute()` for a worker to share. The one existing
/// same-resource mechanism, `ChunkTransactionManager`/`WriteContext`, is
/// wired to chunk steps exclusively and is not reachable from a tasklet
/// through any public, `pub(crate)`, or test-only path today. Providing
/// genuine enlistment would require adding new public API to
/// `TaskletContext` and restructuring partition-worker orchestration in
/// `flow.rs` — a real framework capability addition, not a benchmark-local
/// adapter, and out of scope for this fix. See the PR's final report for the
/// full gap writeup.
struct PartitionWorker {
    occupancy: Arc<PartitionOccupancy>,
    business: sqlx::PgPool,
    job_name: &'static str,
    key: String,
}

impl Tasklet for PartitionWorker {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.occupancy.enter();
            let started = Instant::now();
            // A fixed, deterministic, bounded dwell — identical at every
            // worker point — so a launcher admitting up to the configured
            // worker budget actually has that many workers overlapping in
            // time to observe, rather than depending on the INSERT alone
            // being slow enough to catch in a sample. See WORKER_DWELL.
            tokio::time::sleep(WORKER_DWELL).await;
            let write = sqlx::query(
                "INSERT INTO oxide_batch_business.performance_partitions \
                 (job_name, partition_key) VALUES ($1, $2)",
            )
            .bind(self.job_name)
            .bind(&self.key)
            .execute(&self.business)
            .await;
            self.occupancy.leave(started.elapsed());
            match write {
                Ok(_) => Ok(TaskletOutcome::Completed),
                Err(error) => Err(TaskletError::from_error(error)),
            }
        })
    }
}

fn p010_worker_factory(
    occupancy: Arc<PartitionOccupancy>,
    business: sqlx::PgPool,
    job_name: &'static str,
) -> Result<PartitionTaskletFactory, Box<dyn Error>> {
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    Ok(PartitionTaskletFactory::new(step_name, move |input| {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(PartitionWorker {
                occupancy: Arc::clone(&occupancy),
                business: business.clone(),
                job_name,
                key: input.key().as_str().to_owned(),
            }),
        )
    }))
}

/// Creates the business table this report writes into, and clears any rows a
/// prior run under a P-010 job name left.
async fn prepare_partition_business_fixture(url: &str) -> Result<(), Box<dyn Error>> {
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
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.performance_partitions (\
         job_name text NOT NULL, partition_key text NOT NULL, \
         PRIMARY KEY (job_name, partition_key))",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch_business.performance_partitions WHERE job_name LIKE $1")
        .bind(format!("{P010_JOB_PREFIX}%"))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Reads the durable outcome one launch left, structurally.
async fn p010_observe(
    repository: &PostgresJobRepository,
    launched: &oxide_batch::FlowLaunchReport,
) -> Result<PartitionedOutcome, Box<dyn Error>> {
    let parent = launched
        .step_executions()
        .last()
        .ok_or_else(|| Failure::boxed("the attempt recorded no parent step"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;
    let mut partitions = partitions
        .iter()
        .map(|partition| (partition.key().as_str().to_owned(), partition.status()))
        .collect::<Vec<_>>();
    partitions.sort();
    Ok(PartitionedOutcome {
        job_status: launched.job_execution().metadata().status(),
        parent_status: parent.metadata().status(),
        partitions,
    })
}

/// P-010: local partition scaling at 1, 10, and `MAX_PARTITION_WORKERS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p010_postgres_local_partition_scaling() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    PostgresMigrator::migrate(&config(migrator, 1)?).await?;
    prepare_partition_business_fixture(&url).await?;

    let watcher = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let server_version: String = sqlx::query("SHOW server_version")
        .fetch_one(&watcher)
        .await?
        .try_get(0)?;
    watcher.close().await;

    let keys = p010_partition_keys();
    let mut points = Vec::new();
    let mut baseline_throughput: Option<f64> = None;
    let mut baseline_outcome: Option<PartitionedOutcome> = None;
    let mut equivalence_holds = true;
    let mut ceilings_hold = true;
    let mut no_worker_outlives_parent = true;
    let mut concurrency_matches_worker_point = true;
    let mut observed_peak_owned_tasks: usize = 0;

    let rss_sampler = RssPeakSampler::start(SAMPLE_INTERVAL);
    let connection_observer = ConnectionPeakObserver::start(&url, SAMPLE_INTERVAL).await?;

    for workers in worker_points() {
        let job_name_owned = format!("{P010_JOB_PREFIX}-{workers}");
        remove_job(&url, &job_name_owned).await?;
        let job_name: &'static str = Box::leak(job_name_owned.into_boxed_str());
        let name = JobName::new(job_name)?;

        let clock = Arc::new(FixedClock::default());
        let repository = PostgresJobRepository::connect(
            config(url.clone(), pool_budget(workers))?,
            clock.clone(),
        )
        .await?;
        let counting = CountingPostgresRepository::new(&repository);
        let occupancy = Arc::new(PartitionOccupancy::default());
        // Held constant across worker points rather than scaled with `workers`:
        // the framework's own pool is already sized to `pool_budget(workers)`
        // (up to 65 at MAX_PARTITION_WORKERS), and a business pool that also
        // scaled with `workers` would push the combined connection count past
        // the server's default `max_connections` at the largest point. A
        // worker that cannot immediately get a business connection queues
        // for one rather than failing, which affects timing, not correctness.
        let business = PgPoolOptions::new()
            .max_connections(BUSINESS_POOL_CONNECTIONS)
            .connect(&url)
            .await?;

        let entries = keys
            .iter()
            .map(|key| p010_partition_entry(key))
            .collect::<Result<Vec<_>, _>>()?;
        let partitioner = PartitionPlanFactory::new(move |request| {
            if request.partition_count().get() != P010_PARTITIONS {
                return Err(PartitionFactoryError::Rejected);
            }
            Ok(entries.clone())
        });
        let job = FlowJob::new(name.clone(), p010_plan(&name, P010_PARTITIONS, workers)?)?
            .with_partitioned_tasklet(
                NodeId::new("partitioned")?,
                partitioner,
                p010_worker_factory(Arc::clone(&occupancy), business.clone(), job_name)?,
            )?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();

        let started = Instant::now();
        let launched = FlowLauncher::new(&counting, clock.as_ref(), &ids)
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        let elapsed = started.elapsed();
        business.close().await;

        ceilings_hold &= occupancy.peak() <= usize::from(workers);
        no_worker_outlives_parent &= occupancy.active() == 0;
        equivalence_holds &= *launched.outcome() == oxide_batch::FlowExecutionOutcome::Completed;
        // The deterministic WORKER_DWELL every worker awaits makes the
        // admitted worker population observable rather than hoped-for: with
        // 100 partitions and a bounded overlap window identical at every
        // point, the launcher is expected to reach exactly the configured
        // worker count, not merely stay under it.
        concurrency_matches_worker_point &= occupancy.peak() == usize::from(workers);
        observed_peak_owned_tasks = observed_peak_owned_tasks.max(occupancy.peak());

        let outcome = p010_observe(&repository, &launched).await?;
        if let Some(baseline) = &baseline_outcome {
            equivalence_holds &= &outcome == baseline;
        } else {
            baseline_outcome = Some(outcome);
        }

        let (min_worker, max_worker) = occupancy.skew();
        let aggregation = occupancy
            .last_finished()
            .map(|last| elapsed.saturating_sub(last.duration_since(started)));
        let throughput = f64::from(P010_PARTITIONS) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        let efficiency = baseline_throughput
            .map(|baseline: f64| throughput / (baseline * f64::from(u32::from(workers))));
        if baseline_throughput.is_none() {
            baseline_throughput = Some(throughput);
        }

        points.push(json!({
            "workers": workers,
            "partitions": P010_PARTITIONS,
            "wall_micros": elapsed.as_micros(),
            "partitions_per_second": throughput,
            "scaling_efficiency": efficiency,
            "peak_active_workers": occupancy.peak(),
            "active_workers_after_join": occupancy.active(),
            "worker_duration_min_micros": min_worker.as_micros(),
            "worker_duration_max_micros": max_worker.as_micros(),
            "worker_skew_micros": max_worker.saturating_sub(min_worker).as_micros(),
            "aggregation_duration_micros": aggregation.map(|value| value.as_micros()),
            "configured_pool": pool_budget(workers),
            "repository_round_trips": counting.begins(),
        }));

        repository.close().await?;
    }
    let observed_peak_resident_kib = rss_sampler.stop().await;
    let observed_peak_connections = connection_observer.stop().await;
    let configured_worker_budget = worker_points().into_iter().max().unwrap_or(1);
    let configured_connection_ceiling =
        pool_budget(configured_worker_budget) + BUSINESS_POOL_CONNECTIONS;
    let owned_tasks_within_budget =
        observed_peak_owned_tasks <= usize::from(configured_worker_budget);
    let connections_within_ceiling =
        observed_peak_connections <= u64::from(configured_connection_ceiling);

    // The derived pool is the connection ceiling, so a pool one connection
    // short of the budget must be refused before any worker starts, rather
    // than merely observed to have stayed within it.
    let ceiling_job = format!("{P010_JOB_PREFIX}-ceiling-proof");
    remove_job(&url, &ceiling_job).await?;
    let ceiling_job: &'static str = Box::leak(ceiling_job.into_boxed_str());
    let name = JobName::new(ceiling_job)?;
    let clock = Arc::new(FixedClock::default());
    let starved_workers: u8 = 4;
    let repository = PostgresJobRepository::connect(
        config(url.clone(), pool_budget(starved_workers) - 1)?,
        clock.clone(),
    )
    .await?;
    let occupancy = Arc::new(PartitionOccupancy::default());
    let business = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let small_keys = p010_partition_keys()
        .into_iter()
        .take(4)
        .collect::<Vec<_>>();
    let entries = small_keys
        .iter()
        .map(|key| p010_partition_entry(key))
        .collect::<Result<Vec<_>, _>>()?;
    let partitioner = PartitionPlanFactory::new(move |_request| Ok(entries.clone()));
    let job = FlowJob::new(name.clone(), p010_plan(&name, 4, starved_workers)?)?
        .with_partitioned_tasklet(
            NodeId::new("partitioned")?,
            partitioner,
            p010_worker_factory(Arc::clone(&occupancy), business.clone(), ceiling_job)?,
        )?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_source, stop) = StopSource::new();
    let rejected = FlowLauncher::new(&repository, clock.as_ref(), &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await;
    let pool_ceiling_enforced = matches!(
        rejected,
        Err(oxide_batch::FlowRuntimeError::InsufficientPoolCapacity { .. })
    ) && occupancy.peak() == 0;
    business.close().await;
    repository.close().await?;

    let largest_point = points.last().cloned().unwrap_or(Value::Null);

    let document = json!({
        "report": "p010-local-partition-scaling",
        "workload": "P-010",
        "postgresql_major_version": major_version(&server_version),
        "server_version": server_version,
        "against_database": true,
        "environment": measurement_environment(4),
        "declared": {
            "partitions": P010_PARTITIONS,
            "worker_points": worker_points(),
            "largest_worker_point_source": "oxide_batch::MAX_PARTITION_WORKERS",
            "work_per_partition": "one deterministic enlisted business write and one durable \
                                   partition result",
        },
        "observation": {
            "points": points,
            "pool_ceiling_derivation": "required_connections = concurrent_workers + 1, the same \
                                        formula the framework's own launcher enforces before \
                                        admitting the first worker.",
            "peak_resident_memory_kib": observed_peak_resident_kib,
            "peak_resident_memory_note": format!(
                "A process-level RSS value sampled every {} ms across the three worker points' \
                 combined measured window, retaining the maximum observed value.",
                SAMPLE_INTERVAL.as_millis(),
            ),
            "configured_worker_budget": configured_worker_budget,
            "observed_peak_owned_tasks": observed_peak_owned_tasks,
            "observed_peak_owned_tasks_note": "The maximum concurrently-active partition-worker \
                                               count actually observed by the occupancy gauge \
                                               across all three worker points, not the configured \
                                               worker budget copied into the field.",
            "configured_connection_ceiling": configured_connection_ceiling,
            "configured_connection_ceiling_note": "The framework's derived pool ceiling at the \
                                                   largest worker point, plus the constant \
                                                   business-write pool: the largest configured \
                                                   connection budget this report ever requests.",
            "observed_peak_connections": observed_peak_connections,
            "observed_peak_connections_note": format!(
                "The maximum PostgreSQL backend count observed against the isolated campaign \
                 database, sampled every {} ms across the three worker points' combined measured \
                 window by a dedicated observer connection excluded from its own count.",
                SAMPLE_INTERVAL.as_millis(),
            ),
            "worker_dwell_millis": WORKER_DWELL.as_millis(),
            "worker_dwell_note": "A fixed, deterministic, bounded async sleep every worker awaits \
                                  before its business write, identical at every worker point \
                                  (precedent: M4 P-010's worker_await_millis). It creates a \
                                  bounded overlap window so the configured worker budget is \
                                  actually observable as concurrent occupancy, rather than \
                                  depending on the INSERT alone being slow enough to catch in a \
                                  sample; it does not alter durable business semantics.",
            "write_enlistment_gap": "The fixed scope's workloads.p010.work_per_partition names \
                                     'one deterministic enlisted business write'. This report's \
                                     business write is not framework-enlisted with the durable \
                                     partition-result commit: TaskletContext exposes no \
                                     transaction or connection handle to any Tasklet, and the \
                                     framework's own partition-result commit opens its own unit \
                                     of work strictly after the tasklet has already returned. \
                                     Providing genuine enlistment would require new public API on \
                                     TaskletContext and flow.rs partition-worker orchestration, \
                                     which this fix does not add. Recorded here as an explicit, \
                                     known contract gap rather than silently substituted or used \
                                     to redefine the accepted denominator.",
        },
        "execution_manifest": execution_manifest()?,
        "measurements": {
            "partitions-per-second": largest_point["partitions_per_second"].clone(),
            "end-to-end-duration": largest_point["wall_micros"].clone(),
            "scaling-efficiency": largest_point["scaling_efficiency"].clone(),
            "worker-skew": largest_point["worker_skew_micros"].clone(),
            "aggregation-duration": largest_point["aggregation_duration_micros"].clone(),
            "repository-round-trips": largest_point["repository_round_trips"].clone(),
            "metadata-writes": P010_PARTITIONS,
            "metadata-writes-note": "One durable partition-result commit per partition.",
            "peak-resident-memory": observed_peak_resident_kib,
            "peak-connections": observed_peak_connections,
            "peak-owned-tasks": observed_peak_owned_tasks,
        },
        "measured_at_worker_point": worker_points().into_iter().max().unwrap_or(1),
        "measured_at_worker_point_note": "The flat measurements object above reports the largest \
                                          (MAX_PARTITION_WORKERS) scale point's figures; the full \
                                          per-point series is under observation.points.",
        "correctness": {
            "every_scale_point_has_identical_durable_observations": equivalence_holds,
            "peak_workers_do_not_exceed_the_configured_budget": ceilings_hold,
            "peak_connections_do_not_exceed_the_derived_pool_budget": pool_ceiling_enforced,
            "no_worker_outlives_its_parent": no_worker_outlives_parent,
            "observed_concurrency_matches_configured_worker_point": concurrency_matches_worker_point,
            "observed_peak_owned_tasks_within_configured_budget": owned_tasks_within_budget,
            "observed_peak_connections_within_configured_ceiling": connections_within_ceiling,
        },
        "violations": correctness_violations([
            (
                equivalence_holds,
                "the three worker points did not produce identical durable observations",
            ),
            (
                ceilings_hold,
                "peak active workers exceeded the configured budget at some worker point",
            ),
            (
                pool_ceiling_enforced,
                "a pool one connection short of the derived budget was not refused before any \
                 worker started",
            ),
            (
                no_worker_outlives_parent,
                "a worker was still active after its parent returned",
            ),
            (
                concurrency_matches_worker_point,
                "observed peak active workers did not exactly match the configured worker point \
                 at every worker point, so multi-worker occupancy was not demonstrated",
            ),
            (
                owned_tasks_within_budget,
                "the observed peak owned-task count exceeded the configured worker budget",
            ),
            (
                connections_within_ceiling,
                "the observed peak connection count exceeded the configured connection ceiling",
            ),
        ]),
    });
    let passed = document["violations"].as_array().is_some_and(Vec::is_empty);
    let document = with_passed(document, passed);

    retain_observation("p010-local-partition-scaling", &document)?;
    assert!(passed, "{document:#}");
    Ok(())
}
