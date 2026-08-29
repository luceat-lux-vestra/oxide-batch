//! Shared foundation for Gate B (#153 §3): typed-vs-`Boxed*` transaction and
//! restart equivalence over real `PostgreSQL` crash/restart.
//!
//! This module owns three things every B-01..B-08 scenario needs and none of
//! them should reimplement:
//!
//! - [`Representation`]: the axis a scenario builds the same logical
//!   pipeline over twice, via [`typed_chunk_job`] and [`boxed_chunk_job`].
//!   Both call the same private [`assemble`] wiring, so "the same pipeline"
//!   is enforced by construction rather than by two functions kept in sync
//!   by hand.
//! - [`GateBObservation`]: a structured, `Serialize`/`PartialEq` snapshot of
//!   everything a scenario compares -- business rows, checkpoint, counts,
//!   component state, and job/step status -- read from durable `PostgreSQL`
//!   state via [`snapshot`], so a scenario asserts on structured values
//!   rather than diffing log strings.
//! - [`ParkAt`]/[`ParkingWriter`]/[`ParkingCompletion`]: hooks that park a
//!   worker process at a chosen chunk-commit boundary, for a caller to kill
//!   from outside with a real `SIGKILL`, the same way
//!   `postgres_commit_phase_process_kill.rs` does.
//!
//! Process spawn/handshake/park mechanics are *not* reused from
//! `oxide-batch-test`: that crate depends on `oxide-batch`, so `oxide-batch`
//! depending back on it even in `dev-dependencies` is a workspace dependency
//! cycle `cargo xtask deps` rejects (`docs/architecture/crate-extraction.md`).
//! This module instead reuses `crash_restore`'s `announce`/`park_until_killed`/
//! `wait_for_file`, the same shared local module
//! `postgres_commit_phase_process_kill.rs` and the M5 crash campaign already
//! use for exactly this.
//!
//! ## What this does not attempt
//!
//! [`postgres_commit_phase_process_kill.rs`](../postgres_commit_phase_process_kill.rs)
//! already proves five *sub-commit* phases (mid-`COMMIT`, acknowledgement
//! race, etc.) using an advisory-lock-blocked deferred trigger, at the raw
//! transaction-manager layer rather than through a full [`ChunkJob`]. This
//! module's [`ParkAt`] gives chunk-boundary precision (before a chunk's
//! writer call returns, or after its commit is acknowledged) through the
//! real item-component pipeline, which is the representation axis Gate B
//! needs and that lower-level harness does not exercise. Reaching the same
//! sub-commit precision *through* a representation-parameterized `ChunkJob`
//! (for scenarios that need it, e.g. an exact B-06 acknowledgement race) is
//! left to the scenario that needs it, reusing the same advisory-lock
//! technique against the business table this module writes to
//! (`oxide_batch_business.gate_b_output`).

#![cfg(feature = "postgres")]
#![allow(dead_code, reason = "not every scenario file uses every export yet")]

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BoxFuture, BoxedProcessor, BoxedReader, BoxedWriter, Checkpoint, ChunkCommitReceipt,
    ChunkCompletion, ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome,
    ChunkComponentRevisions, ChunkCounts, ChunkDeliveryMode, ChunkJob, ChunkRestartContract,
    ChunkSize, ChunkStep, ChunkTransactionManager, Clock, ComponentRevision, DefinitionRevision,
    ExecutionContext, ExecutionCounts, FailureCategory, ItemProcessor, ItemReader, ItemWriter,
    JobInstanceKey, JobName, JobParameters, JobRepository, PostgresChunkStateError,
    PostgresChunkStateProvider, PostgresChunkTransactionManager, PostgresConfig,
    PostgresJobRepository, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StateLimits, StateSchemaId, StateSchemaVersion, StepName, TlsMode,
    WriteContext, WriteOutcome, WriterError,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

#[path = "../crash_restore/mod.rs"]
pub(crate) mod crash_restore;

/// Which item-component erasure a Gate B worker builds its pipeline with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Representation {
    /// Fully monomorphized reader/processor/writer.
    Typed,
    /// The same reader/processor/writer, wrapped in `BoxedReader` /
    /// `BoxedProcessor` / `BoxedWriter`.
    Boxed,
}

impl Representation {
    /// Both representations, for a scenario that iterates them.
    pub const ALL: [Self; 2] = [Self::Typed, Self::Boxed];

    /// The identifier used in the environment and in retained observations.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Typed => "typed",
            Self::Boxed => "boxed",
        }
    }

    /// Parses a representation from [`REPRESENTATION_ENV`]'s value.
    ///
    /// # Errors
    ///
    /// Returns a description of the unrecognized value.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "typed" => Ok(Self::Typed),
            "boxed" => Ok(Self::Boxed),
            other => Err(format!("unknown Gate B representation {other}")),
        }
    }
}

/// The environment variable a Gate B worker reads to select its
/// [`Representation`], set by [`spawn_worker_with_representation`].
pub const REPRESENTATION_ENV: &str = "OXIDEBATCH_GATE_B_REPRESENTATION";

/// How long a scenario waits for a worker handshake or a durable write to
/// appear before treating the wait as failed.
pub const HANDSHAKE_BOUND: Duration = Duration::from_secs(30);

/// Returns the runtime connection string, when the fixture supplies one.
#[must_use]
pub fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

/// Returns the migrating connection string, when the fixture supplies one.
#[must_use]
pub fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

/// Builds a plaintext-`TLS` config for the campaign's local fixture.
///
/// # Errors
///
/// Returns the domain failure when `url` is not a valid `PostgreSQL` URL.
pub fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

/// A clock fixed at a caller-chosen instant, for deterministic timestamps.
#[derive(Clone, Copy)]
pub struct FixedClock(pub SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// A fixed instant this many seconds after the Unix epoch.
#[must_use]
pub fn epoch(offset_seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(offset_seconds)
}

/// A deterministic `ItemReader<i64>` over `0..len`, read one item at a time.
pub struct SequenceReader {
    next: i64,
    len: i64,
}

impl SequenceReader {
    /// Builds a reader that yields `0..len`.
    #[must_use]
    pub const fn new(len: i64) -> Self {
        Self { next: 0, len }
    }
}

impl ItemReader<i64> for SequenceReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        if self.next >= self.len {
            return Ok(ReadOutcome::EndOfInput);
        }
        let item = self.next;
        self.next += 1;
        Ok(ReadOutcome::Item(item))
    }
}

/// An `ItemProcessor<i64, i64>` that passes every item through unchanged.
pub struct IdentityProcessor;

impl ItemProcessor<i64, i64> for IdentityProcessor {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

/// An `ItemWriter<i64>` that durably enlists every item in
/// `oxide_batch_business.gate_b_output`, tagged by job name.
///
/// This is the evidence surface: whether these rows survive a crash exactly
/// where the durable checkpoint says they should is what
/// [`GateBObservation::business_rows`] checks.
pub struct BusinessWriter {
    pool: PgPool,
    job_name: &'static str,
}

impl BusinessWriter {
    /// Builds a writer that enlists rows tagged `job_name` through `pool`.
    #[must_use]
    pub const fn new(pool: PgPool, job_name: &'static str) -> Self {
        Self { pool, job_name }
    }
}

impl ItemWriter<i64> for BusinessWriter {
    async fn write(
        &self,
        items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        for item in items {
            sqlx::query(
                "INSERT INTO oxide_batch_business.gate_b_output (job_name, value) VALUES ($1, $2)",
            )
            .bind(self.job_name)
            .bind(item)
            .execute(&self.pool)
            .await
            .map_err(|_| WriterError::with_category(FailureCategory::PermanentInfrastructure))?;
        }
        Ok(WriteOutcome::Written)
    }
}

/// A no-op [`ChunkCompletion`] that only acknowledges, unless wrapped by
/// [`ParkingCompletion`].
struct Completion;

impl ChunkCompletion for Completion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

/// Ensures the campaign's business table and metadata schema exist, and
/// clears every durable row belonging to `job_name`.
///
/// # Errors
///
/// Returns the database failure when the fixture cannot be prepared.
pub async fn prepare_fixture(migrator_url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    oxide_batch::PostgresMigrator::migrate(&config(migrator_url.to_owned())?).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(migrator_url)
        .await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.gate_b_output (\
         id bigserial PRIMARY KEY, job_name text NOT NULL, value bigint NOT NULL)",
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
        "DELETE FROM oxide_batch_business.gate_b_output WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(job_name).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

/// Reads every durably committed business row for `job_name`, in write order.
///
/// # Errors
///
/// Returns the database failure when the rows cannot be read.
pub async fn business_rows(url: &str, job_name: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT value FROM oxide_batch_business.gate_b_output WHERE job_name = $1 ORDER BY id",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(rows)
}

/// Encodes a reader position as the durable checkpoint every Gate B job uses.
fn checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    Ok(Checkpoint::from_json(
        &serde_json::to_vec(&serde_json::json!({
            "format": "oxide-batch.checkpoint",
            "format_version": 1,
            "schema": "gate-b.position",
            "schema_version": 1,
            "payload": {"position": position},
        }))?,
        StateLimits::default(),
    )?)
}

/// Decodes the position [`checkpoint`] encoded.
///
/// # Errors
///
/// Returns a description when the checkpoint carries no position.
pub fn checkpoint_position(value: &Checkpoint) -> Result<u64, Box<dyn Error>> {
    let envelope: serde_json::Value = serde_json::from_slice(&value.to_json()?)?;
    envelope
        .pointer("/payload/position")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "checkpoint carries no position".into())
}

/// An empty execution context, since Gate B's pipeline needs none.
fn execution_context() -> Result<ExecutionContext, Box<dyn Error>> {
    Ok(ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"gate-b.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )?)
}

/// The restart contract every Gate B job declares, over the checkpoint
/// schema [`checkpoint`] encodes.
///
/// # Errors
///
/// Returns the domain failure when a schema identifier or version is invalid.
pub fn restart_contract() -> Result<ChunkRestartContract, Box<dyn Error>> {
    Ok(ChunkRestartContract::new(
        StateSchemaId::new("gate-b.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("gate-b.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

/// Builds a transaction manager whose checkpoint provider encodes the
/// reader position as `committed reads + this chunk's reads`.
#[must_use]
pub fn transaction_manager(repository: &PostgresJobRepository) -> PostgresChunkTransactionManager {
    let provider: Arc<dyn PostgresChunkStateProvider> =
        Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            Ok(ChunkCommitReceipt::new(
                checkpoint(position).map_err(|_| PostgresChunkStateError::new())?,
                execution_context().map_err(|_| PostgresChunkStateError::new())?,
            ))
        });
    PostgresChunkTransactionManager::new(repository.clone(), provider)
}

/// Everything one representation's construction needs, shared by both
/// [`typed_chunk_job`] and [`boxed_chunk_job`] so "the same pipeline" is one
/// set of values rather than two call sites that must be kept in sync.
pub struct GateBParams<M> {
    /// The job name, also the business table's job-name tag.
    pub job_name: &'static str,
    /// How many items the reader yields.
    pub items: i64,
    /// The chunk size every representation commits at.
    pub chunk_size: u32,
    /// The `PostgreSQL` pool the writer enlists business rows through.
    pub pool: PgPool,
    /// The transaction manager, shared identically by both representations.
    pub transactions: Arc<M>,
}

/// This module's job type, generic over reader/processor/writer so the
/// representation-specific aliases below only need to fix those three.
type Job<R, P, W> = ChunkJob<i64, i64, R, P, W>;

/// The fully typed representation, generic over its writer so
/// [`typed_chunk_job_with_writer`] can share it with a caller-supplied one.
pub type TypedJob<W = BusinessWriter> = Job<SequenceReader, IdentityProcessor, W>;

/// The `Boxed` representation -- fixed, since erasure behind
/// `BoxedReader`/`BoxedProcessor`/`BoxedWriter` is the point of it.
pub type BoxedJob = Job<BoxedReader<i64>, BoxedProcessor<i64, i64>, BoxedWriter<i64>>;

/// Wires one representation's reader/processor/writer into a [`ChunkJob`].
///
/// Both [`typed_chunk_job`] and [`boxed_chunk_job`] call this with the only
/// difference being whether `reader`/`processor`/`writer` are wrapped in
/// `BoxedReader`/`BoxedProcessor`/`BoxedWriter` before the call -- every
/// other construction parameter (chunk size, transactions, completion,
/// definition revision, component revisions, restart contract) is identical.
fn assemble<M, R, P, W>(
    params: &GateBParams<M>,
    reader: R,
    processor: P,
    writer: W,
) -> Result<Job<R, P, W>, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    R: ItemReader<i64> + Send + 'static,
    P: ItemProcessor<i64, i64> + Send + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let step = ChunkStep::new(
        StepName::new(params.job_name)?,
        ChunkSize::new(params.chunk_size)?,
        reader,
        processor,
        writer,
        Arc::clone(&params.transactions) as Arc<dyn ChunkTransactionManager>,
        Arc::new(Completion),
    );
    Ok(ChunkJob::new(
        JobName::new(params.job_name)?,
        step,
        DefinitionRevision::new("gate-b-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            restart_contract()?,
        ),
    )?)
}

/// Builds the fully typed representation of a Gate B pipeline.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn typed_chunk_job<M: ChunkTransactionManager + 'static>(
    params: &GateBParams<M>,
) -> Result<TypedJob, Box<dyn Error>> {
    assemble(
        params,
        SequenceReader::new(params.items),
        IdentityProcessor,
        BusinessWriter {
            pool: params.pool.clone(),
            job_name: params.job_name,
        },
    )
}

/// Builds the typed representation with a caller-supplied writer, for a
/// scenario that needs to observe or park inside the writer call (see
/// [`ParkingWriter`]) while keeping the reader and processor identical to
/// [`typed_chunk_job`].
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn typed_chunk_job_with_writer<M, W>(
    params: &GateBParams<M>,
    writer: W,
) -> Result<TypedJob<W>, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    assemble(
        params,
        SequenceReader::new(params.items),
        IdentityProcessor,
        writer,
    )
}

/// Builds the `Boxed` representation with a caller-supplied writer, boxed
/// the same way [`boxed_chunk_job`] boxes its default writer. Pair with
/// [`typed_chunk_job_with_writer`] to compare the same [`ParkAt`] boundary
/// under both representations.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn boxed_chunk_job_with_writer<M, W>(
    params: &GateBParams<M>,
    writer: W,
) -> Result<BoxedJob, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    assemble(
        params,
        BoxedReader::new(SequenceReader::new(params.items)),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
    )
}

/// Builds the same logical pipeline as [`typed_chunk_job`], erased behind
/// `BoxedReader`/`BoxedProcessor`/`BoxedWriter`.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn boxed_chunk_job<M: ChunkTransactionManager + 'static>(
    params: &GateBParams<M>,
) -> Result<BoxedJob, Box<dyn Error>> {
    assemble(
        params,
        BoxedReader::new(SequenceReader::new(params.items)),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(BusinessWriter {
            pool: params.pool.clone(),
            job_name: params.job_name,
        }),
    )
}

/// A structured, comparable snapshot of everything a Gate B scenario checks.
///
/// `PartialEq`/`Debug` so `assert_eq!` between a typed and a `Boxed` run
/// gives a real field-by-field diff instead of a log-string comparison;
/// [`GateBObservation::to_json`] renders it as machine-readable evidence,
/// matching this workspace's existing `serde_json::json!` convention rather
/// than adding a `serde` derive dependency this crate does not otherwise use.
#[derive(Clone, Debug, PartialEq)]
pub struct GateBObservation {
    /// The durably committed business rows, in write order.
    pub business_rows: Vec<i64>,
    /// The last committed checkpoint position, if any attempt has committed.
    pub checkpoint_position: Option<u64>,
    /// The durable counters as of the last committed chunk.
    pub counts: CountsSnapshot,
    /// The job instance's executions, oldest first, as
    /// `(status, step_status)` -- the normalized lifecycle trace.
    pub lifecycle_trace: Vec<(String, String)>,
}

impl GateBObservation {
    /// Renders this observation as machine-readable evidence.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "business_rows": self.business_rows,
            "checkpoint_position": self.checkpoint_position,
            "counts": self.counts.to_json(),
            "lifecycle_trace": self.lifecycle_trace,
        })
    }
}

/// A projection of [`ExecutionCounts`] this module owns the fields of.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountsSnapshot {
    /// Items read.
    pub read: u64,
    /// Items processed.
    pub processed: u64,
    /// Items written.
    pub written: u64,
    /// Items filtered out.
    pub filtered: u64,
    /// Committed chunks.
    pub committed: u64,
    /// Rolled-back chunks.
    pub rolled_back: u64,
}

impl CountsSnapshot {
    /// Renders this snapshot as machine-readable evidence.
    #[must_use]
    pub fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "read": self.read,
            "processed": self.processed,
            "written": self.written,
            "filtered": self.filtered,
            "committed": self.committed,
            "rolled_back": self.rolled_back,
        })
    }
}

impl From<ExecutionCounts> for CountsSnapshot {
    fn from(value: ExecutionCounts) -> Self {
        Self {
            read: value.read(),
            processed: value.processed(),
            written: value.written(),
            filtered: value.filtered(),
            committed: value.committed(),
            rolled_back: value.rolled_back(),
        }
    }
}

/// Reads a [`GateBObservation`] of `job_name`'s durable state through a
/// fresh connection -- the authoritative source is `PostgreSQL`, never a
/// killed worker's process-local memory.
///
/// # Errors
///
/// Returns the repository or database failure when the durable state cannot
/// be read.
pub async fn snapshot(
    runtime_url: &str,
    repository: &PostgresJobRepository,
    job_name: &str,
) -> Result<GateBObservation, Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new(job_name)?, &JobParameters::new());
    let mut unit = repository.begin().await?;
    let mut lifecycle_trace = Vec::new();
    let mut checkpoint_position = None;
    let mut counts = CountsSnapshot::default();
    if let Some(instance) = unit.find_job_instance(&key).await? {
        for execution in unit.job_executions(instance.id()).await? {
            let steps = unit.step_executions(execution.id()).await?;
            let step_status = steps.first().map_or_else(
                || "none".to_owned(),
                |step| step.metadata().status().as_str().to_owned(),
            );
            lifecycle_trace.push((
                execution.metadata().status().as_str().to_owned(),
                step_status,
            ));
            if let Some(step) = steps.first() {
                counts = step.metadata().counts().into();
            }
        }
        if let Some(execution) = unit.job_executions(instance.id()).await?.last()
            && let Some(step) = unit.step_executions(execution.id()).await?.first()
        {
            let manager = transaction_manager(repository);
            let scope = oxide_batch::ChunkTransactionContext::new(execution.id(), step.id());
            if let Ok(durable) = manager.load_committed_state(scope).await {
                checkpoint_position = Some(self::checkpoint_position(durable.checkpoint())?);
            }
        }
    }
    unit.rollback().await?;

    Ok(GateBObservation {
        business_rows: business_rows(runtime_url, job_name).await?,
        checkpoint_position,
        counts,
        lifecycle_trace,
    })
}

/// Which chunk-commit boundary a worker parks at, and on which side.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParkAt {
    /// Park after the writer for the `ordinal`-th chunk (1-based) returns,
    /// before that chunk's commit is issued.
    BeforeCommit { ordinal: usize },
    /// Park after the `ordinal`-th chunk's commit is acknowledged.
    AfterCommit { ordinal: usize },
}

/// Announces `path` and parks forever, for a caller to terminate from outside
/// with a real `SIGKILL`.
fn announce_and_park(path: &std::path::Path) -> ! {
    crash_restore::announce(path);
    crash_restore::park_until_killed()
}

/// A writer decorator that parks after its `ordinal`-th call, when
/// `park` selects [`ParkAt::BeforeCommit`] for that ordinal.
pub struct ParkingWriter<W> {
    inner: W,
    ordinal: Arc<AtomicUsize>,
    park: ParkAt,
    reached: std::path::PathBuf,
}

impl<W> ParkingWriter<W> {
    /// Wraps `inner`, parking at `park` and announcing at `reached`.
    #[must_use]
    pub fn new(inner: W, park: ParkAt, reached: std::path::PathBuf) -> Self {
        Self {
            inner,
            ordinal: Arc::new(AtomicUsize::new(0)),
            park,
            reached,
        }
    }
}

impl<W: ItemWriter<i64>> ItemWriter<i64> for ParkingWriter<W> {
    async fn write<'a>(
        &'a self,
        items: &'a [i64],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let outcome = self.inner.write(items, context).await?;
        let ordinal = self.ordinal.fetch_add(1, Ordering::SeqCst) + 1;
        if let ParkAt::BeforeCommit {
            ordinal: target, ..
        } = self.park
            && ordinal == target
        {
            announce_and_park(&self.reached);
        }
        Ok(outcome)
    }
}

/// A completion decorator that parks after the `ordinal`-th chunk's commit
/// is acknowledged, when `park` selects [`ParkAt::AfterCommit`] for that
/// ordinal.
pub struct ParkingCompletion {
    ordinal: AtomicUsize,
    park: ParkAt,
    reached: std::path::PathBuf,
}

impl ParkingCompletion {
    /// Builds a completion observer that parks at `park` and announces at
    /// `reached`.
    #[must_use]
    pub const fn new(park: ParkAt, reached: std::path::PathBuf) -> Self {
        Self {
            ordinal: AtomicUsize::new(0),
            park,
            reached,
        }
    }
}

impl ChunkCompletion for ParkingCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async move {
            let ordinal = self.ordinal.fetch_add(1, Ordering::SeqCst) + 1;
            if let ParkAt::AfterCommit {
                ordinal: target, ..
            } = self.park
                && ordinal == target
            {
                announce_and_park(&self.reached);
            }
            Ok(ChunkCompletionOutcome::Acknowledged)
        })
    }
}

/// Names the directory a Gate B worker and its scenario hand off through,
/// mirroring `postgres_commit_phase_process_kill.rs`'s `HANDSHAKE_ENV`.
pub const HANDSHAKE_ENV: &str = "OXIDEBATCH_GATE_B_HANDSHAKE";

/// Re-launches the current test binary as a Gate B worker running exactly
/// `test_name`, with [`REPRESENTATION_ENV`] and [`HANDSHAKE_ENV`] set.
///
/// A worker test function detects it is running as a worker the same way
/// `commit_phase_kill_worker_process` does: by checking whether
/// [`REPRESENTATION_ENV`] is set, and returning `Ok(())` immediately when it
/// is not (an ordinary `cargo test` run of that function name).
///
/// # Errors
///
/// Returns the OS failure when the current executable cannot be re-launched.
pub fn spawn_worker_with_representation(
    test_name: &str,
    representation: Representation,
    handshake: &std::path::Path,
) -> std::io::Result<std::process::Child> {
    std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(REPRESENTATION_ENV, representation.id())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()
}
