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
    BoxFuture, BoxedProcessor, BoxedReader, BoxedWriter, BusinessStatement, BusinessValue,
    Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
    ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCounts, ChunkDeliveryMode, ChunkJob,
    ChunkRestartContract, ChunkSize, ChunkStep, ChunkTransaction, ChunkTransactionError,
    ChunkTransactionManager, Clock, CodecId, CodecVersion, ComponentRevision,
    ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec, DefinitionRevision,
    ExecutionContext, ExecutionCounts, FailureCategory, ItemProcessor, ItemReader, ItemStream,
    ItemWriter, JobInstanceKey, JobName, JobParameters, JobRepository, PostgresChunkStateError,
    PostgresChunkStateProvider, PostgresChunkTransactionManager, PostgresConfig,
    PostgresJobRepository, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError, StateLimits,
    StateSchemaId, StateSchemaVersion, StepName, StreamCloseContext, StreamCloseError,
    StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamStateContract,
    StreamUpdateContext, StreamUpdateError, TlsMode, VersionedStateCodec, WriteContext,
    WriteOutcome, WriterError,
};
use sqlx::Connection as _;
use sqlx::PgPool;
use sqlx::Row as _;
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

const SEQUENCE_NAMESPACE: &str = "gate-b.sequence-reader";
const SEQUENCE_SCHEMA: &str = "gate-b.sequence-reader-position";
const SEQUENCE_CODEC: &str = "gate-b.sequence-reader-position-codec";

#[derive(Clone, Copy)]
struct SequencePositionSchema;

impl VersionedStateCodec<u64> for SequencePositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::expect_used,
            reason = "the fixed campaign schema identity is a valid literal"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(SEQUENCE_SCHEMA).expect("valid schema id"))
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::expect_used,
            reason = "the fixed campaign schema version is nonzero"
        )]
        StateSchemaVersion::new(1).expect("nonzero schema version")
    }

    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "position": value }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        serde_json::from_slice::<serde_json::Value>(payload)
            .map_err(|_| StateCodecError::InvalidPayload)?
            .get("position")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)
    }
}

#[allow(
    clippy::expect_used,
    reason = "the fixed campaign codec identities are valid literals"
)]
fn sequence_codec() -> DefaultComponentCodec<SequencePositionSchema> {
    DefaultComponentCodec::new(
        SequencePositionSchema,
        CodecId::new(SEQUENCE_CODEC).expect("valid codec id"),
        CodecVersion::new(1).expect("nonzero codec version"),
        RestartabilityDeclaration::Restartable,
    )
}

fn sequence_identity() -> Result<ComponentStreamIdentity, Box<dyn Error>> {
    Ok(ComponentStreamIdentity::new(SEQUENCE_NAMESPACE)?)
}

/// A deterministic `ItemReader<i64>` over `0..len`, read one item at a time,
/// paired with a durable [`ItemStream`] carrying the next input position.
pub struct SequenceReader {
    next: Arc<std::sync::atomic::AtomicU64>,
    len: u64,
}

impl SequenceReader {
    /// Builds a reader that yields `0..len`.
    #[must_use]
    pub fn new(len: i64) -> Self {
        Self {
            next: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            len: len.max(0).cast_unsigned(),
        }
    }

    /// Pairs this reader with its durable component-state participant.
    pub fn stream(&self) -> Result<SequenceReaderStream, Box<dyn Error>> {
        Ok(SequenceReaderStream {
            next: Arc::clone(&self.next),
            namespace: sequence_identity()?,
        })
    }
}

impl ItemReader<i64> for SequenceReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        let next = self.next.load(Ordering::Acquire);
        if next >= self.len {
            return Ok(ReadOutcome::EndOfInput);
        }
        let item = next.cast_signed();
        self.next.store(next + 1, Ordering::Release);
        Ok(ReadOutcome::Item(item))
    }
}

/// The restartable component-state half of [`SequenceReader`].
pub struct SequenceReaderStream {
    next: Arc<std::sync::atomic::AtomicU64>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for SequenceReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        if let Some(inherited) = context.inherited_state() {
            let position = inherited
                .decode::<u64>(&sequence_codec())
                .map_err(|_| StreamOpenError::new())?;
            self.next.store(position, Ordering::SeqCst);
            Ok(StreamOpenOutcome::Restored)
        } else {
            self.next.store(0, Ordering::SeqCst);
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let position = self.next.load(Ordering::Acquire);
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &position,
            &sequence_codec(),
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        Ok(StreamCloseOutcome::Closed)
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
/// [`GateBObservation::business_rows`] checks. It writes through the
/// [`WriteContext`]'s enlisted [`BusinessTransaction`] rather than a
/// standalone connection -- a writer that used its own pool would durably
/// commit independently of the chunk's checkpoint/counter commit regardless
/// of the declared [`ChunkDeliveryMode::AtomicSameResource`], which would
/// make every Gate B scenario compare a business write that was never
/// actually part of the atomic boundary it claims to test. Confirmed the hard
/// way: an earlier, non-enlisted version of this writer let B-03's forced
/// checkpoint-provider failure roll back nothing, because the writer had
/// already committed on its own connection before the provider ever ran.
pub struct BusinessWriter {
    job_name: &'static str,
}

impl BusinessWriter {
    /// Builds a writer that enlists rows tagged `job_name`.
    #[must_use]
    pub const fn new(job_name: &'static str) -> Self {
        Self { job_name }
    }
}

impl ItemWriter<i64> for BusinessWriter {
    async fn write<'a>(
        &'a self,
        items: &'a [i64],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let business = context
            .transaction()
            .ok_or_else(|| WriterError::with_category(FailureCategory::PermanentInfrastructure))?;
        for item in items {
            let values = [
                BusinessValue::text(self.job_name),
                BusinessValue::i64(*item),
            ];
            business
                .execute(BusinessStatement::new(
                    "INSERT INTO oxide_batch_business.gate_b_output (job_name, value) \
                     VALUES ($1, $2)",
                    &values,
                ))
                .await
                .map_err(|_| {
                    WriterError::with_category(FailureCategory::PermanentInfrastructure)
                })?;
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

/// Reads the durable metadata projections used by Gate B's structured
/// observation. Database-generated IDs are deliberately excluded; stable
/// logical attempt/order and persisted values remain.
async fn durable_gate_observations(
    url: &str,
    job_name: &str,
) -> Result<
    (
        Vec<ComponentStateSnapshot>,
        Vec<OptimisticVersionSnapshot>,
        Vec<RepositoryWriteSnapshot>,
    ),
    Box<dyn Error>,
> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let mut repository_writes = Vec::new();

    for row in sqlx::query(
        "SELECT definition_revision \
         FROM oxide_batch.ob_job_definition WHERE job_name = $1 ORDER BY id",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        repository_writes.push(RepositoryWriteSnapshot {
            relation: "ob_job_definition".to_owned(),
            operation: "registered".to_owned(),
            values: serde_json::json!({
                "definition_revision": row.try_get::<String, _>("definition_revision")?,
            }),
        });
    }

    for row in sqlx::query(
        "SELECT identifying_parameters::text AS identifying_parameters \
         FROM oxide_batch.ob_job_instance \
         WHERE job_name = $1 ORDER BY id",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        let parameters = row.try_get::<String, _>("identifying_parameters")?;
        repository_writes.push(RepositoryWriteSnapshot {
            relation: "ob_job_instance".to_owned(),
            operation: "created".to_owned(),
            values: serde_json::json!({
                "identifying_parameters": serde_json::from_str::<serde_json::Value>(&parameters)?,
            }),
        });
    }

    for row in sqlx::query(
        "SELECT execution.attempt, execution.status, execution.exit_code, execution.version, \
         execution.restart_of_execution_id IS NOT NULL AS restarted \
         FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 ORDER BY execution.attempt",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        repository_writes.push(RepositoryWriteSnapshot {
            relation: "ob_job_execution".to_owned(),
            operation: "lifecycle".to_owned(),
            values: serde_json::json!({
                "attempt": row.try_get::<i32, _>("attempt")?,
                "status": row.try_get::<String, _>("status")?,
                "exit_code": row.try_get::<String, _>("exit_code")?,
                "version": u64::try_from(row.try_get::<i64, _>("version")?)?,
                "restarted": row.try_get::<bool, _>("restarted")?,
            }),
        });
    }

    for row in sqlx::query(
        "SELECT execution.attempt, step.status, step.exit_code, step.read_count, \
         step.processed_count, step.write_count, step.filter_count, step.commit_count, \
         step.rollback_count, step.checkpoint_schema, step.checkpoint_schema_version, \
         step.version FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 ORDER BY execution.attempt, step.step_name",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        repository_writes.push(RepositoryWriteSnapshot {
            relation: "ob_step_execution".to_owned(),
            operation: "progress".to_owned(),
            values: serde_json::json!({
                "attempt": row.try_get::<i32, _>("attempt")?,
                "status": row.try_get::<String, _>("status")?,
                "exit_code": row.try_get::<String, _>("exit_code")?,
                "read_count": u64::try_from(row.try_get::<i64, _>("read_count")?)?,
                "processed_count": u64::try_from(row.try_get::<i64, _>("processed_count")?)?,
                "write_count": u64::try_from(row.try_get::<i64, _>("write_count")?)?,
                "filter_count": u64::try_from(row.try_get::<i64, _>("filter_count")?)?,
                "commit_count": u64::try_from(row.try_get::<i64, _>("commit_count")?)?,
                "rollback_count": u64::try_from(row.try_get::<i64, _>("rollback_count")?)?,
                "checkpoint_schema": row.try_get::<String, _>("checkpoint_schema")?,
                "checkpoint_schema_version": u32::try_from(
                    row.try_get::<i32, _>("checkpoint_schema_version")?,
                )?,
                "version": u64::try_from(row.try_get::<i64, _>("version")?)?,
            }),
        });
    }

    for row in sqlx::query(
        "SELECT execution.attempt, decision.execution_version, decision.prior_status, \
         decision.resulting_status, decision.reason_code, decision.operator_reference, \
         encode(decision.evidence_digest, 'hex') AS evidence_digest \
         FROM oxide_batch.ob_recovery_decision decision \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = decision.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 ORDER BY execution.attempt, decision.execution_version",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        repository_writes.push(RepositoryWriteSnapshot {
            relation: "ob_recovery_decision".to_owned(),
            operation: "recovery".to_owned(),
            values: serde_json::json!({
                "attempt": row.try_get::<i32, _>("attempt")?,
                "execution_version": u64::try_from(
                    row.try_get::<i64, _>("execution_version")?,
                )?,
                "prior_status": row.try_get::<String, _>("prior_status")?,
                "resulting_status": row.try_get::<String, _>("resulting_status")?,
                "reason_code": row.try_get::<String, _>("reason_code")?,
                "operator_reference": row.try_get::<String, _>("operator_reference")?,
                "evidence_digest": row.try_get::<String, _>("evidence_digest")?,
            }),
        });
    }

    let optimistic_versions = sqlx::query(
        "SELECT execution.attempt, execution.version AS job_version, step.version AS step_version \
         FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 ORDER BY execution.attempt, step.step_name",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|row| {
        Ok(OptimisticVersionSnapshot {
            attempt: u32::try_from(row.try_get::<i32, _>("attempt")?)?,
            job_execution: u64::try_from(row.try_get::<i64, _>("job_version")?)?,
            step_execution: u64::try_from(row.try_get::<i64, _>("step_version")?)?,
        })
    })
    .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

    let mut component_state = Vec::new();
    for row in sqlx::query(
        "SELECT execution.attempt, state.namespace, state.schema_id, state.schema_version, \
         state.codec_id, state.codec_version, state.version, state.payload \
         FROM oxide_batch.ob_component_state state \
         JOIN oxide_batch.ob_step_execution step ON step.id = state.step_execution_id \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 ORDER BY execution.attempt, state.namespace",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?
    {
        let payload = row
            .try_get::<Option<Vec<u8>>, _>("payload")?
            .ok_or("Gate B component state payload is unexpectedly external")?;
        let payload = serde_json::from_slice::<serde_json::Value>(&payload)?;
        let position = payload
            .get("position")
            .and_then(serde_json::Value::as_u64)
            .ok_or("Gate B component state has no position")?;
        component_state.push(ComponentStateSnapshot {
            attempt: u32::try_from(row.try_get::<i32, _>("attempt")?)?,
            namespace: row.try_get("namespace")?,
            schema_id: row.try_get("schema_id")?,
            schema_version: u32::try_from(row.try_get::<i32, _>("schema_version")?)?,
            codec_id: row.try_get("codec_id")?,
            codec_version: u32::try_from(row.try_get::<i32, _>("codec_version")?)?,
            version: u64::try_from(row.try_get::<i64, _>("version")?)?,
            position,
        });
    }
    pool.close().await;

    Ok((component_state, optimistic_versions, repository_writes))
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

/// A real `PostgreSQL` transaction manager whose successful commit is followed
/// by an intentionally untrusted acknowledgement. The database transaction
/// has already committed; only the caller-visible acknowledgement is replaced
/// with [`ChunkTransactionError::CommitOutcomeUnknown`]. This is the frozen
/// B-04 runtime path, distinct from killing a process before it can observe
/// the result of a real server-side commit.
pub struct UnknownAfterCommitManager {
    inner: PostgresChunkTransactionManager,
}

/// Builds the B-04 manager that converts a successful durable commit into an
/// unknown caller-visible outcome.
#[must_use]
pub fn transaction_manager_unknown_after_commit(
    repository: &PostgresJobRepository,
) -> UnknownAfterCommitManager {
    UnknownAfterCommitManager {
        inner: transaction_manager(repository),
    }
}

struct UnknownAfterCommitTransaction<'a> {
    inner: Box<dyn ChunkTransaction + 'a>,
}

impl ChunkTransaction for UnknownAfterCommitTransaction<'_> {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        self.inner.business_transaction()
    }

    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: oxide_batch::ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            let _receipt = self.inner.commit(counts, fault).await?;
            Err(ChunkTransactionError::CommitOutcomeUnknown)
        })
    }

    fn commit_with_component_state<'a>(
        &'a mut self,
        counts: ChunkCounts,
        fault: oxide_batch::ChunkFaultProgress,
        component_state: &'a [ComponentStateEnvelope],
    ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            let _receipt = self
                .inner
                .commit_with_component_state(counts, fault, component_state)
                .await?;
            Err(ChunkTransactionError::CommitOutcomeUnknown)
        })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        self.inner.rollback()
    }
}

impl ChunkTransactionManager for UnknownAfterCommitManager {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move {
            let inner = self.inner.begin().await?;
            Ok(Box::new(UnknownAfterCommitTransaction { inner }) as Box<dyn ChunkTransaction>)
        })
    }

    fn begin_for(
        &self,
        context: oxide_batch::ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move {
            let inner = self.inner.begin_for(context).await?;
            Ok(Box::new(UnknownAfterCommitTransaction { inner }) as Box<dyn ChunkTransaction>)
        })
    }

    fn inherited_progress(
        &self,
        context: oxide_batch::ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<oxide_batch::InheritedStepProgress, ChunkTransactionError>> {
        self.inner.inherited_progress(context)
    }

    fn inherited_component_state(
        &self,
        context: oxide_batch::ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
        self.inner.inherited_component_state(context)
    }
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
    stream: SequenceReaderStream,
    completion: Arc<dyn ChunkCompletion>,
) -> Result<Job<R, P, W>, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    R: ItemReader<i64> + Send + 'static,
    P: ItemProcessor<i64, i64> + Send + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let stream_identity = sequence_identity()?;
    let step = ChunkStep::new(
        StepName::new(params.job_name)?,
        ChunkSize::new(params.chunk_size)?,
        reader,
        processor,
        writer,
        Arc::clone(&params.transactions) as Arc<dyn ChunkTransactionManager>,
        completion,
    )
    .with_item_stream(
        stream_identity.clone(),
        stream,
        StreamStateContract::new(sequence_codec()),
    );
    let components = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        restart_contract()?,
    )
    .with_stream_revision(stream_identity, ComponentRevision::new("reader-stream-v1")?);
    Ok(ChunkJob::new(
        JobName::new(params.job_name)?,
        step,
        DefinitionRevision::new("gate-b-v1")?,
        &components,
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
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        reader,
        IdentityProcessor,
        BusinessWriter::new(params.job_name),
        stream,
        Arc::new(Completion),
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
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        reader,
        IdentityProcessor,
        writer,
        stream,
        Arc::new(Completion),
    )
}

/// Builds the typed representation with a fresh reader and caller-supplied
/// writer. On restart the paired `ItemStream` restores the reader position
/// from durable component state before item work begins.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn typed_chunk_job_with_reader_and_writer<M, W>(
    params: &GateBParams<M>,
    writer: W,
) -> Result<TypedJob<W>, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        reader,
        IdentityProcessor,
        writer,
        stream,
        Arc::new(Completion),
    )
}

/// Builds the typed representation with a caller-supplied writer and
/// [`ChunkCompletion`], for a scenario that needs to observe or park at the
/// post-commit-acknowledgement boundary (see [`ParkingCompletion`]) while
/// keeping the reader and processor identical to [`typed_chunk_job`].
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn typed_chunk_job_with_writer_and_completion<M, W>(
    params: &GateBParams<M>,
    writer: W,
    completion: Arc<dyn ChunkCompletion>,
) -> Result<TypedJob<W>, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        reader,
        IdentityProcessor,
        writer,
        stream,
        completion,
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
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        BoxedReader::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
        stream,
        Arc::new(Completion),
    )
}

/// Builds the `Boxed` representation with a fresh reader and caller-supplied
/// writer. On restart the paired `ItemStream` restores the reader position
/// from durable component state before item work begins.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn boxed_chunk_job_with_reader_and_writer<M, W>(
    params: &GateBParams<M>,
    writer: W,
) -> Result<BoxedJob, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        BoxedReader::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
        stream,
        Arc::new(Completion),
    )
}

/// Builds the `Boxed` representation with a caller-supplied writer and
/// [`ChunkCompletion`]. Pair with [`typed_chunk_job_with_writer_and_completion`]
/// to compare the same [`ParkAt::AfterCommit`] boundary under both
/// representations.
///
/// # Errors
///
/// Returns the domain failure when any declared identity is invalid.
pub fn boxed_chunk_job_with_writer_and_completion<M, W>(
    params: &GateBParams<M>,
    writer: W,
    completion: Arc<dyn ChunkCompletion>,
) -> Result<BoxedJob, Box<dyn Error>>
where
    M: ChunkTransactionManager + 'static,
    W: ItemWriter<i64> + Send + 'static,
{
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        BoxedReader::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
        stream,
        completion,
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
    let reader = SequenceReader::new(params.items);
    let stream = reader.stream()?;
    assemble(
        params,
        BoxedReader::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(BusinessWriter::new(params.job_name)),
        stream,
        Arc::new(Completion),
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
    /// Every committed component-state envelope, normalized away from
    /// database-generated row and execution IDs.
    pub component_state: Vec<ComponentStateSnapshot>,
    /// Job/step optimistic versions observed in durable repository rows.
    pub optimistic_versions: Vec<OptimisticVersionSnapshot>,
    /// The normalized durable repository-write projection. This is a
    /// structured database observation, not a process-local log trace.
    pub repository_writes: Vec<RepositoryWriteSnapshot>,
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
            "component_state": self
                .component_state
                .iter()
                .map(ComponentStateSnapshot::to_json)
                .collect::<Vec<_>>(),
            "optimistic_versions": self
                .optimistic_versions
                .iter()
                .map(OptimisticVersionSnapshot::to_json)
                .collect::<Vec<_>>(),
            "repository_writes": self
                .repository_writes
                .iter()
                .map(RepositoryWriteSnapshot::to_json)
                .collect::<Vec<_>>(),
            "lifecycle_trace": self.lifecycle_trace,
        })
    }
}

/// A normalized durable `ItemStream` component-state row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStateSnapshot {
    /// The execution attempt owning the copied or newly committed state.
    pub attempt: u32,
    /// The stable stream namespace.
    pub namespace: String,
    /// The application state schema identity.
    pub schema_id: String,
    /// The application state schema version.
    pub schema_version: u32,
    /// The codec identity.
    pub codec_id: String,
    /// The codec version.
    pub codec_version: u32,
    /// The component-state row version.
    pub version: u64,
    /// The decoded, non-sensitive campaign position.
    pub position: u64,
}

impl ComponentStateSnapshot {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "attempt": self.attempt,
            "namespace": self.namespace,
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "codec_id": self.codec_id,
            "codec_version": self.codec_version,
            "version": self.version,
            "position": self.position,
        })
    }
}

/// Job/step optimistic versions that were durable for each attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimisticVersionSnapshot {
    /// The logical attempt number.
    pub attempt: u32,
    /// The job-execution optimistic version.
    pub job_execution: u64,
    /// The step-execution optimistic version.
    pub step_execution: u64,
}

impl OptimisticVersionSnapshot {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "attempt": self.attempt,
            "job_execution": self.job_execution,
            "step_execution": self.step_execution,
        })
    }
}

/// One normalized durable repository-write observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryWriteSnapshot {
    /// The durable repository relation observed.
    pub relation: String,
    /// The normalized operation represented by the row.
    pub operation: String,
    /// Value-redacted, ID-independent durable fields.
    pub values: serde_json::Value,
}

impl RepositoryWriteSnapshot {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "relation": self.relation,
            "operation": self.operation,
            "values": self.values,
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
                // The step-execution row exists (and so decodes) once a step
                // starts, before its first chunk ever commits, carrying
                // whatever placeholder checkpoint bytes it was created with
                // -- not gate-b's `gate-b.position` schema. Decode failure
                // here means "nothing has committed yet", not a real error.
                checkpoint_position = self::checkpoint_position(durable.checkpoint()).ok();
            }
        }
    }
    unit.rollback().await?;
    let (component_state, optimistic_versions, repository_writes) =
        durable_gate_observations(runtime_url, job_name).await?;

    Ok(GateBObservation {
        business_rows: business_rows(runtime_url, job_name).await?,
        checkpoint_position,
        counts,
        component_state,
        optimistic_versions,
        repository_writes,
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

/// A writer decorator that fails its `fail_at`-th call (1-based) with a
/// `UserComponent` error instead of delegating to `inner`, for B-02's
/// writer-failure-before-commit scenario.
pub struct FailingWriter<W> {
    inner: W,
    ordinal: Arc<AtomicUsize>,
    fail_at: usize,
}

impl<W> FailingWriter<W> {
    /// Wraps `inner`, failing its `fail_at`-th call (1-based) rather than
    /// delegating.
    #[must_use]
    pub fn new(inner: W, fail_at: usize) -> Self {
        Self {
            inner,
            ordinal: Arc::new(AtomicUsize::new(0)),
            fail_at,
        }
    }
}

impl<W: ItemWriter<i64>> ItemWriter<i64> for FailingWriter<W> {
    async fn write<'a>(
        &'a self,
        items: &'a [i64],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let ordinal = self.ordinal.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == self.fail_at {
            return Err(WriterError::with_category(FailureCategory::UserComponent));
        }
        self.inner.write(items, context).await
    }
}

/// Builds a transaction manager identical to [`transaction_manager`], except
/// its checkpoint provider fails on the `fail_at`-th chunk attempt (1-based)
/// instead of returning a receipt.
///
/// B-03 uses this to prove atomicity: the writer for that chunk can succeed
/// (its rows staged inside the open transaction) while the provider that
/// computes the checkpoint fails, and the scenario asserts that failure rolls
/// the whole chunk back -- staged business rows included -- rather than
/// leaving a commit split between "rows written" and "checkpoint advanced".
#[must_use]
pub fn transaction_manager_failing_at(
    repository: &PostgresJobRepository,
    fail_at: usize,
) -> PostgresChunkTransactionManager {
    let ordinal = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn PostgresChunkStateProvider> =
        Arc::new(move |committed: ExecutionCounts, chunk: ChunkCounts| {
            let attempt = ordinal.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == fail_at {
                return Err(PostgresChunkStateError::new());
            }
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

/// The session advisory-lock key [`install_commit_gate`] blocks on, distinct
/// from `postgres_commit_phase_process_kill.rs`'s own `ADVISORY_KEY` so the
/// two campaigns' locks can never collide if they ever ran concurrently
/// against the same database.
const COMMIT_GATE_KEY: i64 = 22_262_274;

/// Statements that remove [`install_commit_gate`]'s trigger and function.
const DROP_COMMIT_GATE: [&str; 2] = [
    "DROP TRIGGER IF EXISTS gate_b_commit_gate ON oxide_batch_business.gate_b_output",
    "DROP FUNCTION IF EXISTS oxide_batch_business.gate_b_commit_gate()",
];

/// Installs a deferred constraint trigger on `gate_b_output` that blocks on
/// [`COMMIT_GATE_KEY`] when a chunk's `COMMIT` reaches it, and holds that
/// lock itself -- so the next `INSERT`-then-`COMMIT` against this table
/// blocks server-side until [`release_commit_gate`] runs, exactly
/// reproducing B-04/B-06's window: a real `COMMIT` genuinely in flight on the
/// server when the client that issued it dies, whose outcome the client can
/// therefore never learn. Mirrors
/// `postgres_commit_phase_process_kill.rs`'s `Block::arrange` for
/// `Phase::CommitInFlight`, scoped to `gate_b_output` with its own
/// [`COMMIT_GATE_KEY`] rather than reusing that file's `ADVISORY_KEY`.
///
/// # Errors
///
/// Returns the database failure when the gate cannot be installed.
pub async fn install_commit_gate(url: &str) -> Result<sqlx::PgConnection, Box<dyn Error>> {
    let mut connection = sqlx::PgConnection::connect(url).await?;
    sqlx::query(
        "CREATE OR REPLACE FUNCTION oxide_batch_business.gate_b_commit_gate() \
         RETURNS trigger LANGUAGE plpgsql AS $gate$ BEGIN \
         PERFORM pg_advisory_xact_lock(22262274); RETURN NULL; END; $gate$",
    )
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "CREATE CONSTRAINT TRIGGER gate_b_commit_gate \
         AFTER INSERT ON oxide_batch_business.gate_b_output \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION oxide_batch_business.gate_b_commit_gate()",
    )
    .execute(&mut connection)
    .await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(COMMIT_GATE_KEY)
        .execute(&mut connection)
        .await?;
    Ok(connection)
}

/// Marks `job_name`'s latest execution `Failed` through the framework's own
/// audited recovery request, so a subsequent [`JobLauncher::launch_chunk`]
/// is allowed to start a new attempt.
///
/// A killed worker's execution is left at `Started`: nothing ever told the
/// repository the attempt ended, so
/// [`RepositoryError::ExecutionAlreadyActive`](oxide_batch::RepositoryError)
/// correctly refuses a second concurrent attempt for the same instance --
/// discovered directly while building B-05, whose first draft tried to
/// restart by calling `launch_chunk` a second time without this step.
/// Recovery is a distinct, explicit, audited decision in this framework
/// (`RecoveryRequest::mark_failed`), never an automatic side effect of
/// restarting, which is exactly the "never infer, never auto-replay" B-04
/// invariant applied to the instance-liveness check itself. Mirrors
/// `postgres_fault_crash_recovery.rs`'s `inspect_recover_and_restart`.
///
/// # Errors
///
/// Returns the repository failure when the instance, its latest execution,
/// or the recovery request itself cannot be resolved or applied.
pub async fn mark_crashed_execution_failed(
    repository: &PostgresJobRepository,
    job_name: &str,
) -> Result<(), Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new(job_name)?, &JobParameters::new());
    let mut unit = repository.begin().await?;
    let instance = unit
        .find_job_instance(&key)
        .await?
        .ok_or("no job instance to recover")?;
    let execution = unit
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next_back()
        .ok_or("no execution to recover")?;
    let request = oxide_batch::RecoveryRequest::mark_failed(
        execution.version(),
        "GATE_B_CRASH_RECOVERY",
        "gate-b-harness",
        [0; 32],
        FailureCategory::PermanentInfrastructure,
        oxide_batch::FailureId::new(1)?,
    )?;
    unit.recover_job_execution(execution.id(), &request).await?;
    unit.commit().await?;
    Ok(())
}

/// Releases [`install_commit_gate`]'s lock, letting a killed worker's
/// already-blocked `COMMIT` finish server-side, then drops the trigger and
/// function so a later scenario's writes are not gated.
///
/// # Errors
///
/// Returns the database failure when the gate cannot be released or removed.
pub async fn release_commit_gate(mut connection: sqlx::PgConnection) -> Result<(), Box<dyn Error>> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(COMMIT_GATE_KEY)
        .execute(&mut connection)
        .await?;
    for statement in DROP_COMMIT_GATE {
        sqlx::query(statement).execute(&mut connection).await?;
    }
    connection.close().await?;
    Ok(())
}
