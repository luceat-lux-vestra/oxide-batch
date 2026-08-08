//! Shared fixture, observation, and handshake support for the M5 crash and
//! restore campaign.
//!
//! Three campaign targets share this module: the commit-phase process-kill
//! scenario, the P-013 restart-after-many-chunks report, and the logical
//! backup and restore report. Each uses a subset of it, so unused items are
//! allowed here rather than split into three fixtures that would drift apart.
//!
//! Everything the campaign observes about durable state is read back through
//! the repository and explorer contracts. A direct SQL read appears only where
//! the campaign needs something those contracts do not expose — the enlisted
//! business rows, which are the application's own table, and the server-side
//! wait state that tells the campaign a child has reached a commit phase.

#![allow(
    dead_code,
    reason = "each campaign target uses a subset of one shared fixture"
)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    ActorRef, BatchStatus, BusinessStatement, BusinessValue, Checkpoint, ChunkCommitReceipt,
    ChunkComponentRevisions, ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkFaultProgress,
    ChunkRestartContract, ChunkSize, ChunkTransactionContext, ChunkTransactionManager, Clock,
    ComponentRevision, DefinitionIdentity, DefinitionRevision, ExecutionContext, ExecutionCounts,
    ExecutionVersion, ExplorerRepository, FailureCategory, FailureId, FailureSummary, FlowDecision,
    JobExecution, JobExecutionId, JobExecutionProjection, JobInstance, JobInstanceKey, JobName,
    JobOperator, JobParameters, JobRepository, LifecycleTransition, OperationId,
    OperatorOutcomeClass, OperatorRequest, OwnerToken, PostgresChunkStateError,
    PostgresChunkStateProvider, PostgresChunkTransactionManager, PostgresConfig, PostgresExplorer,
    PostgresJobRepository, ReasonCode, RecoveryDecision, RecoveryDirective, RecoveryProposal,
    RecoveryProposer, StaleThreshold, StateLimits, StateSchemaId, StateSchemaVersion,
    StepExecution, StepName, StepPartition, SystemClock, SystemMonotonicClock, TlsMode,
};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

/// Directory a campaign runner sets so scenarios retain machine-readable
/// observations.
///
/// When it is unset the scenarios still run and still assert; they simply
/// retain nothing. The runner requires an observation per declared phase, so
/// an unset variable cannot turn into a campaign pass.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_CRASH_RESTORE_OBSERVATIONS";

/// The business schema the campaign's enlisted writes land in.
pub const BUSINESS_SCHEMA: &str = "oxide_batch_business";

/// The business table the campaign's enlisted writes land in.
pub const BUSINESS_TABLE: &str = "oxide_batch_business.m5_crash_restore_output";

/// The durable checkpoint schema the campaign's steps declare.
pub const POSITION_SCHEMA: &str = "m5.crash-restore.position";

/// The durable execution-context schema the campaign's steps declare.
pub const CONTEXT_SCHEMA: &str = "m5.crash-restore.context";

/// The step every campaign job runs.
pub const STEP_NAME: &str = "import";

/// Exit code a parked child uses when nothing killed it inside its bound.
///
/// A campaign that observes this code observes a scenario that did not reach
/// its phase, which is a violation rather than a pass.
pub const UNKILLED_EXIT_CODE: i32 = 93;

/// How long a parked child waits to be killed before giving up.
pub const PARK_BOUND: Duration = Duration::from_mins(3);

/// How long the campaign waits for one handshake or server-side condition.
pub const HANDSHAKE_BOUND: Duration = Duration::from_mins(2);

/// How often the campaign polls a handshake or server-side condition.
pub const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// A clock pinned to one instant so durable timestamps stay reproducible.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// Returns the runtime connection string, when the fixture supplies one.
#[must_use]
pub fn runtime_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_TEST_URL")
}

/// Returns the migrating connection string, when the fixture supplies one.
#[must_use]
pub fn migrator_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL")
}

/// Returns the backup connection string, when the fixture supplies one.
///
/// The backup and restore report needs a role that may create and drop a
/// database, which the runtime role deliberately may not. Requiring a separate
/// variable also keeps the report out of runs that supply only a runtime
/// database.
#[must_use]
pub fn backup_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_BACKUP_TEST_URL")
}

/// Reads one environment variable, treating an empty value as absent.
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Builds the campaign's plaintext repository configuration.
///
/// The lock and statement timeouts are raised well past their defaults on
/// purpose. Two commit phases are reached by holding the lock the commit needs
/// until the kill lands, and the default five-second lock timeout would abort
/// the blocked statement rather than park it there.
///
/// # Errors
///
/// Returns the configuration failure when the URL or a timeout is rejected.
pub fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::Plaintext)
        .with_statement_timeout(Duration::from_secs(150))?
        .with_lock_timeout(Duration::from_secs(150))?)
}

/// Builds the restart contract every campaign step declares.
///
/// # Errors
///
/// Returns the domain failure when a schema identifier or version is invalid.
pub fn restart_contract() -> Result<ChunkRestartContract, Box<dyn Error>> {
    Ok(ChunkRestartContract::new(
        StateSchemaId::new(POSITION_SCHEMA)?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new(CONTEXT_SCHEMA)?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

/// Builds the definition identity one campaign job restarts against.
///
/// # Errors
///
/// Returns the domain failure when a name, revision, or chunk size is invalid.
pub fn definition(job_name: &str, chunk_size: u32) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let job_name = JobName::new(job_name)?;
    let step_name = StepName::new(STEP_NAME)?;
    Ok(DefinitionIdentity::chunk(
        &job_name,
        &step_name,
        ChunkSize::new(chunk_size)?,
        DefinitionRevision::new("m5-crash-restore-v1")?,
        &ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            restart_contract()?,
        ),
    )?)
}

/// Builds the instance key one campaign job runs under.
///
/// # Errors
///
/// Returns the domain failure when the job name is invalid.
pub fn instance_key(job_name: &str) -> Result<JobInstanceKey, Box<dyn Error>> {
    Ok(JobInstanceKey::new(
        JobName::new(job_name)?,
        &JobParameters::new(),
    ))
}

/// Encodes the campaign's reader position as a durable checkpoint.
///
/// # Errors
///
/// Returns the state failure when the envelope is rejected.
pub fn checkpoint(position: u64) -> Result<Checkpoint, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": POSITION_SCHEMA,
        "schema_version": 1,
        "payload": {"position": position},
    }))?;
    Ok(Checkpoint::from_json(&bytes, StateLimits::default())?)
}

/// Builds the campaign's durable execution context.
///
/// # Errors
///
/// Returns the state failure when the envelope is rejected.
pub fn execution_context() -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": CONTEXT_SCHEMA,
        "schema_version": 1,
        "payload": {"fixture": "m5-crash-restore"},
    }))?;
    Ok(ExecutionContext::from_json(&bytes, StateLimits::default())?)
}

/// Reads the reader position back out of a durable checkpoint.
///
/// # Errors
///
/// Returns a failure when the checkpoint carries no position.
pub fn checkpoint_position(value: &Checkpoint) -> Result<u64, Box<dyn Error>> {
    let envelope: Value = serde_json::from_slice(&value.to_json()?)?;
    envelope
        .pointer("/payload/position")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            Box::new(Failure("checkpoint carries no position".to_owned())) as Box<dyn Error>
        })
}

/// Where and when a state provider parks instead of returning.
#[derive(Clone, Debug)]
pub struct ProviderPark {
    /// The one-based commit ordinal that parks.
    pub ordinal: usize,
    /// The handshake file created immediately before parking.
    pub reached: PathBuf,
}

/// Builds the campaign's chunk state provider.
///
/// The provider runs inside `commit`, after the counters are checked and
/// before the progress bind. `park` is how the campaign reaches that phase: on
/// the chosen commit ordinal the provider announces itself and never returns,
/// leaving the open transaction exactly where the kill should find it.
#[must_use]
pub fn state_provider(park: Option<ProviderPark>) -> Arc<dyn PostgresChunkStateProvider> {
    let commits = AtomicUsize::new(0);
    Arc::new(
        move |committed: ExecutionCounts,
              chunk: ChunkCounts|
              -> Result<_, PostgresChunkStateError> {
            let ordinal = commits.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(park) = &park
                && ordinal == park.ordinal
            {
                announce(&park.reached);
                park_until_killed();
            }

            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint = checkpoint(position).map_err(|_| PostgresChunkStateError::new())?;
            let context = execution_context().map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    )
}

/// Builds a chunk transaction manager over one repository.
#[must_use]
pub fn transaction_manager(
    repository: &PostgresJobRepository,
    park: Option<ProviderPark>,
) -> PostgresChunkTransactionManager {
    PostgresChunkTransactionManager::new(repository.clone(), state_provider(park))
}

/// Applies the metadata migrations and clears every durable row of one job.
///
/// # Errors
///
/// Returns the database failure when the fixture cannot be prepared.
pub async fn prepare_fixture(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.m5_crash_restore_output (\
         job_name text NOT NULL, item bigint NOT NULL, \
         PRIMARY KEY (job_name, item))",
    )
    .execute(&pool)
    .await?;
    pool.close().await;

    remove_job(url, job_name).await
}

/// Clears every durable row one campaign job owns, business rows included.
///
/// # Errors
///
/// Returns the database failure when a statement cannot run.
pub async fn remove_job(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_step_partition WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_flow_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_operator_request WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
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
    ] {
        sqlx::query(statement).bind(job_name).execute(&pool).await?;
    }
    sqlx::query("DELETE FROM oxide_batch_business.m5_crash_restore_output WHERE job_name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Reads the business rows one campaign job durably wrote, in item order.
///
/// # Errors
///
/// Returns the database failure when the rows cannot be read.
pub async fn business_items(url: &str, job_name: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let items = sqlx::query_scalar(
        "SELECT item FROM oxide_batch_business.m5_crash_restore_output \
         WHERE job_name = $1 ORDER BY item",
    )
    .bind(job_name)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok(items)
}

/// Writes one chunk's enlisted business rows and commits it atomically.
///
/// # Errors
///
/// Returns the transaction failure, including the unknown-outcome case, which
/// the campaign never converts into a guess.
pub async fn commit_chunk(
    manager: &PostgresChunkTransactionManager,
    scope: ChunkTransactionContext,
    job_name: &str,
    items: &[i64],
) -> Result<(), Box<dyn Error>> {
    let mut transaction = manager.begin_for(scope).await?;
    write_items(&mut *transaction, job_name, items).await?;
    let count = ChunkCount::new(u64::try_from(items.len())?);
    transaction
        .commit(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
        )
        .await?;
    Ok(())
}

/// Writes one chunk's enlisted business rows into an open chunk transaction.
///
/// # Errors
///
/// Returns the failure when the fixture is not enlisted or a write is rejected.
pub async fn write_items(
    transaction: &mut dyn oxide_batch::ChunkTransaction,
    job_name: &str,
    items: &[i64],
) -> Result<(), Box<dyn Error>> {
    let business = transaction
        .business_transaction()
        .ok_or_else(|| Failure("the campaign fixture was not enlisted".to_owned()))?;
    for item in items {
        let values = [BusinessValue::text(job_name), BusinessValue::i64(*item)];
        business
            .execute(BusinessStatement::new(
                "INSERT INTO oxide_batch_business.m5_crash_restore_output \
                 (job_name, item) VALUES ($1, $2)",
                &values,
            ))
            .await?;
    }
    Ok(())
}

/// One reading of everything the repository and explorer contracts durably
/// report about a single job.
///
/// Equality over this value is what the backup and restore report compares:
/// a restored database that reports the same identities, statuses, counters,
/// versions, checkpoints, decisions, and partition metadata has restored the
/// durable state, and one that differs anywhere has not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableObservation {
    /// The logical instance, when the job has one.
    pub instance: Option<JobInstance>,
    /// Every attempt, oldest first.
    pub executions: Vec<JobExecution>,
    /// The explorer projection of every attempt, including its definition
    /// descriptor and durable version.
    pub projections: Vec<Option<JobExecutionProjection>>,
    /// Every step attempt, by enclosing attempt.
    pub steps: Vec<Vec<StepExecution>>,
    /// The durable checkpoint and context of every step attempt.
    ///
    /// A step that has never carried chunk state — a partitioned parent, for
    /// instance — reports `None` rather than failing the whole reading. Both
    /// sides of a comparison read the same way, so an absence is compared as
    /// faithfully as a value.
    pub durable_state: Vec<Vec<Option<DurableStep>>>,
    /// The append-only flow decisions of every attempt.
    pub flow_decisions: Vec<Vec<FlowDecision>>,
    /// The append-only recovery decision of every attempt.
    pub recovery_decisions: Vec<Option<RecoveryDecision>>,
    /// The partition plan of every step attempt.
    pub partitions: Vec<Vec<Vec<StepPartition>>>,
}

/// The durable chunk state of one step attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableStep {
    /// The committed reader position the checkpoint encodes, when it encodes
    /// one.
    ///
    /// A step that carries a checkpoint of another schema — a flow or tasklet
    /// step, for instance — reports `None`. The campaign compares the whole
    /// envelope bytes either way, so an absent position is compared as
    /// faithfully as a present one.
    pub position: Option<u64>,
    /// The canonical checkpoint envelope bytes.
    pub checkpoint: Vec<u8>,
    /// The canonical execution-context envelope bytes.
    pub context: Vec<u8>,
    /// The durable counters committed with that checkpoint.
    pub counts: ExecutionCounts,
}

/// Reads everything the durable contracts report about one job.
///
/// Flow decisions and partition plans are read as absent when the port rejects
/// the reading, because a chunk step has neither and the campaign compares jobs
/// that do and jobs that do not. That cannot weaken a comparison: the side the
/// backup is taken from asserts both are present before the archive is written,
/// so a reading that lost them on the restored side compares unequal.
///
/// # Errors
///
/// Returns the repository, explorer, or state failure that prevented a
/// complete reading. A partial reading is never returned, because a campaign
/// comparison over one is not evidence.
pub async fn observe(
    repository: &PostgresJobRepository,
    key: &JobInstanceKey,
) -> Result<DurableObservation, Box<dyn Error>> {
    let explorer = PostgresExplorer::new(repository.clone());
    let mut unit = repository.begin().await?;

    let Some(instance) = unit.find_job_instance(key).await? else {
        unit.rollback().await?;
        return Ok(DurableObservation {
            instance: None,
            executions: Vec::new(),
            projections: Vec::new(),
            steps: Vec::new(),
            durable_state: Vec::new(),
            flow_decisions: Vec::new(),
            recovery_decisions: Vec::new(),
            partitions: Vec::new(),
        });
    };

    let executions = unit.job_executions(instance.id()).await?;
    let mut steps = Vec::new();
    let mut flow_decisions = Vec::new();
    let mut recovery_decisions = Vec::new();
    let mut partitions = Vec::new();
    for execution in &executions {
        let attempt_steps = unit.step_executions(execution.id()).await?;
        let mut attempt_partitions = Vec::new();
        for step in &attempt_steps {
            attempt_partitions.push(
                unit.step_partition_plan(step.id())
                    .await
                    .unwrap_or_default(),
            );
        }
        partitions.push(attempt_partitions);
        steps.push(attempt_steps);
        flow_decisions.push(
            unit.flow_decisions(execution.id())
                .await
                .unwrap_or_default(),
        );
        recovery_decisions.push(unit.recovery_decision(execution.id()).await?);
    }
    unit.rollback().await?;

    let manager = transaction_manager(repository, None);
    let mut durable_state = Vec::new();
    let mut projections = Vec::new();
    for (execution, attempt_steps) in executions.iter().zip(&steps) {
        projections.push(explorer.execution(execution.id()).await?);
        let mut attempt_state = Vec::new();
        for step in attempt_steps {
            let scope = ChunkTransactionContext::new(execution.id(), step.id());
            let Ok(state) = manager.load_committed_state(scope).await else {
                attempt_state.push(None);
                continue;
            };
            attempt_state.push(Some(DurableStep {
                position: checkpoint_position(state.checkpoint()).ok(),
                checkpoint: state.checkpoint().to_json()?,
                context: state.execution_context().to_json()?,
                counts: state.step_execution().metadata().counts(),
            }));
        }
        durable_state.push(attempt_state);
    }

    Ok(DurableObservation {
        instance: Some(instance),
        executions,
        projections,
        steps,
        durable_state,
        flow_decisions,
        recovery_decisions,
        partitions,
    })
}

impl DurableObservation {
    /// Summarizes the observation for a retained report.
    ///
    /// The summary is a description, not the comparison: campaign assertions
    /// compare the whole observation, and this exists so a reader of the
    /// report can see what was compared.
    #[must_use]
    pub fn summary(&self) -> Value {
        json!({
            "instance_present": self.instance.is_some(),
            "attempts": self.executions.len(),
            "attempt_statuses": self
                .executions
                .iter()
                .map(|execution| execution.metadata().status().as_str())
                .collect::<Vec<_>>(),
            "attempt_versions": self
                .executions
                .iter()
                .map(|execution| execution.version().get())
                .collect::<Vec<_>>(),
            "definition_digests": self
                .projections
                .iter()
                .map(|projection| projection
                    .as_ref()
                    .and_then(|projection| projection.definition())
                    .map(oxide_batch::DefinitionDescriptor::manifest_digest_hex))
                .collect::<Vec<_>>(),
            "step_statuses": self
                .steps
                .iter()
                .map(|attempt| attempt
                    .iter()
                    .map(|step| step.metadata().status().as_str())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "checkpoint_positions": self
                .durable_state
                .iter()
                .map(|attempt| attempt
                    .iter()
                    .map(|state| state.as_ref().map(|state| state.position))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "durable_counts": self
                .durable_state
                .iter()
                .map(|attempt| attempt
                    .iter()
                    .map(|state| state.as_ref().map(|state| counts(state.counts)))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "flow_decisions": self
                .flow_decisions
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            "recovery_decisions": self
                .recovery_decisions
                .iter()
                .map(|decision| decision
                    .as_ref()
                    .map(|decision| decision.reason_code().to_owned()))
                .collect::<Vec<_>>(),
            "partitions": self
                .partitions
                .iter()
                .map(|attempt| attempt.iter().map(Vec::len).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        })
    }

    /// Returns the durable state of the newest attempt's first step.
    #[must_use]
    pub fn latest_step_state(&self) -> Option<&DurableStep> {
        self.durable_state
            .last()
            .and_then(|attempt| attempt.first())
            .and_then(Option::as_ref)
    }
}

/// The shape a run leaves behind once it has finished, whatever happened to it.
///
/// Attempt identifiers and attempt counts differ between an uninterrupted run
/// and one that was killed and restarted. Everything a user or an operator
/// observes about the outcome does not, and this is that.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalShape {
    /// The committed reader position of the final attempt.
    pub position: Option<u64>,
    /// The durable counters of the final attempt.
    pub counts: ExecutionCounts,
    /// The business rows the job durably wrote.
    pub items: Vec<i64>,
    /// The final job status.
    pub job_status: BatchStatus,
    /// The final step status.
    pub step_status: BatchStatus,
}

impl TerminalShape {
    /// Renders the shape for a retained report.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "checkpoint_position": self.position,
            "counts": counts(self.counts),
            "business_rows": self.items.len(),
            "job_status": self.job_status.as_str(),
            "step_status": self.step_status.as_str(),
        })
    }
}

/// One connected campaign job and everything it is observed through.
///
/// The campaign reads durable state through several ports, and every one of
/// them needs the same three things. Passing them together keeps the reading
/// helpers to their subject rather than to their plumbing.
pub struct CampaignJob<'a> {
    /// The repository the campaign writes and reads through.
    pub repository: &'a PostgresJobRepository,
    /// The chunk transaction manager bound to that repository.
    pub manager: &'a PostgresChunkTransactionManager,
    /// The connection string the enlisted business rows are read back through.
    pub runtime_url: &'a str,
    /// The job name the campaign runs under.
    pub job_name: &'a str,
    /// The instance key that job runs under.
    pub key: JobInstanceKey,
}

/// Creates one instance, attempt, and step attempt for a campaign job.
///
/// # Errors
///
/// Returns the repository failure that prevented the attempt.
pub async fn create_attempt(
    repository: &PostgresJobRepository,
    key: &JobInstanceKey,
    job_name: &str,
    chunk_size: u32,
) -> Result<(JobExecution, StepExecution), Box<dyn Error>> {
    let step_name = StepName::new(STEP_NAME)?;
    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(key)
        .await?
        .instance()
        .clone();
    let execution = unit
        .create_job_execution_with_definition(instance.id(), &definition(job_name, chunk_size)?)
        .await?;
    let step = unit
        .create_step_execution(execution.id(), &step_name)
        .await?;
    unit.commit().await?;
    Ok((execution, step))
}

/// Starts one attempt and its step attempt in a single unit of work.
///
/// # Errors
///
/// Returns the repository or lifecycle failure that prevented the transition.
pub async fn start_attempt(
    repository: &PostgresJobRepository,
    execution: &JobExecution,
    step: &StepExecution,
    at: SystemTime,
) -> Result<(JobExecution, StepExecution), Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let execution = unit
        .transition_job_execution(
            execution.id(),
            execution.version(),
            LifecycleTransition::new(BatchStatus::Started, at),
        )
        .await?;
    let step = unit
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, at),
        )
        .await?;
    unit.commit().await?;
    Ok((execution, step))
}

/// Reads the newest attempt and its first step attempt.
///
/// # Errors
///
/// Returns a failure when the job left no instance, attempt, or step attempt.
pub async fn latest_attempt(
    repository: &PostgresJobRepository,
    key: &JobInstanceKey,
) -> Result<(JobExecution, StepExecution), Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let instance = unit
        .find_job_instance(key)
        .await?
        .ok_or_else(|| Failure("the campaign job created no instance".to_owned()))?;
    let execution = unit
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next_back()
        .ok_or_else(|| Failure("the campaign job created no attempt".to_owned()))?;
    let step = unit
        .step_executions(execution.id())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Failure("the campaign job created no step attempt".to_owned()))?;
    unit.rollback().await?;
    Ok((execution, step))
}

/// Terminates a step and its job and reads back the shape they leave.
///
/// # Errors
///
/// Returns the repository, lifecycle, or observation failure.
pub async fn complete_and_shape(
    job: &CampaignJob<'_>,
    scope: ChunkTransactionContext,
    job_version: ExecutionVersion,
    at: SystemTime,
) -> Result<TerminalShape, Box<dyn Error>> {
    let state = job.manager.load_committed_state(scope).await?;

    let mut complete = job.repository.begin().await?;
    complete
        .transition_step_execution(
            scope.step_execution_id(),
            state.step_execution().version(),
            LifecycleTransition::new(BatchStatus::Completed, at),
        )
        .await?;
    complete
        .transition_job_execution(
            scope.job_execution_id(),
            job_version,
            LifecycleTransition::new(BatchStatus::Completed, at),
        )
        .await?;
    complete.commit().await?;

    let observation = observe(job.repository, &job.key).await?;
    let final_state = observation
        .latest_step_state()
        .ok_or_else(|| Failure("the completed run left no durable step state".to_owned()))?;
    let execution = observation
        .executions
        .last()
        .ok_or_else(|| Failure("the completed run left no attempt".to_owned()))?;
    let step = observation
        .steps
        .last()
        .and_then(|attempt| attempt.first())
        .ok_or_else(|| Failure("the completed run left no step attempt".to_owned()))?;

    Ok(TerminalShape {
        position: final_state.position,
        counts: final_state.counts,
        items: business_items(job.runtime_url, job.job_name).await?,
        job_status: execution.metadata().status(),
        step_status: step.metadata().status(),
    })
}

/// Runs restart discovery over one crashed attempt.
///
/// The proposer reads real wall and monotonic clocks even though every durable
/// timestamp the campaign writes is fixed. That is deliberate: durable
/// inactivity is measured against the database server, and a fixed local clock
/// would make the skew evidence unusable rather than reproducible.
///
/// # Errors
///
/// Returns the discovery failure, including a candidate that is not stale.
pub async fn discover(
    repository: &PostgresJobRepository,
    execution: JobExecutionId,
) -> Result<RecoveryProposal, Box<dyn Error>> {
    let proposer = RecoveryProposer::new(
        PostgresExplorer::new(repository.clone()),
        Arc::new(SystemClock),
        Arc::new(SystemMonotonicClock::new()),
        OwnerToken::from_bytes([5; 16]),
    )
    .with_stale_threshold(StaleThreshold::new(Duration::from_mins(1))?);
    Ok(proposer.propose(execution).await?)
}

/// Resolves one crashed attempt through the audited operator path.
///
/// An unknown commit outcome may only be resolved under the reason code the
/// accepted contract reserves for it, so the campaign reads the marker rather
/// than choosing a code that would be accepted either way.
///
/// # Errors
///
/// Returns the operator failure, or a rejection the campaign does not accept.
pub async fn resolve(
    repository: &PostgresJobRepository,
    proposal: &RecoveryProposal,
    operation: &str,
    at: SystemTime,
) -> Result<Value, Box<dyn Error>> {
    let failure = FailureSummary::new(
        FailureCategory::PermanentInfrastructure,
        FailureId::new(945)?,
    );
    let reason = if proposal.evidence().unknown_commit() {
        "UNKNOWN_EFFECT"
    } else {
        "PROCESS_KILL_INSPECTED"
    };
    let prior = proposal.evidence().status();
    let request = OperatorRequest::recover(
        OperationId::new(operation)?,
        ActorRef::new("operator:m5-crash-restore")?,
        ReasonCode::new(reason)?,
        RecoveryDirective::MarkFailed(failure),
        proposal,
    );
    let operator = JobOperator::new(repository.clone(), Arc::new(FixedClock(at)));
    let outcome = operator.execute(&request).await?;
    assert_eq!(
        outcome.class(),
        OperatorOutcomeClass::Applied,
        "the audited operator path must resolve the crashed attempt",
    );
    assert_eq!(
        outcome.record().prior_status(),
        Some(prior),
        "the decision records the durable status it replaced",
    );
    assert_eq!(
        outcome.record().result_status(),
        Some(BatchStatus::Failed),
        "an inspected crash resolves to a failure rather than to a success",
    );

    Ok(json!({
        "path": "operator recover",
        "reason_code": reason,
        "prior_status": prior.as_str(),
        "resulting_status": BatchStatus::Failed.as_str(),
        "class": "APPLIED",
    }))
}

/// Renders durable counters for a retained report.
#[must_use]
pub fn counts(value: ExecutionCounts) -> Value {
    json!({
        "read": value.read(),
        "processed": value.processed(),
        "written": value.written(),
        "filtered": value.filtered(),
        "committed": value.committed(),
        "rolled_back": value.rolled_back(),
    })
}

/// Creates one handshake file, ignoring a failure the campaign cannot report.
///
/// The announcing side is often a child that is about to be killed and has no
/// way to fail a test. The waiting side times out instead, which is the
/// campaign violation that a lost announcement should produce.
pub fn announce(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(path, b"reached\n");
}

/// Waits for one handshake file within the campaign's bound.
///
/// # Errors
///
/// Returns a failure when the bound elapses, which the campaign reports as a
/// scenario that never reached its phase.
pub async fn wait_for_file(path: &Path, bound: Duration) -> Result<Duration, Box<dyn Error>> {
    let started = std::time::Instant::now();
    while started.elapsed() < bound {
        if path.exists() {
            return Ok(started.elapsed());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Err(Box::new(Failure(format!(
        "{} did not appear within {bound:?}",
        path.display()
    ))))
}

/// Parks the calling thread until something kills the process.
///
/// A phase is reached by parking rather than by exiting, so the campaign kills
/// a live process that is genuinely sitting at the phase. The bound exists so
/// a campaign that never delivers its signal fails with a distinguishable exit
/// code instead of hanging.
pub fn park_until_killed() -> ! {
    let started = std::time::Instant::now();
    while started.elapsed() < PARK_BOUND {
        std::thread::sleep(POLL_INTERVAL);
    }
    eprintln!("campaign child was never killed at its phase");
    std::process::exit(UNKILLED_EXIT_CODE)
}

/// Waits until one backend is blocked on a lock while running a statement.
///
/// This is the only server-side observation the campaign makes, and it is what
/// makes two commit phases deterministic rather than timed: a backend blocked
/// on a lock stays blocked, so the kill lands at the phase every time instead
/// of racing a fast statement.
///
/// # Errors
///
/// Returns a failure when no backend reaches that state inside the bound.
pub async fn wait_for_blocked_statement(
    url: &str,
    statement_prefix: &str,
    bound: Duration,
) -> Result<Duration, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let started = std::time::Instant::now();
    let mut found = None;
    while started.elapsed() < bound {
        let blocked: i64 = sqlx::query(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() \
             AND state = 'active' AND wait_event_type = 'Lock' \
             AND query LIKE $1",
        )
        .bind(format!("{statement_prefix}%"))
        .fetch_one(&pool)
        .await?
        .try_get(0)?;
        if blocked > 0 {
            found = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    pool.close().await;

    found.ok_or_else(|| {
        Box::new(Failure(format!(
            "no backend blocked on a lock while running {statement_prefix} within {bound:?}"
        ))) as Box<dyn Error>
    })
}

/// Retains one machine-readable observation, when a runner asked for them.
///
/// # Errors
///
/// Returns the filesystem failure when the configured directory cannot be
/// written, which the campaign treats as a failure rather than as an absence.
pub fn retain_observation(name: &str, document: &Value) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let Some(directory) = variable(OBSERVATIONS_ENV) else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{name}.json"));
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(document)?),
    )?;
    Ok(Some(path))
}

/// Returns a per-scenario handshake directory under the target directory.
///
/// # Errors
///
/// Returns the filesystem failure when the directory cannot be created.
pub fn handshake_directory(scenario: &str) -> Result<PathBuf, Box<dyn Error>> {
    let directory = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(scenario);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory)?;
    Ok(directory)
}

/// The instant the campaign's fixed clocks start from.
#[must_use]
pub fn epoch(offset_seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(offset_seconds)
}

/// A campaign precondition the fixture could not satisfy.
#[derive(Debug)]
pub struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
