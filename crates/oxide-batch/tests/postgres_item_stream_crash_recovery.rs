//! Process-kill and commit/restart evidence for M6 `#144` component state.
//!
//! Reuses the existing crash-and-restore harness's `ProviderPark` injection
//! point (`crash_restore::state_provider`), which already parks inside
//! `PostgresChunkStateProvider::state_for_commit` -- synchronously, before
//! any durable write for that attempt, and therefore before this issue's new
//! `ob_component_state` UPSERT, which rides inside the same commit
//! statement/transaction as the existing checkpoint update. Killing the
//! process there proves "crash before commit restores the previous
//! (possibly absent) component state"; committing one chunk cleanly and then
//! parking the next proves "crash after a proven commit leaves the newly
//! committed component state in place, and it does not disappear under a
//! later attempt."

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::expect_used, clippy::panic)]

mod crash_restore;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Command;

use oxide_batch::{
    ChunkCount, ChunkCounts, ChunkFaultProgress, ChunkTransactionContext, ChunkTransactionManager,
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    PostgresJobRepository, PostgresMigrator, RestartabilityDeclaration, StateCodecError,
    StateLimits, StateSchemaId, StateSchemaVersion, VersionedStateCodec,
};
use serde_json::{Map, Value, json};

use crash_restore::{
    Failure, FixedClock, HANDSHAKE_BOUND, ProviderPark, config, create_attempt, epoch,
    handshake_directory, migrator_url, park_until_killed, prepare_fixture, remove_job, runtime_url,
    start_attempt, state_provider, wait_for_file,
};

const PHASE_ENV: &str = "OXIDEBATCH_M6_STREAM_PHASE";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M6_STREAM_HANDSHAKE";
const SIGKILL: i32 = 9;
const NAMESPACE: &str = "reader.row_count";

fn schema() -> Result<StateSchemaId, Box<dyn Error>> {
    Ok(StateSchemaId::new("m6.crash-recovery.row-count")?)
}

struct CounterSchema {
    schema: StateSchemaId,
}

impl VersionedStateCodec<u64> for CounterSchema {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }
    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).expect("nonzero")
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&json!({ "rows": value })).map_err(|_| StateCodecError::InvalidPayload)
    }
    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        let value: Map<String, Value> =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        value
            .get("rows")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)
    }
}

fn codec() -> Result<DefaultComponentCodec<CounterSchema>, Box<dyn Error>> {
    Ok(DefaultComponentCodec::new(
        CounterSchema { schema: schema()? },
        CodecId::new("m6.crash-recovery.row-count-codec")?,
        CodecVersion::new(1)?,
        RestartabilityDeclaration::Restartable,
    ))
}

fn envelope(rows: u64) -> Result<ComponentStateEnvelope, Box<dyn Error>> {
    Ok(ComponentStateEnvelope::encode(
        ComponentStreamIdentity::new(NAMESPACE)?,
        &rows,
        &codec()?,
        StateLimits::default(),
    )?)
}

/// Commits one chunk with a business item and a candidate component-state
/// envelope, mirroring `crash_restore::commit_chunk` with the M6 addition.
async fn commit_chunk_with_state(
    manager: &oxide_batch::PostgresChunkTransactionManager,
    scope: ChunkTransactionContext,
    job_name: &str,
    item: i64,
    rows: u64,
) -> Result<(), Box<dyn Error>> {
    let mut transaction = manager.begin_for(scope).await?;
    crash_restore::write_items(&mut *transaction, job_name, &[item]).await?;
    let count = ChunkCount::new(1);
    transaction
        .commit_with_component_state(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
            &[envelope(rows)?],
        )
        .await?;
    Ok(())
}

/// Selects the component-state row for one step execution.
async fn component_state_row(
    runtime_url: &str,
    step_execution_id: i64,
) -> Result<Option<(String, i32, i64)>, Box<dyn Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(runtime_url)
        .await?;
    let row: Option<(String, i32, i64)> = sqlx::query_as(
        "SELECT namespace, schema_version, version FROM oxide_batch.ob_component_state \
         WHERE step_execution_id = $1 AND namespace = $2",
    )
    .bind(step_execution_id)
    .bind(NAMESPACE)
    .fetch_optional(&pool)
    .await?;
    pool.close().await;
    Ok(row)
}

async fn resolved_step_id(
    repository: &PostgresJobRepository,
    key: &oxide_batch::JobInstanceKey,
) -> Result<i64, Box<dyn Error>> {
    let (_, step) = crash_restore::latest_attempt(repository, key).await?;
    Ok(i64::try_from(step.id().get())?)
}

/// Spawns this test binary re-executed as the killable worker process.
fn spawn_worker(
    job_name: &str,
    ordinal: usize,
    handshake: &Path,
) -> Result<std::process::Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("stream_crash_worker")
        .arg("--nocapture")
        .env(PHASE_ENV, format!("{job_name}:{ordinal}"))
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

#[test]
fn process_kill_before_commit_restores_previous_stream_state() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "m6_stream_kill_before_commit";
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            prepare_fixture(&migrator_url, JOB).await?;

            let repository = PostgresJobRepository::connect(
                config(runtime_url.clone())?,
                std::sync::Arc::new(FixedClock(epoch(500))),
            )
            .await?;
            let key = crash_restore::instance_key(JOB)?;
            let (execution, step) = create_attempt(&repository, &key, JOB, 1).await?;
            start_attempt(&repository, &execution, &step, epoch(501)).await?;

            let handshake = handshake_directory("m6-stream-before")?;
            let mut child = spawn_worker(JOB, 1, &handshake)?;
            wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;

            child.kill()?;
            let status = child.wait()?;
            assert_eq!(status.signal(), Some(SIGKILL));
            assert_eq!(status.code(), None);

            let step_id = resolved_step_id(&repository, &key).await?;
            let row = component_state_row(&runtime_url, step_id).await?;
            assert!(
                row.is_none(),
                "a process killed before any durable write must leave no component-state row: {row:?}"
            );

            remove_job(&migrator_url, JOB).await?;
            Ok(())
        })
}

#[test]
fn process_kill_after_commit_restores_new_stream_state() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "m6_stream_kill_after_commit";
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            prepare_fixture(&migrator_url, JOB).await?;

            let repository = PostgresJobRepository::connect(
                config(runtime_url.clone())?,
                std::sync::Arc::new(FixedClock(epoch(600))),
            )
            .await?;
            let key = crash_restore::instance_key(JOB)?;
            let (execution, step) = create_attempt(&repository, &key, JOB, 1).await?;
            start_attempt(&repository, &execution, &step, epoch(601)).await?;

            let handshake = handshake_directory("m6-stream-after")?;
            // Ordinal 2: the worker commits chunk 1 (rows = 1) cleanly, then
            // parks inside the state provider for chunk 2's commit, so chunk
            // 1's component state is already durable when the kill lands.
            let mut child = spawn_worker(JOB, 2, &handshake)?;
            wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;

            child.kill()?;
            let status = child.wait()?;
            assert_eq!(status.signal(), Some(SIGKILL));
            assert_eq!(status.code(), None);

            let step_id = resolved_step_id(&repository, &key).await?;
            let row = component_state_row(&runtime_url, step_id).await?;
            let (namespace, schema_version, version) = row.ok_or_else(|| {
                Failure("chunk 1's component state must already be durable".to_owned())
            })?;
            assert_eq!(namespace, NAMESPACE);
            assert_eq!(schema_version, 1);
            assert_eq!(version, 0, "the first committed row starts at version 0");

            let manager = crash_restore::transaction_manager(&repository, None);
            let scope = ChunkTransactionContext::new(execution.id(), step.id());
            let inherited = manager.inherited_component_state(scope).await?;
            let restored = inherited
                .iter()
                .find(|candidate| candidate.namespace().as_str() == NAMESPACE)
                .ok_or_else(|| Failure("inherited component state is missing".to_owned()))?;
            let rows: u64 = restored.decode(&codec()?)?;
            assert_eq!(
                rows, 1,
                "restart must see chunk 1's committed value, not chunk 2's uncommitted candidate"
            );

            remove_job(&migrator_url, JOB).await?;
            Ok(())
        })
}

/// The re-exec'd worker: commits up to `ordinal` chunks, parking inside the
/// state provider on the last one so the parent's kill lands there.
#[test]
fn stream_crash_worker() -> Result<(), Box<dyn Error>> {
    let Ok(phase) = std::env::var(PHASE_ENV) else {
        return Ok(());
    };
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let (job_name, ordinal) = phase
        .split_once(':')
        .ok_or_else(|| Failure("malformed phase".to_owned()))?;
    let ordinal: usize = ordinal.parse()?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let Some(runtime_url) = runtime_url() else {
                return Ok(());
            };
            let repository = PostgresJobRepository::connect(
                config(runtime_url)?,
                std::sync::Arc::new(FixedClock(epoch(900))),
            )
            .await?;
            let key = crash_restore::instance_key(job_name)?;
            let (execution, step) = crash_restore::latest_attempt(&repository, &key).await?;
            let scope = ChunkTransactionContext::new(execution.id(), step.id());

            let park = ProviderPark {
                ordinal,
                reached: handshake.join("reached"),
            };
            let manager = oxide_batch::PostgresChunkTransactionManager::new(
                repository.clone(),
                state_provider(Some(park)),
            );

            for chunk in 1..ordinal {
                let item = i64::try_from(chunk)?;
                commit_chunk_with_state(&manager, scope, job_name, item, item.try_into()?).await?;
            }
            // The final commit's state provider parks before returning, so
            // this call never completes; the parent kills this process while
            // it is blocked here.
            let item = i64::try_from(ordinal)?;
            commit_chunk_with_state(&manager, scope, job_name, item, item.try_into()?).await?;
            park_until_killed();
        })
}
