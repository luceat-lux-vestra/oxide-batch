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
    CodecId, CodecVersion, ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity,
    DefaultComponentCodec, FailureCategory, FailureId, FailureSummary, ItemStream, JobRepository,
    LifecycleTransition, PostgresJobRepository, PostgresMigrator, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StopSource,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamUpdateContext, StreamUpdateError, VersionedStateCodec,
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

/// A minimal `ItemStream` that decodes an inherited envelope through
/// [`codec`] and records the observed value, so a restart fixture can prove
/// the runtime actually hands `open` the inherited state -- not just that
/// the transaction manager's query returns it.
#[derive(Default)]
struct RowCountStream {
    observed: std::sync::Mutex<Option<u64>>,
}

impl RowCountStream {
    fn observed(&self) -> Option<u64> {
        *self.observed.lock().expect("lock poisoned")
    }
}

impl ItemStream for RowCountStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let Some(envelope) = context.inherited_state() else {
            return Ok(StreamOpenOutcome::Initial);
        };
        let codec = codec().map_err(|_| StreamOpenError::new())?;
        let rows: u64 = envelope
            .decode(&codec)
            .map_err(|_| StreamOpenError::new())?;
        *self.observed.lock().expect("lock poisoned") = Some(rows);
        Ok(StreamOpenOutcome::Restored)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        Err(StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Proves cross-attempt restart inheritance, not just same-attempt
/// SIGKILL-and-resume atomicity: attempt A commits component state, is
/// terminated, and attempt B -- a genuinely new job/step execution id
/// created through the normal framework restart path
/// (`create_job_execution_with_definition` + `create_step_execution`, the
/// same path a real operator-triggered restart uses) -- inherits it.
#[test]
fn restart_with_new_step_execution_id_inherits_committed_stream_state() -> Result<(), Box<dyn Error>>
{
    const JOB: &str = "m6_stream_restart_inherits_state";
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
                std::sync::Arc::new(FixedClock(epoch(1000))),
            )
            .await?;
            let key = crash_restore::instance_key(JOB)?;

            // Attempt A: create, start, commit chunk 1 (rows = 1) durably.
            let (execution_a, step_a) = create_attempt(&repository, &key, JOB, 1).await?;
            let (execution_a, step_a) =
                start_attempt(&repository, &execution_a, &step_a, epoch(1001)).await?;

            let manager = crash_restore::transaction_manager(&repository, None);
            let scope_a = ChunkTransactionContext::new(execution_a.id(), step_a.id());
            commit_chunk_with_state(&manager, scope_a, JOB, 1, 1).await?;

            // An attempted second chunk is explicitly rolled back: its
            // candidate component state (rows = 2) must never commit, and
            // therefore must never be visible to a later restart.
            let mut aborted = manager.begin_for(scope_a).await?;
            crash_restore::write_items(&mut *aborted, JOB, &[2]).await?;
            aborted.rollback().await?;

            // Terminate attempt A so the next attempt is a genuine restart.
            // Re-read the current (execution, step) first: `commit_chunk_with_state`
            // already bumped the step execution's optimistic version past what
            // `step_a` captured before the commit, so failing against the stale
            // version would be rejected as a concurrent modification.
            let (current_execution_a, current_step_a) =
                crash_restore::latest_attempt(&repository, &key).await?;
            let failed_at = epoch(1002);
            let mut fail = repository.begin().await?;
            fail.transition_step_execution(
                current_step_a.id(),
                current_step_a.version(),
                LifecycleTransition::failed(
                    failed_at,
                    FailureSummary::new(FailureCategory::UserComponent, FailureId::new(1)?),
                ),
            )
            .await?;
            fail.transition_job_execution(
                current_execution_a.id(),
                current_execution_a.version(),
                LifecycleTransition::failed(
                    failed_at,
                    FailureSummary::new(FailureCategory::UserComponent, FailureId::new(2)?),
                ),
            )
            .await?;
            fail.commit().await?;

            // Attempt B: the normal framework restart path. Same job
            // instance, same definition -> a brand-new job/step execution
            // id, with `restart_of_execution_id` chaining back to attempt A.
            let (execution_b, step_b) = create_attempt(&repository, &key, JOB, 1).await?;

            assert_ne!(
                execution_a.id(),
                execution_b.id(),
                "a genuine restart must create a new job execution id"
            );
            assert_ne!(
                step_a.id(),
                step_b.id(),
                "a genuine restart must create a new step execution id"
            );

            let scope_b = ChunkTransactionContext::new(execution_b.id(), step_b.id());
            let inherited = manager.inherited_component_state(scope_b).await?;
            let restored = inherited
                .iter()
                .find(|candidate| candidate.namespace().as_str() == NAMESPACE)
                .ok_or_else(|| {
                    Failure(
                        "attempt B must inherit attempt A's committed component state".to_owned(),
                    )
                })?;
            let rows: u64 = restored.decode(&codec()?)?;
            assert_eq!(
                rows, 1,
                "a genuine restart must inherit A's last COMMITTED value, \
                 not the rolled-back candidate"
            );

            // Opening the stream in B: the runtime hands exactly this
            // inherited, checksum-verified envelope to `ItemStream::open`.
            let (_stop_source, stop_token) = StopSource::new();
            let stream = RowCountStream::default();
            let outcome = stream
                .open(StreamOpenContext::new(Some(restored), &stop_token))
                .await?;
            assert_eq!(outcome, StreamOpenOutcome::Restored);
            assert_eq!(stream.observed(), Some(1));

            remove_job(&migrator_url, JOB).await?;
            Ok(())
        })
}

/// A codec whose encoded bytes are valid JSON but deliberately not what
/// `serde_json::to_vec` would produce for the same logical content: reversed
/// key order and interior whitespace around the colon, matching the `{ "z":
/// 1, "a": 2 }` example the corrective review names.
struct NonCanonicalSchema {
    schema: StateSchemaId,
}

const NON_CANONICAL_BYTES: &[u8] = b"{ \"z\": 1, \"a\": 2 }";

impl VersionedStateCodec<()> for NonCanonicalSchema {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }
    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).expect("nonzero")
    }
    fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
        Ok(NON_CANONICAL_BYTES.to_vec())
    }
    fn decode(&self, payload: &[u8]) -> Result<(), StateCodecError> {
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        (value.get("z").and_then(Value::as_u64) == Some(1)
            && value.get("a").and_then(Value::as_u64) == Some(2))
        .then_some(())
        .ok_or(StateCodecError::InvalidPayload)
    }
}

fn non_canonical_codec() -> Result<DefaultComponentCodec<NonCanonicalSchema>, Box<dyn Error>> {
    Ok(DefaultComponentCodec::new(
        NonCanonicalSchema {
            schema: StateSchemaId::new("m6.crash-recovery.non-canonical")?,
        },
        CodecId::new("m6.crash-recovery.non-canonical-codec")?,
        CodecVersion::new(1)?,
        RestartabilityDeclaration::Restartable,
    ))
}

const NON_CANONICAL_NAMESPACE: &str = "reader.non_canonical";

/// Corrective-review evidence (PR #161, fix 2): `PostgreSQL` preserves the
/// exact codec-produced bytes -- not a `jsonb` reserialization of them -- so
/// a non-canonically-formatted but valid payload survives a commit/reload
/// round trip byte-for-byte, and a mutation of the actually persisted bytes
/// is still caught as a checksum failure.
#[test]
fn postgres_preserves_non_canonical_json_bytes_exactly() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "m6_stream_non_canonical_bytes";
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
                std::sync::Arc::new(FixedClock(epoch(1100))),
            )
            .await?;
            let key = crash_restore::instance_key(JOB)?;
            let (execution, step) = create_attempt(&repository, &key, JOB, 1).await?;
            let (execution, step) =
                start_attempt(&repository, &execution, &step, epoch(1101)).await?;

            let manager = crash_restore::transaction_manager(&repository, None);
            let scope = ChunkTransactionContext::new(execution.id(), step.id());
            let codec = non_canonical_codec()?;
            let envelope = ComponentStateEnvelope::encode(
                ComponentStreamIdentity::new(NON_CANONICAL_NAMESPACE)?,
                &(),
                &codec,
                StateLimits::default(),
            )?;

            let mut transaction = manager.begin_for(scope).await?;
            crash_restore::write_items(&mut *transaction, JOB, &[1]).await?;
            let count = ChunkCount::new(1);
            transaction
                .commit_with_component_state(
                    ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
                    ChunkFaultProgress::NONE,
                    &[envelope],
                )
                .await?;

            // Reload: the exact codec-produced bytes come back, checksum
            // verification passes against them, and decode succeeds.
            let inherited = manager.inherited_component_state(scope).await?;
            let restored = inherited
                .iter()
                .find(|candidate| candidate.namespace().as_str() == NON_CANONICAL_NAMESPACE)
                .ok_or_else(|| Failure("non-canonical envelope must round-trip".to_owned()))?;
            let ComponentStatePayload::Inline(bytes) = restored.payload()? else {
                return Err(
                    Box::new(Failure("expected an inline payload".to_owned())) as Box<dyn Error>
                );
            };
            assert_eq!(
                bytes, NON_CANONICAL_BYTES,
                "postgres must preserve the exact codec-produced bytes, not a jsonb reserialization"
            );
            restored.decode(&codec)?;

            // Corruption detection: mutate the bytes actually persisted in
            // `ob_component_state.payload`, then prove a reload fails
            // checksum verification rather than silently decoding.
            let step_id = i64::try_from(step.id().get())?;
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&runtime_url)
                .await?;
            sqlx::query(
                "UPDATE oxide_batch.ob_component_state SET payload = $1 \
                 WHERE step_execution_id = $2 AND namespace = $3",
            )
            .bind(b"{ \"z\": 9, \"a\": 2 }".as_slice())
            .bind(step_id)
            .bind(NON_CANONICAL_NAMESPACE)
            .execute(&pool)
            .await?;
            pool.close().await;

            let reloaded = manager.inherited_component_state(scope).await;
            assert!(
                reloaded.is_err(),
                "mutated persisted bytes must fail checksum verification, not silently decode"
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
