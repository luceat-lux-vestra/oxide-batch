//! Logical backup and restore of durable metadata.
//!
//! The M5 crash and restore campaign owes a logical backup restore on every
//! supported `PostgreSQL` version. This target is that report, and it performs
//! a real one: `pg_dump` writes a custom-format archive of the metadata and
//! business schemas, a separate database is created, `pg_restore` loads the
//! archive into it, and everything afterwards runs against the restored
//! database.
//!
//! What is compared is what the repository and explorer contracts report, not
//! rows read out of a table. The comparison is equality over one reading of
//! instance identity, every attempt and its explorer projection, the definition
//! descriptor and its fingerprint, optimistic versions, step attempts, durable
//! checkpoints and execution contexts, counters, recovery decisions, flow
//! decisions, and partition metadata. A restore that lost or changed any of
//! them fails here.
//!
//! The report then does the thing a restore exists for: it restarts the job on
//! the restored database and requires the finished run to match an
//! uninterrupted one.

#![cfg(all(feature = "postgres", unix))]

mod crash_restore;

use std::error::Error;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_batch::{
    BatchStatus, ChunkTransactionContext, ExecutionContext, ExitCode, FlowDecisionRequest,
    FlowDecisionSequence, FlowTarget, FlowTransitionKind, JobRepository, NodeId, PartitionKey,
    PartitionPlanEntry, PostgresJobRepository, PostgresMigrator, StateLimits, StepName,
    TerminalKind,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Row};

use crash_restore::{
    CampaignJob, DurableObservation, Failure, FixedClock, TerminalShape, business_items,
    checkpoint_position, commit_chunk, complete_and_shape, config, counts, create_attempt,
    discover, epoch, instance_key, latest_attempt, migrator_url, observe, prepare_fixture,
    remove_job, resolve, retain_observation, runtime_url, start_attempt, transaction_manager,
};

/// The chunk size the report runs at.
const CHUNK_SIZE: u32 = 5;

/// How many chunks the whole workload has.
const TOTAL_CHUNKS: u64 = 5;

/// How many chunks the first, resolved attempt commits.
const FIRST_ATTEMPT_CHUNKS: u64 = 1;

/// How many chunks are committed by the time the backup is taken.
const CHUNKS_AT_BACKUP: u64 = 3;

/// The job that is backed up, restored, and restarted on the restored copy.
const RESTORED_JOB: &str = "m5_backup_restore";

/// The job the uninterrupted comparison run uses.
const CANONICAL_JOB: &str = "m5_backup_restore_canonical";

/// The database the archive is restored into.
const RESTORE_DATABASE: &str = "oxide_batch_m5_restore";

/// The schemas the logical backup covers.
const DUMPED_SCHEMAS: [&str; 2] = ["oxide_batch", "oxide_batch_business"];

#[test]
fn logical_backup_restores_the_durable_state_and_the_job_restarts_on_it()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let Some(backup_url) = crash_restore::backup_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_BACKUP_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_report(Fixture {
            runtime: runtime_url,
            migrator: migrator_url,
            admin: backup_url,
        }))
}

/// The connection strings the report runs against.
struct Fixture {
    /// The database the job runs in and is backed up from.
    runtime: String,
    /// The database the metadata migrations are applied through.
    migrator: String,
    /// A database on the same server whose role may create and drop databases.
    admin: String,
}

/// Builds durable state, backs it up, restores it, compares it, and restarts.
#[allow(
    clippy::too_many_lines,
    reason = "building the durable state, backing it up, restoring it, comparing it, and \
              restarting on the restored copy form one report that is only meaningful in order"
)]
async fn run_report(fixture: Fixture) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(fixture.migrator.clone())?).await?;
    let server_version = server_version(&fixture.runtime).await?;

    let canonical = run_uninterrupted(&fixture).await?;
    let source = build_restart_relevant_state(&fixture).await?;

    let archive = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("m5-backup.dump");
    let dump = run_tool(
        "pg_dump",
        &[
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            &format!("--file={}", archive.display()),
            &format!("--schema={}", DUMPED_SCHEMAS[0]),
            &format!("--schema={}", DUMPED_SCHEMAS[1]),
            &fixture.runtime,
        ],
    )?;
    let archive_bytes = std::fs::metadata(&archive)?.len();
    assert!(
        archive_bytes > 0,
        "a logical backup that wrote nothing is not a backup",
    );

    let restored_url = with_database(&fixture.admin, RESTORE_DATABASE)?;
    recreate_database(&fixture.admin, RESTORE_DATABASE).await?;
    let restore = run_tool(
        "pg_restore",
        &[
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            &format!("--dbname={restored_url}"),
            &archive.display().to_string(),
        ],
    )?;

    let restored_repository = PostgresJobRepository::connect(
        config(restored_url.clone())?,
        Arc::new(FixedClock(epoch(902))),
    )
    .await?;
    let key = instance_key(RESTORED_JOB)?;
    let restored = observe(&restored_repository, &key).await?;
    assert_eq!(
        restored, source,
        "the restored database must report the same durable state the backup was taken from",
    );
    assert_eq!(
        business_items(&restored_url, RESTORED_JOB).await?,
        business_items(&fixture.runtime, RESTORED_JOB).await?,
        "the restored database must report the same enlisted business rows",
    );

    let restarting = Instant::now();
    let restored_shape = restart_on_restored(&restored_repository, &restored_url).await?;
    let restarting_in = restarting.elapsed();
    assert_eq!(
        restored_shape, canonical,
        "a job restarted on a restored database must reach the same durable observation as an \
         uninterrupted run of the same work",
    );

    let terminal = observe(&restored_repository, &key).await?;
    assert_eq!(
        terminal
            .executions
            .iter()
            .map(|execution| execution.metadata().status())
            .collect::<Vec<_>>(),
        vec![
            BatchStatus::Failed,
            BatchStatus::Failed,
            BatchStatus::Completed
        ],
        "the restored attempts stay visible and only the attempt started on the restore completes",
    );
    let summary = terminal.summary();
    restored_repository.close().await?;

    drop_database(&fixture.admin, RESTORE_DATABASE).await?;
    remove_job(&fixture.migrator, RESTORED_JOB).await?;
    remove_job(&fixture.migrator, CANONICAL_JOB).await?;

    retain_observation(
        "logical-backup-restore",
        &json!({
            "report": "logical backup and restore",
            "scenario": "logical_backup_restores_the_durable_state_and_the_job_restarts_on_it",
            "fixture": "postgres-backup",
            "server_version": server_version,
            "tooling": {
                "pg_dump": dump,
                "pg_restore": restore,
                "format": "custom",
                "schemas": DUMPED_SCHEMAS,
                "archive_bytes": archive_bytes,
            },
            "databases": {
                "restored_into": RESTORE_DATABASE,
            },
            "state_at_backup": {
                "attempts": source.executions.len(),
                "chunks_committed": CHUNKS_AT_BACKUP,
                "checkpoint_position": source.latest_step_state().map(|state| state.position),
                "counts": source.latest_step_state().map(|state| counts(state.counts)),
                "summary": source.summary(),
            },
            "compared": [
                "job instance identity",
                "job executions and their explorer projections",
                "definition revision and manifest fingerprint",
                "optimistic versions",
                "step executions",
                "durable checkpoints and execution contexts",
                "durable counters",
                "recovery decisions",
                "flow decisions",
                "partition metadata",
                "enlisted business rows",
            ],
            "restored_matches_source": true,
            "restart_on_restored": {
                "resumed_chunks": TOTAL_CHUNKS - CHUNKS_AT_BACKUP,
                "terminal": restored_shape.to_json(),
                "duration_ms": millis(restarting_in),
            },
            "canonical": canonical.to_json(),
            "terminal": summary,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Runs the whole workload once without interruption and records its shape.
async fn run_uninterrupted(fixture: &Fixture) -> Result<TerminalShape, Box<dyn Error>> {
    prepare_fixture(&fixture.migrator, CANONICAL_JOB).await?;

    let repository = PostgresJobRepository::connect(
        config(fixture.runtime.clone())?,
        Arc::new(FixedClock(epoch(900))),
    )
    .await?;
    let key = instance_key(CANONICAL_JOB)?;
    let (execution, step) = create_attempt(&repository, &key, CANONICAL_JOB, CHUNK_SIZE).await?;
    let (execution, _) = start_attempt(&repository, &execution, &step, epoch(901)).await?;

    let manager = transaction_manager(&repository, None);
    let job = CampaignJob {
        repository: &repository,
        manager: &manager,
        runtime_url: &fixture.runtime,
        job_name: CANONICAL_JOB,
        key,
    };
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    for chunk in 0..TOTAL_CHUNKS {
        commit_chunk(&manager, scope, CANONICAL_JOB, &chunk_items(chunk)).await?;
    }

    let shape = complete_and_shape(&job, scope, execution.version(), epoch(904)).await?;
    repository.close().await?;
    Ok(shape)
}

/// Builds every class of restart-relevant durable state the report compares.
///
/// The first attempt is resolved rather than abandoned, so the backup carries a
/// recovery decision, a flow decision, and a partition plan alongside the live
/// attempt's checkpoint and counters. A backup of a job that only ever ran
/// straight through would not exercise what a restore has to bring back.
async fn build_restart_relevant_state(
    fixture: &Fixture,
) -> Result<DurableObservation, Box<dyn Error>> {
    prepare_fixture(&fixture.migrator, RESTORED_JOB).await?;

    let repository = PostgresJobRepository::connect(
        config(fixture.runtime.clone())?,
        Arc::new(FixedClock(epoch(900))),
    )
    .await?;
    let manager = transaction_manager(&repository, None);
    let key = instance_key(RESTORED_JOB)?;

    let (first, first_step) = create_attempt(&repository, &key, RESTORED_JOB, CHUNK_SIZE).await?;
    let (first, first_step) = start_attempt(&repository, &first, &first_step, epoch(901)).await?;
    let first_scope = ChunkTransactionContext::new(first.id(), first_step.id());
    for chunk in 0..FIRST_ATTEMPT_CHUNKS {
        commit_chunk(&manager, first_scope, RESTORED_JOB, &chunk_items(chunk)).await?;
    }

    let mut unit = repository.begin().await?;
    let partitioned = unit
        .create_step_execution(first.id(), &StepName::new("partitioned")?)
        .await?;
    unit.create_step_partition_plan(
        partitioned.id(),
        &[partition_entry("alpha")?, partition_entry("beta")?],
    )
    .await?;
    unit.append_flow_decision(&FlowDecisionRequest::new(
        first.id(),
        FlowDecisionSequence::new(1)?,
        NodeId::new(crash_restore::STEP_NAME)?,
        Some(first_step.id()),
        FlowTransitionKind::StepExit,
        ExitCode::new("COMPLETED")?,
        FlowTarget::Terminal(TerminalKind::Complete),
        [7; 32],
        [9; 32],
        None,
        epoch(901),
    ))
    .await?;
    unit.commit().await?;

    let proposal = discover(&repository, first.id()).await?;
    resolve(
        &repository,
        &proposal,
        "m5-crash-restore-backup-first",
        epoch(902),
    )
    .await?;

    let (second, second_step) = create_attempt(&repository, &key, RESTORED_JOB, CHUNK_SIZE).await?;
    let (_, _) = start_attempt(&repository, &second, &second_step, epoch(903)).await?;
    let second_scope = ChunkTransactionContext::new(second.id(), second_step.id());
    let inherited = manager.load_committed_state(second_scope).await?;
    assert_eq!(
        checkpoint_position(inherited.checkpoint())?,
        FIRST_ATTEMPT_CHUNKS * u64::from(CHUNK_SIZE),
        "the second attempt inherits what the resolved attempt durably committed",
    );
    for chunk in FIRST_ATTEMPT_CHUNKS..CHUNKS_AT_BACKUP {
        commit_chunk(&manager, second_scope, RESTORED_JOB, &chunk_items(chunk)).await?;
    }

    let observation = observe(&repository, &key).await?;
    assert_eq!(
        observation.latest_step_state().map(|state| state.position),
        Some(CHUNKS_AT_BACKUP * u64::from(CHUNK_SIZE)),
        "the backup is taken with a live attempt part way through its work",
    );
    repository.close().await?;
    Ok(observation)
}

/// Resolves the restored live attempt, restarts it, and finishes the workload.
async fn restart_on_restored(
    repository: &PostgresJobRepository,
    restored_url: &str,
) -> Result<TerminalShape, Box<dyn Error>> {
    let key = instance_key(RESTORED_JOB)?;
    let manager = transaction_manager(repository, None);
    let (live, _) = latest_attempt(repository, &key).await?;

    let proposal = discover(repository, live.id()).await?;
    resolve(
        repository,
        &proposal,
        "m5-crash-restore-backup-restored",
        epoch(903),
    )
    .await?;

    let (execution, step) = create_attempt(repository, &key, RESTORED_JOB, CHUNK_SIZE).await?;
    let (execution, _) = start_attempt(repository, &execution, &step, epoch(903)).await?;
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    let inherited = manager.load_committed_state(scope).await?;
    assert_eq!(
        checkpoint_position(inherited.checkpoint())?,
        CHUNKS_AT_BACKUP * u64::from(CHUNK_SIZE),
        "the restored database carries the checkpoint the backup was taken at",
    );
    for chunk in CHUNKS_AT_BACKUP..TOTAL_CHUNKS {
        commit_chunk(&manager, scope, RESTORED_JOB, &chunk_items(chunk)).await?;
    }

    let job = CampaignJob {
        repository,
        manager: &manager,
        runtime_url: restored_url,
        job_name: RESTORED_JOB,
        key,
    };
    complete_and_shape(&job, scope, execution.version(), epoch(904)).await
}

/// Builds one bounded partition plan entry.
fn partition_entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"{}\",\"schema_version\":1,\"payload\":{{\"key\":\"{key}\"}}}}",
            crash_restore::CONTEXT_SCHEMA
        )
        .as_bytes(),
        StateLimits::default(),
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

/// The business rows one chunk writes.
fn chunk_items(chunk: u64) -> Vec<i64> {
    let first = chunk * u64::from(CHUNK_SIZE) + 1;
    (first..first + u64::from(CHUNK_SIZE))
        .map(|item| i64::try_from(item).unwrap_or(i64::MAX))
        .collect()
}

/// Renders one measured duration for the retained report.
fn millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// Reads the server version the backup and restore ran against.
async fn server_version(url: &str) -> Result<String, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let version: String = sqlx::query("SHOW server_version")
        .fetch_one(&pool)
        .await?
        .try_get(0)?;
    pool.close().await;
    Ok(version)
}

/// Drops and recreates the database the archive is restored into.
async fn recreate_database(admin_url: &str, name: &str) -> Result<(), Box<dyn Error>> {
    drop_database(admin_url, name).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await?;
    // The database name is a compile-time constant of this campaign, so the
    // statement carries no caller-supplied text.
    sqlx::query(AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Drops the restore database, disconnecting anything still attached to it.
async fn drop_database(admin_url: &str, name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await?;
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(name)
    .execute(&pool)
    .await?;
    sqlx::query(AssertSqlSafe(format!("DROP DATABASE IF EXISTS \"{name}\"")))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Replaces the database in a connection URL, keeping everything else.
fn with_database(url: &str, name: &str) -> Result<String, Box<dyn Error>> {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(base, query)| (base, Some(query.to_owned())));
    let prefix = base
        .rsplit_once('/')
        .map(|(prefix, _)| prefix)
        .ok_or_else(|| Failure(format!("{url} names no database")))?;
    Ok(match query {
        Some(query) => format!("{prefix}/{name}?{query}"),
        None => format!("{prefix}/{name}"),
    })
}

/// Runs one `PostgreSQL` client tool and returns the version that ran.
///
/// The version is recorded rather than assumed: an archive is only evidence if
/// the report says which tool wrote it, and a client older than the server
/// refuses to dump at all, which must fail the campaign rather than be skipped.
fn run_tool(program: &str, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    let version = Command::new(program)
        .arg("--version")
        .output()
        .map_err(|error| {
            Failure(format!(
                "the campaign needs {program} on PATH and could not run it: {error}"
            ))
        })?;
    if !version.status.success() {
        return Err(Box::new(Failure(format!("{program} --version failed"))));
    }

    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(Box::new(Failure(format!(
            "{program} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))));
    }
    Ok(String::from_utf8_lossy(&version.stdout).trim().to_owned())
}
