//! P-013 restart after many chunks.
//!
//! The [performance plan](../../../docs/engineering/performance-plan.md) owes
//! P-013 as discovery and recovery time after a crash that follows many
//! committed chunks, and the M5 crash and restore campaign owes it as one of
//! its required reports.
//!
//! The measurement is only meaningful if the recovery it times is correct, so
//! the report is a correctness scenario first. A run that commits many chunks
//! is killed with `SIGKILL`; the campaign then requires the durable state to
//! name exactly the chunks that committed, requires discovery to agree with
//! that durable state, restarts, resumes, and requires the finished job to be
//! indistinguishable from an uninterrupted run of the same work.
//!
//! "Indistinguishable" is the point of the report. A restart that reprocessed a
//! committed chunk would collide with the business table's primary key, and a
//! restart that skipped one would leave fewer rows; the campaign asserts the
//! exact row set rather than a count, so neither can pass.

#![cfg(all(feature = "postgres", unix))]

mod crash_restore;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_batch::{
    BatchStatus, ChunkTransactionContext, ExecutionCounts, PostgresJobRepository, PostgresMigrator,
};
use serde_json::json;

use crash_restore::{
    CampaignJob, Failure, FixedClock, HANDSHAKE_BOUND, TerminalShape, announce, business_items,
    checkpoint_position, commit_chunk, complete_and_shape, config, counts, create_attempt,
    discover, epoch, execution_manifest, handshake_directory, instance_key, migrator_url, observe,
    park_until_killed, prepare_fixture, resolve, retain_observation, runtime_url, start_attempt,
    transaction_manager, wait_for_file,
};

/// Marks the child process that commits chunks and parks.
const WORKER_ENV: &str = "OXIDEBATCH_M5_P013_WORKER";

/// Names the directory the campaign and its child hand off through.
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M5_P013_HANDSHAKE";

/// The signal the campaign requires the killed child to report.
const SIGKILL: i32 = 9;

/// The chunk size the report runs at.
const CHUNK_SIZE: u32 = 5;

/// How many chunks one complete run commits.
const TOTAL_CHUNKS: u64 = 200;

/// How many chunks commit before the process is killed.
///
/// "Many" is the requirement, and the value matters less than the fact that it
/// is neither the first nor the last chunk: the restart has a long committed
/// prefix to inherit and real work left to do.
const CHUNKS_BEFORE_KILL: u64 = 130;

/// The job the killed and restarted run uses.
const CRASHED_JOB: &str = "m5_restart_many_chunks";

/// The job the uninterrupted comparison run uses.
const CANONICAL_JOB: &str = "m5_restart_many_chunks_canonical";

#[test]
fn restart_after_many_chunks_matches_an_uninterrupted_run() -> Result<(), Box<dyn Error>> {
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
        .block_on(run_report(runtime_url, migrator_url))
}

/// Runs the uninterrupted comparison, then the killed run, then compares them.
#[allow(
    clippy::too_many_lines,
    reason = "the kill, the durable inspection, the timed discovery, the timed resume, and the \
              equivalence assertion form one report that is only meaningful in order"
)]
async fn run_report(runtime_url: String, migrator_url: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;

    let uninterrupted = Instant::now();
    let canonical = run_uninterrupted(&runtime_url, &migrator_url).await?;
    let uninterrupted_in = uninterrupted.elapsed();

    prepare_fixture(&migrator_url, CRASHED_JOB).await?;
    let handshake = handshake_directory("p013")?;
    let mut child = spawn_worker(&handshake)?;
    let pid = child.id();

    let committing = Instant::now();
    let reached = wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await;
    if reached.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    reached?;
    let committing_in = committing.elapsed();

    child.kill()?;
    let status = child.wait()?;
    assert_eq!(
        status.signal(),
        Some(SIGKILL),
        "the report must measure recovery from a killed process rather than from an exit",
    );

    let repository = PostgresJobRepository::connect(
        config(runtime_url.clone())?,
        Arc::new(FixedClock(epoch(902))),
    )
    .await?;
    let manager = transaction_manager(&repository, None);
    let key = instance_key(CRASHED_JOB)?;
    let job = CampaignJob {
        repository: &repository,
        manager: &manager,
        runtime_url: &runtime_url,
        job_name: CRASHED_JOB,
        key: key.clone(),
    };

    let discovery = Instant::now();
    let after_kill = observe(&repository, &key).await?;
    let killed_execution = after_kill
        .executions
        .first()
        .ok_or_else(|| Failure("the killed process left no attempt".to_owned()))?;
    let killed_step = after_kill
        .steps
        .first()
        .and_then(|attempt| attempt.first())
        .ok_or_else(|| Failure("the killed process left no step attempt".to_owned()))?;
    let killed_state = after_kill
        .latest_step_state()
        .ok_or_else(|| Failure("the killed attempt has no durable state".to_owned()))?;
    let proposal = discover(&repository, killed_execution.id()).await?;
    let discovery_in = discovery.elapsed();

    let committed_position = u64::from(CHUNK_SIZE) * CHUNKS_BEFORE_KILL;
    assert_eq!(
        killed_execution.metadata().status(),
        BatchStatus::Started,
        "a killed process must not leave a terminal status behind",
    );
    assert_eq!(
        killed_state.position,
        Some(committed_position),
        "the durable checkpoint names the last chunk that committed and no later work",
    );
    assert_eq!(
        killed_state.counts,
        ExecutionCounts::new(
            committed_position,
            committed_position,
            committed_position,
            0,
            CHUNKS_BEFORE_KILL,
            0,
        ),
        "the durable counters agree with the durable checkpoint",
    );
    assert_eq!(
        business_items(&runtime_url, CRASHED_JOB).await?,
        items_through(committed_position),
        "the enlisted business rows are exactly the committed prefix",
    );

    assert_eq!(
        proposal.evidence().status(),
        BatchStatus::Started,
        "discovery reports the durable status rather than a guess",
    );
    assert_eq!(
        proposal.observed_version(),
        killed_execution.version(),
        "discovery observes the durable optimistic version",
    );
    assert_eq!(
        proposal
            .evidence()
            .latest_step()
            .map(oxide_batch::RecoveryStepEvidence::id),
        Some(killed_step.id()),
        "discovery reports the durable step the crash left",
    );
    assert_eq!(
        proposal
            .evidence()
            .latest_step()
            .and_then(oxide_batch::RecoveryStepEvidence::checkpoint)
            .map(|checkpoint| checkpoint.schema_id().as_str().to_owned()),
        Some(crash_restore::POSITION_SCHEMA.to_owned()),
        "discovery reports the durable checkpoint the step declared",
    );

    let decision = resolve(&repository, &proposal, "m5-crash-restore-p013", epoch(903)).await?;

    let resume = Instant::now();
    let (execution, step) = create_attempt(&repository, &key, CRASHED_JOB, CHUNK_SIZE).await?;
    let (execution, _) = start_attempt(&repository, &execution, &step, epoch(903)).await?;
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    let inherited = manager.load_committed_state(scope).await?;
    let inherited_position = checkpoint_position(inherited.checkpoint())?;
    assert_eq!(
        inherited_position, committed_position,
        "the restart inherits exactly what the killed attempt durably committed",
    );
    for chunk in CHUNKS_BEFORE_KILL..TOTAL_CHUNKS {
        commit_chunk(&manager, scope, CRASHED_JOB, &chunk_items(chunk)).await?;
    }
    let resume_in = resume.elapsed();

    let shape = complete_and_shape(&job, scope, execution.version(), epoch(904)).await?;
    assert_eq!(
        shape, canonical,
        "a run killed after many chunks and restarted reaches the same durable observation as an \
         uninterrupted run of the same work",
    );

    let terminal = observe(&repository, &key).await?;
    assert_eq!(
        terminal
            .executions
            .iter()
            .map(|execution| execution.metadata().status())
            .collect::<Vec<_>>(),
        vec![BatchStatus::Failed, BatchStatus::Completed],
        "the killed attempt stays resolved and visible, and only the restart completes",
    );
    let summary = terminal.summary();
    repository.close().await?;

    retain_observation(
        "p013-restart-after-many-chunks",
        &json!({
            "report": "P-013 restart after many chunks",
            "scenario": "restart_after_many_chunks_matches_an_uninterrupted_run",
            "workload": {
                "chunk_size": CHUNK_SIZE,
                "chunks": TOTAL_CHUNKS,
                "items": TOTAL_CHUNKS * u64::from(CHUNK_SIZE),
                "chunks_before_kill": CHUNKS_BEFORE_KILL,
                "delivery": "atomic same resource",
            },
            "fixture": "postgres",
            "child_pid": pid,
            "termination": {
                "signal": "SIGKILL",
                "signal_number": SIGKILL,
                "exit_code": Option::<i32>::None,
            },
            "expected": "the durable checkpoint names exactly the committed chunks, the restart \
                         inherits it, and the finished job matches an uninterrupted run",
            "observed": {
                "checkpoint_position": killed_state.position,
                "counts": counts(killed_state.counts),
                "business_rows": committed_position,
                "crashed_attempt_status": killed_execution.metadata().status().as_str(),
            },
            "discovery": {
                "proposal_digest": proposal.digest_hex(),
                "observed_status": proposal.evidence().status().as_str(),
                "observed_version": proposal.observed_version().get(),
                "unknown_commit": proposal.evidence().unknown_commit(),
                "agrees_with_durable_metadata": true,
            },
            "decision": decision,
            "restart": {
                "inherited_position": inherited_position,
                "resumed_chunks": TOTAL_CHUNKS - CHUNKS_BEFORE_KILL,
            },
            "canonical": canonical.to_json(),
            "terminal": summary,
            "durations_ms": {
                "uninterrupted_run": millis(uninterrupted_in),
                "commit_chunks_before_kill": millis(committing_in),
                "discovery": millis(discovery_in),
                "resume": millis(resume_in),
            },
            "execution_manifest": execution_manifest()?,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Runs the whole workload once without interruption and records its shape.
async fn run_uninterrupted(
    runtime_url: &str,
    migrator_url: &str,
) -> Result<TerminalShape, Box<dyn Error>> {
    prepare_fixture(migrator_url, CANONICAL_JOB).await?;

    let repository = PostgresJobRepository::connect(
        config(runtime_url.to_owned())?,
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
        runtime_url,
        job_name: CANONICAL_JOB,
        key,
    };
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    for chunk in 0..TOTAL_CHUNKS {
        commit_chunk(&manager, scope, CANONICAL_JOB, &chunk_items(chunk)).await?;
    }

    let shape = complete_and_shape(&job, scope, execution.version(), epoch(904)).await?;
    assert_eq!(
        shape.position,
        Some(TOTAL_CHUNKS * u64::from(CHUNK_SIZE)),
        "the uninterrupted run must commit the whole workload before it is a comparison",
    );
    repository.close().await?;
    Ok(shape)
}

/// The business rows one chunk writes.
fn chunk_items(chunk: u64) -> Vec<i64> {
    let first = chunk * u64::from(CHUNK_SIZE) + 1;
    (first..first + u64::from(CHUNK_SIZE))
        .map(|item| i64::try_from(item).unwrap_or(i64::MAX))
        .collect()
}

/// Every business row a committed prefix must have written.
fn items_through(position: u64) -> Vec<i64> {
    (1..=position)
        .map(|item| i64::try_from(item).unwrap_or(i64::MAX))
        .collect()
}

/// Renders one measured duration for the retained report.
fn millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// Starts the child process that commits chunks and parks.
fn spawn_worker(handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("restart_after_many_chunks_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, "1")
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

#[test]
fn restart_after_many_chunks_worker_process() -> Result<(), Box<dyn Error>> {
    if std::env::var(WORKER_ENV).is_err() {
        return Ok(());
    }
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or_else(|| Failure("the worker has no database URL".to_owned()))?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(url, handshake))?;
    Err(Box::new(Failure(
        "the campaign child returned instead of being killed".to_owned(),
    )))
}

/// Commits the chunks that precede the kill and parks on the last one.
async fn run_worker(url: String, handshake: PathBuf) -> Result<(), Box<dyn Error>> {
    let repository =
        PostgresJobRepository::connect(config(url)?, Arc::new(FixedClock(epoch(900)))).await?;
    let key = instance_key(CRASHED_JOB)?;
    let (execution, step) = create_attempt(&repository, &key, CRASHED_JOB, CHUNK_SIZE).await?;
    let (execution, _) = start_attempt(&repository, &execution, &step, epoch(901)).await?;

    let manager = transaction_manager(&repository, None);
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    for chunk in 0..CHUNKS_BEFORE_KILL {
        commit_chunk(&manager, scope, CRASHED_JOB, &chunk_items(chunk)).await?;
    }

    announce(&handshake.join("reached"));
    park_until_killed()
}
