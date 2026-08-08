//! Process-kill evidence at every phase of the chunk commit protocol.
//!
//! The M5 design gate names
//! `process_kill_at_each_commit_phase_recovers_without_a_forged_status` as the
//! evidence the crash and restore campaign owes. This target is that scenario.
//!
//! The M2-M4 crash targets already kill a process on either side of a durable
//! write, and the campaign reuses them rather than rewriting them. What they do
//! not cover is the inside of one commit: they leave the process by calling
//! `exit`, which is an orderly departure, and only where application code can
//! choose to. The phases below are the commit protocol's own boundaries, and
//! every one of them is reached by a live process that is then killed with
//! `SIGKILL` from outside.
//!
//! Two of the five phases are inside the adapter, where no application hook
//! exists. They are reached without changing a line of the adapter, by holding
//! the lock the commit is about to need:
//!
//! - the progress bind blocks on a row lock the campaign holds on the step
//!   execution row, so the metadata write is issued and never commits;
//! - the commit itself blocks in a deferred constraint trigger on the business
//!   table, so `COMMIT` is in flight when the process dies and the server
//!   completes it afterwards. That is the unknown-outcome boundary: the work is
//!   durable and the process that did it never learned so.
//!
//! A backend blocked on a lock stays blocked, so the kill lands at the phase on
//! every run rather than racing a fast statement.

#![cfg(all(feature = "postgres", unix))]

mod crash_restore;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_batch::{
    BatchStatus, ChunkCount, ChunkCounts, ChunkFaultProgress, ChunkTransactionContext,
    ChunkTransactionManager, ExecutionCounts, ExecutionVersion, JobExecutionId, JobInstanceKey,
    PostgresChunkTransactionManager, PostgresJobRepository, PostgresMigrator,
};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};

use crash_restore::{
    CampaignJob, Failure, FixedClock, HANDSHAKE_BOUND, ProviderPark, TerminalShape, announce,
    business_items, checkpoint_position, commit_chunk, complete_and_shape, config, counts,
    create_attempt, discover, epoch, handshake_directory, instance_key, latest_attempt,
    migrator_url, observe, park_until_killed, prepare_fixture, remove_job, resolve,
    retain_observation, runtime_url, start_attempt, transaction_manager,
    wait_for_blocked_statement, wait_for_file, write_items,
};

/// Selects which commit phase a child process runs to.
const PHASE_ENV: &str = "OXIDEBATCH_M5_COMMIT_PHASE";

/// Names the directory the campaign and its child hand off through.
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M5_COMMIT_HANDSHAKE";

/// The advisory-lock key the deferred business trigger waits on.
///
/// The trigger body cannot take a parameter, so the key appears twice: here and
/// in the function the campaign creates. They must agree.
const ADVISORY_KEY: i64 = 11_131_137;

/// The signal the campaign requires every killed child to report.
const SIGKILL: i32 = 9;

/// The chunks every campaign job commits, in order.
const CHUNKS: [[i64; 2]; 3] = [[10, 20], [30, 40], [50, 60]];

/// The chunk size every campaign job declares.
const CHUNK_SIZE: u32 = 2;

/// The job the uninterrupted comparison run uses.
const CANONICAL_JOB: &str = "m5_commit_phase_canonical";

/// One phase of the chunk commit protocol a process can die in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// The enlisted business rows are written and `commit` has not been called.
    BusinessWritten,
    /// `commit` has checked the counters and produced the durable state, and
    /// has not yet bound the progress write.
    StateProvided,
    /// The progress write is issued and blocked, so it can never commit.
    ProgressBlocked,
    /// `COMMIT` is in flight and the server completes it after the kill.
    CommitInFlight,
    /// `commit` returned successfully and the process knows the chunk is
    /// durable.
    CommitAcknowledged,
}

/// Every phase the campaign requires, in commit order.
const PHASES: [Phase; 5] = [
    Phase::BusinessWritten,
    Phase::StateProvided,
    Phase::ProgressBlocked,
    Phase::CommitInFlight,
    Phase::CommitAcknowledged,
];

impl Phase {
    /// The identifier the campaign scope document and the report both use.
    const fn id(self) -> &'static str {
        match self {
            Self::BusinessWritten => "business-written",
            Self::StateProvided => "state-provided",
            Self::ProgressBlocked => "progress-blocked",
            Self::CommitInFlight => "commit-in-flight",
            Self::CommitAcknowledged => "commit-acknowledged",
        }
    }

    /// The job this phase runs, kept separate so phases never share state.
    const fn job_name(self) -> &'static str {
        match self {
            Self::BusinessWritten => "m5_commit_phase_business_written",
            Self::StateProvided => "m5_commit_phase_state_provided",
            Self::ProgressBlocked => "m5_commit_phase_progress_blocked",
            Self::CommitInFlight => "m5_commit_phase_commit_in_flight",
            Self::CommitAcknowledged => "m5_commit_phase_commit_acknowledged",
        }
    }

    /// How many chunks are durable once the killed process is gone.
    const fn durable_chunks(self) -> u64 {
        match self {
            Self::BusinessWritten | Self::StateProvided | Self::ProgressBlocked => 1,
            Self::CommitInFlight | Self::CommitAcknowledged => 2,
        }
    }

    /// What the accepted contract requires the durable state to say.
    const fn expectation(self) -> &'static str {
        match self {
            Self::BusinessWritten => {
                "the enlisted rows were never committed, so the chunk replays whole"
            }
            Self::StateProvided => {
                "durable state was produced inside the commit and never bound, so the chunk \
                 replays whole"
            }
            Self::ProgressBlocked => {
                "the progress write was issued and blocked, so neither it nor the enlisted rows \
                 are durable and the chunk replays whole"
            }
            Self::CommitInFlight => {
                "the server completed the commit the killed process never saw acknowledged, so \
                 the chunk is durable and must not replay"
            }
            Self::CommitAcknowledged => {
                "the commit was acknowledged before the kill, so the chunk is durable and must \
                 not replay"
            }
        }
    }

    /// Parses the phase a child process was asked to run to.
    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        PHASES
            .into_iter()
            .find(|phase| phase.id() == value)
            .ok_or_else(|| {
                Box::new(Failure(format!("unknown commit phase {value}"))) as Box<dyn Error>
            })
    }
}

#[test]
fn process_kill_at_each_commit_phase_recovers_without_a_forged_status() -> Result<(), Box<dyn Error>>
{
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
        .block_on(run_campaign(runtime_url, migrator_url))
}

/// Runs the uninterrupted comparison and then every phase against it.
async fn run_campaign(runtime_url: String, migrator_url: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
    let canonical = run_uninterrupted(&runtime_url, &migrator_url).await?;

    let mut phases = Vec::new();
    for phase in PHASES {
        phases.push(run_phase(phase, &runtime_url, &migrator_url, &canonical).await?);
    }

    retain_observation(
        "commit-phase-process-kill",
        &json!({
            "report": "process kill at each commit phase",
            "scenario": "process_kill_at_each_commit_phase_recovers_without_a_forged_status",
            "protocol": "chunk commit",
            "signal": "SIGKILL",
            "canonical": {
                "job": CANONICAL_JOB,
                "checkpoint_position": canonical.position,
                "counts": counts(canonical.counts),
                "business_items": canonical.items,
                "job_status": canonical.job_status.as_str(),
                "step_status": canonical.step_status.as_str(),
            },
            "phases": phases,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;
    Ok(())
}

/// Runs one job to completion without interruption and records its shape.
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
    for chunk in CHUNKS {
        commit_chunk(&manager, scope, CANONICAL_JOB, &chunk).await?;
    }

    let shape = complete_and_shape(&job, scope, execution.version(), epoch(904)).await?;
    repository.close().await?;
    Ok(shape)
}

/// Runs one phase end to end, cleaning up whatever the phase leaves behind.
async fn run_phase(
    phase: Phase,
    runtime_url: &str,
    migrator_url: &str,
    canonical: &TerminalShape,
) -> Result<Value, Box<dyn Error>> {
    prepare_fixture(migrator_url, phase.job_name()).await?;

    let handshake = handshake_directory(phase.id())?;
    let mut child = spawn_worker(phase, &handshake)?;
    let pid = child.id();

    let observed = drive_phase(
        phase,
        runtime_url,
        migrator_url,
        canonical,
        &mut child,
        &handshake,
    )
    .await;
    if observed.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        let _ = drop_commit_gate(migrator_url).await;
    }
    let mut observed = observed?;
    if let Some(map) = observed.as_object_mut() {
        map.insert("child_pid".to_owned(), json!(pid));
    }

    remove_job(migrator_url, phase.job_name()).await?;
    Ok(observed)
}

/// Drives one phase against a spawned child.
#[allow(
    clippy::too_many_lines,
    reason = "the kill, the durable inspection, the discovery, the restart, and the equivalence \
              assertion form one evidence chain that is only meaningful in order"
)]
async fn drive_phase(
    phase: Phase,
    runtime_url: &str,
    migrator_url: &str,
    canonical: &TerminalShape,
    child: &mut Child,
    handshake: &Path,
) -> Result<Value, Box<dyn Error>> {
    let job_name = phase.job_name();
    let key = instance_key(job_name)?;

    wait_for_file(&handshake.join("ready"), HANDSHAKE_BOUND).await?;

    let repository = PostgresJobRepository::connect(
        config(runtime_url.to_owned())?,
        Arc::new(FixedClock(epoch(902))),
    )
    .await?;
    let manager = transaction_manager(&repository, None);
    let job = CampaignJob {
        repository: &repository,
        manager: &manager,
        runtime_url,
        job_name,
        key: key.clone(),
    };
    let (execution, step) = latest_attempt(&repository, &key).await?;
    let scope = ChunkTransactionContext::new(execution.id(), step.id());

    let mut block = Block::arrange(phase, migrator_url, step.id().get()).await?;
    announce(&handshake.join("go"));

    let reaching = Instant::now();
    match phase {
        Phase::ProgressBlocked => {
            wait_for_blocked_statement(
                migrator_url,
                "UPDATE oxide_batch.ob_step_execution",
                HANDSHAKE_BOUND,
            )
            .await?;
        }
        Phase::CommitInFlight => {
            wait_for_blocked_statement(migrator_url, "COMMIT", HANDSHAKE_BOUND).await?;
        }
        _ => {
            wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;
        }
    }
    let reached_in = reaching.elapsed();

    child.kill()?;
    let status = child.wait()?;
    // Read before asserting, so the retained observation records what the
    // child actually reported rather than what the assertion demanded. The
    // runner cross-checks the recorded value, and a cross-check of a constant
    // the scenario wrote unconditionally would check nothing.
    let signal = status.signal();
    let exit_code = status.code();
    assert_eq!(
        signal,
        Some(SIGKILL),
        "{}: the campaign must observe a process that was killed rather than one that exited",
        phase.id(),
    );
    assert_eq!(
        exit_code,
        None,
        "{}: a killed process reports no exit code",
        phase.id(),
    );

    let durable_position = u64::from(CHUNK_SIZE) * phase.durable_chunks();
    block
        .release(&manager, scope, durable_position)
        .await
        .inspect_err(|_| eprintln!("{}: the phase block could not be released", phase.id()))?;

    let after_kill = observe(&repository, &key).await?;
    assert_eq!(
        after_kill.executions.len(),
        1,
        "{}: the killed process must leave exactly the attempt it created",
        phase.id(),
    );
    let killed_execution = after_kill
        .executions
        .first()
        .ok_or_else(|| Failure("the killed attempt is missing".to_owned()))?;
    assert_eq!(
        killed_execution.metadata().status(),
        BatchStatus::Started,
        "{}: a killed process must not leave a terminal status behind",
        phase.id(),
    );
    let killed_step = after_kill
        .steps
        .first()
        .and_then(|attempt| attempt.first())
        .ok_or_else(|| Failure("the killed step attempt is missing".to_owned()))?;
    assert_eq!(
        killed_step.metadata().status(),
        BatchStatus::Started,
        "{}: a killed process must not leave a terminal step status behind",
        phase.id(),
    );

    let killed_state = after_kill
        .latest_step_state()
        .ok_or_else(|| Failure("the killed attempt has no durable state".to_owned()))?;
    assert_eq!(
        killed_state.position,
        Some(durable_position),
        "{}: {}",
        phase.id(),
        phase.expectation(),
    );
    assert_eq!(
        killed_state.counts,
        ExecutionCounts::new(
            durable_position,
            durable_position,
            durable_position,
            0,
            phase.durable_chunks(),
            0,
        ),
        "{}: the durable counters must agree with the durable checkpoint",
        phase.id(),
    );
    let durable_items = business_items(runtime_url, job_name).await?;
    assert_eq!(
        durable_items,
        expected_items(phase.durable_chunks()),
        "{}: the enlisted business rows and the durable checkpoint commit together or not at all",
        phase.id(),
    );

    let discovery = Instant::now();
    let proposal = discover(&repository, killed_execution.id()).await?;
    let discovery_in = discovery.elapsed();
    assert_eq!(
        proposal.evidence().status(),
        BatchStatus::Started,
        "{}: discovery must report the durable status rather than a guess",
        phase.id(),
    );
    assert_eq!(
        proposal.observed_version(),
        killed_execution.version(),
        "{}: discovery must observe the durable optimistic version",
        phase.id(),
    );
    assert_eq!(
        proposal
            .evidence()
            .latest_step()
            .map(oxide_batch::RecoveryStepEvidence::id),
        Some(killed_step.id()),
        "{}: discovery must report the durable step the crash left",
        phase.id(),
    );

    let decision = resolve(
        &repository,
        &proposal,
        &format!("m5-crash-restore-{}", phase.id()),
        epoch(903),
    )
    .await?;
    let after_decision = observe(&repository, &key).await?;
    let resolved_state = after_decision
        .latest_step_state()
        .ok_or_else(|| Failure("the resolved attempt has no durable state".to_owned()))?;
    assert_eq!(
        (resolved_state.position, resolved_state.counts),
        (killed_state.position, killed_state.counts),
        "{}: resolving the crashed attempt must not rewrite what it durably committed",
        phase.id(),
    );
    assert_eq!(
        business_items(runtime_url, job_name).await?,
        durable_items,
        "{}: resolving the crashed attempt must not undo a durable business commit",
        phase.id(),
    );

    let resume = Instant::now();
    let restarted = restart_and_resume(
        &repository,
        &manager,
        &key,
        job_name,
        killed_execution.id(),
        phase.durable_chunks(),
    )
    .await?;
    let resume_in = resume.elapsed();

    let shape =
        complete_and_shape(&job, restarted.scope, restarted.job_version, epoch(904)).await?;
    assert_eq!(
        shape,
        *canonical,
        "{}: a killed and restarted run must reach the same durable observation as an \
         uninterrupted one",
        phase.id(),
    );

    let terminal = observe(&repository, &key).await?;
    assert_eq!(
        terminal
            .executions
            .iter()
            .map(|execution| execution.metadata().status())
            .collect::<Vec<_>>(),
        vec![BatchStatus::Failed, BatchStatus::Completed],
        "{}: the crashed attempt stays resolved and visible, and only the restart completes",
        phase.id(),
    );
    let summary = terminal.summary();
    repository.close().await?;

    Ok(json!({
        "phase": phase.id(),
        "protocol": "chunk commit",
        "expected": phase.expectation(),
        "fixture": "postgres",
        "termination": {
            "signal": signal.map(|signal| if signal == SIGKILL { "SIGKILL" } else { "OTHER" }),
            "signal_number": signal,
            "exit_code": exit_code,
        },
        "observed": {
            "durable_chunks": phase.durable_chunks(),
            "checkpoint_position": killed_state.position,
            "counts": counts(killed_state.counts),
            "business_items": durable_items,
            "crashed_attempt_status": killed_execution.metadata().status().as_str(),
            "crashed_step_status": killed_step.metadata().status().as_str(),
        },
        "discovery": {
            "proposal_digest": proposal.digest_hex(),
            "observed_status": proposal.evidence().status().as_str(),
            "observed_version": proposal.observed_version().get(),
            "unknown_commit": proposal.evidence().unknown_commit(),
        },
        "decision": decision,
        "restart": {
            "inherited_position": restarted.inherited_position,
            "resumed_chunks": restarted.resumed_chunks,
        },
        "terminal": summary,
        "durations_ms": {
            "reach_phase": millis(reached_in),
            "discovery": millis(discovery_in),
            "resume": millis(resume_in),
        },
        "violations": Vec::<String>::new(),
        // Derived from what was observed rather than asserted as a constant.
        // Every one of these is checked above; recording them again is what
        // gives the runner something of its own to disagree with.
        "passed": signal == Some(SIGKILL)
            && exit_code.is_none()
            && killed_state.position == Some(durable_position)
            && durable_items == expected_items(phase.durable_chunks())
            && shape == *canonical,
    }))
}

/// Renders one measured duration for the retained report.
fn millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// The business rows a given number of durable chunks must have written.
fn expected_items(chunks: u64) -> Vec<i64> {
    CHUNKS
        .iter()
        .take(usize::try_from(chunks).unwrap_or(CHUNKS.len()))
        .flatten()
        .copied()
        .collect()
}

/// What one restart observed and did.
struct Restarted {
    /// The chunk scope of the restarted attempt.
    scope: ChunkTransactionContext,
    /// The optimistic version of the restarted attempt after it started.
    job_version: ExecutionVersion,
    /// The committed position the restart inherited.
    inherited_position: u64,
    /// How many chunks the restart still had to commit.
    resumed_chunks: usize,
}

/// Starts a new attempt, resumes from the inherited checkpoint, and reports it.
async fn restart_and_resume(
    repository: &PostgresJobRepository,
    manager: &PostgresChunkTransactionManager,
    key: &JobInstanceKey,
    job_name: &str,
    crashed: JobExecutionId,
    durable_chunks: u64,
) -> Result<Restarted, Box<dyn Error>> {
    let (execution, step) = create_attempt(repository, key, job_name, CHUNK_SIZE).await?;
    assert_ne!(
        execution.id(),
        crashed,
        "a restart is a new attempt rather than a reopened one",
    );
    let (execution, _) = start_attempt(repository, &execution, &step, epoch(903)).await?;

    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    let inherited = manager.load_committed_state(scope).await?;
    let inherited_position = checkpoint_position(inherited.checkpoint())?;
    assert_eq!(
        inherited_position,
        u64::from(CHUNK_SIZE) * durable_chunks,
        "a restart inherits exactly what the crashed attempt durably committed",
    );

    let committed = usize::try_from(durable_chunks).unwrap_or(CHUNKS.len());
    for chunk in CHUNKS.iter().skip(committed) {
        commit_chunk(manager, scope, job_name, chunk).await?;
    }

    Ok(Restarted {
        scope,
        job_version: execution.version(),
        inherited_position,
        resumed_chunks: CHUNKS.len() - committed,
    })
}

/// The block one phase needs in order to be reachable.
enum Block {
    /// Nothing is held; the child parks at the phase by itself.
    Parked,
    /// A row lock on the step execution row the progress write updates.
    StepRow(PgConnection),
    /// A session advisory lock the deferred business trigger waits on.
    CommitGate(PgConnection),
}

impl Block {
    /// Puts the phase's block in place before the child is released.
    async fn arrange(phase: Phase, url: &str, step_id: u64) -> Result<Self, Box<dyn Error>> {
        match phase {
            Phase::ProgressBlocked => {
                let mut connection = PgConnection::connect(url).await?;
                sqlx::query("BEGIN").execute(&mut connection).await?;
                sqlx::query(
                    "SELECT id FROM oxide_batch.ob_step_execution WHERE id = $1 FOR UPDATE",
                )
                .bind(i64::try_from(step_id)?)
                .fetch_optional(&mut connection)
                .await?;
                Ok(Self::StepRow(connection))
            }
            Phase::CommitInFlight => {
                let mut connection = PgConnection::connect(url).await?;
                sqlx::query(
                    "CREATE OR REPLACE FUNCTION oxide_batch_business.m5_commit_gate() \
                     RETURNS trigger LANGUAGE plpgsql AS $gate$ BEGIN \
                     PERFORM pg_advisory_xact_lock(11131137); RETURN NULL; END; $gate$",
                )
                .execute(&mut connection)
                .await?;
                sqlx::query(
                    "CREATE CONSTRAINT TRIGGER m5_commit_gate \
                     AFTER INSERT ON oxide_batch_business.m5_crash_restore_output \
                     DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
                     EXECUTE FUNCTION oxide_batch_business.m5_commit_gate()",
                )
                .execute(&mut connection)
                .await?;
                sqlx::query("SELECT pg_advisory_lock($1)")
                    .bind(ADVISORY_KEY)
                    .execute(&mut connection)
                    .await?;
                Ok(Self::CommitGate(connection))
            }
            _ => Ok(Self::Parked),
        }
    }

    /// Releases the block and waits for whatever the release lets happen.
    async fn release(
        &mut self,
        manager: &PostgresChunkTransactionManager,
        scope: ChunkTransactionContext,
        expected_position: u64,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Parked => Ok(()),
            Self::StepRow(connection) => {
                sqlx::query("ROLLBACK").execute(&mut *connection).await?;
                Ok(())
            }
            Self::CommitGate(connection) => {
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(ADVISORY_KEY)
                    .execute(&mut *connection)
                    .await?;
                wait_for_position(manager, scope, expected_position).await?;
                for statement in DROP_COMMIT_GATE {
                    sqlx::query(statement).execute(&mut *connection).await?;
                }
                Ok(())
            }
        }
    }
}

/// Removes the deferred commit gate the in-flight phase installs.
const DROP_COMMIT_GATE: [&str; 2] = [
    "DROP TRIGGER IF EXISTS m5_commit_gate ON oxide_batch_business.m5_crash_restore_output",
    "DROP FUNCTION IF EXISTS oxide_batch_business.m5_commit_gate()",
];

/// Removes the commit gate after a phase failed part way through.
async fn drop_commit_gate(url: &str) -> Result<(), Box<dyn Error>> {
    let mut connection = PgConnection::connect(url).await?;
    for statement in DROP_COMMIT_GATE {
        sqlx::query(statement).execute(&mut connection).await?;
    }
    connection.close().await?;
    Ok(())
}

/// Waits for the server to finish a commit the killed process never saw.
async fn wait_for_position(
    manager: &PostgresChunkTransactionManager,
    scope: ChunkTransactionContext,
    expected: u64,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    while started.elapsed() < HANDSHAKE_BOUND {
        let state = manager.load_committed_state(scope).await?;
        if checkpoint_position(state.checkpoint())? == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(Box::new(Failure(format!(
        "the in-flight commit never reached position {expected}"
    ))))
}

/// Starts one child process at a commit phase.
fn spawn_worker(phase: Phase, handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("commit_phase_kill_worker_process")
        .arg("--nocapture")
        .env(PHASE_ENV, phase.id())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

#[test]
fn commit_phase_kill_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(PHASE_ENV) else {
        return Ok(());
    };
    let phase = Phase::parse(&value)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or_else(|| Failure("the worker has no database URL".to_owned()))?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(phase, url, handshake))?;
    Err(Box::new(Failure(
        "the campaign child returned instead of being killed".to_owned(),
    )))
}

/// Runs one child up to its commit phase and parks or blocks there.
async fn run_worker(phase: Phase, url: String, handshake: PathBuf) -> Result<(), Box<dyn Error>> {
    let job_name = phase.job_name();
    let park = (phase == Phase::StateProvided).then(|| ProviderPark {
        ordinal: 2,
        reached: handshake.join("reached"),
    });
    let repository =
        PostgresJobRepository::connect(config(url)?, Arc::new(FixedClock(epoch(900)))).await?;
    let key = instance_key(job_name)?;
    let (execution, step) = create_attempt(&repository, &key, job_name, CHUNK_SIZE).await?;
    let (execution, _) = start_attempt(&repository, &execution, &step, epoch(901)).await?;

    let manager = transaction_manager(&repository, park);
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    commit_chunk(&manager, scope, job_name, &CHUNKS[0]).await?;

    announce(&handshake.join("ready"));
    wait_for_file(&handshake.join("go"), HANDSHAKE_BOUND).await?;

    let mut transaction = manager.begin_for(scope).await?;
    write_items(&mut *transaction, job_name, &CHUNKS[1]).await?;
    if phase == Phase::BusinessWritten {
        announce(&handshake.join("reached"));
        park_until_killed();
    }

    let count = ChunkCount::new(u64::from(CHUNK_SIZE));
    transaction
        .commit(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
        )
        .await?;

    announce(&handshake.join("reached"));
    park_until_killed()
}
