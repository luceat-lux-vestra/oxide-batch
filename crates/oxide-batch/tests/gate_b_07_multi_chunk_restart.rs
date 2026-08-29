//! Gate B-07 (#153 §3): `multi_chunk_restart_selects_identically`.
//!
//! Required equivalence: after several chunks commit, a real process kill,
//! and a restart, the restart-selected checkpoint, resumed input position,
//! final business output, component state, and counters must be identical on
//! both paths.
//!
//! Distinguished from B-05 (one kill, one restart, verified only at final
//! completion): four chunks of three items each, and *two* separate
//! kill-and-restart cycles rather than one --
//!
//! 1. The initial worker commits chunks 1-2 (items `0..6`), parks inside
//!    chunk 3's writer call, and is killed there (checkpoint durable at `6`).
//! 2. Restart #1 resumes from `6`, commits the next chunk (items `6..9`,
//!    checkpoint durable at `9`), parks inside the following chunk's writer
//!    call, and is killed there too. This is the dimension B-05 does not
//!    exercise: a restart-selected checkpoint (`9`) that is itself the
//!    starting point of a *second* crash, not just the final state.
//! 3. Restart #2 resumes from `9` and runs the last chunk (items `9..12`) to
//!    completion.
//!
//! Every restart-selected checkpoint and the final durable observation are
//! compared between typed and `Boxed*`, not only the end state.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;

use gate_b::{
    GateBParams, HANDSHAKE_BOUND, HANDSHAKE_ENV, ParkAt, ParkingWriter, REPRESENTATION_ENV,
    Representation, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_07_multi_chunk_restart";
const ITEMS: i64 = 12;
const CHUNK_SIZE: u32 = 3;
/// The first worker parks inside the third chunk's writer call.
const FIRST_PARK_ORDINAL: usize = 3;
/// Two chunks (items `0..6`) are durable when the first kill lands.
const FIRST_RESUME_POSITION: i64 = 6;
/// Restart #1 parks inside its own second local chunk's writer call --
/// global chunk 4, items `9..12`.
const SECOND_PARK_ORDINAL: usize = 2;
/// Three chunks (items `0..9`) are durable when the second kill lands.
const SECOND_RESUME_POSITION: i64 = 9;

#[tokio::test]
async fn multi_chunk_restart_selects_identically() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    let mut final_observations = Vec::new();
    let mut mid_restart_checkpoints = Vec::new();
    for representation in Representation::ALL {
        let job_name = format!("{JOB}_{}", representation.id());
        let job_name: &'static str = Box::leak(job_name.into_boxed_str());
        prepare_fixture(&migrator_url, job_name).await?;

        // --- Kill #1: parked inside chunk 3's writer call. ---
        let handshake_1 = std::env::temp_dir().join(format!("gate-b-07-1-{}", representation.id()));
        std::fs::create_dir_all(&handshake_1)?;
        let _ = std::fs::remove_file(handshake_1.join("reached"));

        let mut child = gate_b::spawn_worker_with_representation(
            "multi_chunk_restart_first_worker_process",
            representation,
            &handshake_1,
        )?;
        gate_b::crash_restore::wait_for_file(&handshake_1.join("reached"), HANDSHAKE_BOUND).await?;
        child.kill()?;
        child.wait()?;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&runtime_url)
            .await?;
        let clock = gate_b::FixedClock(gate_b::epoch(1_000));
        let repository =
            PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;

        let after_first_kill = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            after_first_kill.checkpoint_position,
            Some(FIRST_RESUME_POSITION.unsigned_abs()),
            "{}: exactly the first two chunks must be durable when the first kill lands",
            representation.id(),
        );
        assert_eq!(
            after_first_kill
                .component_state
                .last()
                .map(|state| state.position),
            Some(FIRST_RESUME_POSITION.unsigned_abs()),
            "{}: the first restart selection must come from durable component state",
            representation.id(),
        );
        gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

        // --- Restart #1: resumes from 6, commits one chunk, parks inside the
        // next chunk's writer call, and is killed there too. ---
        let handshake_2 = std::env::temp_dir().join(format!("gate-b-07-2-{}", representation.id()));
        std::fs::create_dir_all(&handshake_2)?;
        let _ = std::fs::remove_file(handshake_2.join("reached"));

        let mut child = gate_b::spawn_worker_with_representation(
            "multi_chunk_restart_second_worker_process",
            representation,
            &handshake_2,
        )?;
        gate_b::crash_restore::wait_for_file(&handshake_2.join("reached"), HANDSHAKE_BOUND).await?;
        child.kill()?;
        child.wait()?;

        let after_second_kill = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            after_second_kill.checkpoint_position,
            Some(SECOND_RESUME_POSITION.unsigned_abs()),
            "{}: exactly three chunks must be durable when the second kill lands -- the \
             restart-selected checkpoint that is itself the start of a second crash",
            representation.id(),
        );
        assert_eq!(
            after_second_kill.business_rows,
            (0..SECOND_RESUME_POSITION).collect::<Vec<_>>(),
            "{}: no duplication across the first restart",
            representation.id(),
        );
        assert_eq!(
            after_second_kill
                .component_state
                .last()
                .map(|state| state.position),
            Some(SECOND_RESUME_POSITION.unsigned_abs()),
            "{}: the second restart selection must come from the first restart's state",
            representation.id(),
        );
        assert_eq!(
            after_second_kill.component_state.len(),
            2,
            "{}: both crashed attempts must retain their committed stream state",
            representation.id(),
        );
        mid_restart_checkpoints.push(after_second_kill.checkpoint_position);
        gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

        // --- Restart #2: resumes from 9, runs the last chunk to completion. ---
        let transactions = Arc::new(transaction_manager(&repository));
        let params = GateBParams {
            job_name,
            items: ITEMS,
            chunk_size: CHUNK_SIZE,
            pool,
            transactions,
        };
        let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        match representation {
            Representation::Typed => {
                let mut job = gate_b::typed_chunk_job_with_reader_and_writer(
                    &params,
                    gate_b::BusinessWriter::new(job_name),
                )?;
                launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await?;
            }
            Representation::Boxed => {
                let mut job = gate_b::boxed_chunk_job_with_reader_and_writer(
                    &params,
                    gate_b::BusinessWriter::new(job_name),
                )?;
                launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await?;
            }
        }

        let restarted = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            restarted.business_rows,
            (0..ITEMS).collect::<Vec<_>>(),
            "{}: every item must be durably written exactly once across all three attempts",
            representation.id(),
        );
        assert_eq!(
            restarted.checkpoint_position,
            Some(ITEMS.unsigned_abs()),
            "{}: the checkpoint must reflect every item read across all three attempts",
            representation.id(),
        );
        assert_eq!(
            restarted.component_state.last().map(|state| state.position),
            Some(ITEMS.unsigned_abs()),
            "{}: the final restart must persist the final ItemStream position",
            representation.id(),
        );
        assert_eq!(
            restarted.component_state.len(),
            3,
            "{}: each of the three attempts must have a durable stream-state row",
            representation.id(),
        );
        repository.close().await?;
        final_observations.push(restarted);
    }

    assert_eq!(
        mid_restart_checkpoints[0], mid_restart_checkpoints[1],
        "B-07: typed and boxed representations must select the same restart checkpoint \
         after the first crash-and-restart cycle, before the second crash even happens"
    );
    assert_eq!(
        final_observations[0], final_observations[1],
        "B-07: typed and boxed representations must reach the same final durable \
         observation after two crash-and-restart cycles"
    );
    Ok(())
}

/// The worker body `multi_chunk_restart_first_worker_process` spawns: commits
/// chunks 1-2 (items `0..6`), then parks inside chunk 3's writer call --
/// exactly [`ParkAt::BeforeCommit`] at [`FIRST_PARK_ORDINAL`].
#[test]
fn multi_chunk_restart_first_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(_representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let representation = Representation::parse(&std::env::var(REPRESENTATION_ENV)?)?;
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("the worker has no database URL")?;
    let job_name = format!("{JOB}_{}", representation.id());
    let job_name: &'static str = Box::leak(job_name.into_boxed_str());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&runtime_url)
                .await?;
            let clock = gate_b::FixedClock(gate_b::epoch(1_000));
            let repository =
                PostgresJobRepository::connect(config(runtime_url)?, Arc::new(clock)).await?;
            let transactions = Arc::new(transaction_manager(&repository));

            let writer = ParkingWriter::new(
                gate_b::BusinessWriter::new(job_name),
                ParkAt::BeforeCommit {
                    ordinal: FIRST_PARK_ORDINAL,
                },
                handshake.join("reached"),
            );
            let params = GateBParams {
                job_name,
                items: ITEMS,
                chunk_size: CHUNK_SIZE,
                pool,
                transactions,
            };
            let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
            let (_source, stop) = StopSource::new();
            let launcher = JobLauncher::new(&repository, &clock, &ids);
            match representation {
                Representation::Typed => {
                    let mut job = gate_b::typed_chunk_job_with_writer(&params, writer)?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
                Representation::Boxed => {
                    let mut job = gate_b::boxed_chunk_job_with_writer(&params, writer)?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
            }
            Err::<(), Box<dyn Error>>("worker returned instead of being killed".into())
        })
}

/// The worker body `multi_chunk_restart_second_worker_process` spawns:
/// resumes from [`FIRST_RESUME_POSITION`], commits its own first local chunk
/// (global chunk 3, items `6..9`), then parks inside its own second local
/// chunk's writer call -- global chunk 4, exactly [`ParkAt::BeforeCommit`] at
/// [`SECOND_PARK_ORDINAL`].
#[test]
fn multi_chunk_restart_second_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(_representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let representation = Representation::parse(&std::env::var(REPRESENTATION_ENV)?)?;
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("the worker has no database URL")?;
    let job_name = format!("{JOB}_{}", representation.id());
    let job_name: &'static str = Box::leak(job_name.into_boxed_str());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&runtime_url)
                .await?;
            let clock = gate_b::FixedClock(gate_b::epoch(1_000));
            let repository =
                PostgresJobRepository::connect(config(runtime_url)?, Arc::new(clock)).await?;
            let transactions = Arc::new(transaction_manager(&repository));

            let writer = ParkingWriter::new(
                gate_b::BusinessWriter::new(job_name),
                ParkAt::BeforeCommit {
                    ordinal: SECOND_PARK_ORDINAL,
                },
                handshake.join("reached"),
            );
            let params = GateBParams {
                job_name,
                items: ITEMS,
                chunk_size: CHUNK_SIZE,
                pool,
                transactions,
            };
            let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
            let (_source, stop) = StopSource::new();
            let launcher = JobLauncher::new(&repository, &clock, &ids);
            match representation {
                Representation::Typed => {
                    let mut job = gate_b::typed_chunk_job_with_reader_and_writer(&params, writer)?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
                Representation::Boxed => {
                    let mut job = gate_b::boxed_chunk_job_with_reader_and_writer(&params, writer)?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
            }
            Err::<(), Box<dyn Error>>("worker returned instead of being killed".into())
        })
}
