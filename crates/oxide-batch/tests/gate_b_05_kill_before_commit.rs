//! Gate B-05 (#153 §3): `process_kill_before_commit_restart_is_identical`.
//!
//! Required equivalence: killing a real, separate worker process before its
//! chunk's commit is issued, then restarting, must select the same durable
//! checkpoint, replay range, business-effect duplication (none), counters,
//! and component state on both paths.
//!
//! Three chunks of three items each. The worker commits the first chunk,
//! then parks inside the *second* chunk's writer call -- after the writer
//! has staged that chunk's rows in the open transaction, but before its
//! commit is issued -- and is killed there with a real `SIGKILL`. The
//! restart attempt restores the reader's durable `ItemStream` state before
//! item work begins.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
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

const JOB: &str = "gate_b_05_kill_before_commit";
const ITEMS: i64 = 9;
const CHUNK_SIZE: u32 = 3;
/// Parks inside the second chunk's writer call, before that chunk commits.
const PARK_ORDINAL: usize = 2;
/// One chunk's worth of items must already be durable when the kill lands.
const EXPECTED_RESUME_POSITION: i64 = 3;

#[tokio::test]
async fn process_kill_before_commit_restart_is_identical() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    let mut observations = Vec::new();
    for representation in Representation::ALL {
        let job_name = format!("{JOB}_{}", representation.id());
        let job_name: &'static str = Box::leak(job_name.into_boxed_str());
        prepare_fixture(&migrator_url, job_name).await?;

        let handshake = std::env::temp_dir().join(format!("gate-b-05-{}", representation.id()));
        std::fs::create_dir_all(&handshake)?;
        let _ = std::fs::remove_file(handshake.join("reached"));

        let mut child = gate_b::spawn_worker_with_representation(
            "kill_before_commit_worker_process",
            representation,
            &handshake,
        )?;
        gate_b::crash_restore::wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;
        child.kill()?;
        let status = child.wait()?;
        assert_eq!(
            status.signal(),
            Some(9),
            "{}: expected the worker to be SIGKILLed rather than exit on its own",
            representation.id(),
        );

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&runtime_url)
            .await?;
        let clock = gate_b::FixedClock(gate_b::epoch(1_000));
        let repository =
            PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;

        let killed = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            killed.checkpoint_position,
            Some(EXPECTED_RESUME_POSITION.unsigned_abs()),
            "{}: exactly the first chunk must be durable when the kill lands",
            representation.id(),
        );
        assert_eq!(
            killed.business_rows,
            vec![0, 1, 2],
            "{}: the parked (uncommitted) second chunk's rows must not be durable",
            representation.id(),
        );
        assert_eq!(
            killed.component_state.last().map(|state| state.position),
            Some(EXPECTED_RESUME_POSITION.unsigned_abs()),
            "{}: only the first committed chunk's component state may be durable",
            representation.id(),
        );

        // The killed execution is still Started; recovery must explicitly
        // mark it Failed before a new attempt is allowed (see
        // gate_b::mark_crashed_execution_failed's doc comment).
        gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

        // Restart: a fresh attempt, resuming from the durable checkpoint
        // rather than re-reading from the start.
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
            "{}: every item must be durably written exactly once across the crashed \
             and restarted attempts, with no duplication of the already-committed chunk",
            representation.id(),
        );
        assert_eq!(
            restarted.checkpoint_position,
            Some(ITEMS.unsigned_abs()),
            "{}: the checkpoint must reflect every item read across both attempts",
            representation.id(),
        );
        assert_eq!(
            restarted.component_state.last().map(|state| state.position),
            Some(ITEMS.unsigned_abs()),
            "{}: the restarted ItemStream must restore and persist the final position",
            representation.id(),
        );
        assert_eq!(
            restarted.component_state.len(),
            2,
            "{}: component state must be present for both the killed and restarted attempts",
            representation.id(),
        );
        repository.close().await?;
        observations.push(restarted);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-05: typed and boxed representations must select the same durable checkpoint, \
         replay range, and final business state after a kill before commit and a restart"
    );
    Ok(())
}

/// The worker body `kill_before_commit_worker_process` spawns: commits the
/// first chunk, then parks inside the second chunk's writer call -- exactly
/// [`ParkAt::BeforeCommit`] at [`PARK_ORDINAL`].
#[test]
fn kill_before_commit_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(_representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let representation = Representation::parse(&std::env::var(REPRESENTATION_ENV)?)?;
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("the worker has no database URL")?;
    let job_name = format!("{JOB}_{}", representation.id());

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
                gate_b::BusinessWriter::new(Box::leak(job_name.clone().into_boxed_str())),
                ParkAt::BeforeCommit {
                    ordinal: PARK_ORDINAL,
                },
                handshake.join("reached"),
            );
            let params = GateBParams {
                job_name: Box::leak(job_name.into_boxed_str()),
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
