//! Gate B-06 (#153 §3): `process_kill_around_commit_acknowledgement_is_identical`.
//!
//! Required equivalence: killing a real, separate worker process in the
//! window between a chunk's commit boundary and its acknowledgement, then
//! restarting, must select the same durable checkpoint and final business
//! state on both paths.
//!
//! Distinguished from B-05 (which kills *before* the commit is issued): here
//! the worker's second chunk actually commits durably -- its business rows
//! and checkpoint are on disk -- and the kill lands inside
//! [`gate_b::ParkingCompletion`]'s `after_commit` hook, after the commit
//! succeeded but before the framework's own post-commit acknowledgement
//! bookkeeping returns. The restart must recognize the chunk already
//! committed (from the durable checkpoint) rather than re-applying it.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;

use gate_b::{
    GateBParams, HANDSHAKE_BOUND, HANDSHAKE_ENV, ParkAt, ParkingCompletion, REPRESENTATION_ENV,
    Representation, SequenceReader, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_06_kill_around_acknowledgement";
const ITEMS: i64 = 9;
const CHUNK_SIZE: u32 = 3;
/// Parks after the second chunk's commit is acknowledged.
const PARK_ORDINAL: usize = 2;
/// Two chunks' worth of items are durable once the acknowledgement hook runs.
const EXPECTED_RESUME_POSITION: i64 = 6;

#[tokio::test]
async fn process_kill_around_commit_acknowledgement_is_identical() -> Result<(), Box<dyn Error>> {
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

        let handshake = std::env::temp_dir().join(format!("gate-b-06-{}", representation.id()));
        std::fs::create_dir_all(&handshake)?;
        let _ = std::fs::remove_file(handshake.join("reached"));

        let mut child = gate_b::spawn_worker_with_representation(
            "kill_around_acknowledgement_worker_process",
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
            "{}: the second chunk's commit must be durable before the kill lands, since \
             the kill is inside the post-commit acknowledgement hook, not the commit itself",
            representation.id(),
        );
        assert_eq!(
            killed.business_rows,
            vec![0, 1, 2, 3, 4, 5],
            "{}: both committed chunks' rows must already be durable",
            representation.id(),
        );

        // The killed execution is still Started; recovery must explicitly
        // mark it Failed before a new attempt is allowed (see
        // gate_b::mark_crashed_execution_failed's doc comment).
        gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

        // Restart: a fresh attempt, resuming from the durable checkpoint.
        // A restart that re-derived "what committed" from the killed
        // process's own belief rather than durable state would re-apply the
        // second chunk; this asserts it does not.
        let transactions = Arc::new(transaction_manager(&repository));
        let params = GateBParams {
            job_name,
            items: ITEMS,
            chunk_size: CHUNK_SIZE,
            pool,
            transactions,
        };
        let resuming = SequenceReader::resuming_from(EXPECTED_RESUME_POSITION, ITEMS);
        let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        match representation {
            Representation::Typed => {
                let mut job = gate_b::typed_chunk_job_with_reader_and_writer(
                    &params,
                    resuming,
                    gate_b::BusinessWriter::new(job_name),
                )?;
                launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await?;
            }
            Representation::Boxed => {
                let mut job = gate_b::boxed_chunk_job_with_reader_and_writer(
                    &params,
                    resuming,
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
             and restarted attempts, with no re-application of the already-acknowledged chunk",
            representation.id(),
        );
        assert_eq!(
            restarted.checkpoint_position,
            Some(ITEMS.unsigned_abs()),
            "{}: the checkpoint must reflect every item read across both attempts",
            representation.id(),
        );
        repository.close().await?;
        observations.push(restarted);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-06: typed and boxed representations must select the same durable checkpoint \
         and final business state after a kill around commit acknowledgement and a restart"
    );
    Ok(())
}

/// The worker body `kill_around_acknowledgement_worker_process` spawns:
/// commits the first two chunks, parking inside the *second* chunk's
/// post-commit acknowledgement hook -- exactly [`ParkAt::AfterCommit`] at
/// [`PARK_ORDINAL`], after that chunk's `COMMIT` has already succeeded.
#[test]
fn kill_around_acknowledgement_worker_process() -> Result<(), Box<dyn Error>> {
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

            let completion = Arc::new(ParkingCompletion::new(
                ParkAt::AfterCommit {
                    ordinal: PARK_ORDINAL,
                },
                handshake.join("reached"),
            ));
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
                    let mut job = gate_b::typed_chunk_job_with_writer_and_completion(
                        &params,
                        gate_b::BusinessWriter::new(job_name),
                        completion,
                    )?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
                Representation::Boxed => {
                    let mut job = gate_b::boxed_chunk_job_with_writer_and_completion(
                        &params,
                        gate_b::BusinessWriter::new(job_name),
                        completion,
                    )?;
                    let _ = launcher
                        .launch_chunk(&mut job, &JobParameters::new(), &stop)
                        .await;
                }
            }
            Err::<(), Box<dyn Error>>("worker returned instead of being killed".into())
        })
}
