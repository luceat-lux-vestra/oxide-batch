//! Compile and construction smoke test for the Gate B shared foundation
//! (`support/gate_b.rs`). The eight B-01..B-08 scenarios are a follow-up
//! task; this only proves the shared harness itself is sound: both
//! representations build the same logical pipeline, and a real crash/kill
//! against `PostgreSQL` produces a comparable [`gate_b::GateBObservation`].

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;

use gate_b::{
    GateBParams, HANDSHAKE_BOUND, HANDSHAKE_ENV, ParkAt, ParkingWriter, REPRESENTATION_ENV,
    Representation, business_rows, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_foundation_smoke";

#[tokio::test]
async fn typed_and_boxed_representations_produce_identical_durable_observations()
-> Result<(), Box<dyn Error>> {
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

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&runtime_url)
            .await?;
        let clock = gate_b::FixedClock(gate_b::epoch(1_000));
        let repository =
            PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;
        let transactions = Arc::new(transaction_manager(&repository));
        let params = GateBParams {
            job_name,
            items: 6,
            chunk_size: 6,
            pool,
            transactions,
        };

        let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        match representation {
            Representation::Typed => {
                let mut job = gate_b::typed_chunk_job(&params)?;
                launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await?;
            }
            Representation::Boxed => {
                let mut job = gate_b::boxed_chunk_job(&params)?;
                launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await?;
            }
        }

        let observation = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(observation.business_rows, vec![0, 1, 2, 3, 4, 5]);
        repository.close().await?;
        observations.push(observation);
    }

    assert_eq!(
        observations[0], observations[1],
        "typed and boxed representations must reach the same durable observation"
    );
    Ok(())
}

/// Proves the `ParkAt`/`ParkingWriter` hook actually parks a worker where it
/// says it will, and that a real `SIGKILL` from a fresh process is what ends
/// it -- the primitive B-05/B-06/B-07 build on, not those scenarios
/// themselves.
#[test]
fn parking_writer_reaches_its_announced_point_and_is_killed() -> Result<(), Box<dyn Error>> {
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
        .block_on(async move {
            prepare_fixture(&migrator_url, "gate_b_park_smoke").await?;

            let handshake = std::env::temp_dir().join("gate-b-park-smoke-handshake");
            std::fs::create_dir_all(&handshake)?;
            let _ = std::fs::remove_file(handshake.join("reached"));

            let mut child = gate_b::spawn_worker_with_representation(
                "park_smoke_worker_process",
                Representation::Typed,
                &handshake,
            )?;
            gate_b::crash_restore::wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND)
                .await?;
            child.kill()?;
            let status = child.wait()?;
            assert_eq!(
                status.signal(),
                Some(9),
                "expected the worker to be SIGKILLed rather than exit on its own"
            );

            let rows = business_rows(&runtime_url, "gate_b_park_smoke").await?;
            assert_eq!(rows, vec![0, 1, 2]);
            Ok::<(), Box<dyn Error>>(())
        })
}

/// The worker body `park_smoke_worker_process` spawns: commits one chunk of
/// three items, then parks right after the writer call for that chunk
/// returns -- exactly [`ParkAt::BeforeCommit`] at ordinal 1.
#[test]
fn park_smoke_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(_representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("the worker has no database URL")?;

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
                gate_b::BusinessWriter::new(pool.clone(), "gate_b_park_smoke"),
                ParkAt::BeforeCommit { ordinal: 1 },
                handshake.join("reached"),
            );
            let params = GateBParams {
                job_name: "gate_b_park_smoke",
                items: 3,
                chunk_size: 3,
                pool,
                transactions,
            };
            let mut job = gate_b::typed_chunk_job_with_writer(&params, writer)?;
            let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
            let (_source, stop) = StopSource::new();
            let launcher = JobLauncher::new(&repository, &clock, &ids);
            let _ = launcher
                .launch_chunk(&mut job, &JobParameters::new(), &stop)
                .await;
            Err::<(), Box<dyn Error>>("worker returned instead of being killed".into())
        })
}
