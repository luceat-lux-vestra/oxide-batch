//! Gate B-01 (#153 §3): `normal_enlisted_commit_is_representation_identical`.
//!
//! Required equivalence: same business statements, business rows, checkpoint,
//! component state, counters, repository writes, and normalized lifecycle
//! trace on typed and `Boxed*`, for a job with nothing unusual in it -- no
//! failure, no kill. This is the baseline every other Gate B scenario departs
//! from by exactly one variable.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;

use gate_b::{
    GateBParams, Representation, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_01_normal_commit";
const ITEMS: i64 = 6;
const CHUNK_SIZE: u32 = 3;

#[tokio::test]
async fn normal_enlisted_commit_is_representation_identical() -> Result<(), Box<dyn Error>> {
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
        assert_eq!(
            observation.business_rows,
            vec![0, 1, 2, 3, 4, 5],
            "{}: every item must be durably written, in read order",
            representation.id(),
        );
        assert_eq!(
            observation.checkpoint_position,
            Some(ITEMS.unsigned_abs()),
            "{}: the checkpoint must reflect all items read",
            representation.id(),
        );
        assert_eq!(
            observation.counts.committed,
            2,
            "{}: two chunks of three items each must both commit",
            representation.id(),
        );
        assert_eq!(
            observation.counts.rolled_back,
            0,
            "{}: a normal run rolls nothing back",
            representation.id(),
        );
        repository.close().await?;
        observations.push(observation);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-01: typed and boxed representations must reach the same durable \
         observation for a normal enlisted commit"
    );
    Ok(())
}
