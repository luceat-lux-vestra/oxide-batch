//! Gate B-02 (#153 §3): `writer_failure_before_commit_rolls_back_identically`.
//!
//! Required equivalence: a typed writer failure before commit produces
//! identical rollback of business writes, no checkpoint advancement, no
//! component-state advancement, and no committed-counter advancement on both
//! paths.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;

use gate_b::{
    FailingWriter, GateBParams, Representation, config, migrator_url, prepare_fixture, runtime_url,
    snapshot, transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_02_writer_failure_rollback";
const ITEMS: i64 = 3;
const CHUNK_SIZE: u32 = 3;

#[tokio::test]
async fn writer_failure_before_commit_rolls_back_identically() -> Result<(), Box<dyn Error>> {
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

        // The job's only chunk fails on its first (and only) writer call, so
        // the whole run has nothing that ever committed.
        let writer = FailingWriter::new(gate_b::BusinessWriter::new(job_name), 1);

        let ids = SequentialIdGenerator::new(std::num::NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        match representation {
            Representation::Typed => {
                let mut job = gate_b::typed_chunk_job_with_writer(&params, writer)?;
                // The failure is expected and observed durably; a launch
                // error here would only mean the launcher itself surfaces
                // the chunk failure as its own Err, which is an acceptable
                // outcome shape this scenario does not constrain -- what it
                // constrains is the durable state.
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

        let observation = snapshot(&runtime_url, &repository, job_name).await?;
        assert!(
            observation.business_rows.is_empty(),
            "{}: a writer failure before commit must leave no durable business rows, found {:?}",
            representation.id(),
            observation.business_rows,
        );
        assert_eq!(
            observation.checkpoint_position,
            None,
            "{}: a writer failure before commit must not advance the checkpoint",
            representation.id(),
        );
        assert_eq!(
            observation.counts.committed,
            0,
            "{}: a writer failure before commit must not advance the committed counter",
            representation.id(),
        );
        assert!(
            observation.component_state.is_empty(),
            "{}: a pre-commit writer failure must not persist component state",
            representation.id(),
        );
        assert_eq!(
            observation.optimistic_versions.len(),
            1,
            "{}: the failed execution's durable optimistic versions must still be observed",
            representation.id(),
        );
        repository.close().await?;
        observations.push(observation);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-02: typed and boxed representations must reach the same durable \
         observation after a writer failure before commit"
    );
    Ok(())
}
