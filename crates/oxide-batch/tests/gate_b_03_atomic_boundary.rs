//! Gate B-03 (#153 §3): `state_checkpoint_counter_share_one_atomic_boundary`.
//!
//! Required equivalence: business effects, checkpoint, component state,
//! counters, and optimistic version share one commit/rollback boundary in a
//! same-resource enlisted transaction, identically on both paths.
//!
//! The scenario forces exactly the failure that would reveal a non-atomic
//! boundary if one existed: the second chunk's writer succeeds -- its rows
//! are staged inside the open transaction -- and then the checkpoint
//! provider for that same chunk fails
//! ([`gate_b::transaction_manager_failing_at`]). If business rows, checkpoint,
//! and counters were not one atomic unit, this would leave the writer's rows
//! durably committed while the checkpoint stayed behind (or vice versa). The
//! scenario asserts the whole chunk rolls back together, and that the first
//! chunk (committed before the forced failure) is unaffected.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;

use gate_b::{
    GateBParams, Representation, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager_failing_at,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_03_atomic_boundary";
const ITEMS: i64 = 6;
const CHUNK_SIZE: u32 = 3;
/// Fails the second chunk's checkpoint provider call, after its writer has
/// already staged that chunk's rows inside the same open transaction.
const FAIL_AT_CHUNK: usize = 2;

#[tokio::test]
async fn state_checkpoint_counter_share_one_atomic_boundary() -> Result<(), Box<dyn Error>> {
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
        let transactions = Arc::new(transaction_manager_failing_at(&repository, FAIL_AT_CHUNK));
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
                let _ = launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await;
            }
            Representation::Boxed => {
                let mut job = gate_b::boxed_chunk_job(&params)?;
                let _ = launcher
                    .launch_chunk(&mut job, &JobParameters::new(), &stop)
                    .await;
            }
        }

        let observation = snapshot(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            observation.business_rows,
            vec![0, 1, 2],
            "{}: the first chunk's rows must survive and the second chunk's \
             rows -- staged by a writer that itself succeeded -- must not, \
             proving the writer and the failed checkpoint share one boundary",
            representation.id(),
        );
        assert_eq!(
            observation.checkpoint_position,
            Some(3),
            "{}: the checkpoint must reflect only the first, fully committed \
             chunk",
            representation.id(),
        );
        assert_eq!(
            observation.counts.committed,
            1,
            "{}: exactly one chunk committed",
            representation.id(),
        );
        assert!(
            observation.counts.rolled_back >= 1,
            "{}: the second chunk's forced provider failure must be recorded \
             as a rollback, not silently dropped",
            representation.id(),
        );
        repository.close().await?;
        observations.push(observation);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-03: typed and boxed representations must reach the same durable \
         observation when a chunk's checkpoint provider fails after its \
         writer succeeded"
    );
    Ok(())
}
