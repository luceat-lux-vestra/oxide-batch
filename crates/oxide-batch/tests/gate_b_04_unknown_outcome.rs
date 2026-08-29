//! Gate B-04 (#153 §3): `unknown_commit_outcome_forces_recovery_not_inference`.
//!
//! Required equivalence: when a chunk's `COMMIT` is genuinely in flight on
//! the server when the client that issued it dies, both representations
//! must reach the same durable outcome, and that outcome must come from a
//! fresh connection reading actual durable state -- never from guessing
//! success or failure, and never from an automatic replay issued before that
//! state is known.
//!
//! [`gate_b::install_commit_gate`] arranges exactly this: a deferred
//! constraint trigger on `gate_b_output` blocks the worker's real `COMMIT`
//! on a session advisory lock the scenario itself holds. The worker is
//! killed while that `COMMIT` is genuinely in flight -- durable work whose
//! outcome the process can never learn, because it is dead. The scenario
//! then releases the lock, lets the orphaned backend finish the commit it
//! had already dispatched to the server, and only then reads the outcome --
//! through a fresh connection, never by asking the dead process. A final
//! restart attempt proves recovery does not duplicate work whose outcome it
//! correctly discovered rather than guessed.

#![cfg(feature = "postgres")]

#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use gate_b::{
    GateBParams, HANDSHAKE_BOUND, REPRESENTATION_ENV, Representation, SequenceReader, config,
    migrator_url, prepare_fixture, runtime_url, snapshot, transaction_manager,
};
use oxide_batch::{
    JobLauncher, JobParameters, PostgresJobRepository, SequentialIdGenerator, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_04_unknown_outcome";
const ITEMS: i64 = 3;
const CHUNK_SIZE: u32 = 3;

#[tokio::test]
async fn unknown_commit_outcome_forces_recovery_not_inference() -> Result<(), Box<dyn Error>> {
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

        // Arrange the gate before the worker exists, so its COMMIT is
        // guaranteed to block rather than racing the gate's installation.
        let gate = gate_b::install_commit_gate(&migrator_url).await?;

        let mut child = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("unknown_outcome_worker_process")
            .arg("--nocapture")
            .env(REPRESENTATION_ENV, representation.id())
            .spawn()?;

        gate_b::crash_restore::wait_for_blocked_statement(&migrator_url, "COMMIT", HANDSHAKE_BOUND)
            .await
            .map_err(|error| -> Box<dyn Error> {
                format!("{}: {error}", representation.id()).into()
            })?;

        // The commit is genuinely in flight on the server right now. Killing
        // the client cannot roll it back or reveal its outcome -- that is
        // exactly the condition this scenario proves both representations
        // handle identically.
        child.kill()?;
        child.wait()?;

        // Only now -- after the kill, with the outcome still genuinely
        // unknown to any live process -- is the gate released. If the
        // framework needed the dead worker to acknowledge its commit, this
        // would hang or the durable state would stay empty.
        gate_b::release_commit_gate(gate).await?;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&runtime_url)
            .await?;
        let clock = gate_b::FixedClock(gate_b::epoch(1_000));
        let repository =
            PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;

        // Discover the outcome through a fresh connection, bounded rather
        // than assumed instantaneous -- the server finishes the orphaned
        // commit asynchronously once the gate releases.
        let discovered = wait_for_durable_commit(&runtime_url, &repository, job_name).await?;
        assert_eq!(
            discovered.business_rows,
            vec![0, 1, 2],
            "{}: the in-flight commit's work must be durable once discovered, even though \
             the process that issued it never learned so",
            representation.id(),
        );
        assert_eq!(
            discovered.checkpoint_position,
            Some(ITEMS.unsigned_abs()),
            "{}: the checkpoint must reflect the completed commit",
            representation.id(),
        );

        // The killed execution is still Started -- the durable chunk commit
        // completed on the server, but the process that would have marked
        // the execution's own completion never survived to do so. Recovery
        // must explicitly mark it Failed before a new attempt is allowed
        // (see gate_b::mark_crashed_execution_failed's doc comment).
        gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

        // Restart: prove recovery does not duplicate the work it correctly
        // discovered. A restart that inferred "unknown means retry" instead
        // of checking durable state would re-insert these items.
        let transactions = Arc::new(transaction_manager(&repository));
        let params = GateBParams {
            job_name,
            items: ITEMS,
            chunk_size: CHUNK_SIZE,
            pool,
            transactions,
        };
        let resuming = SequenceReader::resuming_from(ITEMS, ITEMS);
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
            vec![0, 1, 2],
            "{}: a restart after the outcome was correctly discovered must not duplicate \
             the already-durable rows",
            representation.id(),
        );
        repository.close().await?;
        observations.push(restarted);
    }

    assert_eq!(
        observations[0], observations[1],
        "B-04: typed and boxed representations must reach the same durable outcome for a \
         commit whose result was genuinely unknown to the process that issued it"
    );
    Ok(())
}

/// Polls durable state until the orphaned commit's checkpoint appears, or
/// [`HANDSHAKE_BOUND`] elapses.
///
/// # Errors
///
/// Returns a description when the bound elapses without the commit landing.
async fn wait_for_durable_commit(
    runtime_url: &str,
    repository: &PostgresJobRepository,
    job_name: &str,
) -> Result<gate_b::GateBObservation, Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let observation = snapshot(runtime_url, repository, job_name).await?;
        if observation.checkpoint_position.is_some() {
            return Ok(observation);
        }
        if started.elapsed() >= HANDSHAKE_BOUND {
            return Err(format!(
                "{job_name}: the in-flight commit never landed within {HANDSHAKE_BOUND:?}"
            )
            .into());
        }
        tokio::time::sleep(gate_b::crash_restore::POLL_INTERVAL).await;
    }
}

/// The worker body `unknown_outcome_worker_process` spawns: an ordinary,
/// unparked run of one chunk. Nothing in the worker itself blocks -- the
/// block is [`gate_b::install_commit_gate`]'s deferred trigger on the server
/// side, armed by the scenario before this process starts.
#[test]
fn unknown_outcome_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let representation = Representation::parse(&representation)?;
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
            // This call blocks server-side inside the gated COMMIT and is
            // never expected to return: the scenario kills this process
            // while it is blocked.
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
            Ok::<(), Box<dyn Error>>(())
        })
}
