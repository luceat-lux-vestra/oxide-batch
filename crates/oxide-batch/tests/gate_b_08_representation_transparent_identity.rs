//! Gate B-08 (#153 §3): `representation_does_not_change_definition_or_restart_identity`.
//!
//! Required equivalence: a typed ↔ `Boxed*` representation change must not
//! change the definition fingerprint, restart compatibility, restart
//! selection, or logical component identity.
//!
//! Two parts:
//!
//! 1. In-process, no database: a typed and a `Boxed*` instantiation of the
//!    identical job (same revisions, same restart contract) produce a
//!    byte-identical `definition_identity().manifest_digest()` -- restated
//!    here for `ChunkJob::new`'s own hand-assembled construction path (the
//!    same one `support/gate_b.rs::assemble` uses) so B-08's evidence does
//!    not rest only on `chunk_builder.rs`'s
//!    `typed_and_boxed_pipelines_share_one_fingerprint`, which proves the
//!    same claim for `ChunkPipelineBuilder` output (#152, config-only).
//! 2. Against a real database: a job started under one representation,
//!    killed partway through, and *restarted under the other representation*
//!    -- same job name, same definition revision, same component revisions
//!    -- is accepted as a restart of the same job instance (not a new one,
//!    not rejected), resumes from the correct durable checkpoint, and
//!    reaches the same final durable state regardless of which direction the
//!    swap runs. `JobInstanceKey` is job name and parameters only, with no
//!    dependency on `R`/`P`/`W`, so nothing in the repository lookup path
//!    *could* distinguish representations -- this proves that structural fact
//!    holds in practice, both directions, against a real crash and restart.

#![cfg(feature = "postgres")]

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;
#[path = "support/gate_b.rs"]
mod gate_b;

use std::error::Error;
use std::sync::Arc;

use chunk_fixture::{Double, NoopCompletion, NoopTransactions, Sink, Source};
use gate_b::{
    GateBParams, HANDSHAKE_BOUND, HANDSHAKE_ENV, ParkAt, ParkingWriter, REPRESENTATION_ENV,
    Representation, config, migrator_url, prepare_fixture, runtime_url, snapshot,
    transaction_manager,
};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkComponentRevisions, ChunkDeliveryMode, ChunkJob,
    ChunkRestartContract, ChunkSize, ChunkStep, ComponentRevision, DefinitionRevision, JobLauncher,
    JobName, JobParameters, PostgresJobRepository, SequentialIdGenerator, StateSchemaId,
    StateSchemaVersion, StepName, StopSource,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "gate_b_08_representation_transparent_identity";
const ITEMS: i64 = 6;
const CHUNK_SIZE: u32 = 3;
/// Parks inside the second (last) chunk's writer call, after the first has
/// already committed.
const PARK_ORDINAL: usize = 2;
/// One chunk (items `0..3`) is durable when the kill lands.
const RESUME_POSITION: i64 = 3;

/// Part 1: the definition fingerprint does not depend on representation,
/// with no database involved -- the same claim
/// `chunk_builder.rs::typed_and_boxed_pipelines_share_one_fingerprint`
/// already proves for `ChunkPipelineBuilder` output, restated here for
/// `ChunkJob::new`'s hand-assembled construction path so B-08's evidence
/// does not rest only on the builder's own indirection over that path.
#[test]
fn definition_fingerprint_is_representation_independent() -> Result<(), Box<dyn Error>> {
    let reader_revision = ComponentRevision::new("reader-v1")?;
    let processor_revision = ComponentRevision::new("processor-v1")?;
    let writer_revision = ComponentRevision::new("writer-v1")?;
    let checkpoint_revision = ComponentRevision::new("checkpoint-v1")?;
    let contract = ChunkRestartContract::new(
        StateSchemaId::new("test.checkpoint")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("test.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtLeastOnce,
    );
    let revisions = ChunkComponentRevisions::new(
        reader_revision,
        processor_revision,
        writer_revision,
        checkpoint_revision,
        contract,
    );

    let typed_step = ChunkStep::new(
        StepName::new("double")?,
        ChunkSize::new(10)?,
        Source::range(3),
        Double,
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let typed = ChunkJob::new(
        JobName::new(JOB)?,
        typed_step,
        DefinitionRevision::new("v1")?,
        &revisions,
    )?;

    let boxed_step = ChunkStep::new(
        StepName::new("double")?,
        ChunkSize::new(10)?,
        BoxedReader::new(Source::range(3)),
        BoxedProcessor::new(Double),
        BoxedWriter::new(Sink(Arc::new(std::sync::Mutex::new(Vec::new())))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let boxed = ChunkJob::new(
        JobName::new(JOB)?,
        boxed_step,
        DefinitionRevision::new("v1")?,
        &revisions,
    )?;

    assert_eq!(
        typed.definition_identity().manifest_digest(),
        boxed.definition_identity().manifest_digest(),
        "a typed and a Boxed instantiation of the identical job must share one definition \
         fingerprint"
    );
    Ok(())
}

/// Part 2: a real crash-and-restart, with the representation swapped between
/// the killed attempt and the restart, run in both directions.
#[tokio::test]
async fn representation_does_not_change_definition_or_restart_identity()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    let typed_then_boxed = cross_representation_restart(
        &runtime_url,
        &migrator_url,
        "gate_b_08_typed_then_boxed",
        Representation::Typed,
        Representation::Boxed,
    )
    .await?;
    let boxed_then_typed = cross_representation_restart(
        &runtime_url,
        &migrator_url,
        "gate_b_08_boxed_then_typed",
        Representation::Boxed,
        Representation::Typed,
    )
    .await?;

    assert_eq!(
        typed_then_boxed, boxed_then_typed,
        "B-08: swapping which representation starts the job and which restarts it must not \
         change the final durable observation, in either direction"
    );
    Ok(())
}

/// Kills a worker running as `killed_as` partway through, then restarts the
/// same job instance as `restarted_as`. Returns the final durable
/// observation.
async fn cross_representation_restart(
    runtime_url: &str,
    migrator_url: &str,
    job_name: &'static str,
    killed_as: Representation,
    restarted_as: Representation,
) -> Result<gate_b::GateBObservation, Box<dyn Error>> {
    prepare_fixture(migrator_url, job_name).await?;

    let handshake = std::env::temp_dir().join(format!("gate-b-08-{job_name}"));
    std::fs::create_dir_all(&handshake)?;
    let _ = std::fs::remove_file(handshake.join("reached"));

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("cross_representation_worker_process")
        .arg("--nocapture")
        .env(REPRESENTATION_ENV, killed_as.id())
        .env(HANDSHAKE_ENV, &handshake)
        .env("OXIDEBATCH_GATE_B_08_JOB_NAME", job_name)
        .spawn()?;
    gate_b::crash_restore::wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;
    child.kill()?;
    child.wait()?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(runtime_url)
        .await?;
    let clock = gate_b::FixedClock(gate_b::epoch(1_000));
    let repository =
        PostgresJobRepository::connect(config(runtime_url.to_owned())?, Arc::new(clock)).await?;

    let killed = snapshot(runtime_url, &repository, job_name).await?;
    assert_eq!(
        killed.checkpoint_position,
        Some(RESUME_POSITION.unsigned_abs()),
        "{job_name}: exactly the first chunk must be durable when the kill lands, regardless \
         of the killed representation",
    );
    assert_eq!(
        killed.component_state.last().map(|state| state.position),
        Some(RESUME_POSITION.unsigned_abs()),
        "{job_name}: restart selection must include the durable ItemStream state",
    );
    gate_b::mark_crashed_execution_failed(&repository, job_name).await?;

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
    match restarted_as {
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

    let restarted = snapshot(runtime_url, &repository, job_name).await?;
    assert_eq!(
        restarted.business_rows,
        (0..ITEMS).collect::<Vec<_>>(),
        "{job_name}: every item must be durably written exactly once across the killed \
         ({killed_as:?}) and restarted ({restarted_as:?}) attempts",
    );
    assert_eq!(
        restarted.checkpoint_position,
        Some(ITEMS.unsigned_abs()),
        "{job_name}: the checkpoint must reflect every item read across both attempts",
    );
    assert_eq!(
        restarted.component_state.last().map(|state| state.position),
        Some(ITEMS.unsigned_abs()),
        "{job_name}: the swapped representation must inherit and persist the same stream state",
    );
    assert_eq!(
        restarted.component_state.len(),
        2,
        "{job_name}: both representation-swapped attempts must have durable stream state",
    );
    assert_eq!(
        restarted.lifecycle_trace.len(),
        2,
        "{job_name}: exactly two executions (the killed attempt and the restart) must belong \
         to one job instance -- a representation-keyed lookup would either reject the restart \
         or silently start a second, unrelated instance instead of resuming this one",
    );
    repository.close().await?;
    Ok(restarted)
}

/// The worker body `cross_representation_worker_process` spawns: commits the
/// first chunk (items `0..3`), then parks inside the second chunk's writer
/// call -- exactly [`ParkAt::BeforeCommit`] at [`PARK_ORDINAL`]. Its
/// representation is [`REPRESENTATION_ENV`]; its job name is
/// `OXIDEBATCH_GATE_B_08_JOB_NAME`, since B-08 runs two independent job
/// instances (one per swap direction) rather than one fixed name.
#[test]
fn cross_representation_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(_representation) = std::env::var(REPRESENTATION_ENV) else {
        return Ok(());
    };
    let representation = Representation::parse(&std::env::var(REPRESENTATION_ENV)?)?;
    let handshake = std::path::PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("the worker has no database URL")?;
    let job_name: &'static str =
        Box::leak(std::env::var("OXIDEBATCH_GATE_B_08_JOB_NAME")?.into_boxed_str());

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
                    ordinal: PARK_ORDINAL,
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
