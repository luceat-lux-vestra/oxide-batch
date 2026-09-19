//! Runs a durable, restartable CSV-reader chunk job against PostgreSQL.
//!
//! The CSV reader's parser position is persisted through its paired
//! `ItemStream`; PostgreSQL owns the authoritative chunk checkpoint,
//! execution context, counters, and component-state envelopes.
//!
//! Usage:
//! `cargo run -p oxide-batch --features postgres --example postgres_durable_csv -- input.csv`

#[cfg(feature = "postgres")]
mod postgres {
    use std::error::Error;
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use oxide_batch::item_components::{
        DelimitedDialect, DelimitedRecord, IdentityProcessor, NoopWriter, delimited_file_reader,
    };
    use oxide_batch::{
        Checkpoint, ChunkCommitReceipt, ChunkCounts, ChunkDeliveryMode, ChunkPipelineBuilder,
        ChunkRestartContract, ChunkSize, ComponentRevision, ComponentStreamIdentity,
        DefinitionRevision, ExecutionContext, ExecutionCounts, JobLauncher, JobName, JobParameters,
        PostgresChunkStateError, PostgresChunkStateProvider, PostgresChunkTransactionManager,
        PostgresConfig, PostgresJobRepository, PostgresMigrator, SequentialIdGenerator,
        StateLimits, StateSchemaId, StateSchemaVersion, StepName, StopSource, SystemClock,
    };

    const MIGRATOR_URL: &str = "MIGRATOR_DATABASE_URL";
    const RUNTIME_URL: &str = "RUNTIME_DATABASE_URL";

    fn config(variable: &str) -> Result<PostgresConfig, Box<dyn Error>> {
        Ok(PostgresConfig::new(std::env::var(variable)?)?)
    }

    fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
        Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;

            let checkpoint = Checkpoint::from_json(
                format!(
                    r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"example.csv-position","schema_version":1,"payload":{{"position":{position}}}}}"#
                )
                .as_bytes(),
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;

            let context = ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"example.csv-context","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;

            Ok(ChunkCommitReceipt::new(checkpoint, context))
        })
    }

    pub(super) async fn run() -> Result<(), Box<dyn Error>> {
        let input = std::env::args()
            .nth(1)
            .ok_or("usage: postgres_durable_csv <input.csv>")?;

        let migrator = config(MIGRATOR_URL)?;
        PostgresMigrator::migrate(&migrator).await?;

        let clock = Arc::new(SystemClock);
        let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
        let repository =
            PostgresJobRepository::connect(config(RUNTIME_URL)?, clock.clone()).await?;

        let namespace = ComponentStreamIdentity::new("example.csv-reader")?;
        let (reader, stream, stream_contract) = delimited_file_reader::<DelimitedRecord>(
            input,
            DelimitedDialect::csv(),
            namespace.clone(),
        )?;

        let restart = ChunkRestartContract::new(
            StateSchemaId::new("example.csv-position")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("example.csv-context")?,
            StateSchemaVersion::new(1)?,
            ChunkDeliveryMode::AtomicSameResource,
        );
        let transactions = Arc::new(PostgresChunkTransactionManager::new(
            repository.clone(),
            state_provider(),
        ));

        let mut job = ChunkPipelineBuilder::new(
            StepName::new("csv-import")?,
            ChunkSize::new(100)?,
            reader,
            ComponentRevision::new("csv-reader-v1")?,
            IdentityProcessor,
            ComponentRevision::new("identity-v1")?,
            NoopWriter,
            ComponentRevision::new("noop-writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            restart,
            transactions,
        )
        .with_stream(
            namespace,
            stream,
            stream_contract,
            ComponentRevision::new("csv-reader-stream-v1")?,
        )
        .build_chunk_job(
            JobName::new("csv-import-job")?,
            DefinitionRevision::new("v1")?,
        )?;

        let launcher = JobLauncher::new(&repository, clock.as_ref(), ids.as_ref());
        let (_stop_source, stop) = StopSource::new();
        let report = launcher
            .launch_chunk(&mut job, &JobParameters::new(), &stop)
            .await?;

        println!(
            "job execution {} ended as {}",
            report.launch().job_execution().id(),
            report.launch().job_execution().metadata().status()
        );
        repository.close().await?;
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    postgres::run().await
}

#[cfg(not(feature = "postgres"))]
fn main() {
    eprintln!("postgres_durable_csv requires --features postgres");
}
