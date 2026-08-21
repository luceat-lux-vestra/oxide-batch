//! Runs an attempt, injects a real pre-commit failure after some chunks
//! commit, then restarts through the real production launch path and shows
//! the second attempt resumes from the last committed position, not the
//! rolled-back candidate. Requires `OXIDEBATCH_POSTGRES_TEST_URL` and the
//! `postgres` feature.

use std::sync::Arc;
use std::time::SystemTime;

use oxide_batch::{
    Checkpoint, ChunkCommitReceipt, ChunkDeliveryMode, ChunkJob, ChunkSize, ChunkStep,
    ComponentRevision, ComponentStreamIdentity, DefinitionRevision, ExecutionContext,
    ExecutionCounts, ItemProcessor, ItemWriter, JobName, JobParameters, PostgresChunkStateError,
    ProcessContext, ProcessOutcome, ProcessorError, StateLimits, StepName, WriteContext,
    WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{InjectedTransactions, InjectionId, InjectionLog, PreCommitAction};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::restart::range_reader;
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};

struct Identity;

impl ItemProcessor<u64, u64> for Identity {
    async fn process(
        &self,
        item: &u64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<u64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

struct Sink;

impl ItemWriter<u64> for Sink {
    async fn write(
        &self,
        items: &[u64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        println!("wrote {items:?}");
        Ok(WriteOutcome::Written)
    }
}

fn state_provider() -> Arc<dyn oxide_batch::PostgresChunkStateProvider> {
    Arc::new(
        |committed: ExecutionCounts, chunk: oxide_batch::ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint_bytes = format!(
                r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.restart-example","schema_version":1,"payload":{{"position":{position}}}}}"#
            );
            let checkpoint =
                Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
                    .map_err(|_| PostgresChunkStateError::new())?;
            let context = ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.restart-example","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("OXIDEBATCH_POSTGRES_TEST_URL") else {
        println!("set OXIDEBATCH_POSTGRES_TEST_URL to run this example against PostgreSQL");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!("oxide_batch_test_restart_example_{nonce}"))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.range-example")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(namespace.clone(), ComponentRevision::new("range-v1")?);

    // Attempt A: chunk size 2 over 6 items is 3 chunks. The 2nd chunk's
    // reader/processor/writer/ItemStream::update all run for real, but its
    // *commit* is injected to fail, so it rolls back.
    let log = InjectionLog::new();
    let (reader_a, stream_a, contract_a, position_a) = range_reader(namespace.clone(), 6);
    let step_a = ChunkStep::new(
        StepName::new("range")?,
        ChunkSize::new(2)?,
        reader_a,
        Identity,
        Sink,
        Arc::new(InjectedTransactions::new(
            fixture.transaction_manager(state_provider()),
            2,
            PreCommitAction::Fail,
            InjectionId::new(1),
            log.clone(),
        )),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("restart-example-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;
    println!(
        "attempt A finished as {:?}, injection fired: {}, rolled-back candidate reached position {}",
        report_a.launch().job_execution().metadata().status(),
        log.fired(InjectionId::new(1)),
        position_a.get(),
    );

    let (reader_b, stream_b, contract_b, position_b) = range_reader(namespace.clone(), 6);
    let step_b = ChunkStep::new(
        StepName::new("range")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        Sink,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("restart-example-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;
    println!(
        "attempt B (new execution {:?}, same instance {:?}) finished as {:?}, resumed to position {}",
        report_b.launch().job_execution().id(),
        report_b.launch().instance().id(),
        report_b.launch().job_execution().metadata().status(),
        position_b.get(),
    );
    Ok(())
}
