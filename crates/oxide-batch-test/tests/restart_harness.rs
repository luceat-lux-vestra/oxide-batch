//! `restart_harness_resumes_from_the_last_committed_checkpoint`.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::sync::{Arc, Mutex};

use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCommitReceipt, ChunkCount, ChunkDeliveryMode, ChunkJob,
    ChunkSize, ChunkStep, ComponentRevision, ComponentStreamIdentity, DefinitionRevision,
    ExecutionContext, ExecutionCounts, ItemProcessor, ItemWriter, JobName, JobParameters,
    PostgresChunkStateError, ProcessContext, ProcessOutcome, ProcessorError, StateLimits, StepName,
    WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{InjectedPreCommit, InjectionId, InjectionLog, PreCommitAction};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::restart::range_reader;
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

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

struct RecordingWriter(Arc<Mutex<Vec<u64>>>);

impl ItemWriter<u64> for RecordingWriter {
    async fn write(
        &self,
        items: &[u64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

fn state_provider() -> Arc<dyn oxide_batch::PostgresChunkStateProvider> {
    Arc::new(
        |_committed: ExecutionCounts, _chunk: oxide_batch::ChunkCounts| {
            let checkpoint = Checkpoint::from_json(
                br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.restart-harness","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
            let context = ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.restart-harness","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
            Ok(ChunkCommitReceipt::new(checkpoint, context))
        },
    )
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the two attempts and their equivalence assertions are only meaningful together"
)]
async fn restart_harness_resumes_from_the_last_committed_checkpoint() -> Result<(), Box<dyn Error>>
{
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!("oxide_batch_test_restart_harness_{nonce}"))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.range")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(namespace.clone(), ComponentRevision::new("range-v1")?);

    let written: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // Attempt A: injects a pre-commit failure immediately before the 3rd
    // chunk, so only the first two chunks (positions 0..4) commit durably.
    let (reader_a, stream_a, contract_a) = range_reader(namespace.clone(), 10);
    let log_a = InjectionLog::new();
    let step_a = ChunkStep::new(
        StepName::new("range_step")?,
        ChunkSize::new(2)?,
        reader_a,
        Identity,
        RecordingWriter(Arc::clone(&written)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a)
    .with_chunk_listener(Arc::new(InjectedPreCommit::new(
        ChunkCount::new(3),
        PreCommitAction::Fail,
        InjectionId::new(1),
        log_a.clone(),
    )));
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("restart-harness-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(
        log_a.fired(InjectionId::new(1)),
        "the injected pre-commit failure must have fired"
    );
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(chunk_report_a.committed_chunks(), ChunkCount::new(2));
    assert_eq!(chunk_report_a.committed_counts().read().get(), 4);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Failed,
        "the injected failure must leave attempt A failed, not completed",
    );
    assert_eq!(
        *written
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0, 1, 2, 3],
        "only the committed prefix reached the writer",
    );

    // Attempt B: a fresh reader/stream pair (attempt A's is exhausted and
    // moved), launched again through the same production API against the
    // same job instance -- the real restart path, not a manual shortcut.
    let (reader_b, stream_b, contract_b) = range_reader(namespace.clone(), 10);
    let step_b = ChunkStep::new(
        StepName::new("range_step")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(Arc::clone(&written)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name.clone(),
        step_b,
        DefinitionRevision::new("restart-harness-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
        "the restart must finish the remaining work",
    );
    let chunk_report_b = report_b
        .chunk()
        .ok_or("attempt B must have reached the chunk step")?;
    assert_eq!(
        chunk_report_b.committed_counts().read().get(),
        6,
        "the restart must read exactly the uncommitted remainder, not the whole input again",
    );

    let mut all_written = written
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    all_written.sort_unstable();
    assert_eq!(
        all_written,
        (0..10).collect::<Vec<_>>(),
        "the two attempts together wrote every item exactly once, with no gap or duplicate",
    );

    assert_ne!(
        report_a.launch().job_execution().id(),
        report_b.launch().job_execution().id(),
        "the restart is a new execution attempt, not a reused attempt identity",
    );
    assert_eq!(
        report_a.launch().instance().id(),
        report_b.launch().instance().id(),
        "the restart selects the same logical job instance",
    );

    Ok(())
}
