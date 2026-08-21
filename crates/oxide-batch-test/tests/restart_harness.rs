//! `restart_harness_resumes_from_the_last_committed_checkpoint`.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::sync::{Arc, Mutex};

use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCommitReceipt, ChunkCount, ChunkDeliveryMode, ChunkJob,
    ChunkSize, ChunkStep, ChunkTransactionManager, ComponentRevision, ComponentStreamIdentity,
    DefinitionRevision, ExecutionContext, ExecutionCounts, ItemProcessor, ItemWriter, JobName,
    JobParameters, PostgresChunkStateError, ProcessContext, ProcessOutcome, ProcessorError,
    StateLimits, StepName, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{InjectedTransactions, InjectionId, InjectionLog, PreCommitAction};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::restart::{ObservingTransactions, range_reader};
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

/// Encodes the real cumulative read position into the legacy checkpoint, so
/// `InheritedStepProgress::checkpoint_digest` reflects genuine commit-to-commit
/// progress instead of a constant placeholder.
fn state_provider() -> Arc<dyn oxide_batch::PostgresChunkStateProvider> {
    Arc::new(
        |committed: ExecutionCounts, chunk: oxide_batch::ChunkCounts| {
            let position = committed
                .read()
                .checked_add(chunk.read().get())
                .ok_or_else(PostgresChunkStateError::new)?;
            let checkpoint_bytes = format!(
                r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.restart-harness","schema_version":1,"payload":{{"position":{position}}}}}"#
            );
            let checkpoint =
                Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
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
    reason = "the two attempts and their committed-vs-candidate assertions are only meaningful together"
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

    // Attempt A: chunk size 2 over 10 items is 5 chunks. Chunks 1 and 2
    // (items 0..4) commit. Chunk 3's reader, processor, writer, and
    // ItemStream::update all genuinely run -- producing a real candidate at
    // position 6 -- but its *commit* is injected to fail, so the runtime
    // rolls the candidate back. This is the real pre-commit boundary
    // (`ChunkTransaction::commit`), not `ChunkListener::before_chunk`, which
    // the production contract documents as running before the transaction
    // begins -- before this chunk's item work would ever have started.
    let (reader_a, stream_a, contract_a, position_a) = range_reader(namespace.clone(), 10);
    let log = InjectionLog::new();
    let injection_id = InjectionId::new(1);
    let writer_a: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        StepName::new("range")?,
        ChunkSize::new(2)?,
        reader_a,
        Identity,
        RecordingWriter(Arc::clone(&writer_a)),
        Arc::new(InjectedTransactions::new(
            fixture.transaction_manager(state_provider()),
            3,
            PreCommitAction::Fail,
            injection_id,
            log.clone(),
        )),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
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
        log.fired(injection_id),
        "the injected pre-commit failure on the 3rd chunk must have fired"
    );
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(
        chunk_report_a.committed_chunks(),
        ChunkCount::new(2),
        "only the first two chunks commit"
    );
    assert_eq!(
        chunk_report_a.committed_counts().read().get(),
        4,
        "durable committed count is exactly 4 items"
    );
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Failed,
        "the injected failure must leave attempt A failed, not completed",
    );

    // The 3rd chunk's reader/processor/writer/ItemStream::update genuinely
    // ran before the injected commit failure: the in-memory position
    // reached 6 (not 4), proving there really was uncommitted candidate
    // work to discard, not merely a chunk that never started.
    assert_eq!(
        position_a.get(),
        6,
        "the rolled-back chunk's reader/update genuinely reached position 6 in memory",
    );
    assert_eq!(
        *writer_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0, 1, 2, 3, 4, 5],
        "writer invocation trace, not durable evidence: items 4 and 5 were passed to the \
         writer before the commit that would have made them durable was rejected",
    );

    // Attempt B: a fresh reader/stream pair, launched again through the
    // same production API against the same job instance -- the real
    // restart path, not a manual shortcut. An ObservingTransactions wrapper
    // records exactly what the framework itself inherited, at the moment it
    // asked for it while opening this new attempt.
    let (reader_b, stream_b, contract_b, position_b) = range_reader(namespace.clone(), 10);
    let writer_b: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(ObservingTransactions::new(
        fixture.transaction_manager(state_provider()),
    ));
    let step_b = ChunkStep::new(
        StepName::new("range")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(Arc::clone(&writer_b)),
        Arc::clone(&observed) as Arc<dyn ChunkTransactionManager>,
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
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
        "the restart reads exactly the uncommitted remainder (6 items), never the whole input again",
    );
    assert_eq!(
        *writer_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![4, 5, 6, 7, 8, 9],
        "the restart resumed from item 4 -- the last COMMITTED position -- not 0 (start over) \
         and not 6 (the discarded candidate)",
    );
    assert_eq!(position_b.get(), 10);

    // Directly verify what the durable adapter reported this new attempt
    // inherited, captured at the exact moment the framework asked for it.
    let observed_progress = observed.observed_progress();
    assert_eq!(
        observed_progress.len(),
        1,
        "inherited_progress is queried exactly once, when the new attempt opens",
    );
    assert_eq!(
        observed_progress[0].read_ordinal(),
        4,
        "the framework's own inherited read ordinal is exactly the last COMMITTED count (4), \
         never the rolled-back candidate (6) and never zero",
    );
    assert_ne!(
        observed_progress[0].checkpoint_digest(),
        [0u8; 32],
        "a real committed checkpoint generation was inherited, not the NONE sentinel",
    );

    let observed_state = observed.observed_component_state();
    assert_eq!(observed_state.len(), 1);
    assert!(
        observed_state[0]
            .iter()
            .any(|envelope| envelope.namespace() == &namespace),
        "the new attempt inherited a committed component-state envelope for this stream",
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
