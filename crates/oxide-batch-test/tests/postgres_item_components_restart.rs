//! Restart evidence for a decorated (#146) typed pipeline (acceptance 5.6):
//! wrapping a durable, restartable reader in [`PeekReader`] -- and calling
//! `peek` before *every* real read, not merely leaving it unused -- must not
//! falsely advance the checkpoint, lose an uncommitted lookahead, or corrupt
//! the real production restart path. This mirrors
//! `oxide-batch-test`'s own `restart_harness_resumes_from_the_last_committed_checkpoint`
//! evidence but decorates the reader, which is the part #146 adds.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::error::Error;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::PeekReader;
use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCommitReceipt, ChunkCount, ChunkCounts, ChunkDeliveryMode,
    ChunkJob, ChunkSize, ChunkStep, ChunkTransactionManager, ComponentRevision,
    ComponentStreamIdentity, DefinitionRevision, ExecutionContext, ExecutionCounts, ItemProcessor,
    ItemReader, ItemWriter, JobName, JobParameters, PostgresChunkStateError,
    PostgresChunkStateProvider, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StateLimits, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectionId, InjectionLog, Trigger,
};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::restart::{ObservingTransactions, range_reader};
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};

/// Encodes the real cumulative read position into the legacy checkpoint, so
/// `InheritedStepProgress::checkpoint_digest` reflects genuine commit-to-commit
/// progress instead of a constant placeholder (mirrors
/// `oxide-batch-test`'s own `restart_harness.rs` fixture).
fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
    Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
        let position = committed
            .read()
            .checked_add(chunk.read().get())
            .ok_or_else(PostgresChunkStateError::new)?;
        let checkpoint_bytes = format!(
            r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.peek-restart","schema_version":1,"payload":{{"position":{position}}}}}"#
        );
        let checkpoint = Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
            .map_err(|_| PostgresChunkStateError::new())?;
        let context = ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.peek-restart","schema_version":1,"payload":{}}"#,
                StateLimits::default(),
            )
            .map_err(|_| PostgresChunkStateError::new())?;
        Ok(ChunkCommitReceipt::new(checkpoint, context))
    })
}

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

/// Wraps a delegate reader, calling [`PeekReader::peek`] before every real
/// [`ItemReader::read`] and discarding the peeked value -- simulating the
/// realistic usage pattern (a custom reader peeking internally to decide
/// something) rather than leaving the lookahead capability entirely unused.
struct PeekingReader<I, R>(PeekReader<I, R>);

impl<I: Send + 'static, R: ItemReader<I>> ItemReader<I> for PeekingReader<I, R> {
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        let _ = self.0.peek(context).await?;
        self.0.read(context).await
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the two attempts and their committed-vs-candidate assertions are only meaningful together"
)]
async fn peek_decorated_reader_restarts_from_the_last_committed_checkpoint()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!("oxide_batch_146_peek_restart_{nonce}"))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.range")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(namespace.clone(), ComponentRevision::new("range-v1")?);

    // Attempt A: chunk size 2 over 10 items. A stop is injected on the 5th
    // *real* underlying read (item index 4) -- chunks 1 and 2 (items 0..4)
    // commit, chunk 3 observes cooperative stop on its first read, and its
    // preceding `peek` call is the one that actually reaches the delegate.
    let (reader_a, stream_a, contract_a, position_a) = range_reader(namespace.clone(), 10);
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(4),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let peeking_reader_a = PeekingReader(PeekReader::new(injected_reader_a));
    let writer_a: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("range")?,
        ChunkSize::new(2)?,
        peeking_reader_a,
        Identity,
        RecordingWriter(Arc::clone(&writer_a)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("peek-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(chunk_report_a.committed_chunks(), ChunkCount::new(2));
    assert_eq!(chunk_report_a.committed_counts().read().get(), 4);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(
        position_a.get(),
        4,
        "peeking never reads past a genuine stop"
    );
    assert_eq!(
        *writer_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0, 1, 2, 3],
    );

    // Attempt B: a fresh peek-decorated reader/stream pair, launched again
    // through the real production restart path.
    let (reader_b, stream_b, contract_b, position_b) = range_reader(namespace.clone(), 10);
    let peeking_reader_b = PeekingReader(PeekReader::new(reader_b));
    let writer_b: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(ObservingTransactions::new(
        fixture.transaction_manager(state_provider()),
    ));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("range")?,
        ChunkSize::new(2)?,
        peeking_reader_b,
        Identity,
        RecordingWriter(Arc::clone(&writer_b)),
        Arc::clone(&observed) as Arc<dyn ChunkTransactionManager>,
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("peek-restart-v1")?,
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
    );
    let chunk_report_b = report_b
        .chunk()
        .ok_or("attempt B must have reached the chunk step")?;
    assert_eq!(
        chunk_report_b.committed_counts().read().get(),
        6,
        "the restart reads exactly the uncommitted remainder, never the whole input again",
    );
    assert_eq!(
        *writer_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![4, 5, 6, 7, 8, 9],
        "the peek-decorated restart resumed from item 4, not 0 and not duplicated",
    );
    assert_eq!(position_b.get(), 10);

    let observed_progress = observed.observed_progress();
    assert_eq!(observed_progress.len(), 1);
    assert_eq!(
        observed_progress[0].read_ordinal(),
        4,
        "the inherited read ordinal is exactly the last committed count, unaffected by peeking",
    );

    Ok(())
}

fn fixture_stop_source() -> oxide_batch::StopSource {
    let (source, _token) = oxide_batch::StopSource::new();
    source
}
