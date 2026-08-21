//! `TEST-REPO-001` postgres evidence: `repository_fixture_cleans_up_isolated_metadata`.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, ChunkJob, ChunkSize, ChunkStep, DefinitionRevision, ItemProcessor, ItemReader,
    ItemWriter, JobName, JobParameters, OperationId, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, StepName, WriteContext, WriteOutcome,
    WriterError,
};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::{
    ManualClock, NoCompletion, StandaloneTransactions, TestJob, default_chunk_component_revisions,
};

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

struct OneItem(bool);

impl ItemReader<i64> for OneItem {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(if self.0 {
            self.0 = false;
            ReadOutcome::Item(1)
        } else {
            ReadOutcome::EndOfInput
        })
    }
}

struct Identity;

impl ItemProcessor<i64, i64> for Identity {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

struct NoopWriter;

impl ItemWriter<i64> for NoopWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        Ok(WriteOutcome::Written)
    }
}

#[tokio::test]
async fn repository_fixture_cleans_up_isolated_metadata() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;

    let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
    let fixture = PostgresFixture::connect_with_clock(url, clock.clone()).await?;

    let job_name = JobName::new("oxide_batch_test_fixture_cleanup")?;
    let step = ChunkStep::new(
        StepName::new("load")?,
        ChunkSize::new(1)?,
        OneItem(true),
        Identity,
        NoopWriter,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    );
    let chunk_job = ChunkJob::new(
        job_name.clone(),
        step,
        DefinitionRevision::new("fixture-cleanup-v1")?,
        &default_chunk_component_revisions(),
    )?;
    let mut test_job = TestJob::new(
        chunk_job,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report = test_job.launch(&JobParameters::new()).await?;
    assert_eq!(
        report.launch().job_execution().metadata().status(),
        BatchStatus::Completed
    );

    // Isolation: the fixture-scoped job name never collides with another
    // job's durable rows -- nothing else in this database shares it.

    // Cleanup: the real production purge path requires MIN_PURGE_AGE, which
    // this fixture's own injected clock satisfies deterministically, with no
    // real wall-clock wait.
    clock.advance(Duration::from_mins(61))?;
    let operation_id = OperationId::new("oxide-batch-test-fixture-cleanup-1")?;
    let purge_report = fixture.purge_job(job_name, operation_id).await?;
    assert!(purge_report.counts().job_executions() >= 1);
    assert!(purge_report.counts().job_instances() >= 1);

    Ok(())
}
