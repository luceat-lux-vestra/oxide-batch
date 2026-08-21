//! Launches a complete job with a deterministic clock and ID source,
//! entirely through published `oxide-batch`/`oxide-batch-test` public API.

use std::sync::Arc;

use oxide_batch::{
    ChunkJob, ChunkSize, ChunkStep, DefinitionRevision, ItemProcessor, ItemReader, ItemWriter,
    JobName, JobParameters, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StepName, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::{
    NoCompletion, StandaloneTransactions, TestJob, default_chunk_component_revisions,
};
use std::collections::VecDeque;

struct Source(VecDeque<i64>);

impl ItemReader<i64> for Source {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

struct Double;

impl ItemProcessor<i64, i64> for Double {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(item * 2))
    }
}

struct Sink;

impl ItemWriter<i64> for Sink {
    async fn write(
        &self,
        items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        println!("wrote {items:?}");
        Ok(WriteOutcome::Written)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let step = ChunkStep::new(
        StepName::new("double")?,
        ChunkSize::new(2)?,
        Source((0..6).collect()),
        Double,
        Sink,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    );
    let chunk_job = ChunkJob::new(
        JobName::new("full_job_example")?,
        step,
        DefinitionRevision::new("full-job-example-v1")?,
        &default_chunk_component_revisions(),
    )?;

    let mut job = TestJob::embedded(chunk_job);
    let report = job.launch(&JobParameters::new()).await?;

    println!(
        "job execution {:?} finished as {:?}",
        report.launch().job_execution().id(),
        report.launch().job_execution().metadata().status(),
    );
    Ok(())
}
