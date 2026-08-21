//! Injects a typed failure at the reader boundary and shows how a test
//! proves it was the injection that fired, not a genuine framework defect.

use std::collections::VecDeque;

use oxide_batch::{
    ChunkSize, FailureCategory, ItemProcessor, ItemReader, ItemWriter, ProcessContext,
    ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError, StepName, WriteContext,
    WriteOutcome, WriterError,
};
use oxide_batch_test::TestStep;
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectionId, InjectionLog, Trigger,
};

struct Source(VecDeque<i64>);

impl ItemReader<i64> for Source {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
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

struct Sink;

impl ItemWriter<i64> for Sink {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        Ok(WriteOutcome::Written)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = InjectionLog::new();
    let id = InjectionId::new(1);

    let mut step = TestStep::new(
        StepName::new("inject_example")?,
        ChunkSize::new(2)?,
        InjectedReader::new(
            Source((0..4).collect()),
            Trigger::immediately(),
            ComponentAction::Fail(FailureCategory::UserComponent),
            id,
            log.clone(),
        ),
        Identity,
        Sink,
    );

    let report = step.run().await;
    println!("outcome: {:?}", report.outcome());
    println!(
        "was this the injection we fired, not a genuine defect? {}",
        log.fired(id)
    );
    Ok(())
}
