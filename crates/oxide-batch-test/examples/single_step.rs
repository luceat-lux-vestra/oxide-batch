//! Exercises a bare component through a scoped fixture, then drives a
//! single step through the real production `ChunkStep::execute` path --
//! both entirely through published public API.

use std::collections::VecDeque;

use oxide_batch::{
    ChunkSize, ItemProcessor, ItemReader, ItemWriter, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, StepName, WriteContext, WriteOutcome,
    WriterError,
};
use oxide_batch_test::{ComponentFixture, TestStep};

struct Source(VecDeque<i64>);

impl ItemReader<i64> for Source {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

struct Square;

impl ItemProcessor<i64, i64> for Square {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(item * item))
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
    // TEST-SCOPE-001: exercise one component in isolation.
    let fixture = ComponentFixture::new();
    let mut reader = Source(VecDeque::from([3]));
    let outcome = reader.read(fixture.read_context()).await?;
    println!("scoped reader call: {outcome:?}");

    // TEST-STEP-001: drive a real step end to end without a full job.
    let mut step = TestStep::new(
        StepName::new("square")?,
        ChunkSize::new(2)?,
        Source((1..=4).collect()),
        Square,
        Sink,
    );
    let report = step.run().await;
    println!("single-step outcome: {:?}", report.outcome());
    Ok(())
}
