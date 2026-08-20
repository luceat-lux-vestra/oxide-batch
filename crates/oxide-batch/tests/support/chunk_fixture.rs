//! Minimal item components and a standalone chunk transaction shared by the
//! ADR-0008 typed/erased path and allocation-regression tests.

#![allow(dead_code, clippy::expect_used)]

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkCounts, ChunkFaultProgress,
    ChunkTransaction, ChunkTransactionError, ChunkTransactionManager, ExecutionAttempt,
    ExecutionContext, ExecutionCorrelation, ItemProcessor, ItemReader, ItemWriter, JobExecutionId,
    JobInstanceId, JobName, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StateLimits, StepExecutionId, StepName, WriteContext, WriteOutcome,
    WriterError,
};

pub struct Source(VecDeque<i64>);

impl Source {
    pub fn range(items: u32) -> Self {
        Self((0..i64::from(items)).collect())
    }
}

impl ItemReader<i64> for Source {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

pub struct Double;

impl ItemProcessor<i64, i64> for Double {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(item * 2))
    }
}

pub struct Sink(pub Arc<Mutex<Vec<i64>>>);

impl ItemWriter<i64> for Sink {
    async fn write(
        &self,
        items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .expect("sink lock poisoned")
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

pub struct NoopTransaction;

impl ChunkTransaction for NoopTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { Ok(receipt()) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

pub struct NoopTransactions;

impl ChunkTransactionManager for NoopTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move { Ok(Box::new(NoopTransaction) as Box<dyn ChunkTransaction>) })
    }
}

pub struct NoopCompletion;

impl ChunkCompletion for NoopCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

pub fn receipt() -> ChunkCommitReceipt {
    let checkpoint = Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"test.position","schema_version":1,"payload":{"position":0}}"#,
        StateLimits::default(),
    )
    .expect("checkpoint fixture must be valid");
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"test.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .expect("context fixture must be valid");
    ChunkCommitReceipt::new(checkpoint, context)
}

pub fn correlation() -> ExecutionCorrelation {
    let attempt = |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("nonzero"));
    ExecutionCorrelation::new(
        JobName::new("item_component_contract").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("item_component_contract").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}
