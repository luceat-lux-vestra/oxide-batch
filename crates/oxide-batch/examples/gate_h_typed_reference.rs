//! Gate H (#153 §6) binary-size/compile-time reference: the typed
//! representation of the M6 P-002 reference pipeline, built as its own
//! binary so its compiled size and build time can be compared against
//! [`gate_h_boxed_reference`](../gate_h_boxed_reference.rs) in isolation.
//!
//! Deliberately minimal and self-contained (no `tests/support` dependency,
//! no `oxide-batch-test`) so nothing beyond the typed reference pipeline
//! itself contributes to this binary's size. The same CSV reader/writer and
//! processor as `gate_h_allocation.rs`/`gate_h_dispatch.rs`/
//! `gate_h_throughput.rs` (`DelimitedReader`/`DelimitedWriter` around
//! `IdentityProcessor`), run once against a tiny embedded fixture, with a
//! no-op transaction manager (equivalent in structural cost either way,
//! since this binary's whole point is measuring reader/processor/writer
//! representation, not the transaction port).

#![allow(clippy::expect_used, clippy::similar_names)]

use std::error::Error;
use std::io::Write;
use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{DelimitedDialect, DelimitedRecord, delimited_file_reader};
use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkCounts, ChunkExecutionOutcome,
    ChunkExecutionReport, ChunkFaultProgress, ChunkSize, ChunkStep, ChunkTransaction,
    ChunkTransactionError, ChunkTransactionManager, ComponentStreamIdentity, ExecutionAttempt,
    ExecutionContext, ExecutionCorrelation, JobExecutionId, JobInstanceId, JobName, StateLimits,
    StepExecutionId, StepName, StopSource,
};

struct NoopTransaction;

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

struct NoopTransactions;

impl ChunkTransactionManager for NoopTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move { Ok(Box::new(NoopTransaction) as Box<dyn ChunkTransaction>) })
    }
}

struct NoopCompletion;

impl ChunkCompletion for NoopCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

fn receipt() -> ChunkCommitReceipt {
    let checkpoint = Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"gate-h.position","schema_version":1,"payload":{"position":0}}"#,
        StateLimits::default(),
    )
    .expect("checkpoint fixture must be valid");
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"gate-h.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .expect("context fixture must be valid");
    ChunkCommitReceipt::new(checkpoint, context)
}

fn correlation() -> ExecutionCorrelation {
    let attempt = |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("nonzero"));
    ExecutionCorrelation::new(
        JobName::new("gate_h_typed_reference").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("gate_h_typed_reference").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let input = std::env::temp_dir().join("oxide-batch-gate-h-typed-reference.csv");
    let output = std::env::temp_dir().join("oxide-batch-gate-h-typed-reference-out.csv");
    let mut file = std::fs::File::create(&input)?;
    for index in 0..1_000_u32 {
        writeln!(file, "{index},value-{index},filler-field")?;
    }
    file.sync_all()?;

    let (reader, _reader_stream, _reader_contract) = delimited_file_reader::<DelimitedRecord>(
        &input,
        DelimitedDialect::csv(),
        ComponentStreamIdentity::new("gate-h.typed-reference.reader")?,
    )?;
    let (writer, _writer_stream, _writer_contract) =
        oxide_batch::item_components::delimited_writer(
            &output,
            DelimitedDialect::csv(),
            ComponentStreamIdentity::new("gate-h.typed-reference.writer")?,
        )?;

    let mut step: ChunkStep<DelimitedRecord, DelimitedRecord, _, _, _> = ChunkStep::new(
        StepName::new("gate-h-typed-reference")?,
        ChunkSize::new(1_000)?,
        reader,
        IdentityProcessor,
        writer,
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let _ = std::fs::remove_file(&input);
    let _ = std::fs::remove_file(&output);
    println!("gate-h typed reference pipeline completed: {report:?}");
    Ok(())
}
