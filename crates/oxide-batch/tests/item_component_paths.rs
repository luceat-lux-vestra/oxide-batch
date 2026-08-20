//! ADR-0008: typed and erased `ChunkStep` composition produce the same
//! observable behavior.
//!
//! [`chunk_allocation`] carries the companion allocation-regression evidence
//! for these same two paths, in its own process so its global allocator
//! measurement is not shared with any other test.

#![allow(clippy::expect_used, clippy::similar_names)]

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use chunk_fixture::{Double, NoopCompletion, NoopTransactions, Sink, Source, correlation};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, StepName, StopSource,
};
use std::sync::{Arc, Mutex};

/// Runs `items` through the concrete, monomorphized `ChunkStep` path in one
/// chunk and returns the committed report and the writer's accepted output.
async fn run_typed(items: u32) -> (ChunkExecutionReport, Vec<i64>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("typed").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        Source::range(items),
        Double,
        Sink(Arc::clone(&output)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;
    let output = output.lock().expect("sink lock poisoned").clone();
    (report, output)
}

/// Runs the same logical pipeline through the explicit `Boxed*` erasure
/// boundary and returns the committed report and the writer's accepted
/// output.
async fn run_erased(items: u32) -> (ChunkExecutionReport, Vec<i64>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("erased").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        BoxedReader::new(Source::range(items)),
        BoxedProcessor::new(Double),
        BoxedWriter::new(Sink(Arc::clone(&output))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;
    let output = output.lock().expect("sink lock poisoned").clone();
    (report, output)
}

/// Typed correctness: concrete components driven directly by `ChunkStep`
/// produce the expected chunk output and counts.
#[tokio::test]
async fn typed_composition_produces_correct_chunk_output() {
    let (report, output) = run_typed(50).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.committed_counts().read().get(), 50);
    assert_eq!(report.committed_counts().written().get(), 50);
    assert_eq!(output, (0..50i64).map(|item| item * 2).collect::<Vec<_>>());
}

/// Erased correctness and typed/erased equivalence: the same logical
/// pipeline run through `BoxedReader`/`BoxedProcessor`/`BoxedWriter` produces
/// an identical observable outcome to the typed run — same output, same
/// committed counts, chunks, skip counts, and retry counts.
#[tokio::test]
async fn erased_composition_matches_the_typed_composition() {
    let (typed_report, typed_output) = run_typed(50).await;
    let (erased_report, erased_output) = run_erased(50).await;

    assert_eq!(erased_report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(erased_output, typed_output);
    assert_eq!(
        erased_report.committed_counts(),
        typed_report.committed_counts()
    );
    assert_eq!(
        erased_report.committed_chunks(),
        typed_report.committed_chunks()
    );
    assert_eq!(erased_report.skip_counts(), typed_report.skip_counts());
    assert_eq!(erased_report.retry_counts(), typed_report.retry_counts());
}

/// A pipeline spanning multiple chunks behaves the same whether the last
/// (partial) chunk goes through the typed or the erased path.
#[tokio::test]
async fn erased_composition_matches_the_typed_composition_across_a_partial_final_chunk() {
    async fn run_typed_chunked(items: u32, size: u32) -> (ChunkExecutionReport, Vec<i64>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut step = ChunkStep::new(
            StepName::new("typed").expect("static step name is valid"),
            ChunkSize::new(size).expect("static chunk size is nonzero"),
            Source::range(items),
            Double,
            Sink(Arc::clone(&output)),
            Arc::new(NoopTransactions),
            Arc::new(NoopCompletion),
        );
        let (_source, stop) = StopSource::new();
        let report = step.execute(&correlation(), &stop).await;
        let output = output.lock().expect("sink lock poisoned").clone();
        (report, output)
    }

    async fn run_erased_chunked(items: u32, size: u32) -> (ChunkExecutionReport, Vec<i64>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut step = ChunkStep::new(
            StepName::new("erased").expect("static step name is valid"),
            ChunkSize::new(size).expect("static chunk size is nonzero"),
            BoxedReader::new(Source::range(items)),
            BoxedProcessor::new(Double),
            BoxedWriter::new(Sink(Arc::clone(&output))),
            Arc::new(NoopTransactions),
            Arc::new(NoopCompletion),
        );
        let (_source, stop) = StopSource::new();
        let report = step.execute(&correlation(), &stop).await;
        let output = output.lock().expect("sink lock poisoned").clone();
        (report, output)
    }

    let (typed_report, typed_output) = run_typed_chunked(7, 3).await;
    let (erased_report, erased_output) = run_erased_chunked(7, 3).await;

    assert_eq!(typed_report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(typed_report.committed_chunks().get(), 3);
    assert_eq!(erased_report.outcome(), typed_report.outcome());
    assert_eq!(
        erased_report.committed_chunks(),
        typed_report.committed_chunks()
    );
    assert_eq!(erased_output, typed_output);
}
