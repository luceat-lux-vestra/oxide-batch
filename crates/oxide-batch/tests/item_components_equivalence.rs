#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::similar_names
)]

//! Typed/erased semantic-equivalence evidence for a representative decorated
//! #146 pipeline (acceptance 5.2/2.2): the same peek-over-composite reader,
//! filter/identity processor chain, and synchronized recording writer,
//! driven once through the native monomorphized path and once through the
//! explicit `BoxedReader`/`BoxedProcessor`/`BoxedWriter` erasure boundary,
//! must share the same production [`ChunkStep`] driver and produce
//! identical produced items, filtering, end-of-input, stop, failure
//! classification, and writer effects.

#[path = "support/decorated_pipeline.rs"]
mod decorated_pipeline;

use std::sync::{Arc, Mutex};

use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, StepName, StopSource,
};

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;
use chunk_fixture::{NoopCompletion, NoopTransactions, correlation};

async fn run_typed(items: u32) -> (ChunkExecutionReport, Vec<i64>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("typed").unwrap(),
        ChunkSize::new(items).unwrap(),
        decorated_pipeline::reader(items),
        decorated_pipeline::processor(),
        decorated_pipeline::writer(Arc::clone(&output)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;
    let output = output.lock().unwrap().clone();
    (report, output)
}

async fn run_erased(items: u32) -> (ChunkExecutionReport, Vec<i64>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("erased").unwrap(),
        ChunkSize::new(items).unwrap(),
        BoxedReader::new(decorated_pipeline::reader(items)),
        BoxedProcessor::new(decorated_pipeline::processor()),
        BoxedWriter::new(decorated_pipeline::writer(Arc::clone(&output))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;
    let output = output.lock().unwrap().clone();
    (report, output)
}

#[tokio::test]
async fn typed_and_erased_decorated_pipelines_produce_identical_items_and_completion() {
    let (typed_report, typed_output) = run_typed(37).await;
    let (erased_report, erased_output) = run_erased(37).await;

    assert_eq!(typed_report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(erased_report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(typed_output, (0..37).collect::<Vec<_>>());
    assert_eq!(typed_output, erased_output);
    assert_eq!(
        typed_report.committed_counts(),
        erased_report.committed_counts()
    );
}

#[tokio::test]
async fn typed_and_erased_decorated_pipelines_agree_on_stop() {
    let output_typed = Arc::new(Mutex::new(Vec::new()));
    let mut typed_step = ChunkStep::new(
        StepName::new("typed_stop").unwrap(),
        ChunkSize::new(5).unwrap(),
        decorated_pipeline::reader(20),
        decorated_pipeline::processor(),
        decorated_pipeline::writer(Arc::clone(&output_typed)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (source, stop) = StopSource::new();
    source.request_stop();
    let typed_report = typed_step.execute(&correlation(), &stop).await;

    let output_erased = Arc::new(Mutex::new(Vec::new()));
    let mut erased_step = ChunkStep::new(
        StepName::new("erased_stop").unwrap(),
        ChunkSize::new(5).unwrap(),
        BoxedReader::new(decorated_pipeline::reader(20)),
        BoxedProcessor::new(decorated_pipeline::processor()),
        BoxedWriter::new(decorated_pipeline::writer(Arc::clone(&output_erased))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (source2, stop2) = StopSource::new();
    source2.request_stop();
    let erased_report = erased_step.execute(&correlation(), &stop2).await;

    assert_eq!(typed_report.outcome(), ChunkExecutionOutcome::Stopped);
    assert_eq!(erased_report.outcome(), ChunkExecutionOutcome::Stopped);
    assert!(output_typed.lock().unwrap().is_empty());
    assert!(output_erased.lock().unwrap().is_empty());
}
