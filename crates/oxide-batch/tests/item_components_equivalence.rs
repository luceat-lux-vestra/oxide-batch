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

use oxide_batch::item_components::{ChainProcessor, FilterProcessor, IdentityProcessor};
use oxide_batch::{
    BoxFuture, BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome,
    ChunkExecutionReport, ChunkFailure, ChunkSize, ChunkStep, FailureCategory, FaultDescriptor,
    ItemListenerContext, ItemListenerSet, ItemReader, ListenerError, ReadContext, ReadListener,
    ReadOutcome, ReaderError, StepName, StopSource,
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
    // Explicit end-of-input equivalence: the production-observable signal
    // that the delegate reader was genuinely exhausted (37 items read, not
    // merely that the run happened to stop) must agree between paths, not
    // just the coarse `Completed` outcome above.
    assert_eq!(typed_report.committed_counts().read().get(), 37);
    assert_eq!(erased_report.committed_counts().read().get(), 37);
}

/// A predicate that genuinely filters part of the input, unlike the shared
/// pipeline's `keep_all` (which exists only to exercise the filter
/// decorator's dispatch). Takes `&i64` to match `ItemFilter`'s signature.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_even(item: &i64) -> bool {
    item % 2 == 0
}

type FilteringProcessor =
    ChainProcessor<FilterProcessor<i64, fn(&i64) -> bool>, IdentityProcessor, i64>;

fn filtering_processor() -> FilteringProcessor {
    ChainProcessor::new(
        FilterProcessor::new(is_even as fn(&i64) -> bool),
        IdentityProcessor,
    )
}

#[tokio::test]
async fn typed_and_erased_decorated_pipelines_agree_on_real_filtering() {
    const ITEMS: u32 = 21;
    let expected: Vec<i64> = (0..i64::from(ITEMS)).filter(is_even).collect();
    // Sanity on the fixture itself: a vacuous filter (one that keeps
    // everything) would make this whole scenario indistinguishable from the
    // unfiltered equivalence test above.
    assert!(expected.len() < usize::try_from(ITEMS).unwrap());

    let output_typed = Arc::new(Mutex::new(Vec::new()));
    let mut typed_step = ChunkStep::new(
        StepName::new("typed_filter").unwrap(),
        ChunkSize::new(ITEMS).unwrap(),
        decorated_pipeline::reader(ITEMS),
        filtering_processor(),
        decorated_pipeline::writer(Arc::clone(&output_typed)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let typed_report = typed_step.execute(&correlation(), &stop).await;

    let output_erased = Arc::new(Mutex::new(Vec::new()));
    let mut erased_step = ChunkStep::new(
        StepName::new("erased_filter").unwrap(),
        ChunkSize::new(ITEMS).unwrap(),
        BoxedReader::new(decorated_pipeline::reader(ITEMS)),
        BoxedProcessor::new(filtering_processor()),
        BoxedWriter::new(decorated_pipeline::writer(Arc::clone(&output_erased))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source2, stop2) = StopSource::new();
    let erased_report = erased_step.execute(&correlation(), &stop2).await;

    assert_eq!(typed_report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(erased_report.outcome(), ChunkExecutionOutcome::Completed);
    // The filtered-out items must be genuinely absent from the writer's
    // effects on both paths, not merely equal counts.
    assert_eq!(*output_typed.lock().unwrap(), expected);
    assert_eq!(*output_erased.lock().unwrap(), expected);
    // Every item was still read (the filter drops output, not input), and
    // only the kept items were written, identically on both paths.
    assert_eq!(
        typed_report.committed_counts().read().get(),
        u64::from(ITEMS)
    );
    assert_eq!(
        erased_report.committed_counts().read().get(),
        u64::from(ITEMS)
    );
    assert_eq!(
        typed_report.committed_counts().written().get(),
        expected.len() as u64
    );
    assert_eq!(
        typed_report.committed_counts(),
        erased_report.committed_counts()
    );
}

/// Wraps a delegate reader, returning a deterministic typed [`ReaderError`]
/// once `remaining` successful reads are exhausted -- used to prove typed
/// and erased pipelines classify the *same* injected failure identically,
/// not merely that both "fail".
struct FailAfterNReader<R> {
    inner: R,
    remaining: u32,
}

impl<I: 'static, R: ItemReader<I>> ItemReader<I> for FailAfterNReader<R> {
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        if self.remaining == 0 {
            return Err(ReaderError::with_category(FailureCategory::Timeout));
        }
        self.remaining -= 1;
        self.inner.read(context).await
    }
}

/// Captures the [`FailureCategory`] the framework itself observed at the
/// item-listener boundary -- the actual framework-visible classification,
/// not merely the outcome shape.
struct CapturingReadListener(Arc<Mutex<Option<FailureCategory>>>);

impl ReadListener<i64> for CapturingReadListener {
    fn on_read_error<'a>(
        &'a self,
        fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let captured = Arc::clone(&self.0);
        Box::pin(async move {
            *captured.lock().unwrap() = Some(fault.category());
            Ok(())
        })
    }
}

#[tokio::test]
async fn typed_and_erased_decorated_pipelines_agree_on_failure_classification() {
    const ITEMS: u32 = 20;
    const SUCCESSFUL_READS: u32 = 5;

    let category_typed = Arc::new(Mutex::new(None));
    let output_typed = Arc::new(Mutex::new(Vec::new()));
    let listeners_typed = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(&category_typed))))
        .unwrap();
    let mut typed_step = ChunkStep::new(
        StepName::new("typed_failure").unwrap(),
        ChunkSize::new(ITEMS).unwrap(),
        FailAfterNReader {
            inner: decorated_pipeline::reader(ITEMS),
            remaining: SUCCESSFUL_READS,
        },
        decorated_pipeline::processor(),
        decorated_pipeline::writer(Arc::clone(&output_typed)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    )
    .with_item_listeners(listeners_typed);
    let (_source, stop) = StopSource::new();
    let typed_report = typed_step.execute(&correlation(), &stop).await;

    let category_erased = Arc::new(Mutex::new(None));
    let output_erased = Arc::new(Mutex::new(Vec::new()));
    let listeners_erased = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(
            &category_erased,
        ))))
        .unwrap();
    let mut erased_step = ChunkStep::new(
        StepName::new("erased_failure").unwrap(),
        ChunkSize::new(ITEMS).unwrap(),
        BoxedReader::new(FailAfterNReader {
            inner: decorated_pipeline::reader(ITEMS),
            remaining: SUCCESSFUL_READS,
        }),
        BoxedProcessor::new(decorated_pipeline::processor()),
        BoxedWriter::new(decorated_pipeline::writer(Arc::clone(&output_erased))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    )
    .with_item_listeners(listeners_erased);
    let (_source2, stop2) = StopSource::new();
    let erased_report = erased_step.execute(&correlation(), &stop2).await;

    assert_eq!(
        typed_report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader)
    );
    assert_eq!(
        erased_report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader)
    );
    // The actual framework-visible classification the two paths reported
    // through the real item-listener boundary, not merely the "did it fail"
    // shape checked above.
    assert_eq!(
        *category_typed.lock().unwrap(),
        Some(FailureCategory::Timeout)
    );
    assert_eq!(
        *category_erased.lock().unwrap(),
        Some(FailureCategory::Timeout)
    );
    // Neither path committed anything: the chunk containing the failed read
    // never reached a commit boundary.
    assert_eq!(typed_report.committed_counts().read().get(), 0);
    assert_eq!(erased_report.committed_counts().read().get(), 0);
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
