#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Composition/delegation contract tests for the standard catalog (#146):
//! ordering, stop propagation, failure classification, and the writer
//! transaction reborrow.
//!
//! Uses `oxide-batch-test`'s `ComponentFixture` and `inject` module so
//! failure/stop injection is distinguishable from a genuine framework defect
//! (#145's own requirement), rather than a hand-rolled parallel harness.

use std::sync::{Arc, Mutex};

use oxide_batch::item_components::{ChainProcessor, CompositeReader, FanOutWriter};
use oxide_batch::{
    BoxFuture, BoxedWriter, BusinessStatement, BusinessTransaction, BusinessTransactionError,
    BusinessWriteResult, FailureCategory, ItemProcessor, ItemReader, ItemWriter, ProcessContext,
    ProcessOutcome, ProcessorError, ReadOutcome, ReaderError, StopSource, WriteContext,
    WriteOutcome, WriterError,
};
use oxide_batch_test::ComponentFixture;
use oxide_batch_test::inject::{
    ComponentAction, InjectedProcessor, InjectedReader, InjectionId, InjectionLog, Trigger,
};

// ---------------------------------------------------------------------
// CompositeReader: ordering, stop, and failure propagation
// ---------------------------------------------------------------------

#[tokio::test]
async fn composite_reader_concatenates_delegates_in_order() {
    let fixture = ComponentFixture::new();
    let mut reader = CompositeReader::new(vec![
        oxide_batch::item_components::IterReader::new(vec![1, 2]),
        oxide_batch::item_components::IterReader::new(vec![3, 4]),
    ]);
    let mut items = Vec::new();
    loop {
        match reader.read(fixture.read_context()).await.unwrap() {
            ReadOutcome::Item(item) => items.push(item),
            ReadOutcome::EndOfInput => break,
            _ => panic!("unexpected outcome"),
        }
    }
    assert_eq!(items, vec![1, 2, 3, 4]);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput),
        "end of input is stable once every delegate is exhausted"
    );
}

#[tokio::test]
async fn composite_reader_stop_short_circuits_without_advancing_to_next_delegate() {
    let fixture = ComponentFixture::new();
    let (stop_source, _unused) = StopSource::new();
    let log = InjectionLog::new();
    let first = InjectedReader::new(
        oxide_batch::item_components::IterReader::new(vec![1]),
        Trigger::immediately(),
        ComponentAction::Stop(stop_source),
        InjectionId::new(1),
        log.clone(),
    );
    let second = panic_on_touch_reader();
    let mut reader = CompositeReader::new(vec![first, second]);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Stopped),
        "the first delegate's stop must be observed as the composite's outcome"
    );
    assert!(log.fired(InjectionId::new(1)));
}

#[tokio::test]
async fn composite_reader_failure_propagates_unchanged_without_touching_next_delegate() {
    let fixture = ComponentFixture::new();
    let log = InjectionLog::new();
    let first = InjectedReader::new(
        oxide_batch::item_components::IterReader::new(Vec::<i64>::new()),
        Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::Timeout),
        InjectionId::new(2),
        log.clone(),
    );
    let second = panic_on_touch_reader();
    let mut reader = CompositeReader::new(vec![first, second]);
    let outcome = reader.read(fixture.read_context()).await;
    assert_eq!(
        outcome,
        Err(ReaderError::with_category(FailureCategory::Timeout)),
        "the delegate's failure classification must survive unchanged"
    );
}

/// The framework's fault-retry contract re-invokes the same reader instance
/// after a retryable failure, without rewinding it. This proves
/// `CompositeReader` cooperates: its "current delegate" position is not
/// advanced by a failing call, so the very next call -- a real retry -- must
/// resume at the same delegate rather than skip ahead to the next one.
#[tokio::test]
async fn composite_reader_retry_after_failure_resumes_the_same_delegate() {
    let fixture = ComponentFixture::new();
    let log = InjectionLog::new();
    // One-shot: fires on the first call only, so the retry (the composite's
    // second `read` call) reaches the real delegate underneath it.
    let first = InjectedReader::new(
        oxide_batch::item_components::IterReader::new(vec![1]),
        Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::Timeout),
        InjectionId::new(21),
        log,
    );
    let second = panic_on_touch_reader();
    let mut reader = CompositeReader::new(vec![first, second]);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Err(ReaderError::with_category(FailureCategory::Timeout))
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(1)),
        "the retry must resume at the first delegate (reading its real item 1), not skip ahead \
         to the second (which would panic if touched)"
    );
}

/// A reader that panics the moment it is touched, used to prove a composite
/// or chain never invokes a later delegate after an earlier one stops/fails.
fn panic_on_touch_reader()
-> InjectedReader<oxide_batch::item_components::IterReader<std::vec::IntoIter<i64>>> {
    InjectedReader::new(
        oxide_batch::item_components::IterReader::new(Vec::<i64>::new()),
        Trigger::immediately(),
        ComponentAction::Panic,
        InjectionId::new(999),
        InjectionLog::new(),
    )
}

fn panic_on_touch_processor() -> InjectedProcessor<oxide_batch::item_components::IdentityProcessor>
{
    InjectedProcessor::new(
        oxide_batch::item_components::IdentityProcessor,
        Trigger::immediately(),
        ComponentAction::Panic,
        InjectionId::new(998),
        InjectionLog::new(),
    )
}

// ---------------------------------------------------------------------
// ChainProcessor: ordering, filter/stop short-circuit, failure propagation
// ---------------------------------------------------------------------

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

#[tokio::test]
async fn chain_processor_feeds_first_output_into_second() {
    let fixture = ComponentFixture::new();
    let evens = oxide_batch::item_components::FilterProcessor::new(|item: &i64| item % 2 == 0);
    let chain: ChainProcessor<_, _, i64> = ChainProcessor::new(evens, Double);
    assert_eq!(
        chain.process(&4, fixture.process_context()).await,
        Ok(ProcessOutcome::Item(8))
    );
}

#[tokio::test]
async fn chain_processor_filtered_first_stage_short_circuits_second() {
    let fixture = ComponentFixture::new();
    let evens = oxide_batch::item_components::FilterProcessor::new(|item: &i64| item % 2 == 0);
    let chain: ChainProcessor<_, _, i64> = ChainProcessor::new(evens, panic_on_touch_processor());
    assert_eq!(
        chain.process(&5, fixture.process_context()).await,
        Ok(ProcessOutcome::Filtered),
        "a filtered first stage must never invoke the second"
    );
}

#[tokio::test]
async fn chain_processor_first_stage_failure_propagates_unchanged() {
    let fixture = ComponentFixture::new();
    let log = InjectionLog::new();
    let failing = InjectedProcessor::new(
        oxide_batch::item_components::IdentityProcessor,
        Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::UserComponent),
        InjectionId::new(3),
        log,
    );
    let chain: ChainProcessor<_, _, i64> = ChainProcessor::new(failing, panic_on_touch_processor());
    assert_eq!(
        chain.process(&1, fixture.process_context()).await,
        Err(ProcessorError::with_category(
            FailureCategory::UserComponent
        ))
    );
}

#[tokio::test]
async fn chain_processor_second_stage_failure_propagates_unchanged() {
    let fixture = ComponentFixture::new();
    let log = InjectionLog::new();
    let failing_second = InjectedProcessor::new(
        Double,
        Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::Timeout),
        InjectionId::new(4),
        log,
    );
    let chain: ChainProcessor<_, _, i64> = ChainProcessor::new(
        oxide_batch::item_components::IdentityProcessor,
        failing_second,
    );
    assert_eq!(
        chain.process(&1, fixture.process_context()).await,
        Err(ProcessorError::with_category(FailureCategory::Timeout))
    );
}

// ---------------------------------------------------------------------
// FanOutWriter: transaction reborrow, ordering, and failure short-circuit
// ---------------------------------------------------------------------

/// A `BusinessTransaction` that records every executed statement's text, in
/// call order, so a test can prove delegates wrote sequentially through the
/// same enlisted transaction rather than each opening its own.
#[derive(Default)]
struct RecordingTransaction {
    statements: Arc<Mutex<Vec<String>>>,
}

impl BusinessTransaction for RecordingTransaction {
    fn execute<'a>(
        &'a mut self,
        statement: BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        let statements = Arc::clone(&self.statements);
        let text = statement.text().to_owned();
        Box::pin(async move {
            statements.lock().unwrap().push(text);
            Ok(BusinessWriteResult::new(1))
        })
    }
}

struct RecordingWriter(&'static str);

impl ItemWriter<i64> for RecordingWriter {
    async fn write(
        &self,
        _items: &[i64],
        mut context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        if let Some(transaction) = context.transaction() {
            transaction
                .execute(BusinessStatement::new(self.0, &[]))
                .await
                .map_err(WriterError::from_error)?;
        }
        Ok(WriteOutcome::Written)
    }
}

#[tokio::test]
async fn fan_out_writer_reborrows_the_same_transaction_sequentially_in_order() {
    let fixture = ComponentFixture::new();
    let writer = FanOutWriter::new(vec![RecordingWriter("first"), RecordingWriter("second")]);
    let mut transaction = RecordingTransaction::default();
    let statements = Arc::clone(&transaction.statements);
    let stop = fixture.stop_token();
    let context = WriteContext::enlisted(stop, &mut transaction);
    assert_eq!(
        writer.write(&[1, 2, 3], context).await,
        Ok(WriteOutcome::Written)
    );
    assert_eq!(
        *statements.lock().unwrap(),
        vec!["first".to_owned(), "second".to_owned()],
        "both delegates must write through the one reborrowed transaction, in order"
    );
}

struct FailingWriter;

impl ItemWriter<i64> for FailingWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        Err(WriterError::with_category(FailureCategory::Timeout))
    }
}

struct PanicOnTouchWriter;

impl ItemWriter<i64> for PanicOnTouchWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        panic!("fan-out writer must not invoke a delegate after an earlier one failed");
    }
}

#[tokio::test]
async fn fan_out_writer_failure_short_circuits_remaining_delegates() {
    let fixture = ComponentFixture::new();
    let writer = FanOutWriter::new(vec![
        BoxedWriter::new(FailingWriter),
        BoxedWriter::new(PanicOnTouchWriter),
    ]);
    let outcome = writer.write(&[1], fixture.write_context()).await;
    assert_eq!(
        outcome,
        Err(WriterError::with_category(FailureCategory::Timeout))
    );
}

#[tokio::test]
async fn fan_out_writer_stop_short_circuits_before_any_delegate() {
    let fixture = ComponentFixture::new();
    fixture.request_stop();
    let writer = FanOutWriter::new(vec![BoxedWriter::new(PanicOnTouchWriter)]);
    assert_eq!(
        writer.write(&[1], fixture.write_context()).await,
        Ok(WriteOutcome::Stopped)
    );
}
