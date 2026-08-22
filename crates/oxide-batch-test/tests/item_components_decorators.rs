#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Peek, aggregate, and synchronization decorator contract tests (#146).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::{AggregatingReader, IterReader, PeekOutcome, PeekReader};
use oxide_batch::{
    ChunkSize, FailureCategory, ItemProcessor, ItemReader, ItemWriter, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::ComponentFixture;
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectionId, InjectionLog, Trigger,
};

// ---------------------------------------------------------------------
// PeekReader
// ---------------------------------------------------------------------

/// Counts real calls into the wrapped delegate's `read`, so a test can prove
/// repeated `peek` calls the delegate at most once.
struct CountingReader<R> {
    inner: R,
    calls: Arc<AtomicUsize>,
}

impl<I: 'static, R: ItemReader<I>> ItemReader<I> for CountingReader<R> {
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.read(context).await
    }
}

#[tokio::test]
async fn peek_returns_next_item_without_consuming_it() {
    let fixture = ComponentFixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reader = PeekReader::new(CountingReader {
        inner: IterReader::new(vec![1, 2, 3]),
        calls: Arc::clone(&calls),
    });
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::Item(&1))
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "peek called the delegate once"
    );
}

#[tokio::test]
async fn repeated_peek_is_stable_and_calls_the_delegate_once() {
    let fixture = ComponentFixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reader = PeekReader::new(CountingReader {
        inner: IterReader::new(vec![1, 2, 3]),
        calls: Arc::clone(&calls),
    });
    for _ in 0..5 {
        assert_eq!(
            reader.peek(fixture.read_context()).await,
            Ok(PeekOutcome::Item(&1))
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "five peeks without an intervening read must call the delegate exactly once"
    );
}

#[tokio::test]
async fn read_after_peek_consumes_the_buffered_item_exactly_once() {
    let fixture = ComponentFixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut reader = PeekReader::new(CountingReader {
        inner: IterReader::new(vec![1, 2, 3]),
        calls: Arc::clone(&calls),
    });
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::Item(&1))
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(1)),
        "read consumes exactly the peeked item"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(2)),
        "the following read continues from the real logical position"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "no item was read twice");
}

#[tokio::test]
async fn peek_end_of_input_is_stable() {
    let fixture = ComponentFixture::new();
    let mut reader = PeekReader::new(IterReader::new(Vec::<i64>::new()));
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::EndOfInput)
    );
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::EndOfInput)
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
}

#[tokio::test]
async fn peek_preserves_stop() {
    let fixture = ComponentFixture::new();
    let (stop_source, _unused) = oxide_batch::StopSource::new();
    let log = InjectionLog::new();
    let mut reader = PeekReader::new(InjectedReader::new(
        IterReader::new(vec![1]),
        Trigger::immediately(),
        ComponentAction::Stop(stop_source),
        InjectionId::new(10),
        log,
    ));
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::Stopped)
    );
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::Stopped),
        "a cached stop remains stable across repeated peeks"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Stopped)
    );
}

#[tokio::test]
async fn peek_failure_is_not_cached_and_retries_the_delegate() {
    let fixture = ComponentFixture::new();
    let log = InjectionLog::new();
    let mut reader = PeekReader::new(InjectedReader::new(
        IterReader::new(vec![7]),
        Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::Timeout),
        InjectionId::new(11),
        log,
    ));
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Err(ReaderError::with_category(FailureCategory::Timeout))
    );
    // The trigger already fired once (one-shot), so the retry reaches the
    // real delegate and succeeds -- proving the failed attempt was not
    // cached as a buffered outcome.
    assert_eq!(
        reader.peek(fixture.read_context()).await,
        Ok(PeekOutcome::Item(&7))
    );
}

// ---------------------------------------------------------------------
// AggregatingReader
// ---------------------------------------------------------------------

fn sum(group: Vec<i64>) -> i64 {
    group.into_iter().sum()
}

#[tokio::test]
async fn aggregate_emits_exactly_at_the_bound() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(3).unwrap();
    let mut reader = AggregatingReader::new(IterReader::new(vec![1, 2, 3, 4, 5, 6]), bound, sum);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(6)),
        "1+2+3"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(15)),
        "4+5+6"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
}

#[tokio::test]
async fn aggregate_emits_a_partial_final_group_then_stable_end_of_input() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(4).unwrap();
    let mut reader = AggregatingReader::new(IterReader::new(vec![1, 2, 3, 4, 5]), bound, sum);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(10)),
        "1+2+3+4"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Item(5)),
        "the final partial group of one item"
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
}

#[tokio::test]
async fn aggregate_empty_input_is_end_of_input_not_an_empty_group() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(4).unwrap();
    let mut reader = AggregatingReader::new(IterReader::new(Vec::<i64>::new()), bound, sum);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::EndOfInput)
    );
}

#[tokio::test]
async fn aggregate_failure_discards_the_partial_group_rather_than_truncating_it() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(4).unwrap();
    let log = InjectionLog::new();
    let failing = InjectedReader::new(
        IterReader::new(vec![1, 2, 3, 4]),
        Trigger::after(2),
        ComponentAction::Fail(FailureCategory::Timeout),
        InjectionId::new(12),
        log,
    );
    let mut reader = AggregatingReader::new(failing, bound, sum);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Err(ReaderError::with_category(FailureCategory::Timeout)),
        "two items were buffered when the third read failed; no truncated aggregate is emitted"
    );
}

#[tokio::test]
async fn aggregate_stop_discards_the_partial_group() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(4).unwrap();
    let (stop_source, _unused) = oxide_batch::StopSource::new();
    let log = InjectionLog::new();
    let stopping = InjectedReader::new(
        IterReader::new(vec![1, 2, 3, 4]),
        Trigger::after(2),
        ComponentAction::Stop(stop_source),
        InjectionId::new(13),
        log,
    );
    let mut reader = AggregatingReader::new(stopping, bound, sum);
    assert_eq!(
        reader.read(fixture.read_context()).await,
        Ok(ReadOutcome::Stopped)
    );
}

#[tokio::test]
async fn aggregate_never_buffers_beyond_its_bound() {
    let fixture = ComponentFixture::new();
    let bound = ChunkSize::new(2).unwrap();
    // A reader whose aggregation function asserts it never receives more
    // than `bound` items, proving the buffer is genuinely bounded rather
    // than merely usually small.
    let mut reader =
        AggregatingReader::new(IterReader::new(0..10_i64), bound, |group: Vec<i64>| {
            assert!(group.len() <= 2, "aggregate exceeded its configured bound");
            group.len()
        });
    let mut groups = Vec::new();
    loop {
        match reader.read(fixture.read_context()).await.unwrap() {
            ReadOutcome::Item(size) => groups.push(size),
            ReadOutcome::EndOfInput => break,
            _ => panic!("unexpected outcome"),
        }
    }
    assert_eq!(groups, vec![2, 2, 2, 2, 2]);
}

// ---------------------------------------------------------------------
// Synchronization wrappers
// ---------------------------------------------------------------------

struct RecordingProcessor(Arc<Mutex<Vec<i64>>>);

impl ItemProcessor<i64, i64> for RecordingProcessor {
    async fn process(
        &self,
        item: &i64,
        _context: oxide_batch::ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        self.0.lock().unwrap().push(*item);
        Ok(ProcessOutcome::Item(*item))
    }
}

#[tokio::test]
async fn synchronized_processor_serializes_concurrent_callers() {
    use oxide_batch::item_components::SynchronizedProcessor;

    let observed = Arc::new(Mutex::new(Vec::new()));
    let processor = Arc::new(SynchronizedProcessor::new(RecordingProcessor(Arc::clone(
        &observed,
    ))));
    let mut handles = Vec::new();
    for item in 0..8_i64 {
        let processor = Arc::clone(&processor);
        handles.push(tokio::spawn(async move {
            let (_source, local_stop) = oxide_batch::StopSource::new();
            let context = oxide_batch::ProcessContext::new(&local_stop);
            processor.process(&item, context).await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let mut seen = observed.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
}

struct RecordingWriter(Arc<Mutex<Vec<i64>>>);

impl ItemWriter<i64> for RecordingWriter {
    async fn write(
        &self,
        items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0.lock().unwrap().extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

#[tokio::test]
async fn synchronized_writer_delegates_and_preserves_the_write_context() {
    use oxide_batch::item_components::SynchronizedWriter;

    let observed = Arc::new(Mutex::new(Vec::new()));
    let writer = SynchronizedWriter::new(RecordingWriter(Arc::clone(&observed)));
    let fixture = ComponentFixture::new();
    assert_eq!(
        writer.write(&[1, 2, 3], fixture.write_context()).await,
        Ok(WriteOutcome::Written)
    );
    assert_eq!(*observed.lock().unwrap(), vec![1, 2, 3]);
}
