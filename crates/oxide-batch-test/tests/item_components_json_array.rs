//! #148 streaming top-level JSON-array component evidence: basic contracts
//! (A), framing edge semantics that distinguish a real parser from naive
//! comma scanning (B), an in-memory element-boundary restart proof, and
//! typed/erased equivalence through the real production chunk runtime (G).
//!
//! Durable restart (C/D) and bounded-memory (F) evidence live elsewhere:
//! durable restart requires a real `PostgreSQL` fixture
//! (`postgres_json_restart.rs`), and bounded memory needs an
//! allocator-instrumented binary that cannot also link `oxide-batch-test`
//! (`crates/oxide-batch/tests/item_components_json_allocation.rs`).

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{JsonArrayFormat, json_array_reader, json_array_writer};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkSize,
    ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity, FailureCategory,
    ItemReader, ItemStream, ItemWriter, ReadContext, ReadOutcome, ReaderError, WriteOutcome,
    WriterError,
};
use oxide_batch_test::{ComponentFixture, TestStep};
use serde_json::{Value, json};

fn identity() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("oxide-batch-test.json-array").expect("static identity is valid")
}

async fn read_next(
    reader: &mut (impl ItemReader<Value> + Unpin),
    context: ReadContext<'_>,
) -> Result<ReadOutcome<Value>, ReaderError> {
    reader.read(context).await
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-148-json-array-{name}-{nonce}.json"))
}

fn reader_checkpoint_byte(envelope: &ComponentStateEnvelope) -> u64 {
    let ComponentStatePayload::Inline(payload) = envelope.payload().unwrap() else {
        panic!("reader checkpoints must use an inline payload");
    };
    serde_json::from_slice::<Value>(&payload)
        .unwrap()
        .get("byte")
        .and_then(Value::as_u64)
        .expect("reader checkpoint byte")
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceEvent {
    Seek(u64),
    Read { start: u64, end: u64 },
}

struct TracedSource {
    inner: Cursor<Vec<u8>>,
    events: Arc<Mutex<Vec<SourceEvent>>>,
}

impl TracedSource {
    fn new(bytes: Vec<u8>) -> (Self, Arc<Mutex<Vec<SourceEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: Cursor::new(bytes),
                events: Arc::clone(&events),
            },
            events,
        )
    }
}

impl Read for TracedSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let start = self.inner.position();
        let read = self.inner.read(buf)?;
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(SourceEvent::Read {
                start,
                end: start + read as u64,
            });
        Ok(read)
    }
}

impl Seek for TracedSource {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let target = self.inner.seek(position)?;
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(SourceEvent::Seek(target));
        Ok(target)
    }
}

// --------------------------------------------------------------- A: basic --

#[tokio::test]
async fn json_array_reader_produces_expected_heterogeneous_values_in_order_and_eof() {
    let source = Cursor::new(br#"[1,"s",{"a":1},[1,2],null,true,false]"#.to_vec());
    let (mut reader, _stream, _contract) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let expected = [
        json!(1),
        json!("s"),
        json!({"a": 1}),
        json!([1, 2]),
        json!(null),
        json!(true),
        json!(false),
    ];
    for want in &expected {
        let ReadOutcome::Item(got) = read_next(&mut reader, fixture.read_context())
            .await
            .unwrap()
        else {
            panic!("expected element {want:?}");
        };
        assert_eq!(&got, want);
    }
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn json_array_writer_produces_exact_expected_bytes() {
    let path = temp_path("writer-exact-bytes");
    let (writer, stream, _contract) = json_array_writer(&path, identity()).unwrap();
    let fixture = ComponentFixture::new();
    stream
        .open(fixture.stream_open_context(None))
        .await
        .unwrap();

    writer
        .write(
            &[json!(1), json!("b"), json!([3, 4])],
            fixture.write_context(),
        )
        .await
        .unwrap();
    stream
        .close(fixture.stream_close_context(oxide_batch::StreamRuntimeOutcome::Committed))
        .await
        .unwrap();

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, br#"[1,"b",[3,4]]"#.to_vec());
    let reparsed: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reparsed, json!([1, "b", [3, 4]]));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn empty_array_produces_immediate_eof() {
    let source = Cursor::new(b"[]".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn writer_produces_a_valid_empty_array_with_no_items() {
    let path = temp_path("writer-empty");
    let (_writer, stream, _contract) = json_array_writer(&path, identity()).unwrap();
    let fixture = ComponentFixture::new();
    stream
        .open(fixture.stream_open_context(None))
        .await
        .unwrap();
    stream
        .close(fixture.stream_close_context(oxide_batch::StreamRuntimeOutcome::Committed))
        .await
        .unwrap();

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"[]".to_vec());
    let _ = std::fs::remove_file(&path);
}

// -------------------------------------------------------- B: framing edges --

#[tokio::test]
async fn pretty_printed_input_with_newlines_and_extra_whitespace_parses_correctly() {
    let source = Cursor::new(b"[\n  1,\n  {\n    \"a\": 2\n  },\n  [3, 4]\n]\n".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    for want in [json!(1), json!({"a": 2}), json!([3, 4])] {
        let ReadOutcome::Item(got) = read_next(&mut reader, fixture.read_context())
            .await
            .unwrap()
        else {
            panic!("expected {want:?}");
        };
        assert_eq!(got, want);
    }
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn long_number_crossing_growth_bound_is_one_element_and_restartable() {
    let number = format!("1.{}2", "0".repeat(1024));
    let source = format!("[{number},2]").into_bytes();
    let expected_number: Value = serde_json::from_str(&number).unwrap();
    let fixture = ComponentFixture::new();
    let (mut reader_a, stream_a, _c) = json_array_reader::<Value, _>(
        Cursor::new(source.clone()),
        JsonArrayFormat::new(),
        identity(),
    );

    let ReadOutcome::Item(first) = read_next(&mut reader_a, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("the long number must be emitted as one complete element");
    };
    assert_eq!(first, expected_number);
    let checkpoint = stream_a
        .update(fixture.stream_update_context())
        .await
        .unwrap();
    let expected_boundary = 1 + number.len() as u64;
    assert_eq!(
        reader_checkpoint_byte(&checkpoint),
        expected_boundary,
        "the checkpoint must end after the complete number, not at an initial growth boundary"
    );

    let (mut reader_b, stream_b, _c) =
        json_array_reader::<Value, _>(Cursor::new(source), JsonArrayFormat::new(), identity());
    stream_b
        .open(fixture.stream_open_context(Some(&checkpoint)))
        .await
        .unwrap();
    assert_eq!(
        read_next(&mut reader_b, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!(2))
    );
    assert_eq!(
        read_next(&mut reader_b, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn trailing_garbage_after_top_level_array_is_rejected_but_json_whitespace_is_allowed() {
    let fixture = ComponentFixture::new();
    for source in [b"[]".as_slice(), b"[]\n", b"[1]    ", b"[1]\r\n\t"] {
        let (mut reader, _s, _c) = json_array_reader::<Value, _>(
            Cursor::new(source.to_vec()),
            JsonArrayFormat::new(),
            identity(),
        );
        assert_eq!(
            read_next(&mut reader, fixture.read_context())
                .await
                .unwrap(),
            if source.starts_with(b"[]") {
                ReadOutcome::EndOfInput
            } else {
                ReadOutcome::Item(json!(1))
            }
        );
        assert_eq!(
            read_next(&mut reader, fixture.read_context())
                .await
                .unwrap(),
            ReadOutcome::EndOfInput
        );
    }

    for source in [b"[]x".as_slice(), b"[1]garbage", b"[1] {}", b"[1][2]"] {
        let (mut reader, _s, _c) = json_array_reader::<Value, _>(
            Cursor::new(source.to_vec()),
            JsonArrayFormat::new(),
            identity(),
        );
        let result = read_next(&mut reader, fixture.read_context()).await;
        let error = if source.starts_with(b"[]") {
            result.expect_err("trailing garbage must fail the empty array")
        } else {
            assert_eq!(result.unwrap(), ReadOutcome::Item(json!(1)));
            read_next(&mut reader, fixture.read_context())
                .await
                .expect_err("trailing garbage must fail after the final element")
        };
        assert_eq!(error.category(), FailureCategory::UserComponent);
        assert!(!error.has_checkpoint_advanced());
    }
}

/// Counts top-level, comma-separated segments the way a naive, quote-unaware
/// implementation might: every raw comma byte is a separator, full stop.
/// This is the exact class of bug streaming array-element framing must not
/// have.
#[allow(
    clippy::naive_bytecount,
    reason = "this IS the naive baseline the test exists to disprove"
)]
fn naive_comma_count(inner: &[u8]) -> usize {
    inner.iter().filter(|&&byte| byte == b',').count() + 1
}

#[tokio::test]
async fn delimiters_inside_strings_do_not_affect_framing_and_a_naive_scan_would_be_fooled() {
    // Three real elements, each an escaped string containing a literal comma
    // (and, for the second and third, a literal `]`/`"` too) -- exactly the
    // shape a naive, quote-unaware comma/bracket scan cannot parse.
    let source_bytes = br#"["a,b","c]d","e\"f,g"]"#.to_vec();
    let inner = &source_bytes[1..source_bytes.len() - 1];

    let naive_count = naive_comma_count(inner);
    assert_ne!(
        naive_count, 3,
        "sanity: a naive comma scan must be fooled by the embedded delimiters, or this fixture \
         does not exercise the claim"
    );

    let (mut reader, _s, _c) = json_array_reader::<Value, _>(
        Cursor::new(source_bytes),
        JsonArrayFormat::new(),
        identity(),
    );
    let fixture = ComponentFixture::new();
    let mut got = Vec::new();
    loop {
        match read_next(&mut reader, fixture.read_context())
            .await
            .unwrap()
        {
            ReadOutcome::Item(value) => got.push(value),
            ReadOutcome::EndOfInput => break,
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    assert_eq!(
        got,
        vec![json!("a,b"), json!("c]d"), json!("e\"f,g")],
        "the real reader must recover exactly the three elements the naive scan could not"
    );
}

#[tokio::test]
async fn missing_closing_bracket_is_unrecoverable_and_fails_closed() {
    let source = Cursor::new(b"[1,2".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a truncated array missing its closing bracket must fail closed");
    assert!(
        !error.has_checkpoint_advanced(),
        "unrecoverable array framing must not claim forward checkpoint proof, so a skip policy \
         cannot silently resynchronize past it"
    );
}

#[tokio::test]
async fn malformed_element_syntax_is_unrecoverable_and_fails_closed() {
    let source = Cursor::new(b"[1,not_json,3]".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a syntactically invalid element must fail closed");
    assert!(!error.has_checkpoint_advanced());
    let retry = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("retry must re-evaluate the same malformed element boundary");
    assert_eq!(retry.category(), FailureCategory::UserComponent);
    assert!(!retry.has_checkpoint_advanced());
}

#[tokio::test]
async fn missing_separator_between_elements_is_unrecoverable_and_fails_closed() {
    let source = Cursor::new(b"[1 2]".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("two elements with no comma between them must fail closed");
    assert!(!error.has_checkpoint_advanced());
}

#[tokio::test]
async fn an_element_exceeding_the_configured_bound_fails_closed() {
    let long_string = "x".repeat(200);
    let source_bytes = format!("[\"{long_string}\"]").into_bytes();
    let fixture = ComponentFixture::new();

    let generous = JsonArrayFormat::new().with_max_value_bytes(4096);
    let (mut generous_reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(source_bytes.clone()), generous, identity());
    assert!(
        read_next(&mut generous_reader, fixture.read_context())
            .await
            .is_ok(),
        "a generous bound must accept this element"
    );

    let tight = JsonArrayFormat::new().with_max_value_bytes(8);
    let (mut tight_reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(source_bytes), tight, identity());
    let error = read_next(&mut tight_reader, fixture.read_context())
        .await
        .expect_err("an element exceeding the configured bound must fail closed");
    assert!(!error.has_checkpoint_advanced());
}

/// A bounded-buffer boundary is not a value boundary: a non-self-delineated
/// value (a bare number, `true`, `false`, or `null`) can end exactly where a
/// growth step's buffer happens to end even though the real source keeps
/// going with more digits. Accepting a parser success at that point without
/// checking the *real* following byte would silently truncate `123` to `12`
/// -- a data-corrupting false success, not a bounded-memory rejection. This
/// is the exact scenario a naive "buffer ended, `serde_json` returned `Ok`,
/// so the value must be complete" implementation gets wrong.
#[tokio::test]
async fn a_number_exactly_at_the_growth_boundary_is_not_silently_truncated() {
    let fixture = ComponentFixture::new();

    // `123` is genuinely 3 raw bytes -- one more than the 2-byte bound -- so
    // it must be rejected, not accepted as truncated `12`.
    let format = JsonArrayFormat::new().with_max_value_bytes(2);
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(b"[123,4]".to_vec()), format, identity());
    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err(
            "a 3-byte number under a 2-byte bound must fail closed, never be silently \
             truncated to a shorter, different number",
        );
    assert!(
        !error.has_checkpoint_advanced(),
        "an unproven, possibly-truncated value must not advance the checkpoint"
    );

    // Positive control: `12` is genuinely a complete 2-byte number here --
    // the very next real source byte is the array's own comma -- so a
    // 2-byte bound must accept it. This is what distinguishes "the value is
    // actually longer than the bound" from "the bound coincides with where
    // the value legitimately ends."
    let format = JsonArrayFormat::new().with_max_value_bytes(2);
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(b"[12,4]".to_vec()), format, identity());
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!(12)),
        "a value that is genuinely exactly at the bound, followed immediately by the array's \
         own comma, must be accepted"
    );
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!(4))
    );
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn a_zero_byte_bound_rejects_every_element() {
    let fixture = ComponentFixture::new();
    let format = JsonArrayFormat::new().with_max_value_bytes(0);
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(b"[1]".to_vec()), format, identity());
    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err(
            "a zero-byte bound means no element is small enough to accept, including a \
             one-byte value -- it must not be silently accepted",
        );
    assert!(!error.has_checkpoint_advanced());
}

// ------------------------------------------------ in-memory restart proof --

/// Proves restart resumes at exactly the next element boundary -- never
/// re-reading the committed element, never skipping the following one --
/// through the real `ItemStream::open`/`update` calls a committing chunk
/// makes, without needing a durable `PostgreSQL` fixture (see
/// `postgres_json_restart.rs` for the same claim through a full
/// `ChunkStep`/`JobLauncher` restart, including proof against a naive
/// item-count/byte-zero-rescan implementation).
#[tokio::test]
async fn restart_resumes_at_the_next_element_boundary_not_byte_zero() {
    let bytes = br#"["first",{"nested":["a,b",2]},"third"]"#.to_vec();
    let fixture = ComponentFixture::new();

    let (mut reader_a, stream_a, _c) = json_array_reader::<Value, _>(
        Cursor::new(bytes.clone()),
        JsonArrayFormat::new(),
        identity(),
    );
    stream_a
        .open(fixture.stream_open_context(None))
        .await
        .unwrap();
    let ReadOutcome::Item(first) = read_next(&mut reader_a, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the first element");
    };
    assert_eq!(first, json!("first"));
    let ReadOutcome::Item(second) = read_next(&mut reader_a, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the second (nested) element");
    };
    assert_eq!(second, json!({"nested": ["a,b", 2]}));
    let envelope = stream_a
        .update(fixture.stream_update_context())
        .await
        .unwrap();

    let (mut reader_b, stream_b, _c) =
        json_array_reader::<Value, _>(Cursor::new(bytes), JsonArrayFormat::new(), identity());
    stream_b
        .open(fixture.stream_open_context(Some(&envelope)))
        .await
        .unwrap();
    let ReadOutcome::Item(third) = read_next(&mut reader_b, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!(
            "a restart must resume at the third element, not re-read the first two nor rescan \
             from byte zero"
        );
    };
    assert_eq!(third, json!("third"));
    assert_eq!(
        read_next(&mut reader_b, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn restart_instrumentation_observes_byte_zero_rescan_control_and_real_reader_avoids_it() {
    let bytes = br#"["first",{"nested":["a,b",2]},"third"]"#.to_vec();
    let fixture = ComponentFixture::new();

    let (source_a, _events_a) = TracedSource::new(bytes.clone());
    let (mut reader_a, stream_a, _c) =
        json_array_reader::<Value, _>(source_a, JsonArrayFormat::new(), identity());
    assert!(matches!(
        read_next(&mut reader_a, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(_)
    ));
    assert!(matches!(
        read_next(&mut reader_a, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(_)
    ));
    let checkpoint = stream_a
        .update(fixture.stream_update_context())
        .await
        .unwrap();
    let checkpoint_byte = reader_checkpoint_byte(&checkpoint);
    let committed_prefix = br#"["first",{"nested":["a,b",2]}"#;
    assert_eq!(checkpoint_byte, committed_prefix.len() as u64);

    let (source_b, events_b) = TracedSource::new(bytes.clone());
    let (mut reader_b, stream_b, _c) =
        json_array_reader::<Value, _>(source_b, JsonArrayFormat::new(), identity());
    stream_b
        .open(fixture.stream_open_context(Some(&checkpoint)))
        .await
        .unwrap();
    assert_eq!(
        read_next(&mut reader_b, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!("third"))
    );
    let events = events_b
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(
        events.contains(&SourceEvent::Seek(checkpoint_byte)),
        "a restored reader must seek to its persisted boundary: {events:?}"
    );
    assert!(
        events.iter().all(|event| match event {
            SourceEvent::Seek(target) => *target >= checkpoint_byte,
            SourceEvent::Read { start, end } => *start >= checkpoint_byte && *end >= *start,
        }),
        "restored reader must not seek or read before the checkpoint: {events:?}"
    );
    assert!(!events.contains(&SourceEvent::Seek(0)));

    let (mut naive, control_events) = TracedSource::new(bytes);
    naive.seek(SeekFrom::Start(0)).unwrap();
    let mut committed_prefix = vec![0_u8; usize::try_from(checkpoint_byte).unwrap()];
    naive.read_exact(&mut committed_prefix).unwrap();
    let control_events = control_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(
        control_events.contains(&SourceEvent::Seek(0)),
        "the positive control must actually observe its byte-zero seek: {control_events:?}"
    );
    assert!(
        control_events.iter().any(|event| matches!(
            event,
            SourceEvent::Read { start, end }
                if *start == 0 && *end == checkpoint_byte
        )),
        "the positive control must actually observe rereading the committed prefix: {control_events:?}"
    );
}

// -------------------------------------------------- G: typed/erased parity --

#[derive(Clone, Default)]
struct RecordingWriter(Arc<Mutex<Vec<Value>>>);

impl ItemWriter<Value> for RecordingWriter {
    async fn write(
        &self,
        items: &[Value],
        _context: oxide_batch::WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

#[tokio::test]
async fn typed_and_erased_json_array_pipelines_produce_identical_items() {
    let bytes = br#"[1,"a",{"k":2},[3,4],null]"#.to_vec();

    let (typed_reader, _s, _c) = json_array_reader::<Value, _>(
        Cursor::new(bytes.clone()),
        JsonArrayFormat::new(),
        identity(),
    );
    let typed_sink = RecordingWriter::default();
    let mut typed_step = TestStep::new(
        oxide_batch::StepName::new("typed").unwrap(),
        ChunkSize::new(2).unwrap(),
        typed_reader,
        IdentityProcessor,
        typed_sink.clone(),
    );
    let typed_report = typed_step.run().await;
    assert_eq!(typed_report.outcome(), ChunkExecutionOutcome::Completed);

    let (erased_reader, _s, _c) =
        json_array_reader::<Value, _>(Cursor::new(bytes), JsonArrayFormat::new(), identity());
    let erased_sink = RecordingWriter::default();
    let mut erased_step = TestStep::new(
        oxide_batch::StepName::new("erased").unwrap(),
        ChunkSize::new(2).unwrap(),
        BoxedReader::new(erased_reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(erased_sink.clone()),
    );
    let erased_report = erased_step.run().await;
    assert_eq!(erased_report.outcome(), ChunkExecutionOutcome::Completed);

    assert_eq!(
        typed_report.committed_counts().read().get(),
        erased_report.committed_counts().read().get(),
    );
    assert_eq!(typed_report.committed_counts().read().get(), 5);

    let typed_items = typed_sink
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let erased_items = erased_sink
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(typed_items, erased_items);
    assert_eq!(
        typed_items,
        vec![
            json!(1),
            json!("a"),
            json!({"k": 2}),
            json!([3, 4]),
            json!(null)
        ],
    );
}

/// Proves `JsonArrayWriter`'s comma-state decision, physical write, and
/// committed-count update form one coherent, serialized state transition --
/// not merely that the file `Mutex` serializes bytes while a separate
/// `committed_items` lock is read outside that same critical section.
///
/// This drives real OS threads (`std::thread::spawn`, not cooperative
/// `tokio` task scheduling) released simultaneously by a `Barrier`, so the
/// writer's `&self` (`ItemWriter` requires `Send + Sync`) is genuinely
/// exercised under concurrent, overlapping calls rather than sequential
/// `.await` interleaving. The assertion is deterministic and holds
/// regardless of which thread's write lands first: exactly `N` elements,
/// exactly `N - 1` commas, and a file that reparses as a valid JSON array
/// containing exactly the `N` submitted values. Before the fix, this
/// reliably (not occasionally) fails: every one of `N` threads can observe
/// `committed_items == 0` before any of them records its own write, because
/// that count was read outside the file lock that serializes the physical
/// bytes -- the concatenated output then contains zero commas at all.
const CONCURRENT_WRITER_COUNT: usize = 16;

#[allow(
    clippy::naive_bytecount,
    reason = "a handful of bytes in a test fixture; the bytecount crate is not a dependency here"
)]
fn count_commas(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&byte| byte == b',').count()
}

#[test]
fn concurrent_writes_to_a_fresh_array_never_lose_or_duplicate_a_comma() {
    let path = temp_path("writer-concurrency");
    let (writer, stream, _contract) = json_array_writer(&path, identity()).unwrap();
    let fixture = ComponentFixture::new();
    futures_executor::block_on(stream.open(fixture.stream_open_context(None))).unwrap();

    let writer = Arc::new(writer);
    let barrier = Arc::new(std::sync::Barrier::new(CONCURRENT_WRITER_COUNT));
    let mut handles = Vec::new();
    for i in 0..CONCURRENT_WRITER_COUNT {
        let writer = Arc::clone(&writer);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let fixture = ComponentFixture::new();
            futures_executor::block_on(writer.write(&[json!(i)], fixture.write_context())).unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    futures_executor::block_on(
        stream.close(fixture.stream_close_context(oxide_batch::StreamRuntimeOutcome::Committed)),
    )
    .unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let comma_count = count_commas(&bytes);
    assert_eq!(
        comma_count,
        CONCURRENT_WRITER_COUNT - 1,
        "exactly N-1 commas must separate N concurrently-written elements; \
         bytes: {}",
        String::from_utf8_lossy(&bytes),
    );
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "concurrent writes must still produce valid JSON: {error} (bytes: {})",
            String::from_utf8_lossy(&bytes)
        )
    });
    let Value::Array(items) = parsed else {
        panic!("expected a top-level array");
    };
    assert_eq!(
        items.len(),
        CONCURRENT_WRITER_COUNT,
        "exactly one element per concurrent write"
    );
    let mut got: Vec<u64> = items
        .iter()
        .map(|value| {
            value
                .as_u64()
                .expect("each element is one of the written indices")
        })
        .collect();
    got.sort_unstable();
    assert_eq!(got, (0..CONCURRENT_WRITER_COUNT as u64).collect::<Vec<_>>());

    let _ = std::fs::remove_file(&path);
}
