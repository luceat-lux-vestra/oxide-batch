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

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{JsonArrayFormat, json_array_reader, json_array_writer};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkSize,
    ComponentStreamIdentity, ItemReader, ItemStream, ItemWriter, ReadContext, ReadOutcome,
    ReaderError, WriteOutcome, WriterError,
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
}

#[tokio::test]
async fn missing_separator_between_elements_is_unrecoverable_and_fails_closed() {
    let source = Cursor::new(b"[1 2]".to_vec());
    let (mut reader, _s, _c) =
        json_array_reader::<Value, _>(source, JsonArrayFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
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
