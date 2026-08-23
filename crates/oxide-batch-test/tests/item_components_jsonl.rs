//! #148 JSON Lines component evidence: basic contracts (A), line-boundary
//! edge semantics (B), and typed/erased equivalence through the real
//! production chunk runtime (G).
//!
//! Restart (C/D) and bounded-memory (F) evidence live elsewhere: restart
//! requires a real durable `PostgreSQL` fixture (`postgres_json_restart.rs`),
//! and bounded memory needs an allocator-instrumented binary that cannot
//! also link `oxide-batch-test`
//! (`crates/oxide-batch/tests/item_components_json_allocation.rs`).

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{
    JsonLinesFormat, JsonLinesTerminator, jsonl_reader, jsonl_writer,
};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkSize,
    ComponentStreamIdentity, FailureCategory, ItemReader, ItemWriter, ReadContext, ReadOutcome,
    ReaderError, WriteOutcome, WriterError,
};
use oxide_batch_test::{ComponentFixture, TestStep};
use serde_json::{Value, json};

fn identity() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("oxide-batch-test.jsonl").expect("static identity is valid")
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
    std::env::temp_dir().join(format!("oxide-batch-148-jsonl-{name}-{nonce}.jsonl"))
}

// --------------------------------------------------------------- A: basic --

#[tokio::test]
async fn jsonl_reader_produces_expected_heterogeneous_values_and_eof() {
    // One object, one scalar, one string with an escaped quote and an
    // escaped newline (both JSON escapes on a single physical line, never a
    // raw newline byte -- a JSONL record cannot contain one), and one
    // nested array/object -- exercising every JSON value shape on its own
    // line.
    let source = Cursor::new(
        b"{\"a\":1,\"b\":\"x\"}\n42\n\"say \\\"hi\\\"\\nbye\"\n[1,{\"x\":[2,3]}]\n".to_vec(),
    );
    let (mut reader, _stream, _contract) =
        jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(first) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the object record");
    };
    assert_eq!(first, json!({"a": 1, "b": "x"}));

    let ReadOutcome::Item(second) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the scalar record");
    };
    assert_eq!(second, json!(42));

    let ReadOutcome::Item(third) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the string record");
    };
    assert_eq!(third, json!("say \"hi\"\nbye"));

    let ReadOutcome::Item(fourth) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the nested record");
    };
    assert_eq!(fourth, json!([1, {"x": [2, 3]}]));

    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn jsonl_writer_produces_exact_expected_bytes_one_record_per_line() {
    let path = temp_path("writer-exact-bytes");
    let (writer, _stream, _contract) =
        jsonl_writer(&path, JsonLinesFormat::new(), identity()).unwrap();
    let fixture = ComponentFixture::new();
    let items = vec![json!({"a": 1}), json!("b"), json!([1, 2])];

    let outcome = writer.write(&items, fixture.write_context()).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Written);

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"{\"a\":1}\n\"b\"\n[1,2]\n".to_vec());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------- B: line edges --

#[tokio::test]
async fn crlf_and_lf_terminators_parse_identically() {
    let fixture = ComponentFixture::new();

    let (mut lf_reader, _s, _c) = jsonl_reader::<Value, _>(
        Cursor::new(b"1\n2\n".to_vec()),
        JsonLinesFormat::new(),
        identity(),
    );
    let (mut crlf_reader, _s, _c) = jsonl_reader::<Value, _>(
        Cursor::new(b"1\r\n2\r\n".to_vec()),
        JsonLinesFormat::new(),
        identity(),
    );

    for reader in [&mut lf_reader, &mut crlf_reader] {
        let ReadOutcome::Item(first) = read_next(reader, fixture.read_context()).await.unwrap()
        else {
            panic!("expected first record");
        };
        assert_eq!(first, json!(1));
        let ReadOutcome::Item(second) = read_next(reader, fixture.read_context()).await.unwrap()
        else {
            panic!("expected second record");
        };
        assert_eq!(second, json!(2));
        assert_eq!(
            read_next(reader, fixture.read_context()).await.unwrap(),
            ReadOutcome::EndOfInput
        );
    }
}

#[tokio::test]
async fn final_line_without_a_terminator_is_still_a_record() {
    let source = Cursor::new(b"1\n2".to_vec());
    let (mut reader, _s, _c) = jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let ReadOutcome::Item(second) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("a final line with no trailing terminator must still be delivered as a record");
    };
    assert_eq!(second, json!(2));
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn a_trailing_terminator_produces_no_phantom_empty_record() {
    let source = Cursor::new(b"1\n2\n".to_vec());
    let (mut reader, _s, _c) = jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput,
        "a file ending exactly after a terminator must not produce a phantom trailing record"
    );
}

#[tokio::test]
async fn whitespace_around_the_value_inside_a_line_is_tolerated() {
    let source = Cursor::new(b"  { \"a\" : 1 }  \n".to_vec());
    let (mut reader, _s, _c) = jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(value) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record despite surrounding whitespace");
    };
    assert_eq!(value, json!({"a": 1}));
}

#[tokio::test]
async fn an_empty_line_is_a_classified_malformed_failure() {
    // JSONL's grammar is one JSON value per line; an empty line is not a
    // valid JSON value, so it is a malformed record, not silently skipped.
    let source = Cursor::new(b"1\n\n2\n".to_vec());
    let (mut reader, _s, _c) = jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let _ = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("an empty line must be a classified failure, not a silently skipped blank");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(error.has_checkpoint_advanced());

    let ReadOutcome::Item(value) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("the record after the malformed line must still be reachable");
    };
    assert_eq!(value, json!(2));
}

#[tokio::test]
async fn a_syntactically_invalid_line_is_a_classified_failure_with_forward_progress() {
    let source = Cursor::new(b"1\n{not json}\n2\n".to_vec());
    let (mut reader, _s, _c) = jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(_first) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the first well-formed record");
    };

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("malformed JSON on a line must be a typed failure");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(
        error.has_checkpoint_advanced(),
        "the line's bytes were already consumed through its terminator, so forward progress is \
         provable regardless of whether its content parses"
    );

    let ReadOutcome::Item(third) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!(
            "the record after the malformed line must be reachable, proving the reader did \
                not lose its place"
        );
    };
    assert_eq!(third, json!(2));
}

#[tokio::test]
async fn a_line_exceeding_the_configured_bound_fails_closed() {
    let bytes = b"\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n".to_vec();
    let fixture = ComponentFixture::new();

    let generous = JsonLinesFormat::new().with_max_record_bytes(1024);
    let (mut generous_reader, _s, _c) =
        jsonl_reader::<Value, _>(Cursor::new(bytes.clone()), generous, identity());
    assert!(
        read_next(&mut generous_reader, fixture.read_context())
            .await
            .is_ok(),
        "a generous bound must accept this line"
    );

    let tight = JsonLinesFormat::new().with_max_record_bytes(4);
    let (mut tight_reader, _s, _c) =
        jsonl_reader::<Value, _>(Cursor::new(bytes), tight, identity());
    let error = read_next(&mut tight_reader, fixture.read_context())
        .await
        .expect_err("a line exceeding the configured bound must fail closed");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(error.has_checkpoint_advanced());
}

#[tokio::test]
async fn crlf_writer_emits_crlf_terminators() {
    let path = temp_path("writer-crlf");
    let format = JsonLinesFormat::new().with_terminator(JsonLinesTerminator::CrLf);
    let (writer, _stream, _contract) = jsonl_writer(&path, format, identity()).unwrap();
    let fixture = ComponentFixture::new();

    writer
        .write(&[json!(1), json!(2)], fixture.write_context())
        .await
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"1\r\n2\r\n".to_vec());
    let _ = std::fs::remove_file(&path);
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
async fn typed_and_erased_jsonl_pipelines_produce_identical_items() {
    let bytes = b"1\n\"a\"\n{\"k\":2}\n[3,4]\nnull\n".to_vec();

    let (typed_reader, _s, _c) = jsonl_reader::<Value, _>(
        Cursor::new(bytes.clone()),
        JsonLinesFormat::new(),
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
        jsonl_reader::<Value, _>(Cursor::new(bytes), JsonLinesFormat::new(), identity());
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
