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

use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{
    JsonLinesFormat, JsonLinesTerminator, jsonl_reader, jsonl_writer,
};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkSize,
    ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity, FailureCategory,
    ItemReader, ItemStream, ItemWriter, ReadContext, ReadOutcome, ReaderError, WriteOutcome,
    WriterError,
};
use oxide_batch_test::{ComponentFixture, TestStep};
use serde_json::{Value, json};

fn identity() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("oxide-batch-test.jsonl").expect("static identity is valid")
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

/// A deterministic, injected-fault `Read + Seek` source: never a timing- or
/// stress-only double. Each fault fires exactly once, at the exact
/// semantic point named (a seek to a specific target byte, or a read whose
/// source position is a specific byte), then clears itself so the *next*
/// identical operation succeeds -- modeling a transient infrastructure
/// hiccup that a retry genuinely recovers from, not a permanently broken
/// source.
#[derive(Clone, Debug, Eq, PartialEq)]
enum IoEvent {
    Seek(u64),
    Read { start: u64, len: usize },
}

struct FaultyIo {
    inner: Cursor<Vec<u8>>,
    max_chunk: usize,
    fail_seek_once_at: Option<u64>,
    fail_read_once_at: Option<u64>,
    events: Arc<Mutex<Vec<IoEvent>>>,
}

impl FaultyIo {
    fn new(bytes: Vec<u8>) -> (Self, Arc<Mutex<Vec<IoEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: Cursor::new(bytes),
                max_chunk: usize::MAX,
                fail_seek_once_at: None,
                fail_read_once_at: None,
                events: Arc::clone(&events),
            },
            events,
        )
    }

    fn with_max_chunk(mut self, max_chunk: usize) -> Self {
        self.max_chunk = max_chunk;
        self
    }

    fn fail_seek_once_at(mut self, position: u64) -> Self {
        self.fail_seek_once_at = Some(position);
        self
    }

    fn fail_read_once_at(mut self, position: u64) -> Self {
        self.fail_read_once_at = Some(position);
        self
    }
}

impl Read for FaultyIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let start = self.inner.position();
        if self.fail_read_once_at == Some(start) {
            self.fail_read_once_at = None;
            return Err(io::Error::other("injected read failure"));
        }
        let cap = self.max_chunk.min(buf.len());
        let read = self.inner.read(&mut buf[..cap])?;
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(IoEvent::Read { start, len: read });
        Ok(read)
    }
}

impl Seek for FaultyIo {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        if let SeekFrom::Start(target) = position
            && self.fail_seek_once_at == Some(target)
        {
            self.fail_seek_once_at = None;
            return Err(io::Error::other("injected seek failure"));
        }
        let target = self.inner.seek(position)?;
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(IoEvent::Seek(target));
        Ok(target)
    }
}

// ------------------------------------------------------- retry-safety --

/// A failed restart seek is transient infrastructure, not a data problem,
/// and must never leave the reader trusting an unconfirmed position: the
/// next call must re-seek to the *same* authoritative checkpoint and
/// resume there -- never at byte zero, never skipping or duplicating a
/// committed record.
#[tokio::test]
async fn a_failed_restart_seek_is_transient_and_the_retry_reseeks_to_the_checkpoint() {
    let bytes = b"1\n2\n3\n".to_vec();
    let fixture = ComponentFixture::new();

    // Establish a real checkpoint the normal way: read record "1", then ask
    // the stream for the envelope a committing chunk would persist.
    let (mut warm_reader, warm_stream, _c) = jsonl_reader::<Value, _>(
        Cursor::new(bytes.clone()),
        JsonLinesFormat::new(),
        identity(),
    );
    warm_stream
        .open(fixture.stream_open_context(None))
        .await
        .unwrap();
    assert_eq!(
        read_next(&mut warm_reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!(1))
    );
    let envelope = warm_stream
        .update(fixture.stream_update_context())
        .await
        .unwrap();
    let checkpoint = reader_checkpoint_byte(&envelope);
    assert_eq!(checkpoint, 2, "checkpoint must be exactly after \"1\\n\"");

    let (source, events) = FaultyIo::new(bytes);
    let source = source.fail_seek_once_at(checkpoint);
    let (mut reader, stream, _c) =
        jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    stream
        .open(fixture.stream_open_context(Some(&envelope)))
        .await
        .unwrap();

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("the injected restart-seek failure must surface, not be silently absorbed");
    assert_eq!(error.category(), FailureCategory::TransientInfrastructure);

    let value = read_next(&mut reader, fixture.read_context())
        .await
        .expect("the retry's seek is no longer failing, so this must succeed");
    assert_eq!(
        value,
        ReadOutcome::Item(json!(2)),
        "the retry must resume at the checkpoint's own record, never byte-zero data"
    );

    let seeks_to_checkpoint = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|event| **event == IoEvent::Seek(checkpoint))
        .count();
    assert!(
        seeks_to_checkpoint >= 1,
        "the retry must have actually reissued a seek to the authoritative checkpoint byte, \
         not merely continued from wherever the failed attempt left the source"
    );
}

/// A read that fails partway through a record (after some of that record's
/// bytes are already buffered) must not leave the reader resuming from
/// that mid-record position. The retry must re-seek to the record's own
/// starting checkpoint and return the complete, original record exactly
/// once -- never a truncated or corrupted remainder.
#[tokio::test]
async fn a_mid_record_read_failure_is_transient_and_the_retry_returns_the_complete_record_once() {
    // "1\n" (2 bytes, checkpoint after it = 2) then `"hello"` (7 bytes) + \n.
    let bytes = b"1\n\"hello\"\n".to_vec();
    let fixture = ComponentFixture::new();

    // Force the second record's read to happen two bytes at a time, and
    // fail the call whose source position is 4 -- i.e. after `"h` (2 bytes
    // into the record) has already been read into a partial line buffer,
    // but before the record completes.
    let (source, events) = FaultyIo::new(bytes);
    let source = source.with_max_chunk(2).fail_read_once_at(4);
    let (mut reader, stream, _c) =
        jsonl_reader::<Value, _>(source, JsonLinesFormat::new(), identity());
    stream
        .open(fixture.stream_open_context(None))
        .await
        .unwrap();

    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::Item(json!(1))
    );

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("the injected mid-record read failure must surface");
    assert_eq!(error.category(), FailureCategory::TransientInfrastructure);

    let value = read_next(&mut reader, fixture.read_context())
        .await
        .expect("the retry's reads are no longer failing, so this must succeed");
    assert_eq!(
        value,
        ReadOutcome::Item(json!("hello")),
        "the retry must return the complete original record exactly once, not a truncated \
         remainder starting from the partially-buffered \"h"
    );
    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );

    let seeks_to_checkpoint = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|event| **event == IoEvent::Seek(2))
        .count();
    assert!(
        seeks_to_checkpoint >= 1,
        "the retry must have actually reissued a seek to the record's starting checkpoint (byte \
         2), discarding whatever was partially buffered from the failed attempt"
    );
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

fn json_string_with_raw_length(length: usize) -> (Vec<u8>, String) {
    assert!(
        length >= 2,
        "a JSON string needs opening and closing quotes"
    );
    let value = "x".repeat(length - 2);
    (format!("\"{value}\"").into_bytes(), value)
}

#[tokio::test]
async fn max_record_bytes_excludes_lf_and_crlf_terminators() {
    const MAX_RECORD_BYTES: usize = 4096;
    let fixture = ComponentFixture::new();

    for terminator in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let (payload, value) = json_string_with_raw_length(MAX_RECORD_BYTES);
        let mut source = payload;
        source.extend_from_slice(terminator);
        let (mut reader, _s, _c) = jsonl_reader::<Value, _>(
            Cursor::new(source),
            JsonLinesFormat::new().with_max_record_bytes(MAX_RECORD_BYTES),
            identity(),
        );
        assert_eq!(
            read_next(&mut reader, fixture.read_context())
                .await
                .unwrap(),
            ReadOutcome::Item(Value::String(value))
        );
        assert_eq!(
            read_next(&mut reader, fixture.read_context())
                .await
                .unwrap(),
            ReadOutcome::EndOfInput
        );
    }
}

#[tokio::test]
async fn max_record_bytes_rejects_one_extra_payload_byte_for_lf_and_crlf() {
    const MAX_RECORD_BYTES: usize = 4096;
    let fixture = ComponentFixture::new();

    for terminator in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let (payload, _value) = json_string_with_raw_length(MAX_RECORD_BYTES + 1);
        let mut source = payload;
        source.extend_from_slice(terminator);
        let (mut reader, _s, _c) = jsonl_reader::<Value, _>(
            Cursor::new(source),
            JsonLinesFormat::new().with_max_record_bytes(MAX_RECORD_BYTES),
            identity(),
        );
        let error = read_next(&mut reader, fixture.read_context())
            .await
            .expect_err("one payload byte over the bound must fail");
        assert_eq!(error.category(), FailureCategory::UserComponent);
        assert!(error.has_checkpoint_advanced());
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
