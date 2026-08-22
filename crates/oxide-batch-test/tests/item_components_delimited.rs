//! #147 delimited/CSV component evidence: basic contracts (A), CSV edge
//! semantics that distinguish a real parser from naive line splitting (B),
//! and typed/erased equivalence through the real production chunk runtime
//! (G).
//!
//! Restart (C/D) and bounded-memory (F) evidence live elsewhere: restart
//! requires a real durable `PostgreSQL` fixture
//! (`postgres_flat_file_restart.rs`), and bounded memory needs an
//! allocator-instrumented binary that cannot also link `oxide-batch-test`
//! (`crates/oxide-batch/tests/item_components_flat_file_allocation.rs`).

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{
    DelimitedDialect, DelimitedRecord, delimited_reader, delimited_writer,
};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkSize,
    ComponentStreamIdentity, FailureCategory, ItemReader, ItemStream, ItemWriter, ReadContext,
    ReadOutcome, ReaderError, WriteOutcome, WriterError,
};
use oxide_batch_test::{ComponentFixture, TestStep};

fn identity() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("oxide-batch-test.delimited").expect("static identity is valid")
}

/// Pins the reader's item type to [`DelimitedRecord`] so a bare call site
/// does not need a turbofish: `DelimitedReader<Src>` implements
/// `ItemReader<I>` for any `I: From<DelimitedRecord>`, so `I` is otherwise
/// ambiguous at the call site.
async fn read_next(
    reader: &mut (impl ItemReader<DelimitedRecord> + Unpin),
    context: ReadContext<'_>,
) -> Result<ReadOutcome<DelimitedRecord>, ReaderError> {
    reader.read(context).await
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-147-{name}-{nonce}.csv"))
}

// --------------------------------------------------------------- A: basic --

#[tokio::test]
async fn delimited_reader_produces_expected_records_and_eof() {
    let source = Cursor::new(b"a,b,c\nd,e,f\n".to_vec());
    let (mut reader, _stream, _contract) =
        delimited_reader::<DelimitedRecord, _>(source, DelimitedDialect::csv(), identity());
    let fixture = ComponentFixture::new();

    let first = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let ReadOutcome::Item(record) = first else {
        panic!("expected first record, got {first:?}");
    };
    assert_eq!(record.fields(), ["a", "b", "c"]);

    let second = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    let ReadOutcome::Item(record) = second else {
        panic!("expected second record, got {second:?}");
    };
    assert_eq!(record.fields(), ["d", "e", "f"]);

    let third = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap();
    assert_eq!(third, ReadOutcome::EndOfInput);
}

#[tokio::test]
async fn delimited_writer_produces_exact_expected_bytes() {
    let path = temp_path("writer-exact-bytes");
    let (writer, _stream, _contract) =
        delimited_writer(&path, DelimitedDialect::csv(), identity()).unwrap();
    let fixture = ComponentFixture::new();
    let items = vec![
        DelimitedRecord::new(vec!["a".into(), "b".into()]),
        DelimitedRecord::new(vec!["c".into(), "d".into()]),
    ];

    let outcome = writer.write(&items, fixture.write_context()).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Written);

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"a,b\nc,d\n");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn dialect_delimiter_materially_changes_parsing() {
    // The same bytes, parsed under a different configured delimiter, produce
    // a genuinely different field split -- proving the dialect is actually
    // consulted by the parser, not merely stored and read back.
    let bytes = b"a;b,c\n".to_vec();

    let comma_dialect = DelimitedDialect::csv();
    let (mut comma_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(bytes.clone()),
        comma_dialect,
        identity(),
    );
    let fixture = ComponentFixture::new();
    let ReadOutcome::Item(comma_record) = read_next(&mut comma_reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    assert_eq!(comma_record.fields(), ["a;b", "c"]);

    let semicolon_dialect = DelimitedDialect::csv().with_delimiter(b';');
    let (mut semi_reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(Cursor::new(bytes), semicolon_dialect, identity());
    let ReadOutcome::Item(semi_record) = read_next(&mut semi_reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    assert_eq!(semi_record.fields(), ["a", "b,c"]);
}

#[tokio::test]
async fn headers_are_excluded_from_items_and_addressable_by_name() {
    let source = Cursor::new(b"name,age\nAlice,30\n".to_vec());
    let dialect = DelimitedDialect::csv().with_headers(true);
    let (mut reader, _stream, _contract) =
        delimited_reader::<DelimitedRecord, _>(source, dialect, identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(record) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the data record, not the header");
    };
    assert_eq!(record.fields(), ["Alice", "30"]);
    assert_eq!(record.field("name"), Some("Alice"));
    assert_eq!(record.field("age"), Some("30"));
    assert_eq!(record.field("missing"), None);

    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

// ---------------------------------------------------------- B: CSV edges --

#[tokio::test]
async fn quoted_field_hides_the_delimiter_inside_it() {
    let source = Cursor::new(b"\"a,b\",c\n".to_vec());
    let (mut reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(source, DelimitedDialect::csv(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(record) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    // A naive `split(',')` would produce three fields ("\"a", "b\"", "c");
    // the real parser produces exactly two, with the quotes stripped.
    assert_eq!(record.fields(), ["a,b", "c"]);
}

#[tokio::test]
async fn doubled_quote_escapes_a_literal_quote() {
    let source = Cursor::new(b"\"say \"\"hi\"\"\",b\n".to_vec());
    let (mut reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(source, DelimitedDialect::csv(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(record) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    assert_eq!(record.get(0), Some("say \"hi\""));
}

#[tokio::test]
async fn multiline_quoted_field_is_one_record_and_the_next_record_boundary_is_correct() {
    let source = Cursor::new(b"\"line1\nline2\",b\nc,d\n".to_vec());
    let (mut reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(source, DelimitedDialect::csv(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(first) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the multiline record");
    };
    // A line-splitting reader would see 3 "lines" and misparse this as two
    // records; the real parser sees exactly one embedded newline inside the
    // quoted field.
    assert_eq!(first.fields(), ["line1\nline2", "b"]);

    let ReadOutcome::Item(second) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the record after the multiline field");
    };
    assert_eq!(second.fields(), ["c", "d"]);

    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn crlf_and_lf_terminators_parse_identically() {
    let fixture = ComponentFixture::new();

    let (mut lf_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(b"a,b\nc,d\n".to_vec()),
        DelimitedDialect::csv(),
        identity(),
    );
    let (mut crlf_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(b"a,b\r\nc,d\r\n".to_vec()),
        DelimitedDialect::csv(),
        identity(),
    );

    for reader in [&mut lf_reader, &mut crlf_reader] {
        let ReadOutcome::Item(first) = read_next(reader, fixture.read_context()).await.unwrap()
        else {
            panic!("expected first record");
        };
        assert_eq!(first.fields(), ["a", "b"]);
        let ReadOutcome::Item(second) = read_next(reader, fixture.read_context()).await.unwrap()
        else {
            panic!("expected second record");
        };
        assert_eq!(second.fields(), ["c", "d"]);
        assert_eq!(
            read_next(reader, fixture.read_context()).await.unwrap(),
            ReadOutcome::EndOfInput
        );
    }
}

#[tokio::test]
async fn ragged_row_is_a_classified_malformed_failure() {
    // Non-flexible dialect (the default): a record with a different field
    // count than the first is malformed, not silently accepted or padded.
    let source = Cursor::new(b"a,b,c\nd,e\n".to_vec());
    let (mut reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(source, DelimitedDialect::csv(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(_first) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the first well-formed record");
    };

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a ragged row must be a typed failure, not silently accepted");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(
        error.has_checkpoint_advanced(),
        "the parser already consumed the ragged row's bytes, so forward progress is provable"
    );
}

#[tokio::test]
async fn flexible_dialect_accepts_a_ragged_row_that_the_default_dialect_rejects() {
    let bytes = b"a,b,c\nd,e\n".to_vec();
    let fixture = ComponentFixture::new();

    let strict_dialect = DelimitedDialect::csv();
    let (mut strict_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(bytes.clone()),
        strict_dialect,
        identity(),
    );
    let _ = read_next(&mut strict_reader, fixture.read_context())
        .await
        .unwrap();
    assert!(
        read_next(&mut strict_reader, fixture.read_context())
            .await
            .is_err()
    );

    let flexible_dialect = DelimitedDialect::csv().with_flexible(true);
    let (mut flexible_reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(Cursor::new(bytes), flexible_dialect, identity());
    let _ = read_next(&mut flexible_reader, fixture.read_context())
        .await
        .unwrap();
    let ReadOutcome::Item(second) = read_next(&mut flexible_reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("the flexible dialect must accept the ragged row");
    };
    assert_eq!(second.fields(), ["d", "e"]);
}

#[tokio::test]
async fn a_record_exceeding_the_configured_bound_fails_closed() {
    // Every field is 1 byte; the whole record's parsed byte span is well
    // under a generous bound but well over a tight one, so the tight bound
    // is what triggers the rejection -- proving the bound is actually
    // enforced, not merely accepted as configuration.
    let bytes = b"aaaaaaaaaa,bbbbbbbbbb\n".to_vec();
    let fixture = ComponentFixture::new();

    let generous = DelimitedDialect::csv().with_max_record_bytes(1024);
    let (mut generous_reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(Cursor::new(bytes.clone()), generous, identity());
    assert!(
        read_next(&mut generous_reader, fixture.read_context())
            .await
            .is_ok(),
        "a generous bound must accept this record"
    );

    let tight = DelimitedDialect::csv().with_max_record_bytes(4);
    let (mut tight_reader, _s, _c) =
        delimited_reader::<DelimitedRecord, _>(Cursor::new(bytes), tight, identity());
    let error = read_next(&mut tight_reader, fixture.read_context())
        .await
        .expect_err("a record exceeding the configured bound must fail closed");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(
        error.has_checkpoint_advanced(),
        "the parser already consumed the oversized record's bytes"
    );
}

// -------------------------------------------------- G: typed/erased parity --

#[derive(Clone, Default)]
struct RecordingWriter(Arc<Mutex<Vec<DelimitedRecord>>>);

impl ItemWriter<DelimitedRecord> for RecordingWriter {
    async fn write(
        &self,
        items: &[DelimitedRecord],
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
async fn typed_and_erased_delimited_pipelines_produce_identical_items() {
    let bytes = b"1,a\n2,b\n3,c\n4,d\n5,e\n".to_vec();

    let (typed_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(bytes.clone()),
        DelimitedDialect::csv(),
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

    let (erased_reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(bytes),
        DelimitedDialect::csv(),
        identity(),
    );
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
        typed_items
            .iter()
            .map(DelimitedRecord::fields)
            .collect::<Vec<_>>(),
        vec![
            &["1".to_owned(), "a".to_owned()][..],
            &["2".to_owned(), "b".to_owned()][..],
            &["3".to_owned(), "c".to_owned()][..],
            &["4".to_owned(), "d".to_owned()][..],
            &["5".to_owned(), "e".to_owned()][..],
        ],
    );
}

// ---------------------------------------------- header restart evidence --

/// Proves headers remain available after a restart that resumes mid-file,
/// not merely on an initial read: the rustdoc for
/// `DelimitedReader::ensure_headers_and_seek` claims the header row is
/// always re-read from byte 0 before seeking, specifically so a restarted
/// read still resolves field names -- this asserts that claim directly,
/// through the real `ItemStream::open`/`update` calls a committing chunk
/// makes (see `postgres_flat_file_restart.rs` for the same restart through
/// a full `ChunkStep`/`JobLauncher` round trip; this test isolates just the
/// header-survival claim without needing a durable fixture).
#[tokio::test]
async fn headers_survive_a_restart_that_resumes_mid_file() {
    let path = temp_path("headers-restart");
    std::fs::write(&path, "name,age\nAlice,30\nBob,40\nCarol,50\n").unwrap();
    let dialect = DelimitedDialect::csv().with_headers(true);
    let fixture = ComponentFixture::new();

    // Attempt A: read the first data record, then ask the stream for its
    // candidate envelope -- exactly what a committing chunk would persist.
    let (mut reader_a, stream_a, _c) = delimited_reader::<DelimitedRecord, _>(
        std::fs::File::open(&path).unwrap(),
        dialect,
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
        panic!("expected the first data record");
    };
    assert_eq!(first.fields(), ["Alice", "30"]);
    let envelope = stream_a
        .update(fixture.stream_update_context())
        .await
        .unwrap();

    // Attempt B: a fresh reader/stream pair restored from that envelope --
    // resuming mid-file must not lose the header row.
    let (mut reader_b, stream_b, _c) = delimited_reader::<DelimitedRecord, _>(
        std::fs::File::open(&path).unwrap(),
        dialect,
        identity(),
    );
    stream_b
        .open(fixture.stream_open_context(Some(&envelope)))
        .await
        .unwrap();
    let ReadOutcome::Item(second) = read_next(&mut reader_b, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the record after Alice");
    };
    assert_eq!(second.fields(), ["Bob", "40"]);
    assert_eq!(
        second.field("name"),
        Some("Bob"),
        "header names must survive a restart that resumes mid-file, not just an initial read"
    );
    assert_eq!(second.field("age"), Some("40"));

    let _ = std::fs::remove_file(&path);
}
