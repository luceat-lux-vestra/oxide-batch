//! #147 fixed-width component evidence: basic contracts (A) and byte-oriented
//! malformed/UTF-8-boundary semantics (B-equivalent for this family).
//!
//! Restart (C/D) and bounded-memory (F) evidence live elsewhere, mirroring
//! `item_components_delimited.rs`'s split exactly.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::Cursor;

use oxide_batch::item_components::{
    FixedWidthField, FixedWidthLayout, FixedWidthRecord, fixed_width_reader, fixed_width_writer,
};
use oxide_batch::{
    ComponentStreamIdentity, FailureCategory, ItemReader, ItemWriter, ReadContext, ReadOutcome,
    ReaderError, WriteOutcome,
};
use oxide_batch_test::ComponentFixture;

fn identity() -> ComponentStreamIdentity {
    ComponentStreamIdentity::new("oxide-batch-test.fixed-width").expect("static identity is valid")
}

fn layout() -> FixedWidthLayout {
    FixedWidthLayout::new(vec![
        FixedWidthField::named("code", 3),
        FixedWidthField::named("name", 5),
    ])
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-147-fw-{name}-{nonce}.dat"))
}

async fn read_next(
    reader: &mut (impl ItemReader<FixedWidthRecord> + Unpin),
    context: ReadContext<'_>,
) -> Result<ReadOutcome<FixedWidthRecord>, ReaderError> {
    reader.read(context).await
}

// --------------------------------------------------------------- A: basic --

#[tokio::test]
async fn fixed_width_reader_produces_expected_fields_and_eof() {
    let source = Cursor::new(b"001Alice\n002Bobby\n".to_vec());
    let (mut reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(source, layout(), identity());
    let fixture = ComponentFixture::new();

    let ReadOutcome::Item(first) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected first record");
    };
    assert_eq!(first.fields(), ["001", "Alice"]);
    assert_eq!(first.field("code"), Some("001"));
    assert_eq!(first.field("name"), Some("Alice"));

    let ReadOutcome::Item(second) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected second record");
    };
    assert_eq!(second.fields(), ["002", "Bobby"]);

    assert_eq!(
        read_next(&mut reader, fixture.read_context())
            .await
            .unwrap(),
        ReadOutcome::EndOfInput
    );
}

#[tokio::test]
async fn fixed_width_writer_produces_exact_expected_bytes() {
    let path = temp_path("writer-exact-bytes");
    let (writer, _s, _c) = fixed_width_writer(&path, layout(), identity()).unwrap();
    let fixture = ComponentFixture::new();
    let items = vec![
        FixedWidthRecord::new(vec!["001".into(), "Alice".into()]),
        FixedWidthRecord::new(vec!["002".into(), "Bobby".into()]),
    ];

    let outcome = writer.write(&items, fixture.write_context()).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Written);

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"001Alice\n002Bobby\n");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn layout_field_widths_materially_change_parsing() {
    // The same bytes, split by two different layouts, must yield genuinely
    // different field boundaries.
    let bytes = b"abcdef\n".to_vec();
    let fixture = ComponentFixture::new();

    let two_three = FixedWidthLayout::new(vec![FixedWidthField::new(2), FixedWidthField::new(4)]);
    let (mut reader_a, _s, _c) = fixed_width_reader::<FixedWidthRecord, _>(
        Cursor::new(bytes.clone()),
        two_three,
        identity(),
    );
    let ReadOutcome::Item(a) = read_next(&mut reader_a, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    assert_eq!(a.fields(), ["ab", "cdef"]);

    let three_three = FixedWidthLayout::new(vec![FixedWidthField::new(3), FixedWidthField::new(3)]);
    let (mut reader_b, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(Cursor::new(bytes), three_three, identity());
    let ReadOutcome::Item(b) = read_next(&mut reader_b, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected a record");
    };
    assert_eq!(b.fields(), ["abc", "def"]);
}

// ----------------------------------------------- malformed/byte-boundary --

#[tokio::test]
async fn short_record_is_a_classified_malformed_failure_never_padded() {
    let source = Cursor::new(b"001Al\n".to_vec()); // 5 bytes, layout expects 8
    let (mut reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(source, layout(), identity());
    let fixture = ComponentFixture::new();

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a short record must be a typed failure, never silently padded");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(error.has_checkpoint_advanced());
}

#[tokio::test]
async fn long_record_is_a_classified_malformed_failure_never_truncated() {
    let source = Cursor::new(b"001AliceExtra\n".to_vec()); // 13 bytes, layout expects 8
    let (mut reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(source, layout(), identity());
    let fixture = ComponentFixture::new();

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a long record must be a typed failure, never silently truncated");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(error.has_checkpoint_advanced());
}

#[tokio::test]
async fn valid_record_after_a_malformed_one_is_still_read_correctly() {
    let source = Cursor::new(b"bad\n002Bobby\n".to_vec());
    let (mut reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(source, layout(), identity());
    let fixture = ComponentFixture::new();

    assert!(
        read_next(&mut reader, fixture.read_context())
            .await
            .is_err()
    );
    let ReadOutcome::Item(record) = read_next(&mut reader, fixture.read_context())
        .await
        .unwrap()
    else {
        panic!("expected the next well-formed record");
    };
    assert_eq!(record.fields(), ["002", "Bobby"]);
}

#[tokio::test]
async fn a_field_boundary_splitting_a_multibyte_char_is_a_classified_failure() {
    // "é" is 2 UTF-8 bytes (0xC3 0xA9). A 3-byte first field followed by a
    // 5-byte second field splits it in half if the record starts with a
    // 2-byte "é" at position 2..4 -- byte offsets, never character offsets.
    let mut line = Vec::new();
    line.push(b'0');
    line.push(b'0');
    line.extend_from_slice("é".as_bytes()); // bytes at index 2..4, split by width=3
    line.extend_from_slice(b"XXXX");
    line.push(b'\n');
    assert_eq!(line.len(), 9, "fixture must total 8 content bytes + \\n");

    let source = Cursor::new(line);
    let (mut reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(source, layout(), identity());
    let fixture = ComponentFixture::new();

    let error = read_next(&mut reader, fixture.read_context())
        .await
        .expect_err("a field boundary that splits a multi-byte character must fail closed");
    assert_eq!(error.category(), FailureCategory::UserComponent);
}

#[tokio::test]
async fn a_record_exceeding_the_configured_bound_fails_closed() {
    let bytes = b"001Alice\n".to_vec();
    let fixture = ComponentFixture::new();

    let generous = layout().with_max_record_bytes(1024);
    let (mut generous_reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(Cursor::new(bytes.clone()), generous, identity());
    assert!(
        read_next(&mut generous_reader, fixture.read_context())
            .await
            .is_ok(),
        "a generous bound must accept this record"
    );

    let tight = layout().with_max_record_bytes(4);
    let (mut tight_reader, _s, _c) =
        fixed_width_reader::<FixedWidthRecord, _>(Cursor::new(bytes), tight, identity());
    let error = read_next(&mut tight_reader, fixture.read_context())
        .await
        .expect_err("a record exceeding the configured bound must fail closed");
    assert_eq!(error.category(), FailureCategory::UserComponent);
    assert!(error.has_checkpoint_advanced());
}

#[tokio::test]
async fn writer_rejects_a_field_whose_byte_length_does_not_match_the_declared_width() {
    let path = temp_path("writer-wrong-width");
    let (writer, _s, _c) = fixed_width_writer(&path, layout(), identity()).unwrap();
    let fixture = ComponentFixture::new();
    let items = vec![FixedWidthRecord::new(vec!["001".into(), "TooLong".into()])];

    let result = writer.write(&items, fixture.write_context()).await;
    assert!(
        result.is_err(),
        "a field longer than its declared width must fail, never be silently truncated"
    );
    let _ = std::fs::remove_file(&path);
}
