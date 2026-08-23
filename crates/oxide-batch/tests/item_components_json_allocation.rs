//! #148 bounded-memory evidence (F): [`stats_alloc`] (the same instrumented
//! allocator `chunk_allocation.rs`/`item_components_flat_file_allocation.rs`
//! already use -- this workspace forbids `unsafe_code`, ruling out a
//! hand-written `GlobalAlloc`) measures allocator behavior across
//! [`JsonLinesReader`]/[`JsonArrayReader`], mirroring #147's allocation
//! evidence methodology exactly: net-retained bytes for the whole-file
//! streaming claim (a materializing reader's net-retained figure grows with
//! file size; a genuinely bounded one does not), and cumulative
//! `bytes_allocated` for the single-oversized-element claim (the offending
//! buffer is freed, or at least bounded, well before an after-the-fact net
//! snapshot could observe it).
//!
//! Allocator totals are process-global, so this file carries exactly one
//! `#[test]`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::System;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use oxide_batch::item_components::{
    JsonArrayFormat, JsonArrayReader, JsonLinesFormat, JsonLinesReader, json_array_file_reader,
    jsonl_file_reader,
};
use oxide_batch::{ComponentStreamIdentity, ItemReader, ReadContext, ReadOutcome, StopSource};
use serde_json::Value;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn net_retained_bytes(region: &Region<'_, System>) -> i128 {
    let change = region.change();
    i128::try_from(change.bytes_allocated).unwrap_or(i128::MAX)
        - i128::try_from(change.bytes_deallocated).unwrap_or(i128::MAX)
}

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(name).expect("static identity is valid")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-148-alloc-{name}-{nonce}.dat"))
}

fn write_jsonl(path: &std::path::Path, rows: u32) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    for index in 0..rows {
        writeln!(
            file,
            r#"{{"i":{index},"v":"value-{index}","f":"filler-field"}}"#
        )
        .expect("write fixture row");
    }
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

fn write_json_array(path: &std::path::Path, rows: u32) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    write!(file, "[").expect("write opening bracket");
    for index in 0..rows {
        if index > 0 {
            write!(file, ",").expect("write separator");
        }
        write!(
            file,
            r#"{{"i":{index},"v":"value-{index}","f":"filler-field"}}"#
        )
        .expect("write fixture element");
    }
    write!(file, "]").expect("write closing bracket");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

async fn read_jsonl(
    reader: &mut JsonLinesReader<File>,
    context: ReadContext<'_>,
) -> ReadOutcome<Value> {
    reader.read(context).await.expect("read")
}

async fn read_json_array(
    reader: &mut JsonArrayReader<File>,
    context: ReadContext<'_>,
) -> ReadOutcome<Value> {
    reader.read(context).await.expect("read")
}

async fn streamed_jsonl_net_retained(path: &std::path::Path, rows: u32) -> i128 {
    let region = Region::new(ALLOCATOR);
    let (mut reader, _s, _c) = jsonl_file_reader::<Value>(
        path,
        JsonLinesFormat::new(),
        identity("oxide-batch.bounded-jsonl"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let mut count = 0u64;
    loop {
        match read_jsonl(&mut reader, ReadContext::new(&stop)).await {
            ReadOutcome::Item(_) => count += 1,
            ReadOutcome::EndOfInput => break,
            outcome => panic!("stop was never requested, got {outcome:?}"),
        }
    }
    assert_eq!(count, u64::from(rows));
    net_retained_bytes(&region)
}

async fn streamed_json_array_net_retained(path: &std::path::Path, rows: u32) -> i128 {
    let region = Region::new(ALLOCATOR);
    let (mut reader, _s, _c) = json_array_file_reader::<Value>(
        path,
        JsonArrayFormat::new(),
        identity("oxide-batch.bounded-json-array"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let mut count = 0u64;
    loop {
        match read_json_array(&mut reader, ReadContext::new(&stop)).await {
            ReadOutcome::Item(_) => count += 1,
            ReadOutcome::EndOfInput => break,
            outcome => panic!("stop was never requested, got {outcome:?}"),
        }
    }
    assert_eq!(count, u64::from(rows));
    net_retained_bytes(&region)
}

/// One field's byte length in the deliberately huge single-value fixtures.
const HUGE_FIELD_BYTES: usize = 20 * 1024 * 1024;

/// The bound both real readers are configured with when reading the huge
/// single-value fixtures.
const TIGHT_MAX_VALUE_BYTES: usize = 4096;

/// Writes one JSONL line whose sole value is a [`HUGE_FIELD_BYTES`]-long
/// JSON string.
fn write_huge_single_line_jsonl(path: &std::path::Path) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    file.write_all(b"\"").expect("write opening quote");
    file.write_all(&vec![b'x'; HUGE_FIELD_BYTES])
        .expect("write huge string");
    file.write_all(b"\"\n")
        .expect("write closing quote and terminator");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// Writes a one-element top-level JSON array whose sole element is a
/// [`HUGE_FIELD_BYTES`]-long JSON string.
fn write_huge_single_element_json_array(path: &std::path::Path) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    file.write_all(b"[\"")
        .expect("write opening bracket and quote");
    file.write_all(&vec![b'x'; HUGE_FIELD_BYTES])
        .expect("write huge string");
    file.write_all(b"\"]")
        .expect("write closing quote and bracket");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// The number of elements in the pathological nested-array fixture: chosen
/// so total byte span is on the same order of magnitude as
/// [`HUGE_FIELD_BYTES`], exercising `Vec<Value>` growth bookkeeping (not a
/// single giant string) as the source of the oversized byte span.
const HUGE_NESTED_ELEMENT_COUNT: usize = 2_000_000;

/// Writes a one-element top-level JSON array whose sole element is itself a
/// large nested array of small strings -- a "nested/escaped" pathological
/// shape distinct from one giant flat string.
fn write_huge_nested_element_json_array(path: &std::path::Path) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    write!(file, "[[").expect("write opening brackets");
    for index in 0..HUGE_NESTED_ELEMENT_COUNT {
        if index > 0 {
            write!(file, ",").expect("write separator");
        }
        write!(file, "\"n\\\"{index}\"").expect("write escaped nested element");
    }
    write!(file, "]]").expect("write closing brackets");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// The naive "materialize the whole line, then check its length" shape --
/// exactly the bug class the JSONL bound guards against.
fn naive_whole_line_bytes_allocated(path: &std::path::Path) -> (usize, usize) {
    let region = Region::new(ALLOCATOR);
    let file = File::open(path).expect("open fixture file");
    let mut reader = BufReader::new(file);
    let mut buf: Vec<u8> = Vec::new();
    reader.read_until(b'\n', &mut buf).expect("read line");
    let line_len = buf.len();
    drop(buf);
    let allocated = region.change().bytes_allocated;
    (allocated, line_len)
}

/// The naive "materialize the whole remaining input, then fully deserialize
/// it" shape -- exactly the bug class the JSON-array bound guards against:
/// this reads and decodes the entire array (a single huge element, or a
/// huge nested structure) into memory before any bound could ever be
/// consulted.
fn naive_whole_array_bytes_allocated(path: &std::path::Path) -> (usize, usize) {
    let region = Region::new(ALLOCATOR);
    let bytes = std::fs::read(path).expect("read whole file");
    let file_len = bytes.len();
    let value: Value = serde_json::from_slice(&bytes).expect("parse whole array");
    let allocated = region.change().bytes_allocated;
    drop(value);
    (allocated, file_len)
}

#[tokio::test(flavor = "current_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "the whole-file and single-oversized-element scenarios are only meaningful \
              evidence together, sharing one process-global allocator (see the module docs)"
)]
async fn json_readers_do_not_retain_memory_proportional_to_input_size() {
    const SMALL_ROWS: u32 = 500;
    const LARGE_ROWS: u32 = 100_000;

    // ------------------------------------------------------------- JSONL --
    let small_path = temp_path("jsonl-small");
    let small_size = write_jsonl(&small_path, SMALL_ROWS);
    let large_path = temp_path("jsonl-large");
    let large_size = write_jsonl(&large_path, LARGE_ROWS);
    assert!(
        large_size > small_size.saturating_mul(100),
        "the large fixture must dwarf the small one: small={small_size} large={large_size}"
    );

    let small_net = streamed_jsonl_net_retained(&small_path, SMALL_ROWS).await;
    let large_net = streamed_jsonl_net_retained(&large_path, LARGE_ROWS).await;
    let _ = std::fs::remove_file(&small_path);
    let _ = std::fs::remove_file(&large_path);

    println!(
        "jsonl: small_size={small_size} small_net={small_net} large_size={large_size} \
         large_net={large_net}"
    );
    assert!(
        large_net < i128::from(large_size) / 10,
        "streaming JSONL net-retained bytes ({large_net}) approach the file size \
         ({large_size}): consistent with whole-file materialization"
    );
    assert!(
        (large_net - small_net).unsigned_abs() < u128::from(large_size - small_size) / 10,
        "streaming JSONL net-retained bytes grew with file size (small={small_net}, \
         large={large_net}), which a bounded per-line reader would not do"
    );

    // -------------------------------------------------------- JSON array --
    let small_array_path = temp_path("json-array-small");
    let small_array_size = write_json_array(&small_array_path, SMALL_ROWS);
    let large_array_path = temp_path("json-array-large");
    let large_array_size = write_json_array(&large_array_path, LARGE_ROWS);

    let small_array_net = streamed_json_array_net_retained(&small_array_path, SMALL_ROWS).await;
    let large_array_net = streamed_json_array_net_retained(&large_array_path, LARGE_ROWS).await;
    let _ = std::fs::remove_file(&small_array_path);
    let _ = std::fs::remove_file(&large_array_path);

    println!(
        "json-array: small_size={small_array_size} small_net={small_array_net} \
         large_size={large_array_size} large_net={large_array_net}"
    );
    assert!(
        large_array_net < i128::from(large_array_size) / 10,
        "streaming JSON-array net-retained bytes ({large_array_net}) approach the file size \
         ({large_array_size}): consistent with whole-array materialization"
    );
    assert!(
        (large_array_net - small_array_net).unsigned_abs()
            < u128::from(large_array_size - small_array_size) / 10,
        "streaming JSON-array net-retained bytes grew with file size (small={small_array_net}, \
         large={large_array_net}), which a bounded per-element reader would not do"
    );

    // ------------------------------------------------ positive control --
    let control_path = temp_path("jsonl-large-control");
    let control_size = write_jsonl(&control_path, LARGE_ROWS);
    let region = Region::new(ALLOCATOR);
    let materialized = std::fs::read_to_string(&control_path).expect("read whole file");
    assert_eq!(materialized.lines().count(), LARGE_ROWS as usize);
    let control_net = net_retained_bytes(&region);
    drop(materialized);
    let _ = std::fs::remove_file(&control_path);

    println!("positive control: file_size={control_size} net={control_net}");
    assert!(
        control_net >= i128::from(control_size) - 4096,
        "the positive control did not retain close to the whole file ({control_net} vs \
         {control_size}): the harness would not have caught a real whole-file-materialization \
         regression"
    );

    // ------------------------------------------- F: one oversized value --
    let huge_jsonl_path = temp_path("jsonl-huge-line");
    let huge_jsonl_size = write_huge_single_line_jsonl(&huge_jsonl_path);
    assert!(u64::try_from(HUGE_FIELD_BYTES).is_ok_and(|bytes| huge_jsonl_size > bytes));

    let region = Region::new(ALLOCATOR);
    let (mut huge_reader, _s, _c) = jsonl_file_reader::<Value>(
        &huge_jsonl_path,
        JsonLinesFormat::new().with_max_record_bytes(TIGHT_MAX_VALUE_BYTES),
        identity("oxide-batch.oversized-jsonl"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let result = ItemReader::<Value>::read(&mut huge_reader, ReadContext::new(&stop)).await;
    assert!(
        result.is_err(),
        "a line far exceeding the configured bound must be rejected, not accepted"
    );
    drop(huge_reader);
    let real_jsonl_allocated = region.change().bytes_allocated;

    let (naive_jsonl_allocated, naive_jsonl_line_len) =
        naive_whole_line_bytes_allocated(&huge_jsonl_path);
    assert!(
        naive_jsonl_line_len > HUGE_FIELD_BYTES,
        "sanity: the naive positive control must have actually read the huge line \
         (line_len={naive_jsonl_line_len})"
    );
    let _ = std::fs::remove_file(&huge_jsonl_path);

    println!(
        "jsonl oversized value: real_allocated={real_jsonl_allocated} \
         naive_allocated={naive_jsonl_allocated} value_bytes={HUGE_FIELD_BYTES}"
    );
    assert!(
        real_jsonl_allocated < HUGE_FIELD_BYTES / 10,
        "the real JsonLinesReader allocated {real_jsonl_allocated} bytes rejecting a \
         {HUGE_FIELD_BYTES}-byte value: consistent with materializing the line before checking \
         the bound, rather than bounding growth during the read"
    );
    assert!(
        naive_jsonl_allocated >= HUGE_FIELD_BYTES,
        "the naive positive control did not show a large allocation \
         (allocated={naive_jsonl_allocated}, value_bytes={HUGE_FIELD_BYTES}): the harness would \
         not have caught a real regression back to materialize-then-check"
    );

    let huge_array_path = temp_path("json-array-huge-element");
    let huge_array_size = write_huge_single_element_json_array(&huge_array_path);
    assert!(u64::try_from(HUGE_FIELD_BYTES).is_ok_and(|bytes| huge_array_size > bytes));

    let region = Region::new(ALLOCATOR);
    let (mut huge_array_reader, _s, _c) = json_array_file_reader::<Value>(
        &huge_array_path,
        JsonArrayFormat::new().with_max_value_bytes(TIGHT_MAX_VALUE_BYTES),
        identity("oxide-batch.oversized-json-array"),
    )
    .expect("open fixture file");
    let result = ItemReader::<Value>::read(&mut huge_array_reader, ReadContext::new(&stop)).await;
    assert!(
        result.is_err(),
        "an element far exceeding the configured bound must be rejected, not accepted"
    );
    drop(huge_array_reader);
    let real_array_allocated = region.change().bytes_allocated;

    let (naive_array_allocated, naive_array_len) =
        naive_whole_array_bytes_allocated(&huge_array_path);
    assert!(naive_array_len > HUGE_FIELD_BYTES);
    let _ = std::fs::remove_file(&huge_array_path);

    println!(
        "json-array oversized element: real_allocated={real_array_allocated} \
         naive_allocated={naive_array_allocated} value_bytes={HUGE_FIELD_BYTES}"
    );
    assert!(
        real_array_allocated < HUGE_FIELD_BYTES / 10,
        "the real JsonArrayReader allocated {real_array_allocated} bytes rejecting a \
         {HUGE_FIELD_BYTES}-byte element: consistent with growing its buffer past the \
         configured bound before giving up"
    );
    assert!(
        naive_array_allocated >= HUGE_FIELD_BYTES,
        "the naive positive control did not show a large allocation \
         (allocated={naive_array_allocated}, value_bytes={HUGE_FIELD_BYTES})"
    );

    // ------------------------- one nested/escaped pathological element --
    // Distinct from the single-flat-string scenario above: the oversized
    // byte span here comes from a large nested array of small escaped
    // strings, exercising `Vec<Value>`/string-unescaping bookkeeping, not
    // one contiguous allocation.
    let huge_nested_path = temp_path("json-array-huge-nested");
    let huge_nested_size = write_huge_nested_element_json_array(&huge_nested_path);
    let huge_nested_size_usize = usize::try_from(huge_nested_size).unwrap_or(usize::MAX);
    assert!(huge_nested_size_usize > HUGE_NESTED_ELEMENT_COUNT);

    let region = Region::new(ALLOCATOR);
    let (mut huge_nested_reader, _s, _c) = json_array_file_reader::<Value>(
        &huge_nested_path,
        JsonArrayFormat::new().with_max_value_bytes(TIGHT_MAX_VALUE_BYTES),
        identity("oxide-batch.oversized-nested-json-array"),
    )
    .expect("open fixture file");
    let result = ItemReader::<Value>::read(&mut huge_nested_reader, ReadContext::new(&stop)).await;
    assert!(
        result.is_err(),
        "a nested element far exceeding the configured bound must be rejected"
    );
    drop(huge_nested_reader);
    let real_nested_allocated = region.change().bytes_allocated;

    let (naive_nested_allocated, naive_nested_len) =
        naive_whole_array_bytes_allocated(&huge_nested_path);
    assert!(naive_nested_len > HUGE_NESTED_ELEMENT_COUNT);
    let _ = std::fs::remove_file(&huge_nested_path);

    println!(
        "json-array oversized nested element: real_allocated={real_nested_allocated} \
         naive_allocated={naive_nested_allocated} source_bytes={huge_nested_size}"
    );
    assert!(
        real_nested_allocated < huge_nested_size_usize / 10,
        "the real JsonArrayReader allocated {real_nested_allocated} bytes rejecting a nested \
         element spanning {huge_nested_size} raw bytes: consistent with materializing nested \
         Vec<Value>/string bookkeeping proportional to the element's real size rather than the \
         configured bound"
    );
    assert!(
        naive_nested_allocated >= huge_nested_size_usize / 2,
        "the naive positive control did not show a large allocation \
         (allocated={naive_nested_allocated}, source_bytes={huge_nested_size})"
    );
}
