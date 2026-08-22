//! #147 bounded-memory evidence (F): [`stats_alloc`] (the same instrumented
//! allocator `chunk_allocation.rs`/`item_components_allocation.rs` already
//! use -- this workspace forbids `unsafe_code`, so a hand-written
//! `GlobalAlloc` cannot live in this file) measures *net retained* bytes
//! (allocated minus deallocated) across a full streaming pass over
//! [`DelimitedReader`]/[`FixedWidthReader`].
//!
//! A whole-file-materializing implementation retains its entire buffer for
//! the reader's whole lifetime, so its net-retained figure is close to the
//! file size and grows with it. A genuinely record-at-a-time streaming
//! reader's net-retained figure is bounded by its internal buffers (a
//! `BufReader` page plus one reused record buffer), so it does not grow
//! proportionally as the file grows -- this is what distinguishes the two,
//! which a mere "a large file was read successfully" test cannot. A positive
//! control (`std::fs::read_to_string`, which does materialize the whole
//! file) proves the harness can actually observe the violation.
//!
//! A second, distinct claim -- that a single oversized record cannot itself
//! allocate proportionally to its own size before rejection, regardless of
//! how many total records surround it -- needs a different measurement:
//! `bytes_allocated` (a cumulative total, not a net) taken across reading
//! *one* pathological record. For a single-record operation, a multi-MiB
//! one-shot buffer dominates that total, so comparing it between the real
//! reader and a deliberately naive "materialize whole line, then check
//! length" positive control (the exact shape of the bug this evidence
//! guards against) gives a clear, honest signal.
//!
//! Allocator totals are process-global (mirroring `chunk_allocation.rs`'s own
//! reasoning), so this file carries exactly one `#[test]`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::System;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use oxide_batch::item_components::{
    DelimitedDialect, DelimitedReader, DelimitedRecord, FixedWidthField, FixedWidthLayout,
    FixedWidthReader, FixedWidthRecord, delimited_file_reader, fixed_width_file_reader,
};
use oxide_batch::{ComponentStreamIdentity, ItemReader, ReadContext, ReadOutcome, StopSource};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// `bytes_allocated`/`bytes_deallocated` already fold in every `realloc`
/// growth/shrink delta on their respective sides (see `stats_alloc`'s own
/// `realloc` implementation), so the net is just their difference --
/// `bytes_reallocated` is a separate, overlapping view of the same deltas
/// and must not also be added here, or every reallocation is double-counted.
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
    std::env::temp_dir().join(format!("oxide-batch-147-alloc-{name}-{nonce}.dat"))
}

fn write_csv(path: &std::path::Path, rows: u32) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    for index in 0..rows {
        writeln!(file, "{index},value-{index},filler-field").expect("write fixture row");
    }
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

fn write_fixed_width(path: &std::path::Path, rows: u32) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    for index in 0..rows {
        writeln!(file, "{index:08}row{index:08}pad").expect("write fixture row");
    }
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// One field's byte length in the deliberately huge single-record fixtures:
/// large enough that "did this allocate the whole record" and "did this stay
/// bounded" are unmistakably different orders of magnitude.
const HUGE_FIELD_BYTES: usize = 20 * 1024 * 1024;

/// The bound both real readers are configured with when reading the huge
/// single-record fixtures: far smaller than [`HUGE_FIELD_BYTES`], so
/// acceptance would itself be a test bug, and small enough that any
/// proportional-to-record-size allocation is obvious against it.
const TIGHT_MAX_RECORD_BYTES: usize = 4096;

/// Writes one CSV record whose second field is [`HUGE_FIELD_BYTES`] long.
fn write_huge_single_record_csv(path: &std::path::Path) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    write!(file, "a,").expect("write fixture prefix");
    file.write_all(&vec![b'x'; HUGE_FIELD_BYTES])
        .expect("write huge field");
    writeln!(file, ",b").expect("write fixture suffix");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// Writes one fixed-width "line" (no embedded `\n`) that is
/// [`HUGE_FIELD_BYTES`] long before its terminator.
fn write_huge_single_line_fixed_width(path: &std::path::Path) -> u64 {
    let mut file = File::create(path).expect("create fixture file");
    file.write_all(&vec![b'x'; HUGE_FIELD_BYTES])
        .expect("write huge line");
    file.write_all(b"\n").expect("write terminator");
    file.sync_all().expect("flush fixture file");
    file.metadata().expect("fixture metadata").len()
}

/// The naive "materialize the whole line, then check its length" shape --
/// exactly the bug class this evidence guards against -- used only as a
/// positive control to prove the harness can observe a large one-shot
/// allocation when one genuinely happens.
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

async fn read_csv(
    reader: &mut DelimitedReader<File>,
    context: ReadContext<'_>,
) -> ReadOutcome<DelimitedRecord> {
    reader.read(context).await.expect("read")
}

async fn read_fixed_width(
    reader: &mut FixedWidthReader<File>,
    context: ReadContext<'_>,
) -> ReadOutcome<FixedWidthRecord> {
    reader.read(context).await.expect("read")
}

/// Opens `path`, reads every record to end-of-input while `region` is
/// active, and returns the net retained bytes for the whole streaming pass
/// (reader included, since a whole-file-materializing reader would still be
/// holding its buffer at this point).
async fn streamed_csv_net_retained(path: &std::path::Path, rows: u32) -> i128 {
    let region = Region::new(ALLOCATOR);
    let (mut reader, _s, _c) = delimited_file_reader::<DelimitedRecord>(
        path,
        DelimitedDialect::csv(),
        identity("oxide-batch.bounded-csv"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let mut count = 0u64;
    loop {
        match read_csv(&mut reader, ReadContext::new(&stop)).await {
            ReadOutcome::Item(_) => count += 1,
            ReadOutcome::EndOfInput => break,
            outcome => panic!("stop was never requested, got {outcome:?}"),
        }
    }
    assert_eq!(count, u64::from(rows));
    net_retained_bytes(&region)
}

async fn streamed_fixed_width_net_retained(path: &std::path::Path, rows: u32) -> i128 {
    let layout = FixedWidthLayout::new(vec![
        FixedWidthField::new(8),
        FixedWidthField::new(11),
        FixedWidthField::new(3),
    ]);
    let region = Region::new(ALLOCATOR);
    let (mut reader, _s, _c) = fixed_width_file_reader::<FixedWidthRecord>(
        path,
        layout,
        identity("oxide-batch.bounded-fixed-width"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let mut count = 0u64;
    loop {
        match read_fixed_width(&mut reader, ReadContext::new(&stop)).await {
            ReadOutcome::Item(_) => count += 1,
            ReadOutcome::EndOfInput => break,
            outcome => panic!("stop was never requested, got {outcome:?}"),
        }
    }
    assert_eq!(count, u64::from(rows));
    net_retained_bytes(&region)
}

#[tokio::test(flavor = "current_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "the whole-file and single-oversized-record scenarios are only meaningful evidence \
              together, sharing one process-global allocator (see the module docs)"
)]
async fn flat_file_readers_do_not_retain_memory_proportional_to_file_size() {
    const SMALL_ROWS: u32 = 500;
    const LARGE_ROWS: u32 = 300_000;

    // -------------------------------------------------------- delimited --
    let small_path = temp_path("csv-small");
    let small_size = write_csv(&small_path, SMALL_ROWS);
    let large_path = temp_path("csv-large");
    let large_size = write_csv(&large_path, LARGE_ROWS);
    assert!(
        large_size > small_size.saturating_mul(100),
        "the large fixture must dwarf the small one: small={small_size} large={large_size}"
    );

    let small_net = streamed_csv_net_retained(&small_path, SMALL_ROWS).await;
    let large_net = streamed_csv_net_retained(&large_path, LARGE_ROWS).await;
    let _ = std::fs::remove_file(&small_path);
    let _ = std::fs::remove_file(&large_path);

    println!(
        "csv: small_size={small_size} small_net={small_net} large_size={large_size} \
         large_net={large_net}"
    );
    assert!(
        large_net < i128::from(large_size) / 10,
        "streaming CSV net-retained bytes ({large_net}) approach the file size \
         ({large_size}): consistent with whole-file materialization"
    );
    assert!(
        (large_net - small_net).unsigned_abs() < u128::from(large_size - small_size) / 10,
        "streaming CSV net-retained bytes grew with file size (small={small_net}, \
         large={large_net}), which a bounded per-record reader would not do"
    );

    // ------------------------------------------------------- fixed width --
    let small_fw = temp_path("fw-small");
    let small_fw_size = write_fixed_width(&small_fw, SMALL_ROWS);
    let large_fw = temp_path("fw-large");
    let large_fw_size = write_fixed_width(&large_fw, LARGE_ROWS);

    let small_fw_net = streamed_fixed_width_net_retained(&small_fw, SMALL_ROWS).await;
    let large_fw_net = streamed_fixed_width_net_retained(&large_fw, LARGE_ROWS).await;
    let _ = std::fs::remove_file(&small_fw);
    let _ = std::fs::remove_file(&large_fw);

    println!(
        "fixed-width: small_size={small_fw_size} small_net={small_fw_net} \
         large_size={large_fw_size} large_net={large_fw_net}"
    );
    assert!(
        large_fw_net < i128::from(large_fw_size) / 10,
        "streaming fixed-width net-retained bytes ({large_fw_net}) approach the file size \
         ({large_fw_size}): consistent with whole-file materialization"
    );

    // ------------------------------------------------ positive control --
    // A deliberately whole-file-materializing read of the same large CSV
    // fixture: net-retained bytes must be close to the file size, proving
    // this methodology would have caught a real regression rather than being
    // insensitive to allocation size altogether.
    let large_path = temp_path("csv-large-control");
    let large_size = write_csv(&large_path, LARGE_ROWS);
    let region = Region::new(ALLOCATOR);
    let materialized = std::fs::read_to_string(&large_path).expect("read whole file");
    assert_eq!(materialized.lines().count(), LARGE_ROWS as usize);
    let control_net = net_retained_bytes(&region);
    drop(materialized);
    let _ = std::fs::remove_file(&large_path);

    println!("csv positive control: file_size={large_size} net={control_net}");
    assert!(
        control_net >= i128::from(large_size) - 4096,
        "the positive control did not retain close to the whole file ({control_net} vs \
         {large_size}): the harness would not have caught a real whole-file-materialization \
         regression"
    );

    // ---------------------------------------- F: one oversized record --
    // A single pathological record must not itself allocate in proportion
    // to its own size before rejection -- distinct from (and not implied
    // by) the whole-file claims above, which use many small, uniform
    // records and would not catch a reader that materializes one huge
    // record before checking it.
    let huge_csv_path = temp_path("csv-huge-record");
    let huge_csv_size = write_huge_single_record_csv(&huge_csv_path);
    assert!(u64::try_from(HUGE_FIELD_BYTES).is_ok_and(|bytes| huge_csv_size > bytes));

    let region = Region::new(ALLOCATOR);
    let (mut huge_reader, _s, _c) = delimited_file_reader::<DelimitedRecord>(
        &huge_csv_path,
        DelimitedDialect::csv().with_max_record_bytes(TIGHT_MAX_RECORD_BYTES),
        identity("oxide-batch.oversized-csv"),
    )
    .expect("open fixture file");
    let (_source, stop) = StopSource::new();
    let result =
        ItemReader::<DelimitedRecord>::read(&mut huge_reader, ReadContext::new(&stop)).await;
    assert!(
        result.is_err(),
        "a record far exceeding the configured bound must be rejected, not accepted"
    );
    drop(huge_reader);
    let real_csv_allocated = region.change().bytes_allocated;

    let (naive_csv_allocated, naive_csv_line_len) =
        naive_whole_line_bytes_allocated(&huge_csv_path);
    assert!(
        naive_csv_line_len > HUGE_FIELD_BYTES,
        "sanity: the naive positive control must have actually read the huge line \
         (line_len={naive_csv_line_len})"
    );
    let _ = std::fs::remove_file(&huge_csv_path);

    println!(
        "csv oversized record: real_allocated={real_csv_allocated} \
         naive_allocated={naive_csv_allocated} record_bytes={HUGE_FIELD_BYTES}"
    );
    assert!(
        real_csv_allocated < HUGE_FIELD_BYTES / 10,
        "the real DelimitedReader allocated {real_csv_allocated} bytes rejecting a \
         {HUGE_FIELD_BYTES}-byte record: consistent with materializing the record before \
         checking the bound, rather than bounding growth during parsing"
    );
    assert!(
        naive_csv_allocated >= HUGE_FIELD_BYTES,
        "the naive positive control did not show a large allocation \
         (allocated={naive_csv_allocated}, record_bytes={HUGE_FIELD_BYTES}): the harness would \
         not have caught a real regression back to materialize-then-check"
    );

    let huge_fw_path = temp_path("fw-huge-record");
    let huge_fw_size = write_huge_single_line_fixed_width(&huge_fw_path);
    assert!(u64::try_from(HUGE_FIELD_BYTES).is_ok_and(|bytes| huge_fw_size > bytes));

    let region = Region::new(ALLOCATOR);
    let layout = FixedWidthLayout::new(vec![FixedWidthField::new(1)])
        .with_max_record_bytes(TIGHT_MAX_RECORD_BYTES);
    let (mut huge_fw_reader, _s, _c) = fixed_width_file_reader::<FixedWidthRecord>(
        &huge_fw_path,
        layout,
        identity("oxide-batch.oversized-fixed-width"),
    )
    .expect("open fixture file");
    let result =
        ItemReader::<FixedWidthRecord>::read(&mut huge_fw_reader, ReadContext::new(&stop)).await;
    assert!(
        result.is_err(),
        "a line far exceeding the configured bound must be rejected, not accepted"
    );
    drop(huge_fw_reader);
    let real_fw_allocated = region.change().bytes_allocated;

    let (naive_fw_allocated, naive_fw_line_len) = naive_whole_line_bytes_allocated(&huge_fw_path);
    assert!(naive_fw_line_len > HUGE_FIELD_BYTES);
    let _ = std::fs::remove_file(&huge_fw_path);

    println!(
        "fixed-width oversized record: real_allocated={real_fw_allocated} \
         naive_allocated={naive_fw_allocated} record_bytes={HUGE_FIELD_BYTES}"
    );
    assert!(
        real_fw_allocated < HUGE_FIELD_BYTES / 10,
        "the real FixedWidthReader allocated {real_fw_allocated} bytes rejecting a \
         {HUGE_FIELD_BYTES}-byte line: consistent with copying past the configured bound \
         before entering discard mode"
    );
    assert!(
        naive_fw_allocated >= HUGE_FIELD_BYTES,
        "the naive positive control did not show a large allocation for fixed-width \
         (allocated={naive_fw_allocated}, record_bytes={HUGE_FIELD_BYTES})"
    );
}
