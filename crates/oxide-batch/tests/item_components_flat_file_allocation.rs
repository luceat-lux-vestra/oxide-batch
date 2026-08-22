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
//! Allocator totals are process-global (mirroring `chunk_allocation.rs`'s own
//! reasoning), so this file carries exactly one `#[test]`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::System;
use std::fs::File;
use std::io::Write;

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
}
