//! The central RFC-0005 measurement: heap traffic per item.
//!
//! This binary holds exactly one test because the counting allocator's state
//! is process-global. Both runs call the same driver with the same components;
//! only the type arguments differ. Every allocation-bearing structure is built
//! before the window opens, so what the window sees is dispatch.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use oxide_batch::StopSource;
use oxide_batch_m6_spikes::allocation::{self, CountingAllocator};
use oxide_batch_m6_spikes::contract::{BoxedProcessor, BoxedReader, BoxedWriter};
use oxide_batch_m6_spikes::driver::{RunOutcome, RunReport, run};
use oxide_batch_m6_spikes::workload::{
    ChecksumWriter, Output, RangeReader, ScalingProcessor, SharedChecksumWriter,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const ITEMS: u64 = 10_000;
const CHUNK: usize = 100;
const FACTOR: u64 = 3;

fn storage() -> (Vec<Output>, RunReport) {
    (
        Vec::with_capacity(CHUNK),
        RunReport::with_capacity(usize::try_from(ITEMS).unwrap_or(usize::MAX)),
    )
}

#[tokio::test]
async fn the_typed_pipeline_allocates_nothing_per_item() {
    let (_source, stop) = StopSource::new();
    let chunks = ITEMS / CHUNK as u64;

    let typed_state = Arc::new(ChecksumWriter::new());
    let boxed_state = Arc::new(ChecksumWriter::new());
    let typed_writer = SharedChecksumWriter(Arc::clone(&typed_state));
    let boxed_writer = BoxedWriter::new(SharedChecksumWriter(Arc::clone(&boxed_state)));

    let mut typed_reader = RangeReader::new(ITEMS);
    let typed_processor = ScalingProcessor::new(FACTOR);
    let (mut buffer, mut report) = storage();

    allocation::begin();
    run(
        &mut typed_reader,
        &typed_processor,
        &typed_writer,
        &stop,
        CHUNK,
        &mut buffer,
        &mut report,
    )
    .await;
    let typed = allocation::end();
    let typed_report = report;

    let mut boxed_reader = BoxedReader::new(RangeReader::new(ITEMS));
    let boxed_processor = BoxedProcessor::new(ScalingProcessor::new(FACTOR));
    let (mut buffer, mut report) = storage();

    allocation::begin();
    run(
        &mut boxed_reader,
        &boxed_processor,
        &boxed_writer,
        &stop,
        CHUNK,
        &mut buffer,
        &mut report,
    )
    .await;
    let boxed = allocation::end();
    let boxed_report = report;

    println!("items={ITEMS} chunk_size={CHUNK} chunks={chunks}");
    println!(
        "typed  allocations={} bytes={} per_item={:?}",
        typed.allocations,
        typed.bytes,
        typed.per_item(ITEMS)
    );
    println!(
        "boxed  allocations={} bytes={} per_item={:?}",
        boxed.allocations,
        boxed.bytes,
        boxed.per_item(ITEMS)
    );

    assert_eq!(typed_report, boxed_report, "the runs must be equivalent");
    assert_eq!(typed_report.outcome, RunOutcome::Completed);
    assert_eq!(typed_state.checksum(), boxed_state.checksum());
    assert_eq!(typed_report.items_written, ITEMS);

    assert_eq!(
        typed.allocations, 0,
        "the monomorphized path must not allocate at all in steady state"
    );
    assert_eq!(typed.bytes, 0);

    // One boxed future per dynamically dispatched call: one read per item plus
    // the read that reports end of input, one process per item, and one write
    // per chunk.
    let expected_boxed = 2 * ITEMS + 1 + chunks;
    assert_eq!(
        boxed.allocations, expected_boxed,
        "each dispatched call through a Boxed* handle must allocate one future"
    );
}
