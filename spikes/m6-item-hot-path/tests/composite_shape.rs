//! A fully decorated pipeline still runs on the one driver and still allocates
//! nothing per item.
//!
//! This is the shape M6's component catalogue is made of. If decoration
//! reintroduced dispatch or boxing, the contract would buy nothing for real
//! applications, so the assertion here matters as much as the one in
//! `allocation.rs`.
//!
//! Exactly one test lives in this binary: the counting allocator's state is
//! process-global, so a second test running in parallel would be attributed to
//! this measurement. The composite transaction case lives in
//! `contract_shape.rs`, which does not measure allocations.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use oxide_batch::StopSource;
use oxide_batch_m6_spikes::allocation::{self, CountingAllocator};
use oxide_batch_m6_spikes::composite::{CountingReader, FanOutWriter, FilteringProcessor};
use oxide_batch_m6_spikes::driver::{RunOutcome, RunReport, run};
use oxide_batch_m6_spikes::executor::block_on;
use oxide_batch_m6_spikes::workload::{
    ChecksumWriter, Output, RangeReader, Record, ScalingProcessor, SharedChecksumWriter,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const ITEMS: u64 = 5_000;
const CHUNK: usize = 100;

#[test]
fn a_decorated_pipeline_stays_monomorphized_and_allocation_free() {
    let (_source, stop) = StopSource::new();

    let primary = Arc::new(ChecksumWriter::new());
    let secondary = Arc::new(ChecksumWriter::new());

    let mut reader = CountingReader::new(RangeReader::new(ITEMS));
    // The predicate runs before delegation, so a rejected item never reaches
    // the wrapped processor.
    let processor = FilteringProcessor::new(ScalingProcessor::new(3), |item: &Record| {
        !item.id.is_multiple_of(5)
    });
    let writer = FanOutWriter::new(
        SharedChecksumWriter(Arc::clone(&primary)),
        SharedChecksumWriter(Arc::clone(&secondary)),
    );

    let mut buffer: Vec<Output> = Vec::with_capacity(CHUNK);
    let mut report = RunReport::with_capacity(usize::try_from(ITEMS).unwrap_or(usize::MAX));

    allocation::begin();
    block_on(run(
        &mut reader,
        &processor,
        &writer,
        &stop,
        CHUNK,
        &mut buffer,
        &mut report,
    ));
    let measured = allocation::end();

    println!(
        "decorated: allocations={} bytes={} per_item={:?}",
        measured.allocations,
        measured.bytes,
        measured.per_item(ITEMS)
    );

    let kept = ITEMS - ITEMS / 5;
    assert_eq!(report.outcome, RunOutcome::Completed);
    assert_eq!(report.items_read, ITEMS);
    assert_eq!(report.items_filtered, ITEMS / 5);
    assert_eq!(report.items_written, kept);
    assert_eq!(reader.observed(), ITEMS);
    assert_eq!(primary.items(), kept);
    assert_eq!(secondary.items(), kept);
    assert_eq!(primary.checksum(), secondary.checksum());

    assert_eq!(
        measured.allocations, 0,
        "decoration must not reintroduce per-item heap traffic"
    );
}
