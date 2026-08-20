//! ADR-0008 regression: the typed `ChunkStep` item-invocation path does not
//! allocate a boxed future per item, and `BoxedReader`/`BoxedProcessor`/
//! `BoxedWriter` are the only place item-component erasure allocates.
//!
//! # What is measured, and how
//!
//! [`stats_alloc`] wraps the system allocator with one that keeps running
//! totals; [`stats_alloc::Region::new`] snapshots those totals and
//! [`stats_alloc::Region::change`] reads back the delta since that snapshot.
//! The totals are process-global, so this file carries exactly one `#[test]`
//! — `cargo test` gives each integration test file its own process, but runs
//! `#[test]` functions inside one file concurrently by default, and a second
//! test allocating mid-window would corrupt this measurement. Keeping this
//! file single-test is what makes that safe rather than merely usually safe.
//!
//! `execute_chunk_step` is not zero-allocation in the absolute: the transient
//! per-chunk buffers (`ChunkBuffer::slots`, the writer's staged outputs) start
//! at `Vec::new()` and grow with the chunk, so a single large chunk pays
//! `O(log items)` reallocations from ordinary amortized-growth doubling. That
//! cost is unrelated to ADR-0008 and is scoped out here by measuring a
//! *difference* between a small and a large run instead of an absolute count:
//! if the typed path boxed one future per item, growing the item count by
//! `N` would grow its allocation count by very close to `N` (one or two per
//! item, per the spike's `2N + 1 + chunks` formula for the erased path);
//! `O(log items)` growth does not. The erased run is measured the same way
//! as a positive control, proving the harness would actually catch a
//! regression rather than passing by construction.
//!
//! Both runs use one chunk (`chunk_size == items`), so the per-chunk costs
//! that ARE expected to allocate — beginning the boxed
//! `ChunkTransactionManager` transaction and invoking the boxed
//! `ChunkCompletion` callback, both governed by ADR-0002, not ADR-0008 — are
//! paid exactly once in each run and cancel out of the difference.

#![allow(clippy::expect_used, clippy::similar_names)]

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use std::alloc::System;
use std::sync::{Arc, Mutex};

use chunk_fixture::{Double, NoopCompletion, NoopTransactions, Sink, Source, correlation};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, StepName, StopSource,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Runs `items` through the concrete, monomorphized `ChunkStep` path in one
/// chunk and returns the number of allocator calls the run made.
async fn run_typed(items: u32) -> u64 {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("typed").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        Source::range(items),
        Double,
        Sink(output),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let region = Region::new(ALLOCATOR);
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let calls = allocator_calls(&region);
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    calls
}

/// Runs the same logical pipeline through the explicit `Boxed*` erasure
/// boundary and returns the number of allocator calls the run made.
async fn run_erased(items: u32) -> u64 {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("erased").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        BoxedReader::new(Source::range(items)),
        BoxedProcessor::new(Double),
        BoxedWriter::new(Sink(output)),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let region = Region::new(ALLOCATOR);
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let calls = allocator_calls(&region);
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    calls
}

fn allocator_calls(region: &Region<'_, System>) -> u64 {
    let change = region.change();
    let total = change.allocations + change.deallocations + change.reallocations;
    u64::try_from(total).unwrap_or(u64::MAX)
}

/// Allocation regression: growing the item count does not grow the typed
/// path's allocator-call count anywhere near proportionally, which is what
/// one boxed future per item would look like. The erased path is measured
/// the same way as a positive control. This is the file's only test — see
/// the module docs for why that matters.
#[tokio::test(flavor = "current_thread")]
async fn typed_path_allocation_count_does_not_scale_with_item_count() {
    const SMALL: u32 = 200;
    const LARGE: u32 = 20_000;
    let delta_items = u64::from(LARGE - SMALL);

    let typed_small = run_typed(SMALL).await;
    let typed_large = run_typed(LARGE).await;
    let erased_small = run_erased(SMALL).await;
    let erased_large = run_erased(LARGE).await;

    let typed_delta = typed_large.saturating_sub(typed_small);
    let erased_delta = erased_large.saturating_sub(erased_small);

    println!(
        "typed: small={typed_small} large={typed_large} delta={typed_delta} \
         erased: small={erased_small} large={erased_large} delta={erased_delta} \
         items_delta={delta_items}"
    );

    // The transient per-chunk buffers (`ChunkBuffer::slots`, the writer's
    // staged batch) still grow with the chunk on the typed path, but that is
    // ordinary amortized-growth Vec reallocation: O(log items), not O(items).
    // A per-item boxed future would add one allocation (plus its matching
    // deallocation) per item, so it would put `typed_delta` within a small
    // multiple of `delta_items`. A generous two-orders-of-magnitude margin
    // below that easily separates "a few dozen reallocations" from "one
    // allocation per new item" without pinning an exact, allocator-version-
    // sensitive count.
    assert!(
        typed_delta < delta_items / 100,
        "typed path allocator-call count scaled with item count \
         (typed_delta={typed_delta}, items_delta={delta_items}): \
         a per-item boxed future has returned to the concrete typed path"
    );

    // Positive control: the same harness, pointed at the path ADR-0008 says
    // *is* allowed to box, must actually see allocator calls scale with
    // items — otherwise the assertion above would be vacuous.
    assert!(
        erased_delta >= delta_items,
        "erased path allocator-call count did not scale with item count \
         (erased_delta={erased_delta}, items_delta={delta_items}): \
         the measurement would not have caught a regression"
    );
}
