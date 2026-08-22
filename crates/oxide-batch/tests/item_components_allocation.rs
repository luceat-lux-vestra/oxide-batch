//! ADR-0008/#146 allocation regression: the representative decorated
//! pipeline (`support/decorated_pipeline.rs` -- peek over a composite
//! reader, a filter/identity processor chain, and a synchronized recording
//! writer) does not allocate a boxed future per item on the typed
//! `ChunkStep` path, matching `chunk_allocation.rs`'s methodology exactly
//! for an undecorated pipeline. Decoration/composition from #146 does not
//! reintroduce per-item boxing.
//!
//! See `chunk_allocation.rs` for why this file carries exactly one `#[test]`
//! (the allocator totals are process-global) and why the typed path is
//! measured as a *difference* between a small and a large run rather than an
//! absolute count (the transient per-chunk buffers still grow with the
//! chunk, which is ordinary amortized-growth `Vec` reallocation unrelated to
//! ADR-0008).

#![allow(clippy::expect_used, clippy::similar_names)]

#[path = "support/decorated_pipeline.rs"]
mod decorated_pipeline;

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use std::alloc::System;
use std::sync::{Arc, Mutex};

use chunk_fixture::{NoopCompletion, NoopTransactions, correlation};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, StepName, StopSource,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Runs `items` through the decorated, monomorphized `ChunkStep` path in one
/// chunk and returns the number of allocator calls the run made.
async fn run_typed(items: u32) -> u64 {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("typed").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        decorated_pipeline::reader(items),
        decorated_pipeline::processor(),
        decorated_pipeline::writer(output),
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

/// Runs the same decorated pipeline through the explicit `Boxed*` erasure
/// boundary and returns the number of allocator calls the run made.
async fn run_erased(items: u32) -> u64 {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut step = ChunkStep::new(
        StepName::new("erased").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        BoxedReader::new(decorated_pipeline::reader(items)),
        BoxedProcessor::new(decorated_pipeline::processor()),
        BoxedWriter::new(decorated_pipeline::writer(output)),
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

/// Allocation regression for the decorated pipeline: growing the item count
/// does not grow the typed path's allocator-call count anywhere near
/// proportionally. The erased path is measured the same way as a positive
/// control. This is the file's only test -- see the module docs.
#[tokio::test(flavor = "current_thread")]
async fn decorated_typed_path_allocation_count_does_not_scale_with_item_count() {
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
        "decorated typed: small={typed_small} large={typed_large} delta={typed_delta} \
         erased: small={erased_small} large={erased_large} delta={erased_delta} \
         items_delta={delta_items}"
    );

    assert!(
        typed_delta < delta_items / 100,
        "decorated typed path allocator-call count scaled with item count \
         (typed_delta={typed_delta}, items_delta={delta_items}): \
         composition/decoration reintroduced a per-item boxed future"
    );

    assert!(
        erased_delta >= delta_items,
        "erased path allocator-call count did not scale with item count \
         (erased_delta={erased_delta}, items_delta={delta_items}): \
         the measurement would not have caught a regression"
    );
}
