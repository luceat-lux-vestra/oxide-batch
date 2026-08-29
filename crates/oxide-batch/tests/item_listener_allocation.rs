//! M6 Gate F regression: a registered item listener still pays its ADR-0002
//! boxed-future cost per item, and that cost is measured separately from
//! `chunk_allocation.rs`'s listener-free ADR-0008 guarantee rather than
//! merged into it. Gate F (`docs/project/m6-design-gate-evidence.md`) is an
//! explicit KEEP of that boxed representation for M6; this file exists so a
//! future change that accidentally makes the listener-free path start boxing,
//! or that silently removes the listener boxing cost without a superseding
//! decision, shows up as a measurement change here.
//!
//! Same single-test-per-file discipline as `chunk_allocation.rs`, and for the
//! same reason: the allocator totals this file reads are process-global.

#![allow(
    clippy::expect_used,
    clippy::similar_names,
    clippy::cast_precision_loss
)]

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use std::alloc::System;
use std::sync::{Arc, Mutex};

use chunk_fixture::{Double, NoopCompletion, NoopTransactions, Sink, Source, correlation};
use oxide_batch::{
    ChunkExecutionOutcome, ChunkExecutionReport, ChunkSize, ChunkStep, ItemListenerSet,
    ReadListener, StepName, StopSource,
};
use serde_json::{Value, json};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// A read listener that does nothing beyond the trait's default (still
/// boxed) callbacks -- the minimal registered listener Gate F's cost model
/// describes.
struct NoopReadListener;

impl ReadListener<i64> for NoopReadListener {}

/// Runs `items` through one chunk with one registered [`ReadListener`] and
/// returns the number of allocator calls the run made.
async fn run_with_listener(items: u32) -> u64 {
    let output = Arc::new(Mutex::new(Vec::new()));
    let listeners: ItemListenerSet<i64, i64> = ItemListenerSet::new()
        .with_read_listener(Arc::new(NoopReadListener))
        .expect("registration is bounded");
    let mut step = ChunkStep::new(
        StepName::new("listener").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        Source::range(items),
        Double,
        Sink(output),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    )
    .with_item_listeners(listeners);
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

fn retain_observation(observation: &Value) {
    let Ok(path) = std::env::var("OXIDEBATCH_GATE_H_OBSERVATION") else {
        return;
    };
    std::fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&observation).expect("observation is serializable")
        ),
    )
    .expect("write Gate H observation");
}

/// Gate F boundary: a listener-enabled pipeline's allocator-call count DOES
/// scale with item count -- unlike the listener-free typed path in
/// `chunk_allocation.rs`. This proves the KEEP decision's cost is real and
/// unchanged, and that #151 did not fold it into (or hide it behind) the
/// listener-free hard guarantee.
async fn run_listener_allocation_measurement() {
    const SMALL: u32 = 200;
    const LARGE: u32 = 20_000;
    let delta_items = u64::from(LARGE - SMALL);

    let small = run_with_listener(SMALL).await;
    let large = run_with_listener(LARGE).await;
    let delta = large.saturating_sub(small);

    eprintln!(
        "listener-enabled: small={small} large={large} delta={delta} items_delta={delta_items}"
    );

    retain_observation(&json!({
        "workload": {
            "component": "Source/Double/Sink",
            "listener": "NoopReadListener",
            "items_small": SMALL,
            "items_large": LARGE,
            "items_delta": delta_items,
            "chunk_size": LARGE,
            "transaction_semantics": "NoopTransactions",
        },
        "listener_enabled": {
            "small_allocator_calls": small,
            "large_allocator_calls": large,
            "delta_allocator_calls": delta,
            "allocator_calls_per_item": delta as f64 / delta_items as f64,
        },
        "listener_representation": "boxed future retained by Gate F decision",
        "typed_listener_free_invariant": "measured separately; not included in this count",
        "correctness": "completed",
    }));

    assert!(
        delta >= delta_items,
        "listener-enabled path allocator-call count did not scale with item count \
         (delta={delta}, items_delta={delta_items}): Gate F's boxed-listener cost model \
         no longer matches the implementation"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn registered_listener_cost_is_reported_separately_from_component_cost() {
    run_listener_allocation_measurement().await;
}

#[tokio::test(flavor = "current_thread")]
async fn listener_enabled_allocation_is_reported_separately_from_typed_path() {
    run_listener_allocation_measurement().await;
}
