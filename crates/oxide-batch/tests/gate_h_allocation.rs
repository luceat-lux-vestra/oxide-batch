//! Gate H (#153 §6) primary listener-free allocation measurement: the same
//! real-component reference pipeline -- [`DelimitedReader`]/[`DelimitedWriter`]
//! (real CSV parsing/formatting, not a synthetic fixture) around the real
//! shipped [`IdentityProcessor`] -- run through the typed `ChunkStep` path
//! and through the same components erased behind
//! `BoxedReader`/`BoxedProcessor`/`BoxedWriter`.
//!
//! This file reports the required-metrics disclosure numbers (allocations
//! per item, per chunk) for a *real*, non-trivial component on the M6 P-002
//! reference workload. It deliberately does **not** assert "typed allocator
//! calls scale near zero", the way `chunk_allocation.rs`/
//! `item_components_allocation.rs` do for their pass-through fixtures --
//! `DelimitedReader`/`DelimitedWriter` genuinely allocate per record (parsed
//! `String` fields on read, formatted buffers on write), which is real,
//! expected component work, not framework overhead, and it swamps any
//! attempt to isolate a near-zero framework contribution from raw allocator
//! totals alone. Measured directly: typed delta ~24 allocator calls/item,
//! erased delta ~28/item (see the test's own printed numbers for the actual
//! run) -- both dominated by CSV parsing/formatting, not by per-item future
//! boxing.
//!
//! The hard criterion ("framework-controlled per-item future allocation on
//! the typed path == 0") is proved structurally in `gate_h_dispatch.rs` --
//! a fact about which concrete future type each call site's trait resolves
//! to, not something this kind of counter can isolate on an allocating
//! component -- and corroborated empirically by the existing zero-allocation
//! synthetic fixtures (`chunk_allocation.rs`/`item_components_allocation.rs`),
//! where the component itself does no per-item heap work, so any allocator
//! delta is attributable to the framework path. This file's own contribution
//! is the sanity check that *is* meaningful on a real component: the typed
//! path never allocates more per item than the erased path does for the
//! identical component work, which is what "the typed path adds zero
//! additional per-item boxing on top of the same component" implies.
//!
//! Reference workload choice: `DelimitedReader<File>`/`DelimitedWriter` are
//! the first-party CSV components (#147), doing genuine per-item work (field
//! parsing on read, formatting on write) rather than a pass-through --
//! exactly what distinguishes P-002 proper from the RFC-0005 spike's
//! synthetic workload (`docs/engineering/performance-plan.md`'s own framing).
//! [`IdentityProcessor`] is itself a real shipped component
//! (`item_components::basic`), not a test fixture.
//!
//! Methodology mirrors `chunk_allocation.rs`/`item_components_allocation.rs`
//! exactly: one process-global `stats_alloc` allocator (so one `#[test]` per
//! file), one chunk covering every item (`ChunkSize::new(items)`), and the
//! typed path's allocator-call count measured as a *delta* between a small
//! and a large item count rather than an absolute -- the reader's internal
//! `BufReader`/record buffers still grow via ordinary amortized `Vec`
//! reallocation as they establish their working size, which is unrelated to
//! per-item future boxing and would make an absolute-zero assertion a false
//! positive on the first few records.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::similar_names,
    clippy::cast_precision_loss
)]

use std::alloc::System;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use chunk_fixture::{NoopCompletion, NoopTransactions, correlation};
use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{DelimitedDialect, DelimitedRecord, delimited_file_reader};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, ComponentStreamIdentity, StepName, StopSource,
};
use serde_json::{Value, json};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(name).expect("static identity is valid")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos();
    std::env::temp_dir().join(format!("oxide-batch-gate-h-{name}-{nonce}.csv"))
}

/// Writes `rows` CSV records of three fields each -- the same shape
/// `item_components_flat_file_allocation.rs` uses for its own fixtures.
fn write_csv_fixture(path: &std::path::Path, rows: u32) {
    let mut file = File::create(path).expect("create fixture file");
    for index in 0..rows {
        writeln!(file, "{index},value-{index},filler-field").expect("write fixture row");
    }
    file.sync_all().expect("flush fixture file");
}

fn allocator_calls(stats: Stats) -> u64 {
    let total = stats.allocations + stats.deallocations + stats.reallocations;
    u64::try_from(total).unwrap_or(u64::MAX)
}

fn stats_json(stats: Stats) -> Value {
    json!({
        "allocations": stats.allocations,
        "deallocations": stats.deallocations,
        "reallocations": stats.reallocations,
        "bytes_allocated": stats.bytes_allocated,
        "bytes_deallocated": stats.bytes_deallocated,
        "bytes_reallocated": stats.bytes_reallocated,
        "allocator_calls": allocator_calls(stats),
    })
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

/// Runs `items` CSV records through the monomorphized, typed `ChunkStep`
/// path in one chunk and returns the allocator-call count.
async fn run_typed(items: u32) -> Stats {
    let input = temp_path("typed-in");
    let output = temp_path("typed-out");
    write_csv_fixture(&input, items);

    let (reader, _reader_stream, _reader_contract) = delimited_file_reader::<DelimitedRecord>(
        &input,
        DelimitedDialect::csv(),
        identity("gate-h.typed.reader"),
    )
    .expect("open fixture reader");
    let (writer, _writer_stream, _writer_contract) =
        oxide_batch::item_components::delimited_writer(
            &output,
            DelimitedDialect::csv(),
            identity("gate-h.typed.writer"),
        )
        .expect("open fixture writer");

    let mut step: ChunkStep<DelimitedRecord, DelimitedRecord, _, _, _> = ChunkStep::new(
        StepName::new("gate-h-typed").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        reader,
        IdentityProcessor,
        writer,
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let region = Region::new(ALLOCATOR);
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let stats = region.change();
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let _ = std::fs::remove_file(&input);
    let _ = std::fs::remove_file(&output);
    stats
}

/// Runs the same reference pipeline through the explicit `Boxed*` erasure
/// boundary and returns the allocator-call count.
async fn run_erased(items: u32) -> Stats {
    let input = temp_path("erased-in");
    let output = temp_path("erased-out");
    write_csv_fixture(&input, items);

    let (reader, _reader_stream, _reader_contract) = delimited_file_reader::<DelimitedRecord>(
        &input,
        DelimitedDialect::csv(),
        identity("gate-h.erased.reader"),
    )
    .expect("open fixture reader");
    let (writer, _writer_stream, _writer_contract) =
        oxide_batch::item_components::delimited_writer(
            &output,
            DelimitedDialect::csv(),
            identity("gate-h.erased.writer"),
        )
        .expect("open fixture writer");

    let mut step = ChunkStep::new(
        StepName::new("gate-h-erased").expect("static step name is valid"),
        ChunkSize::new(items).expect("static chunk size is nonzero"),
        BoxedReader::<DelimitedRecord>::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();
    let region = Region::new(ALLOCATOR);
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let stats = region.change();
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let _ = std::fs::remove_file(&input);
    let _ = std::fs::remove_file(&output);
    stats
}

/// Gate H's required-metrics disclosure for the real-component reference
/// workload, plus the one comparison that *is* meaningful on an allocating
/// component (typed never allocates more per item than erased). This is the
/// file's only test -- see the module docs for why.
#[tokio::test(flavor = "current_thread")]
async fn typed_csv_pipeline_allocates_no_more_per_item_than_erased() {
    const SMALL: u32 = 200;
    const LARGE: u32 = 20_000;
    let delta_items = u64::from(LARGE - SMALL);

    let typed_small = run_typed(SMALL).await;
    let typed_large = run_typed(LARGE).await;
    let erased_small = run_erased(SMALL).await;
    let erased_large = run_erased(LARGE).await;

    let typed_delta = typed_large - typed_small;
    let erased_delta = erased_large - erased_small;
    let typed_calls_delta = allocator_calls(typed_delta);
    let erased_calls_delta = allocator_calls(erased_delta);
    // u64 division truncates; reported to a few decimal places for
    // readability, not used in the assertion below (which compares the raw
    // deltas directly to avoid any rounding).
    let typed_per_item = typed_calls_delta as f64 / delta_items as f64;
    let erased_per_item = erased_calls_delta as f64 / delta_items as f64;

    // stderr, not stdout: the campaign runner (xtask/src/suite.rs) parses
    // only stdout to correlate each libtest "test <name> ... " prefix with
    // its outcome line. A multi-line stdout print here (needed to make these
    // numbers visible under --nocapture) lands between the prefix and the
    // outcome and breaks that correlation -- caught directly by a real CI
    // run reporting this test as "did not run" despite passing. stderr is
    // inherited straight through to the log without being parsed, so it
    // stays visible without disturbing the parser.
    eprintln!(
        "gate-h allocation disclosure (real component: DelimitedReader/DelimitedWriter CSV \
         parsing/formatting + IdentityProcessor): \
         typed small={} large={} delta={} \
         ({typed_per_item:.3} allocator calls/item) | \
         erased small={} large={} delta={} \
         ({erased_per_item:.3} allocator calls/item) | \
         items_delta={delta_items}",
        allocator_calls(typed_small),
        allocator_calls(typed_large),
        typed_calls_delta,
        allocator_calls(erased_small),
        allocator_calls(erased_large),
        erased_calls_delta,
    );

    retain_observation(&json!({
        "workload": {
            "component": "DelimitedReader/DelimitedWriter",
            "processor": "IdentityProcessor",
            "items_small": SMALL,
            "items_large": LARGE,
            "items_delta": delta_items,
            "chunk_size": LARGE,
            "transaction_semantics": "NoopTransactions",
        },
        "typed": {
            "small": stats_json(typed_small),
            "large": stats_json(typed_large),
            "delta": stats_json(typed_delta),
            "allocator_calls_per_item": typed_per_item,
            "allocator_calls_per_chunk": typed_calls_delta,
        },
        "boxed": {
            "small": stats_json(erased_small),
            "large": stats_json(erased_large),
            "delta": stats_json(erased_delta),
            "allocator_calls_per_item": erased_per_item,
            "allocator_calls_per_chunk": erased_calls_delta,
        },
        "copied_bytes": {
            "value": null,
            "note": "Not measurable at this component boundary."
        },
        "buffer_reuse": {
            "value": null,
            "note": "Delimited component internal buffer reuse is not exposed as a counter."
        },
        "framework_controlled": {
            "typed_per_item_future_allocations": 0,
            "typed_dynamic_dispatch_per_item": 0,
            "typed_future_boxing": 0,
            "proof": "gate_h_dispatch target"
        },
        "correctness": "completed",
    }));

    assert!(
        erased_calls_delta >= delta_items,
        "erased path allocator-call count did not scale with item count \
         (erased_delta={erased_calls_delta}, items_delta={delta_items}): the measurement would not \
         have caught a regression"
    );
    assert!(
        typed_calls_delta <= erased_calls_delta,
        "typed real-component pipeline allocated more per item than the same component erased \
         (typed_delta={typed_calls_delta}, erased_delta={erased_calls_delta}): since both paths run the \
         identical component doing the identical per-item work, typed exceeding erased would \
         mean the typed path carries its own additional per-item allocation on top of that \
         shared cost, which the framework-future-allocation==0 invariant forbids"
    );
}
