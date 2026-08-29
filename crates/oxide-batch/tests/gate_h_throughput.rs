//! Gate H (#153 §6) throughput/latency disclosure: wall-clock measurement of
//! the same real-component reference workload `gate_h_allocation.rs` and
//! `gate_h_dispatch.rs` use (`DelimitedReader`/`DelimitedWriter` CSV
//! parsing/formatting around `IdentityProcessor`), typed vs `Boxed*`.
//!
//! This is disclosure evidence only. The frozen protocol
//! (`docs/project/m6-design-gate-evidence.md`, Gate H) sets no numeric
//! throughput/latency threshold, and this file asserts none -- both paths
//! only need to complete correctly and report their raw numbers. It follows
//! `tests/performance/mod.rs`'s own measurement style (warmup, repetitions,
//! reported variance, no compared-against-a-number assertion) rather than
//! introducing `criterion` or any new dependency.
//!
//! Hardware is not held constant across CI runs (see
//! `performance::measurement_environment`'s own note), so these numbers are
//! retained as evidence of *what ran*, not compared release-over-release
//! here.

#![allow(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::similar_names
)]

#[path = "performance/mod.rs"]
mod performance;

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

use chunk_fixture::{NoopCompletion, NoopTransactions, correlation};
use oxide_batch::item_components::basic::IdentityProcessor;
use oxide_batch::item_components::{DelimitedDialect, DelimitedRecord, delimited_file_reader};
use oxide_batch::{
    BoxedProcessor, BoxedReader, BoxedWriter, ChunkExecutionOutcome, ChunkExecutionReport,
    ChunkSize, ChunkStep, ComponentStreamIdentity, StepName, StopSource,
};
use serde_json::{Value, json};

/// Items per run. Large enough that per-item overhead dominates process
/// startup noise, small enough that the whole warmup+repetition schedule
/// finishes in a few seconds.
const ITEMS: u32 = 50_000;
/// Untimed runs before measurement begins, to let allocator/OS-level warm-up
/// effects (first-touch page faults, allocator arena growth) settle.
const WARMUP_REPETITIONS: u32 = 2;
/// Timed, retained repetitions.
const MEASURED_REPETITIONS: u32 = 5;

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

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(name).expect("static identity is valid")
}

fn temp_path(name: &str, nonce: u64) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("oxide-batch-gate-h-throughput-{name}-{nonce}.csv"))
}

fn write_csv_fixture(path: &std::path::Path, rows: u32) {
    let mut file = File::create(path).expect("create fixture file");
    for index in 0..rows {
        writeln!(file, "{index},value-{index},filler-field").expect("write fixture row");
    }
    file.sync_all().expect("flush fixture file");
}

/// Runs one timed pass of the typed pipeline over a freshly written fixture
/// and returns the elapsed wall-clock time for `step.execute` alone (fixture
/// setup/teardown is excluded, matching what a caller of the framework
/// itself pays).
async fn run_typed_once(nonce: u64) -> Duration {
    let input = temp_path("typed-in", nonce);
    let output = temp_path("typed-out", nonce);
    write_csv_fixture(&input, ITEMS);

    let (reader, _reader_stream, _reader_contract) = delimited_file_reader::<DelimitedRecord>(
        &input,
        DelimitedDialect::csv(),
        identity("gate-h.throughput.typed.reader"),
    )
    .expect("open fixture reader");
    let (writer, _writer_stream, _writer_contract) =
        oxide_batch::item_components::delimited_writer(
            &output,
            DelimitedDialect::csv(),
            identity("gate-h.throughput.typed.writer"),
        )
        .expect("open fixture writer");

    let mut step: ChunkStep<DelimitedRecord, DelimitedRecord, _, _, _> = ChunkStep::new(
        StepName::new("gate-h-throughput-typed").expect("static step name is valid"),
        ChunkSize::new(ITEMS).expect("static chunk size is nonzero"),
        reader,
        IdentityProcessor,
        writer,
        std::sync::Arc::new(NoopTransactions),
        std::sync::Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();

    let started = Instant::now();
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let elapsed = started.elapsed();
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let _ = std::fs::remove_file(&input);
    let _ = std::fs::remove_file(&output);
    elapsed
}

/// Runs one timed pass of the same pipeline through the `Boxed*` erasure
/// boundary.
async fn run_erased_once(nonce: u64) -> Duration {
    let input = temp_path("erased-in", nonce);
    let output = temp_path("erased-out", nonce);
    write_csv_fixture(&input, ITEMS);

    let (reader, _reader_stream, _reader_contract) = delimited_file_reader::<DelimitedRecord>(
        &input,
        DelimitedDialect::csv(),
        identity("gate-h.throughput.erased.reader"),
    )
    .expect("open fixture reader");
    let (writer, _writer_stream, _writer_contract) =
        oxide_batch::item_components::delimited_writer(
            &output,
            DelimitedDialect::csv(),
            identity("gate-h.throughput.erased.writer"),
        )
        .expect("open fixture writer");

    let mut step = ChunkStep::new(
        StepName::new("gate-h-throughput-erased").expect("static step name is valid"),
        ChunkSize::new(ITEMS).expect("static chunk size is nonzero"),
        BoxedReader::<DelimitedRecord>::new(reader),
        BoxedProcessor::new(IdentityProcessor),
        BoxedWriter::new(writer),
        std::sync::Arc::new(NoopTransactions),
        std::sync::Arc::new(NoopCompletion),
    );
    let (_source, stop) = StopSource::new();

    let started = Instant::now();
    let report: ChunkExecutionReport = step.execute(&correlation(), &stop).await;
    let elapsed = started.elapsed();
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let _ = std::fs::remove_file(&input);
    let _ = std::fs::remove_file(&output);
    elapsed
}

/// Summary statistics over one representation's measured repetitions.
struct Summary {
    min: Duration,
    max: Duration,
    mean: Duration,
}

fn summarize(samples: &[Duration]) -> Summary {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let total_nanos: u128 = sorted.iter().map(Duration::as_nanos).sum();
    let mean_nanos = total_nanos / sorted.len() as u128;
    let mean = Duration::from_nanos(u64::try_from(mean_nanos).unwrap_or(u64::MAX));
    Summary { min, max, mean }
}

fn throughput_items_per_sec(items: u32, elapsed: Duration) -> f64 {
    f64::from(items) / elapsed.as_secs_f64()
}

/// Gate H's throughput/latency disclosure: raw numbers only, no
/// pass/fail threshold. This is the file's only test -- allocator-based
/// files in this campaign carry that restriction because their allocator is
/// process-global; this one does not need it, but stays one test per file
/// for consistency with the rest of the Gate H suite and to keep each run's
/// printed report self-contained.
#[tokio::test(flavor = "current_thread")]
async fn throughput_and_latency_recorded_without_an_invented_threshold() {
    let mut nonce: u64 = 0;
    let mut next_nonce = || {
        nonce += 1;
        nonce
    };

    for _ in 0..WARMUP_REPETITIONS {
        run_typed_once(next_nonce()).await;
        run_erased_once(next_nonce()).await;
    }

    let mut typed_samples = Vec::with_capacity(MEASURED_REPETITIONS as usize);
    let mut erased_samples = Vec::with_capacity(MEASURED_REPETITIONS as usize);
    for _ in 0..MEASURED_REPETITIONS {
        typed_samples.push(run_typed_once(next_nonce()).await);
        erased_samples.push(run_erased_once(next_nonce()).await);
    }

    let typed = summarize(&typed_samples);
    let erased = summarize(&erased_samples);

    // stderr, not stdout: see gate_h_allocation.rs's identical comment --
    // the campaign runner parses only stdout to correlate each libtest
    // "test <name> ... " prefix with its outcome, and a multi-line stdout
    // print here breaks that correlation under --nocapture.
    eprintln!(
        "gate-h throughput disclosure (real component: DelimitedReader/DelimitedWriter CSV \
         parsing/formatting + IdentityProcessor, {ITEMS} items, {WARMUP_REPETITIONS} warmup + \
         {MEASURED_REPETITIONS} measured repetitions):\n\
         typed:  min={:?} mean={:?} max={:?} throughput_mean={:.0} items/s\n\
         erased: min={:?} mean={:?} max={:?} throughput_mean={:.0} items/s\n\
         environment: {}",
        typed.min,
        typed.mean,
        typed.max,
        throughput_items_per_sec(ITEMS, typed.mean),
        erased.min,
        erased.mean,
        erased.max,
        throughput_items_per_sec(ITEMS, erased.mean),
        performance::measurement_environment(1),
    );

    retain_observation(&json!({
        "workload": {
            "component": "DelimitedReader/DelimitedWriter",
            "processor": "IdentityProcessor",
            "items": ITEMS,
            "chunk_size": ITEMS,
            "warmup_repetitions": WARMUP_REPETITIONS,
            "measured_repetitions": MEASURED_REPETITIONS,
            "transaction_semantics": "NoopTransactions",
        },
        "typed": {
            "raw_latency_nanoseconds": typed_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
            "min_nanoseconds": typed.min.as_nanos(),
            "mean_nanoseconds": typed.mean.as_nanos(),
            "max_nanoseconds": typed.max.as_nanos(),
            "throughput_items_per_second": throughput_items_per_sec(ITEMS, typed.mean),
        },
        "boxed": {
            "raw_latency_nanoseconds": erased_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
            "min_nanoseconds": erased.min.as_nanos(),
            "mean_nanoseconds": erased.mean.as_nanos(),
            "max_nanoseconds": erased.max.as_nanos(),
            "throughput_items_per_second": throughput_items_per_sec(ITEMS, erased.mean),
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

    // No threshold assertion by design -- disclosure only, per the frozen
    // protocol's "no invented performance threshold" rule. The correctness
    // assertions inside run_typed_once/run_erased_once (report outcome ==
    // Completed) are the only pass/fail condition this file carries.
}
