//! Produces the RFC-0005 raw measurement record on stdout as JSON.
//!
//! Both paths call one function, `driver::run`. The only difference is whether
//! the components reach it concretely or behind `Boxed*` handles, so the
//! numbers below are the cost of that choice and nothing else.
//!
//! Two phases. The allocation phase counts heap traffic with tracing enabled
//! and every allocation-bearing structure — stop token, batch buffer, trace
//! storage, the handles themselves — built before the window opens. The timing
//! phase disables tracing, warms both paths, and interleaves repetitions so
//! drift hits both equally. Neither phase involves an async runtime.
//!
//! Overridable with `OXIDEBATCH_SPIKE_ITEMS`, `OXIDEBATCH_SPIKE_CHUNK`,
//! `OXIDEBATCH_SPIKE_REPEATS`, `OXIDEBATCH_SPIKE_ALLOC_ITEMS`, and
//! `OXIDEBATCH_SPIKE_ALLOC_CHUNK`.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::env;
use std::sync::Arc;
use std::time::Instant;

use oxide_batch::{StopSource, StopToken};
use oxide_batch_m6_spikes::allocation::{self, CountingAllocator, Measurement};
use oxide_batch_m6_spikes::contract::{
    BoxedProcessor, BoxedReader, BoxedWriter, ItemProcessor, ItemReader, ItemWriter,
};
use oxide_batch_m6_spikes::driver::{RunReport, run};
use oxide_batch_m6_spikes::executor::block_on;
use oxide_batch_m6_spikes::workload::{
    ChecksumWriter, Output, RangeReader, Record, ScalingProcessor, SharedChecksumWriter,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const FACTOR: u64 = 3;

fn setting<T: std::str::FromStr>(name: &str, fallback: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// One run of the shared driver over whatever components it is handed.
fn measure_run<R, P, W>(
    reader: &mut R,
    processor: &P,
    writer: &W,
    stop: &StopToken,
    chunk_size: usize,
    buffer: &mut Vec<Output>,
    mut report: RunReport,
) -> RunReport
where
    R: ItemReader<Record>,
    P: ItemProcessor<Record, Output>,
    W: ItemWriter<Output>,
{
    block_on(run(
        reader,
        processor,
        writer,
        stop,
        chunk_size,
        buffer,
        &mut report,
    ));
    report
}

struct Timing {
    best_ns: u128,
    mean_ns: u128,
}

impl Timing {
    fn from(samples: &[u128]) -> Self {
        let best = samples.iter().copied().min().unwrap_or_default();
        let total: u128 = samples.iter().sum();
        Self {
            best_ns: best,
            mean_ns: total / samples.len().max(1) as u128,
        }
    }

    fn ns_per_item(&self, items: u64) -> f64 {
        self.best_ns as f64 / items as f64
    }

    fn items_per_second(&self, items: u64) -> f64 {
        if self.best_ns == 0 {
            return 0.0;
        }
        items as f64 * 1_000_000_000.0 / self.best_ns as f64
    }

    fn json(&self, items: u64) -> String {
        format!(
            r#"{{"best_ns":{},"mean_ns":{},"ns_per_item":{:.4},"items_per_second":{:.1}}}"#,
            self.best_ns,
            self.mean_ns,
            self.ns_per_item(items),
            self.items_per_second(items)
        )
    }
}

fn measurement_json(measurement: Measurement, items: u64) -> String {
    format!(
        r#"{{"allocations":{},"bytes":{},"per_item":{:.4}}}"#,
        measurement.allocations,
        measurement.bytes,
        measurement.per_item(items).unwrap_or(0.0)
    )
}

fn main() {
    let alloc_items: u64 = setting("OXIDEBATCH_SPIKE_ALLOC_ITEMS", 10_000);
    let alloc_chunk: usize = setting("OXIDEBATCH_SPIKE_ALLOC_CHUNK", 100);
    let items: u64 = setting("OXIDEBATCH_SPIKE_ITEMS", 1_000_000);
    let chunk_size: usize = setting("OXIDEBATCH_SPIKE_CHUNK", 1_000);
    let repeats: usize = setting::<usize>("OXIDEBATCH_SPIKE_REPEATS", 7).max(1);

    let capacity = usize::try_from(alloc_items).unwrap_or(usize::MAX);
    let (_source, stop) = StopSource::new();
    let mut buffer: Vec<Output> = Vec::with_capacity(chunk_size.max(alloc_chunk));

    // Both paths write through `SharedChecksumWriter`, so the extra
    // indirection is symmetric and the boxed path's durable state stays
    // observable after its writer moves behind the handle.
    let typed_state = Arc::new(ChecksumWriter::new());
    let boxed_state = Arc::new(ChecksumWriter::new());
    let typed_writer = SharedChecksumWriter(Arc::clone(&typed_state));
    let boxed_writer = BoxedWriter::new(SharedChecksumWriter(Arc::clone(&boxed_state)));
    let typed_processor = ScalingProcessor::new(FACTOR);
    let boxed_processor = BoxedProcessor::new(ScalingProcessor::new(FACTOR));

    // ---- allocation phase ----
    let mut typed_reader = RangeReader::new(alloc_items);
    let report = RunReport::with_capacity(capacity);
    allocation::begin();
    let typed_report = measure_run(
        &mut typed_reader,
        &typed_processor,
        &typed_writer,
        &stop,
        alloc_chunk,
        &mut buffer,
        report,
    );
    let typed_allocations = allocation::end();

    let mut boxed_reader = BoxedReader::new(RangeReader::new(alloc_items));
    let report = RunReport::with_capacity(capacity);
    allocation::begin();
    let boxed_report = measure_run(
        &mut boxed_reader,
        &boxed_processor,
        &boxed_writer,
        &stop,
        alloc_chunk,
        &mut buffer,
        report,
    );
    let boxed_allocations = allocation::end();

    let equivalent =
        typed_report == boxed_report && typed_state.checksum() == boxed_state.checksum();

    // ---- timing phase ----
    let mut timed_typed = RangeReader::new(items);
    let mut timed_boxed = BoxedReader::new(RangeReader::new(items));

    let _ = measure_run(
        &mut timed_typed,
        &typed_processor,
        &typed_writer,
        &stop,
        chunk_size,
        &mut buffer,
        RunReport::untraced(),
    );
    let _ = measure_run(
        &mut timed_boxed,
        &boxed_processor,
        &boxed_writer,
        &stop,
        chunk_size,
        &mut buffer,
        RunReport::untraced(),
    );

    let mut typed_samples = Vec::with_capacity(repeats);
    let mut boxed_samples = Vec::with_capacity(repeats);
    let mut counts_agree = true;

    for _ in 0..repeats {
        // Rewinding the readers is deliberately outside both timed regions.
        timed_typed = RangeReader::new(items);
        timed_boxed = BoxedReader::new(RangeReader::new(items));

        let started = Instant::now();
        let typed = measure_run(
            &mut timed_typed,
            &typed_processor,
            &typed_writer,
            &stop,
            chunk_size,
            &mut buffer,
            RunReport::untraced(),
        );
        typed_samples.push(started.elapsed().as_nanos());

        let started = Instant::now();
        let boxed = measure_run(
            &mut timed_boxed,
            &boxed_processor,
            &boxed_writer,
            &stop,
            chunk_size,
            &mut buffer,
            RunReport::untraced(),
        );
        boxed_samples.push(started.elapsed().as_nanos());

        counts_agree &= typed.items_written == boxed.items_written;
    }

    let typed_timing = Timing::from(&typed_samples);
    let boxed_timing = Timing::from(&boxed_samples);
    let ratio = if typed_timing.best_ns == 0 {
        0.0
    } else {
        boxed_timing.best_ns as f64 / typed_timing.best_ns as f64
    };
    let added_ns_per_item = boxed_timing.ns_per_item(items) - typed_timing.ns_per_item(items);

    println!("{{");
    println!(r#"  "equivalent": {equivalent},"#);
    println!(r#"  "counts_agree": {counts_agree},"#);
    println!(r#"  "allocation": {{"#);
    println!(r#"    "items": {alloc_items},"#);
    println!(r#"    "chunk_size": {alloc_chunk},"#);
    println!(
        r#"    "typed": {},"#,
        measurement_json(typed_allocations, alloc_items)
    );
    println!(
        r#"    "boxed": {}"#,
        measurement_json(boxed_allocations, alloc_items)
    );
    println!("  }},");
    println!(r#"  "timing": {{"#);
    println!(r#"    "items": {items},"#);
    println!(r#"    "chunk_size": {chunk_size},"#);
    println!(r#"    "repeats": {repeats},"#);
    println!(r#"    "typed": {},"#, typed_timing.json(items));
    println!(r#"    "boxed": {},"#, boxed_timing.json(items));
    println!(r#"    "boxed_over_typed": {ratio:.4},"#);
    println!(r#"    "added_ns_per_item": {added_ns_per_item:.4}"#);
    println!("  }}");
    println!("}}");
}
