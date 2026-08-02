//! Reproducible raw-evidence recording for the M4 measurement reports.
//!
//! The [performance plan](../../../../docs/engineering/performance-plan.md)
//! requires every measurement to record its environment, its correctness
//! result, and machine-readable raw evidence. This module owns that envelope so
//! each measurement only describes its own scale points.
//!
//! Reports are written to `OXIDEBATCH_MEASUREMENT_DIR` when it is set and to
//! `target/m4-measurements` otherwise, so an ordinary `cargo test` run never
//! writes into the repository.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use serde_json::{Map, Value, json};

/// Directory used when no explicit measurement directory is configured.
const DEFAULT_DIRECTORY: &str = "target/m4-measurements";

/// Resolves a report directory against the workspace root.
///
/// Cargo runs integration tests from the package directory, so a relative
/// destination is anchored explicitly instead of depending on that detail.
fn directory() -> PathBuf {
    let configured =
        env::var("OXIDEBATCH_MEASUREMENT_DIR").unwrap_or_else(|_| DEFAULT_DIRECTORY.to_owned());
    let configured = PathBuf::from(configured);
    if configured.is_absolute() {
        return configured;
    }
    workspace_root().join(configured)
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// One measurement report and its recorded environment.
pub struct Report {
    id: &'static str,
    workload: &'static str,
    summary: String,
    points: Vec<Value>,
    correctness: Vec<Value>,
    notes: Vec<String>,
}

impl Report {
    /// Opens a report for one performance-plan workload identifier.
    pub fn new(id: &'static str, workload: &'static str, summary: impl Into<String>) -> Self {
        Self {
            id,
            workload,
            summary: summary.into(),
            points: Vec::new(),
            correctness: Vec::new(),
            notes: Vec::new(),
        }
    }

    /// Appends one measured scale point.
    pub fn point(&mut self, value: Value) -> &mut Self {
        self.points.push(value);
        self
    }

    /// Records one named correctness assertion that the measurement enforced.
    ///
    /// A measurement without its correctness result is not evidence, so every
    /// report carries the assertions that ran beside its numbers.
    pub fn correctness(&mut self, assertion: &str, holds: bool) -> &mut Self {
        self.correctness.push(json!({
            "assertion": assertion,
            "holds": holds,
        }));
        self
    }

    /// Records one reviewed limitation or interpretation note.
    pub fn note(&mut self, note: impl Into<String>) -> &mut Self {
        self.notes.push(note.into());
        self
    }

    /// Writes the report and returns the file it produced.
    ///
    /// # Errors
    ///
    /// Returns the underlying filesystem or serialization failure.
    pub fn write(&self) -> Result<PathBuf, Box<dyn Error>> {
        let directory = directory();
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{}.json", self.id.to_lowercase()));
        let document = json!({
            "report": self.id,
            "workload": self.workload,
            "summary": self.summary,
            "environment": environment(),
            "points": self.points,
            "correctness": self.correctness,
            "notes": self.notes,
        });
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&document)?),
        )?;
        Ok(path)
    }
}

/// Records the environment every reported number depends on.
fn environment() -> Value {
    let mut map = Map::new();
    map.insert(
        "source_commit".into(),
        json!(command("git", &["rev-parse", "HEAD"])),
    );
    map.insert(
        "source_tree_clean".into(),
        json!(command("git", &["status", "--porcelain"]).map(|status| status.is_empty())),
    );
    map.insert("rustc".into(), json!(command("rustc", &["--version"])));
    map.insert(
        "profile".into(),
        json!(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }),
    );
    map.insert("os".into(), json!(env::consts::OS));
    map.insert("arch".into(), json!(env::consts::ARCH));
    map.insert(
        "available_parallelism".into(),
        json!(
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .ok()
        ),
    );
    map.insert("tokio_worker_threads".into(), json!(WORKER_THREADS));
    map.insert("resident_kib".into(), json!(resident_kib()));
    Value::Object(map)
}

/// Fixed Tokio worker-thread count used by every measurement in this suite.
///
/// Recording one pinned value keeps the reported scaling numbers comparable
/// between runs on hosts with different core counts.
pub const WORKER_THREADS: usize = 4;

/// Runs one environment-describing command, tolerating an absent tool.
fn command(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Reads process resident memory in KiB where the platform exposes it cheaply.
///
/// This is best-effort context, never an assertion subject: a `None` result
/// means the platform was not sampled rather than that memory was unbounded.
#[must_use]
pub fn resident_kib() -> Option<u64> {
    if cfg!(target_os = "linux") {
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        return Some(resident_pages.saturating_mul(4));
    }
    let pid = std::process::id().to_string();
    let output = command("ps", &["-o", "rss=", "-p", &pid])?;
    output.trim().parse().ok()
}

/// Collects a bounded latency sample and reports its ordered statistics.
#[derive(Debug, Default)]
pub struct Latencies(Vec<Duration>);

impl Latencies {
    /// Opens an empty sample.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Records one observation.
    pub fn record(&mut self, elapsed: Duration) {
        self.0.push(elapsed);
    }

    /// Returns the number of observations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Summarizes the sample as minimum, median, maximum, and total.
    #[must_use]
    pub fn summary(&self) -> Value {
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        json!({
            "samples": sorted.len(),
            "min_micros": sorted.first().copied().unwrap_or_default().as_micros(),
            "median_micros": sorted.get(sorted.len() / 2).copied().unwrap_or_default().as_micros(),
            "max_micros": sorted.last().copied().unwrap_or_default().as_micros(),
            "total_micros": sorted.iter().sum::<Duration>().as_micros(),
        })
    }
}

/// A measurement that could not describe its own scale point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeasurementError(String);

impl MeasurementError {
    /// Builds a measurement failure from a static explanation.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for MeasurementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for MeasurementError {}
