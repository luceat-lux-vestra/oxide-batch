//! Mechanics the M5 performance and reference-workload reports are built from.
//!
//! Three reports share a fixture and a way of describing the process they ran
//! in, and that is what lives here rather than being restated three times. It
//! deliberately does not share a workload shape the way the cancellation
//! campaign's reports do: P-001, P-003, and P-010 are three different
//! workloads by the accepted plan's own words, and forcing them through one
//! mechanism would blur that rather than reuse it.
//!
//! ## Why durations are never judged here
//!
//! No accepted document states a binding M5 throughput, latency, or
//! scaling-efficiency limit. The committed scope says so in as many words, and
//! `cargo xtask performance` enforces the same rule from the other side:
//! nothing here compares a duration or a rate against a number. What is
//! checked is correctness — every declared obligation held — and the finite
//! resource ceilings the workload declares.

#![allow(
    dead_code,
    reason = "the three reports use overlapping subsets of these mechanics"
)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use std::{env, process};

use oxide_batch::{Clock, PostgresConfig, TlsMode};
use serde_json::Value;

/// The variable that tells a report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_PERFORMANCE_OBSERVATIONS";

/// Returns the runtime connection string the fixture supplies.
#[must_use]
pub fn runtime_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_TEST_URL")
}

/// Returns the migrating connection string the fixture supplies.
#[must_use]
pub fn migrator_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL")
}

/// Reads one environment variable, treating an empty value as absent.
#[must_use]
pub fn variable(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

/// Builds the configuration a report's repository is opened with.
///
/// The pool is sized to the derived requirement exactly, so a report that held
/// one connection more than it declared would exhaust the pool rather than
/// have the excess absorbed by spare capacity.
///
/// # Errors
///
/// Returns the configuration failure when the URL, the size, or a timeout is
/// rejected.
pub fn config(url: String, connections: u32) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::Plaintext)
        .with_pool_size(connections)?
        .with_statement_timeout(Duration::from_mins(2))?
        .with_lock_timeout(Duration::from_mins(2))?
        .with_pool_close_timeout(Duration::from_mins(1))?
        .with_acquire_timeout(Duration::from_mins(1))?)
}

/// Removes every durable trace of one job name.
///
/// Each report clears its job name before it runs, so a rerun never observes
/// what an earlier attempt left.
///
/// # Errors
///
/// Returns the database failure that prevented the cleanup.
pub async fn remove_job(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_step_partition WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_flow_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(job_name).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

/// Returns the major version of a `PostgreSQL` server version string.
#[must_use]
pub fn major_version(server: &str) -> String {
    server.split(['.', ' ']).next().unwrap_or(server).to_owned()
}

/// Retains a report's observation where the runner will read it.
///
/// Returns `None` when the campaign is not driving the run, which is what an
/// ordinary `cargo test` does. The campaign requires the file to exist, so a
/// report that never reached its end cannot be counted as evidence.
///
/// # Errors
///
/// Returns the failure when the observation cannot be rendered or written.
pub fn retain_observation(name: &str, document: &Value) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let Some(directory) = variable(OBSERVATIONS_ENV) else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{name}.json"));
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(document)?),
    )?;
    Ok(Some(path))
}

/// The canonical closure of what the campaign executes.
///
/// Read from `tests/fixtures/performance/campaign-semantics.json` rather than
/// listed here, because the verifier reads the same document: a closure kept
/// in two places is one that will disagree.
///
/// # Errors
///
/// Returns the failure when the document cannot be read or parsed.
pub fn semantics_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let path = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("performance")
        .join("campaign-semantics.json");
    let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
    let categories = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure::boxed("the semantics document declares no categories"))?;
    let mut paths = categories
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return Err(Failure::boxed("the semantics document declares no paths"));
    }
    Ok(paths)
}

/// Records the object identity of the campaign's closure, as executed.
///
/// Taken here rather than reconstructed later: this process is the campaign,
/// so the tree it can see is by definition the tree that ran. In CI that is
/// the pull-request merge commit the workflow checked out — an ephemeral
/// object no later clone can resolve — so a verifier that tried to re-derive
/// these identities from a commit name would depend on something GitHub
/// throws away.
///
/// # Errors
///
/// Returns the failure when the closure cannot be read, or when git cannot
/// describe the tree the campaign is running against.
pub fn execution_manifest() -> Result<Value, Box<dyn Error>> {
    let root = workspace_root();
    let commit = git(&root, &["rev-parse", "HEAD"])
        .ok_or_else(|| Failure::boxed("the campaign is not running inside a git tree"))?;
    let mut objects = serde_json::Map::new();
    for path in semantics_paths()? {
        let object = git(&root, &["rev-parse", &format!("HEAD:{path}")]).ok_or_else(|| {
            Failure::boxed(format!(
                "{path} is declared as campaign semantics and is not present"
            ))
        })?;
        objects.insert(path, Value::String(object));
    }
    Ok(serde_json::json!({
        "execution_commit": commit,
        "execution_commit_note": "The tree this run actually executed against, read from the \
                                  checkout the campaign is running in. In CI this is the \
                                  pull-request merge commit rather than the branch head, and it \
                                  is the authority: the objects below are its objects.",
        "tree_clean": git(&root, &["status", "--porcelain"]).map(|status| status.is_empty()),
        "objects": Value::Object(objects),
    }))
}

/// Runs one git command against the workspace, tolerating failure.
fn git(root: &std::path::Path, arguments: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Returns the workspace root that contains this package.
#[must_use]
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Describes the process and host a report's durations were measured on.
///
/// Recorded because the durations are the point of this campaign and they are
/// not portable: a release build on a shared CI runner produces different
/// numbers from an idle development host, and a reader who cannot see which
/// one produced a figure cannot use it. Nothing here is asserted on.
///
/// `runs-on: ubuntu-24.04` names an OS image, not stable hardware: GitHub does
/// not guarantee a fixed CPU model or clock speed for hosted runners, so the
/// CPU model and logical core count are recorded precisely so a reader does
/// not read that stability into the label.
#[must_use]
pub fn measurement_environment(worker_threads: usize) -> Value {
    serde_json::json!({
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "profile_note": "The M5 performance campaign runs release, unlike the other M5 \
                         campaigns, because the accepted denominator requires it: a debug-build \
                         figure would not be comparable to anything release planning could use. \
                         Nothing here is asserted against a number regardless of profile.",
        "tokio_worker_threads": worker_threads,
        "available_parallelism": std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or_default(),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
        "cpu_model": cpu_model(),
        "kernel": kernel_version(),
        "resident_kib": resident_kib(),
        "hardware_stability_note": "runs-on: ubuntu-24.04 names an OS image, not stable CPU \
                                    hardware. GitHub-hosted runners are not a guarantee of \
                                    consistent physical or virtual hardware between runs, so the \
                                    CPU model and core count are recorded rather than assumed \
                                    stable, and no throughput or latency figure here is compared \
                                    across runs as if the hardware were held constant.",
    })
}

/// Reads process resident memory in KiB where the platform exposes it cheaply.
#[must_use]
pub fn resident_kib() -> Option<u64> {
    if cfg!(target_os = "linux") {
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        return Some(resident_pages.saturating_mul(4));
    }
    let pid = process::id().to_string();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

/// Reads the kernel release string where the platform exposes `uname -r`.
#[must_use]
pub fn kernel_version() -> Option<String> {
    let output = std::process::Command::new("uname")
        .arg("-r")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Reads a CPU model string where the platform exposes one cheaply.
///
/// Best effort and never asserted on: this exists so a reader comparing two
/// runs can see whether the hardware actually differed rather than assume the
/// `ubuntu-24.04` label means it did not.
#[must_use]
pub fn cpu_model() -> Option<String> {
    if cfg!(target_os = "linux") {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok()?;
        return cpuinfo
            .lines()
            .find(|line| line.starts_with("model name"))
            .and_then(|line| line.split(':').nth(1))
            .map(|value| value.trim().to_owned());
    }
    let output = std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A clock pinned to one instant so nothing a report reads depends on time.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub SystemTime);

impl Default for FixedClock {
    fn default() -> Self {
        Self(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// A report failure that is not a database failure.
#[derive(Debug)]
pub struct Failure(pub String);

impl Failure {
    /// Boxes a report failure built from a message.
    #[must_use]
    pub fn boxed(message: impl Into<String>) -> Box<dyn Error> {
        Box::new(Self(message.into()))
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
