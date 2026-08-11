//! Mechanics the M5 `PostgreSQL` cancellation campaign's reports are built from.
//!
//! The campaign has four reports and they share a fixture, a workload shape,
//! and — most importantly — a definition of when each of its two latencies
//! starts and stops. That definition is the substance of P-014, so it lives
//! here in one place rather than being restated in each report, where four
//! copies could drift into four different measurements presented under one
//! name.
//!
//! ## What the two latencies are measured between
//!
//! Both start at the same instant: the moment the accepted operator request
//! became durable, which is the instant `request_execution_stop`'s transaction
//! commits. Not the instant the campaign decided to cancel, and not the instant
//! the call was made — a request that has not committed is a request no
//! observer could act on, and starting the clock before it would charge the
//! framework for the campaign's own round trip.
//!
//! - **request to intake stop** ends when the durable transition to `STOPPING`
//!   is first readable. That is the accepted meaning of intake having stopped
//!   on this path: the owning runtime made the transition inside its own
//!   `observe_execution_control` transaction, and from that point the execution
//!   is no longer active. It is read back from the database by [`Watcher`]
//!   rather than reported by the runtime, because a runtime that told the
//!   campaign when it had stopped would be the component under test reporting
//!   its own latency.
//! - **request to durable terminal** ends when the terminal status `STOPPED` is
//!   first readable, read the same way and for the same reason.
//!
//! Both are monotonic ([`std::time::Instant`]), never wall clock. The durable
//! rows carry `SystemTime` values from the framework's clock, which this
//! campaign pins to a fixed instant precisely so that nothing it measures can
//! accidentally be derived from them.
//!
//! ## The sampling floor, stated rather than hidden
//!
//! [`Watcher`] polls, so every duration it produces carries its poll interval
//! as a quantisation floor: a transition that happened immediately is reported
//! as having taken up to one interval. The interval is recorded in the report
//! beside every duration it bounds, so a reader can tell a real latency from
//! the floor. It is deliberately much smaller than the framework's own stop
//! poll interval, which is the dominant term on this path, so the floor is not
//! what the measurement is mostly made of. The alternative — a trigger or hook
//! inside the framework that fired at the transition — would be a test-only
//! production hook, which this campaign is not allowed to add.

#![allow(
    dead_code,
    reason = "the four reports and the scope reader use overlapping subsets of these mechanics"
)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use std::{env, process};

use oxide_batch::{
    BatchStatus, Clock, JobExecution, JobExecutionId, JobInstanceKey, JobRepository,
    PostgresConfig, PostgresJobRepository, TlsMode,
};
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub mod scope;

/// The variable that tells a report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_CANCELLATION_OBSERVATIONS";

/// The application name the adapter connects under.
pub const APPLICATION_NAME: &str = "oxide-batch";

/// How often [`Watcher`] re-reads a durable status.
///
/// This is the campaign's own sampling floor rather than anything the framework
/// declares, and it is recorded in every report beside the durations it bounds.
/// Five milliseconds is two orders of magnitude below the accepted stop poll
/// interval the campaign configures, so it contributes a small fraction of what
/// the operator path measures, and it is cheap enough that a watcher running
/// for the whole of a cancelled launch does not itself load the database.
pub const WATCH_INTERVAL: Duration = Duration::from_millis(5);

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
/// The pool is sized to the derived requirement exactly — one connection per
/// concurrent child plus the parent's — so a report that held one connection
/// more than it returned would exhaust the pool rather than have the excess
/// absorbed by spare capacity.
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

/// A gauge of how much of a bounded resource is concurrently held.
///
/// `active` is what every report checks back to zero once a cancelled launch
/// has returned: a worker that outlives the attempt that owns it is the leaked
/// work half of P-014, observed from inside the work rather than from the
/// runtime.
#[derive(Debug, Default)]
pub struct Occupancy {
    active: AtomicUsize,
    peak: AtomicUsize,
    admitted: AtomicUsize,
}

impl Occupancy {
    /// Opens an empty gauge.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            admitted: AtomicUsize::new(0),
        }
    }

    /// Records one holder taking the resource.
    pub fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.admitted.fetch_add(1, Ordering::SeqCst);
    }

    /// Records one holder releasing the resource.
    pub fn leave(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    /// Returns the greatest number of holders observed at one time.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::SeqCst) as u64
    }

    /// Returns the number of holders still holding.
    #[must_use]
    pub fn active(&self) -> u64 {
        self.active.load(Ordering::SeqCst) as u64
    }

    /// Returns how many holders were admitted over the whole run.
    #[must_use]
    pub fn admitted(&self) -> u64 {
        self.admitted.load(Ordering::SeqCst) as u64
    }
}

/// Watches one execution's durable status from outside the runtime.
///
/// This is the campaign's observation instrument and the reason the two
/// latencies are readings of the database rather than of the framework. It
/// holds its own connection, separate from the repository pool under
/// measurement, so that watching cannot consume a connection the workload needs
/// and cannot appear in the pool occupancy a report records.
pub struct Watcher {
    pool: PgPool,
}

impl Watcher {
    /// Opens the watching connection.
    ///
    /// # Errors
    ///
    /// Returns the database failure that prevented the connection.
    pub async fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        Ok(Self { pool })
    }

    /// Reads one execution's durable status, by the identifier the framework
    /// stores it under.
    ///
    /// # Errors
    ///
    /// Returns the database failure when the reading cannot be taken.
    pub async fn status(
        &self,
        execution: JobExecutionId,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let row = sqlx::query("SELECT status FROM oxide_batch.ob_job_execution WHERE id = $1")
            .bind(i64::try_from(execution.get()).unwrap_or(i64::MAX))
            .fetch_optional(&self.pool)
            .await?;
        Ok(match row {
            Some(row) => Some(row.try_get::<String, _>("status")?),
            None => None,
        })
    }

    /// Waits, bounded, for an execution to reach one of `targets`.
    ///
    /// Returns the instant the status was first observed and the status itself.
    /// A wait that runs out returns `None`, so a report whose transition never
    /// arrived fails on the observation it actually took rather than hanging
    /// until CI kills the job and retains nothing.
    ///
    /// # Errors
    ///
    /// Returns the database failure when a reading cannot be taken.
    pub async fn await_status(
        &self,
        execution: JobExecutionId,
        targets: &[&str],
        limit: Duration,
    ) -> Result<Option<(Instant, String)>, Box<dyn Error>> {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(status) = self.status(execution).await?
                && targets.contains(&status.as_str())
            {
                return Ok(Some((Instant::now(), status)));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(WATCH_INTERVAL).await;
        }
    }

    /// Counts the partitions of one execution that are durably complete.
    ///
    /// Used to find the moment the workload has committed real work, which is
    /// when a cancellation has something to preserve.
    ///
    /// # Errors
    ///
    /// Returns the database failure when the reading cannot be taken.
    pub async fn completed_partitions(
        &self,
        execution: JobExecutionId,
    ) -> Result<i64, Box<dyn Error>> {
        Ok(sqlx::query(
            "SELECT count(*) FROM oxide_batch.ob_step_partition partition_row \
             JOIN oxide_batch.ob_step_execution step \
               ON step.id = partition_row.step_execution_id \
             WHERE step.job_execution_id = $1 AND partition_row.status = 'COMPLETED'",
        )
        .bind(i64::try_from(execution.get()).unwrap_or(i64::MAX))
        .fetch_one(&self.pool)
        .await?
        .try_get(0)?)
    }

    /// Waits, bounded, for at least `target` partitions to be durably complete.
    ///
    /// # Errors
    ///
    /// Returns the database failure when a reading cannot be taken.
    pub async fn await_completed_partitions(
        &self,
        execution: JobExecutionId,
        target: i64,
        limit: Duration,
    ) -> Result<Option<i64>, Box<dyn Error>> {
        let deadline = Instant::now() + limit;
        loop {
            let observed = self.completed_partitions(execution).await?;
            if observed >= target {
                return Ok(Some(observed));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(WATCH_INTERVAL).await;
        }
    }

    /// Returns the server version string.
    ///
    /// # Errors
    ///
    /// Returns the database failure when the reading cannot be taken.
    pub async fn server_version(&self) -> Result<String, Box<dyn Error>> {
        Ok(sqlx::query("SHOW server_version")
            .fetch_one(&self.pool)
            .await?
            .try_get(0)?)
    }

    /// Closes the watching connection.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// Finds the execution a launch is currently running, through the accepted
/// read-only lookup path.
///
/// An operator cancelling a running job has exactly this problem — it knows the
/// job name and parameters and needs the execution identifier — and the
/// accepted contract answers it with `find_job_instance` followed by
/// `job_executions`. The campaign uses that path rather than reading the
/// identifier out of a launch report it happens to be holding, because the
/// launch report is not available to an operator and using it would test a
/// route no deployment has.
///
/// Returns the newest execution recorded for the instance, which is the running
/// attempt while a launch is in flight.
///
/// # Errors
///
/// Returns the repository failure when the lookup cannot be made.
pub async fn find_running_execution(
    repository: &PostgresJobRepository,
    key: &JobInstanceKey,
) -> Result<Option<JobExecution>, Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let instance = unit.find_job_instance(key).await?;
    let found = match instance {
        Some(instance) => {
            let executions = unit.job_executions(instance.id()).await?;
            executions.into_iter().next_back()
        }
        None => None,
    };
    unit.rollback().await?;
    Ok(found)
}

/// Waits, bounded, for a launch to have created an execution.
///
/// # Errors
///
/// Returns the repository failure when the lookup cannot be made.
pub async fn await_running_execution(
    repository: &PostgresJobRepository,
    key: &JobInstanceKey,
    limit: Duration,
) -> Result<Option<JobExecution>, Box<dyn Error>> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(execution) = find_running_execution(repository, key).await?
            && matches!(
                execution.metadata().status(),
                BatchStatus::Starting | BatchStatus::Started
            )
        {
            return Ok(Some(execution));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(WATCH_INTERVAL).await;
    }
}

/// Removes every durable trace of one job name.
///
/// Each report clears its job name before and after it runs, so a report never
/// observes what an earlier one left and a failed run does not poison the next.
///
/// # Errors
///
/// Returns the database failure that prevented the cleanup.
pub async fn remove_job(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
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
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_operator_request WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_retention_action WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
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
/// report that skipped or never reached its end cannot be counted as evidence.
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
/// Read from `tests/fixtures/cancellation/campaign-semantics.json` rather than
/// listed here, because the verifier reads the same document: a closure kept in
/// two places is one that will disagree.
///
/// # Errors
///
/// Returns the failure when the document cannot be read or parsed.
pub fn semantics_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let path = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("cancellation")
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
/// This is the provenance root, and it is taken here rather than reconstructed
/// later for one reason: this process is the campaign, so the tree it can see
/// is by definition the tree that ran. In CI that is the pull-request merge
/// commit the workflow checked out — an ephemeral object no later clone can
/// resolve — so a verifier that tried to re-derive these identities from a
/// commit name would depend on something GitHub throws away.
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
/// not portable: a debug build on a shared CI runner produces different numbers
/// from a release build on idle hardware, and a reader who cannot see which one
/// produced a figure cannot use it. Nothing here is asserted on.
#[must_use]
pub fn measurement_environment(worker_threads: usize) -> Value {
    serde_json::json!({
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "profile_note": "Every M5 campaign runs debug so the matrix points are comparable with \
                         each other. The absolute latencies below are therefore debug-build \
                         latencies and are not release figures.",
        "tokio_worker_threads": worker_threads,
        "available_parallelism": std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or_default(),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
        "resident_kib": resident_kib(),
        "watch_interval_millis": WATCH_INTERVAL.as_millis(),
        "watch_interval_note": "The campaign's own sampling floor. Every duration measured by \
                                reading a durable status back carries this as quantisation: a \
                                transition that happened immediately is reported as having taken \
                                up to one interval.",
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

/// A clock pinned to one instant so nothing a report reads depends on time.
///
/// The campaign measures every duration from [`std::time::Instant`], and this
/// is what makes that a rule rather than a convention: a durable timestamp
/// cannot accidentally become one of the campaign's measurements, because every
/// durable timestamp in the run is the same value.
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
