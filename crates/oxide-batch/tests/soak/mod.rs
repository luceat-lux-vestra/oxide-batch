//! Mechanics the M5 `PostgreSQL` soak campaign's report is built from.
//!
//! The campaign has one report, so this module is not here to share code
//! between several. It is here because a soak report is mostly instrumentation,
//! and instrumentation mixed into the lifecycle it observes is hard to review:
//! the question a reader has about a soak is always *what exactly was counted*,
//! and that question should be answerable without reading the workload.
//!
//! Four things are counted, from four different places, and the distinction
//! between them is the substance of the campaign:
//!
//! - **tasks**, from the Tokio runtime's own alive-task count. Not from a
//!   counter the framework keeps, for the reason the campaign exists: a count
//!   the framework maintained would miss exactly the task that escaped it. The
//!   framework's own [`ShutdownCoordinator`](oxide_batch::ShutdownCoordinator)
//!   accounting is read too, as the drain result, but it answers the narrower
//!   question of whether the tasks it owns were joined.
//! - **connections**, from the adapter's own pool. [`PoolGauge`] reads them,
//!   and how it reads them is worth stating plainly: through the repository's
//!   `Debug` rendering, which is the only place the pool's occupancy is
//!   exposed. That is a real constraint on this campaign rather than a
//!   shortcut, and the alternative — adding a metrics accessor to the facade so
//!   a test could call it — would put a soak instrument in the public API,
//!   which is not something a campaign is allowed to ask for. The gauge fails
//!   loudly rather than returning nothing if the rendering ever stops carrying
//!   the fields.
//! - **handles**, from `/proc/self/fd` on Linux and `/dev/fd` on macOS, both of
//!   which are the process's own descriptor table as directory entries.
//! - **resident memory**, from `/proc/self/statm` on Linux and `ps` elsewhere.
//!
//! The database's own view, [`Observer::backends`], is kept separate from all
//! of them. A pool that has returned a connection and a server that has closed
//! a backend are different events at different times, and reporting one as the
//! other would make a campaign about connection accounting into a campaign
//! about `PostgreSQL`'s idea of it.

#![allow(
    dead_code,
    reason = "the report and its scope reader use overlapping subsets of these mechanics"
)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use std::{env, process};

use oxide_batch::{Clock, PostgresConfig, PostgresJobRepository, TlsMode};
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub mod journal;
pub mod scope;

/// The variable that tells the report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_SOAK_OBSERVATIONS";

/// The application name the adapter connects under.
///
/// The database-side reading is filtered by it so the observer's own connection
/// is not counted as one of the framework's.
pub const APPLICATION_NAME: &str = "oxide-batch";

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

/// Builds the configuration the campaign's repository is opened with.
///
/// The pool is sized to the derived requirement exactly — one connection per
/// concurrent child plus the parent's — so a cycle that held one connection
/// more than it returned would exhaust the pool rather than being absorbed by
/// spare capacity. Idle retirement and maximum lifetime are left at the
/// adapter's defaults and recorded in the report, because a connection the pool
/// retires on its own schedule is not a leak and the report should say which
/// schedule was in force.
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

/// Returns the number of tasks alive on the current Tokio runtime.
///
/// This is the campaign's task observation, and it is deliberately taken from
/// the runtime rather than from the framework. A soak that asked the framework
/// how many tasks it owned would be asking the component under test to report
/// its own leak.
#[must_use]
pub fn alive_tasks() -> u64 {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks() as u64
}

/// Returns the number of handles this process holds, where the platform
/// exposes them as directory entries.
///
/// Linux publishes the descriptor table as `/proc/self/fd` and macOS as
/// `/dev/fd`; both are the process's own table and both are readable without a
/// dependency. Reading the directory opens a descriptor of its own, which is
/// counted in every sample equally and therefore does not move the trend the
/// campaign reads.
#[must_use]
pub fn open_handles() -> Option<u64> {
    let directory = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else if cfg!(target_os = "macos") {
        "/dev/fd"
    } else {
        return None;
    };
    Some(fs::read_dir(directory).ok()?.count() as u64)
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

/// The adapter's own pool occupancy, read from the repository.
///
/// The only surface that publishes it is the repository's `Debug` rendering,
/// so that is what this reads. Every reading is checked rather than defaulted:
/// a rendering this cannot parse produces `None`, and the report treats that as
/// a violation instead of recording an absent number, because a connection
/// observation that silently stopped being taken is the one outcome a
/// connection campaign must not be able to reach.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolGauge {
    /// Connections the pool currently holds, idle and in use together.
    pub connections: u64,
    /// Connections the pool holds and nothing has checked out.
    pub idle: u64,
}

impl PoolGauge {
    /// Reads the pool occupancy the repository publishes.
    #[must_use]
    pub fn read(repository: &PostgresJobRepository) -> Option<Self> {
        let rendered = format!("{repository:?}");
        Some(Self {
            connections: number(&rendered, "pool_size")?,
            idle: number(&rendered, "pool_idle")?,
        })
    }

    /// Returns the connections currently checked out of the pool.
    #[must_use]
    pub const fn in_use(self) -> u64 {
        self.connections.saturating_sub(self.idle)
    }
}

/// Reads one `name: <digits>` field out of a debug rendering.
fn number(rendered: &str, name: &str) -> Option<u64> {
    let rest = rendered.split_once(&format!("{name}: "))?.1;
    let digits = rest
        .split(|character: char| !character.is_ascii_digit())
        .next()?;
    digits.parse().ok()
}

/// Records the greatest pool occupancy seen between two boundary samples.
///
/// A boundary sample cannot see a ceiling: by the time a cycle ends it holds no
/// connections, so the number that says whether the pool was ever exceeded has
/// to be taken while the work is running.
#[derive(Debug, Default)]
pub struct PeakConnections {
    peak: AtomicUsize,
    failures: AtomicUsize,
}

impl PeakConnections {
    /// Opens an empty gauge.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            peak: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
        }
    }

    /// Records one reading, counting a rendering that could not be read.
    pub fn record(&self, gauge: Option<PoolGauge>) {
        match gauge {
            Some(gauge) => {
                self.peak.fetch_max(
                    usize::try_from(gauge.in_use()).unwrap_or(usize::MAX),
                    Ordering::SeqCst,
                );
            }
            None => {
                self.failures.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Returns the greatest occupancy observed and clears the gauge.
    pub fn take(&self) -> u64 {
        self.peak.swap(0, Ordering::SeqCst) as u64
    }

    /// Returns how many readings could not be taken.
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::SeqCst) as u64
    }
}

/// A gauge of how much of a bounded resource is concurrently held.
///
/// `active` is what the campaign checks back to zero at the end of every cycle:
/// a worker that outlives the step that owns it is the task-growth defect this
/// campaign is looking for, observed from inside the work rather than from the
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

/// The database's own view of the campaign, on its own connection.
///
/// It is one long-lived pool rather than a connection per reading, because a
/// reader that opened and closed a connection at every sample would move the
/// handle count it is being read beside.
pub struct Observer {
    pool: PgPool,
}

impl Observer {
    /// Opens the observing connection.
    ///
    /// # Errors
    ///
    /// Returns the database failure that prevented the connection.
    pub async fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        Ok(Self { pool })
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

    /// Counts the backends this application holds on this database.
    ///
    /// # Errors
    ///
    /// Returns the database failure when the reading cannot be taken.
    pub async fn backends(&self) -> Result<i64, Box<dyn Error>> {
        Ok(sqlx::query(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1",
        )
        .bind(APPLICATION_NAME)
        .fetch_one(&self.pool)
        .await?
        .try_get(0)?)
    }

    /// Counts one job name's durable history.
    ///
    /// # Errors
    ///
    /// Returns the database failure when a reading cannot be taken.
    pub async fn history(&self, job_name: &str) -> Result<History, Box<dyn Error>> {
        Ok(History {
            instances: self.count(INSTANCE_COUNT, job_name).await?,
            executions: self.count(EXECUTION_COUNT, job_name).await?,
            step_executions: self.count(STEP_EXECUTION_COUNT, job_name).await?,
            partitions: self.count(PARTITION_COUNT, job_name).await?,
        })
    }

    /// Runs one counting statement bound to a job name.
    async fn count(&self, statement: &'static str, job_name: &str) -> Result<i64, Box<dyn Error>> {
        Ok(sqlx::query(statement)
            .bind(job_name)
            .fetch_one(&self.pool)
            .await?
            .try_get(0)?)
    }

    /// Waits, bounded, for this application's backends to reach `target`.
    ///
    /// Returns the count it settled on and how long that took. A backend
    /// disappears from `pg_stat_activity` when the server finishes tearing it
    /// down, which happens after the client-side close returns, so reading the
    /// count once immediately afterwards measures the race rather than the
    /// framework. Waiting is bounded and the elapsed time is reported, so a
    /// count that never settles is still a finding rather than a hang.
    ///
    /// # Errors
    ///
    /// Returns the database failure when a reading cannot be taken.
    pub async fn await_backends(
        &self,
        target: i64,
        limit: Duration,
    ) -> Result<(i64, u128), Box<dyn Error>> {
        let started = std::time::Instant::now();
        loop {
            let observed = self.backends().await?;
            if observed == target || started.elapsed() >= limit {
                return Ok((observed, started.elapsed().as_millis()));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Closes the observing connection.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// One job name's durable history at one instant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct History {
    /// Instances recorded under the job name.
    pub instances: i64,
    /// Job executions across those instances.
    pub executions: i64,
    /// Step executions across those job executions.
    pub step_executions: i64,
    /// Durable partitions across those step executions.
    pub partitions: i64,
}

impl History {
    /// Returns how much the history grew since an earlier reading.
    #[must_use]
    pub const fn since(self, earlier: Self) -> Self {
        Self {
            instances: self.instances - earlier.instances,
            executions: self.executions - earlier.executions,
            step_executions: self.step_executions - earlier.step_executions,
            partitions: self.partitions - earlier.partitions,
        }
    }
}

/// Counts instances under one job name.
const INSTANCE_COUNT: &str = "SELECT count(*) FROM oxide_batch.ob_job_instance WHERE job_name = $1";

/// Counts job executions under one job name.
const EXECUTION_COUNT: &str = "SELECT count(*) FROM oxide_batch.ob_job_execution execution \
     JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
     WHERE instance.job_name = $1";

/// Counts step executions under one job name.
const STEP_EXECUTION_COUNT: &str = "SELECT count(*) FROM oxide_batch.ob_step_execution step \
     JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
     JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
     WHERE instance.job_name = $1";

/// Counts durable partitions under one job name.
const PARTITION_COUNT: &str = "SELECT count(*) FROM oxide_batch.ob_step_partition partition_row \
     JOIN oxide_batch.ob_step_execution step ON step.id = partition_row.step_execution_id \
     JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
     JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
     WHERE instance.job_name = $1";

/// Removes every durable trace of one job name.
///
/// The campaign clears its job name before and after the run, so a soak never
/// observes what an earlier one left and a failed run does not poison the next.
/// This matters more here than in a single-shot report: the per-cycle durable
/// counts are asserted against exact per-cycle growth, and a leftover instance
/// would make the first cycle's arithmetic wrong rather than the run wrong.
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
///
/// The campaign runs on a supported-version matrix, and two reports from two
/// matrix points are otherwise indistinguishable in the retained evidence.
#[must_use]
pub fn major_version(server: &str) -> String {
    server.split(['.', ' ']).next().unwrap_or(server).to_owned()
}

/// Retains the report's observation where the runner will read it.
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
/// Read from `tests/fixtures/soak/campaign-semantics.json` rather than listed
/// here, because the verifier reads the same document: a closure kept in two
/// places is one that will disagree.
///
/// # Errors
///
/// Returns the failure when the document cannot be read or parsed.
pub fn semantics_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let path = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("soak")
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
/// commit name would be depending on something GitHub throws away. Recording
/// them in the report makes the binding permanent and offline.
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

/// A clock pinned to one instant so nothing the report reads depends on time.
///
/// The soak measures elapsed wall time separately, from [`std::time::Instant`].
/// Durable timestamps stay fixed so that two cycles differing only in when they
/// ran compare equal.
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
