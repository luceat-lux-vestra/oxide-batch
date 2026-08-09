//! Mechanics the M5 resource-bound campaign's `PostgreSQL` reports share.
//!
//! The campaign's three database reports need the same five things: the
//! fixture's connection material, a migrated schema and a clean job name to
//! report on, a way to describe the server they ran against, a gauge that
//! records how much of a resource was concurrently held, and a place to retain
//! the observation the runner reconciles. Those are stated once here.
//!
//! The gauge is the part worth explaining. A ceiling on a worker set is the one
//! bound in this campaign that cannot be checked by calling a constructor: it
//! is a property of a run, and the only way to observe it is from inside the
//! work. So every stressed report hands its workers an [`Occupancy`], each
//! worker enters it on the way in and leaves on the way out, and the report
//! reads the peak afterwards. The peak is what the retained evidence carries,
//! and the runner requires it to equal the configured ceiling rather than to
//! stay under it: a run that never filled the worker set says nothing about the
//! bound, and would otherwise be indistinguishable from one that did.

#![allow(
    dead_code,
    reason = "each report uses a subset of the shared mechanics"
)]

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{Clock, PostgresConfig, TlsMode};
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Barrier;

/// The variable that tells a report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_RESOURCE_OBSERVATIONS";

/// The schema the metadata lives in.
pub const METADATA_SCHEMA: &str = "oxide_batch";

/// How long a stressed worker waits for the rest of its wave to arrive.
///
/// The wait exists to hold the worker set full long enough to be observed, and
/// it is bounded so that a ceiling which is *lower* than configured fails on
/// the peak assertion rather than hanging: the wave never completes, every
/// worker times out, and the report says the peak it actually saw.
pub const WAVE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Builds the configuration a report connects with.
///
/// The transport is left as the fixture URL implies. This campaign is about
/// what the framework holds, not about how it connects; the security campaign
/// owns the transport.
///
/// # Errors
///
/// Returns the configuration failure when the URL or a timeout is rejected.
pub fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::Plaintext)
        .with_statement_timeout(Duration::from_mins(2))?
        .with_lock_timeout(Duration::from_mins(2))?)
}

/// Builds a configuration whose pool is sized for one stressed step.
///
/// The pool is the derived requirement exactly — one connection per concurrent
/// child plus the parent's — so a run that tried to hold more would block on
/// the pool rather than quietly succeed against a pool with room to spare.
///
/// # Errors
///
/// Returns the configuration failure when the URL, the size, or a timeout is
/// rejected.
pub fn config_with_pool(url: String, connections: u32) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(config(url)?
        .with_pool_size(connections)?
        // A saturated worker set asks for every connection the pool has at
        // once, so acquisition is given room to queue. The close timeout has to
        // cover it, because the adapter refuses a configuration that could wait
        // for a connection longer than it is willing to wait to shut down.
        .with_pool_close_timeout(Duration::from_mins(1))?
        .with_acquire_timeout(Duration::from_mins(1))?)
}

/// A gauge of how much of a bounded resource is concurrently held.
///
/// `peak` is the evidence. `active` is checked back to zero at the end of a
/// run, because a resource that is bounded while running and leaks a holder on
/// the way out is not bounded.
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
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// Returns the number of holders still holding.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Returns how many holders were admitted over the whole run.
    #[must_use]
    pub fn admitted(&self) -> usize {
        self.admitted.load(Ordering::SeqCst)
    }
}

/// Holds a worker until its whole wave has arrived, or until the wait expires.
///
/// Returns whether the wave completed. A wave that did not complete is not
/// reported as a failure here: the peak the gauge recorded is the finding, and
/// saying "the barrier timed out" would hide it behind a symptom.
pub async fn join_wave(barrier: &Arc<Barrier>) -> bool {
    tokio::time::timeout(WAVE_TIMEOUT, barrier.wait())
        .await
        .is_ok()
}

/// Reads the server version a report ran against.
///
/// # Errors
///
/// Returns the database failure when the reading cannot be taken.
pub async fn server_version(url: &str) -> Result<String, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let version: String = sqlx::query("SHOW server_version")
        .fetch_one(&pool)
        .await?
        .try_get(0)?;
    pool.close().await;
    Ok(version)
}

/// Returns the major version of a `PostgreSQL` server version string.
///
/// The campaign runs on a supported-version matrix, and two reports from two
/// matrix points are otherwise indistinguishable in the retained evidence.
#[must_use]
pub fn major_version(server: &str) -> String {
    server.split(['.', ' ']).next().unwrap_or(server).to_owned()
}

/// Counts the rows one bounded query would have had to traverse.
///
/// A page bound is only interesting against a history that exceeds it, so the
/// bounded-query report records what it was asked to page over rather than
/// asserting into an empty table.
///
/// # Errors
///
/// Returns the database failure when the count cannot be taken.
pub async fn count(
    url: &str,
    statement: &'static str,
    job_name: &str,
) -> Result<i64, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let total: i64 = sqlx::query(statement)
        .bind(job_name)
        .fetch_one(&pool)
        .await?
        .try_get(0)?;
    pool.close().await;
    Ok(total)
}

/// Removes every durable trace of one job name.
///
/// Each report owns its own job names and clears them before and after it runs,
/// so a report never observes what an earlier one left and a failed run does not
/// poison the next.
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

/// Removes one audited retention action by the operation that wrote it.
///
/// Retention is replay-safe by operation identifier, which is the behaviour an
/// operator wants and the one that would quietly turn a rerun of this campaign
/// into a replay of its predecessor: the second run would report the first
/// run's counts and prove nothing. A purge record also carries no instance when
/// the instances it deleted are gone, so clearing the job's rows does not clear
/// it. It is removed by operation identifier instead.
///
/// # Errors
///
/// Returns the database failure that prevented the cleanup.
pub async fn remove_retention_action(url: &str, operation_id: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    sqlx::query("DELETE FROM oxide_batch.ob_retention_action WHERE operation_id = $1")
        .bind(operation_id)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Retains one report's observation where the runner will read it.
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
