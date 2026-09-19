//! `PostgreSQL` 15/18 SIGKILL evidence for M7 #276 scope-resolution provenance.
//!
//! The harness exercises the durable boundary directly through
//! `RepositoryUnitOfWork::store_scope_resolution_provenance`:
//!
//! 1. SIGKILL while the INSERT is blocked before commit leaves no provenance;
//! 2. SIGKILL after commit preserves exactly one value-free provenance row;
//! 3. a fresh repository session re-reads that row and re-resolves the
//!    referenced immutable job parameter without consulting ambient state.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::panic)]

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_batch::{
    JobExecutionId, JobRepository, ParameterName, PostgresConfig, PostgresJobRepository,
    PostgresMigrator, ScopeKind, ScopedComponentId, SystemClock, TlsMode,
};
use oxide_batch_repository::{ScopeResolutionProvenance, ScopeResolutionSource};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const SIGKILL_JOB: &str = "postgres_m7_scope_resolution_sigkill";
const CANONICAL_JOB: &str = "postgres_m7_scope_resolution_canonical";
const LOW_ENTROPY_VALUE: &str = "0420";
const WORKER_ENV: &str = "OXIDEBATCH_M7_SCOPE_RESOLUTION_WORKER";
const EXECUTION_ENV: &str = "OXIDEBATCH_M7_SCOPE_RESOLUTION_EXECUTION";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_SCOPE_RESOLUTION_HANDSHAKE";
const WAIT_BOUND: Duration = Duration::from_secs(20);
const SIGKILL: i32 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeCommit,
    AfterCommit,
}

impl CrashPoint {
    const fn slug(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before-commit",
            Self::AfterCommit => "after-commit",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-commit" => Ok(Self::BeforeCommit),
            "after-commit" => Ok(Self::AfterCommit),
            _ => Err("unknown scope-resolution crash point".into()),
        }
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

async fn remove_fixture(pool: &PgPool, job: &str) -> Result<(), sqlx::Error> {
    for statement in [
        "DELETE FROM oxide_batch.ob_scope_resolution_provenance WHERE owner_job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1) \
         OR to_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(job).execute(pool).await?;
    }
    Ok(())
}

async fn seed_execution(pool: &PgPool, job: &str) -> Result<JobExecutionId, Box<dyn Error>> {
    let definition_id: i64 = sqlx::query_scalar(
        "INSERT INTO oxide_batch.ob_job_definition (\
         job_name, definition_revision, manifest_format, manifest_digest, manifest, registered_at) \
         VALUES ($1, 'scope-v1', 1, $2, $3, CURRENT_TIMESTAMP) RETURNING id",
    )
    .bind(job)
    .bind(vec![0x61_u8; 32])
    .bind(serde_json::json!({
        "format": 1,
        "fixture": "scope-resolution-sigkill",
        "revision": "scope-v1"
    }))
    .fetch_one(pool)
    .await?;

    let instance_id: i64 = sqlx::query_scalar(
        "INSERT INTO oxide_batch.ob_job_instance (\
         job_name, instance_key, identifying_parameters, created_at) \
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP) RETURNING id",
    )
    .bind(job)
    .bind(vec![0x62_u8; 32])
    .bind(serde_json::json!({
        "tenant": {
            "identifying": true,
            "type": "string",
            "value": LOW_ENTROPY_VALUE
        }
    }))
    .fetch_one(pool)
    .await?;

    let execution_id: i64 = sqlx::query_scalar(
        "INSERT INTO oxide_batch.ob_job_execution (\
         job_instance_id, definition_id, attempt, status, exit_code, parameters, \
         context_format, context_schema, context_schema_version, context_payload, \
         created_at, started_at, ended_at, updated_at, version) \
         VALUES ($1, $2, 1, 'COMPLETED', 'COMPLETED', $3, \
         1, 'scope.crash', 1, '{}'::jsonb, \
         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 3) \
         RETURNING id",
    )
    .bind(instance_id)
    .bind(definition_id)
    .bind(serde_json::json!({
        "tenant": {
            "identifying": true,
            "type": "string",
            "value": LOW_ENTROPY_VALUE
        }
    }))
    .fetch_one(pool)
    .await?;

    Ok(JobExecutionId::new(u64::try_from(execution_id)?)?)
}

fn provenance_named(
    execution_id: JobExecutionId,
    component: &str,
    input: &str,
) -> Result<ScopeResolutionProvenance, Box<dyn Error>> {
    let parameter = ParameterName::new(input)?;
    Ok(ScopeResolutionProvenance::new(
        ScopeKind::Job,
        ScopedComponentId::new(component)?,
        parameter.clone(),
        execution_id,
        None,
        ScopeResolutionSource::JobParameter {
            job_execution_id: execution_id,
            parameter,
        },
    )?)
}

fn provenance(execution_id: JobExecutionId) -> Result<ScopeResolutionProvenance, Box<dyn Error>> {
    provenance_named(execution_id, "client", "tenant")
}

fn handshake_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "oxide-batch-m7-scope-resolution-{}",
        std::process::id()
    ))
}

async fn wait_for_lock_wait(pool: &PgPool) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (\
             SELECT 1 FROM pg_stat_activity \
             WHERE datname = current_database() \
               AND pid <> pg_backend_pid() \
               AND wait_event_type = 'Lock' \
               AND position('ob_scope_resolution_provenance' in query) > 0)",
        )
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if started.elapsed() >= WAIT_BOUND {
            return Err("timed out waiting for provenance INSERT lock".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() >= WAIT_BOUND {
            return Err(format!("timed out waiting for {}", path.display()).into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

fn spawn_worker(
    point: CrashPoint,
    execution_id: JobExecutionId,
    handshake: &Path,
) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("scope_resolution_crash_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, point.slug())
        .env(EXECUTION_ENV, execution_id.get().to_string())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

fn kill_worker(child: &mut Child) -> Result<(), Box<dyn Error>> {
    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL));
    Ok(())
}

async fn provenance_count(pool: &PgPool, execution_id: JobExecutionId) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM oxide_batch.ob_scope_resolution_provenance \
         WHERE owner_job_execution_id = $1",
    )
    .bind(i64::try_from(execution_id.get()).unwrap_or(i64::MAX))
    .fetch_one(pool)
    .await
}

async fn prove_canonical_batch_write(
    runtime_url: String,
    migrator_url: String,
) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&migrator_url)
        .await?;
    remove_fixture(&pool, CANONICAL_JOB).await?;
    let execution_id = seed_execution(&pool, CANONICAL_JOB).await?;
    let repository =
        PostgresJobRepository::connect(config(runtime_url)?, Arc::new(SystemClock)).await?;

    let alpha = provenance_named(execution_id, "client", "alpha")?;
    let zeta = provenance_named(execution_id, "client", "zeta")?;
    let wrong_component = provenance_named(execution_id, "other-client", "beta")?;

    let mut unit = repository.begin().await?;
    let error = match unit
        .store_scope_resolution_provenance(&[alpha.clone(), wrong_component])
        .await
    {
        Ok(()) => return Err("mixed component batch unexpectedly persisted".into()),
        Err(error) => error,
    };
    assert_eq!(
        error,
        oxide_batch::RepositoryError::ScopeResolutionStateCorrupt
    );
    unit.commit().await?;
    assert_eq!(provenance_count(&pool, execution_id).await?, 0);

    let mut unit = repository.begin().await?;
    unit.store_scope_resolution_provenance(&[zeta.clone(), alpha.clone()])
        .await?;
    unit.commit().await?;

    let component = ScopedComponentId::new("client")?;
    let mut unit = repository.begin().await?;
    let persisted = unit
        .scope_resolution_provenance(ScopeKind::Job, &component, execution_id, None)
        .await?;
    unit.store_scope_resolution_provenance(&[alpha.clone(), zeta.clone()])
        .await?;
    unit.commit().await?;
    assert_eq!(persisted, vec![alpha, zeta]);
    assert_eq!(provenance_count(&pool, execution_id).await?, 2);

    repository.close().await?;
    remove_fixture(&pool, CANONICAL_JOB).await?;
    pool.close().await;
    Ok(())
}

async fn run_worker(
    point: CrashPoint,
    execution_id: JobExecutionId,
    handshake: PathBuf,
    url: String,
) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(SystemClock)).await?;
    let entry = provenance(execution_id)?;
    let mut unit = repository.begin().await?;
    unit.store_scope_resolution_provenance(std::slice::from_ref(&entry))
        .await?;
    unit.commit().await?;

    if point == CrashPoint::AfterCommit {
        std::fs::write(&handshake, b"committed")?;
        loop {
            tokio::time::sleep(Duration::from_mins(1)).await;
        }
    }

    Err("scope-resolution worker crossed the before-commit SIGKILL boundary".into())
}

async fn run_parent(runtime_url: String, migrator_url: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&migrator_url)
        .await?;
    remove_fixture(&pool, SIGKILL_JOB).await?;
    let execution_id = seed_execution(&pool, SIGKILL_JOB).await?;
    let handshake = handshake_path();
    if handshake.exists() {
        std::fs::remove_file(&handshake)?;
    }

    let mut lock = pool.begin().await?;
    sqlx::query("LOCK TABLE oxide_batch.ob_scope_resolution_provenance IN SHARE MODE")
        .execute(&mut *lock)
        .await?;
    let mut before = spawn_worker(CrashPoint::BeforeCommit, execution_id, &handshake)?;
    wait_for_lock_wait(&pool).await?;
    kill_worker(&mut before)?;
    lock.rollback().await?;
    assert_eq!(provenance_count(&pool, execution_id).await?, 0);

    let mut after = spawn_worker(CrashPoint::AfterCommit, execution_id, &handshake)?;
    wait_for_file(&handshake).await?;
    kill_worker(&mut after)?;
    assert_eq!(provenance_count(&pool, execution_id).await?, 1);

    let persisted: String = sqlx::query(
        "SELECT row_to_json(provenance)::text AS rendered \
         FROM oxide_batch.ob_scope_resolution_provenance provenance \
         WHERE owner_job_execution_id = $1",
    )
    .bind(i64::try_from(execution_id.get())?)
    .fetch_one(&pool)
    .await?
    .try_get("rendered")?;
    assert!(!persisted.contains(LOW_ENTROPY_VALUE));
    assert!(!persisted.contains("resolved_value"));
    assert!(!persisted.contains("value_digest"));

    let repository =
        PostgresJobRepository::connect(config(runtime_url)?, Arc::new(SystemClock)).await?;
    let component = ScopedComponentId::new("client")?;
    let mut unit = repository.begin().await?;
    let entries = unit
        .scope_resolution_provenance(ScopeKind::Job, &component, execution_id, None)
        .await?;
    let parameters = unit.job_execution_parameters(execution_id).await?;
    unit.rollback().await?;

    assert_eq!(entries.len(), 1);
    let ScopeResolutionSource::JobParameter {
        job_execution_id: source_execution_id,
        parameter,
    } = entries[0].source()
    else {
        return Err("persisted provenance changed source family".into());
    };
    assert_eq!(*source_execution_id, execution_id);
    assert_eq!(
        parameters
            .get(parameter)
            .and_then(|entry| entry.value().as_str()),
        Some(LOW_ENTROPY_VALUE)
    );

    repository.close().await?;
    remove_fixture(&pool, SIGKILL_JOB).await?;
    pool.close().await;
    if handshake.exists() {
        std::fs::remove_file(handshake)?;
    }
    Ok(())
}

#[test]
fn provenance_batch_is_validated_before_write_and_compared_canonically()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(prove_canonical_batch_write(runtime_url, migrator_url))
}

#[test]
fn scope_resolution_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(point) = std::env::var(WORKER_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&point)?;
    let execution_id = JobExecutionId::new(std::env::var(EXECUTION_ENV)?.parse()?)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or("scope-resolution worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(point, execution_id, handshake, url))
}

#[test]
fn sigkill_preserves_only_committed_scope_resolution_provenance() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_parent(runtime_url, migrator_url))
}
