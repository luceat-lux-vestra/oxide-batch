//! Runnable-core repository certification for the constrained `SQLx` `SQLite` adapter.

#![cfg(feature = "sqlite")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contract::run_runnable_core_repository_contract;
use oxide_batch::{
    Clock, JobRepository, RepositoryError, SqliteConfig, SqliteJobRepository, SqliteMigrator,
};
use sqlx::Connection;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn database() -> (PathBuf, String) {
    let ordinal = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "oxide-batch-sqlite-{}-{ordinal}.db",
        std::process::id()
    ));
    let url = format!("sqlite://{}", path.display());
    (path, url)
}

fn remove_database(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
}

#[test]
fn migration_is_idempotent_for_sqlite() -> Result<(), Box<dyn Error>> {
    let (path, url) = database();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = SqliteConfig::new(url)?;
        SqliteMigrator::migrate(&config).await?;
        SqliteMigrator::migrate(&config).await?;
        assert_eq!(
            SqliteMigrator::installed_schema_version(&config).await?,
            Some(SqliteMigrator::supported_schema_version())
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    remove_database(&path);
    Ok(())
}

#[test]
fn migration_upgrades_existing_bootstrap_sqlite_schema() -> Result<(), Box<dyn Error>> {
    let (donor_path, donor_url) = database();
    let (target_path, target_url) = database();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let donor_config = SqliteConfig::new(donor_url.clone())?;
        SqliteMigrator::migrate(&donor_config).await?;

        let donor = sqlx::SqlitePool::connect(&donor_url).await?;
        let migration_table_ddl = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
        )
        .fetch_one(&donor)
        .await?;
        donor.close().await;

        std::fs::File::create(&target_path)?;
        let mut target = sqlx::SqliteConnection::connect(&target_url).await?;
        sqlx::raw_sql(&migration_table_ddl)
            .execute(&mut target)
            .await?;
        sqlx::raw_sql(include_str!("../migrations-sqlite/0001_core_metadata.sql"))
            .execute(&mut target)
            .await?;
        sqlx::query("ATTACH DATABASE ? AS donor")
            .bind(donor_path.to_string_lossy().as_ref())
            .execute(&mut target)
            .await?;
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             SELECT * FROM donor._sqlx_migrations WHERE version = 1",
        )
        .execute(&mut target)
        .await?;
        sqlx::query("DETACH DATABASE donor")
            .execute(&mut target)
            .await?;
        target.close().await?;

        let target_config = SqliteConfig::new(target_url.clone())?;
        assert_eq!(
            SqliteMigrator::installed_schema_version(&target_config).await?,
            Some(1)
        );
        SqliteMigrator::migrate(&target_config).await?;
        assert_eq!(
            SqliteMigrator::installed_schema_version(&target_config).await?,
            Some(SqliteMigrator::supported_schema_version())
        );

        let verified = sqlx::SqlitePool::connect(&target_url).await?;
        sqlx::query("SELECT definition_id FROM ob_job_execution LIMIT 0")
            .execute(&verified)
            .await?;
        sqlx::query("SELECT step_logical_id FROM ob_step_execution LIMIT 0")
            .execute(&verified)
            .await?;
        sqlx::query("SELECT id FROM ob_flow_decision LIMIT 0")
            .execute(&verified)
            .await?;
        verified.close().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    remove_database(&donor_path);
    remove_database(&target_path);
    Ok(())
}

#[test]
fn newer_sqlite_schema_is_rejected_before_migration() -> Result<(), Box<dyn Error>> {
    let (path, url) = database();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = SqliteConfig::new(url.clone())?;
        SqliteMigrator::migrate(&config).await?;

        let pool = sqlx::SqlitePool::connect(&url).await?;
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, installed_on, success, checksum, execution_time) \
             SELECT 3, 'future schema', installed_on, success, checksum, execution_time \
             FROM _sqlx_migrations WHERE version = 2",
        )
        .execute(&pool)
        .await?;
        pool.close().await;

        assert_eq!(
            SqliteMigrator::migrate(&config).await,
            Err(RepositoryError::NewerSchema {
                current: 3,
                supported: SqliteMigrator::supported_schema_version(),
            })
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    remove_database(&path);
    Ok(())
}

#[test]
fn runnable_core_repository_contract_passes_on_real_sqlite_files() -> Result<(), Box<dyn Error>> {
    let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let retained = Arc::clone(&paths);
    runtime.block_on(async move {
        run_runnable_core_repository_contract("sqlite", move |clock| {
            let retained = Arc::clone(&retained);
            async move {
                let (path, url) = database();
                retained
                    .lock()
                    .map_err(|_| RepositoryError::Unavailable)?
                    .push(path);
                let config = SqliteConfig::new(url).map_err(|_| RepositoryError::Unavailable)?;
                SqliteMigrator::migrate(&config).await?;
                SqliteJobRepository::connect(config, clock).await
            }
        })
        .await
    })?;
    for path in paths
        .lock()
        .map_err(|_| RepositoryError::Unavailable)?
        .iter()
    {
        remove_database(path);
    }
    Ok(())
}

#[test]
fn default_profile_is_single_connection_and_declares_no_optional_capabilities()
-> Result<(), Box<dyn Error>> {
    let (path, url) = database();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = SqliteConfig::new(url)?;
        SqliteMigrator::migrate(&config).await?;
        let repository =
            SqliteJobRepository::connect(config, Arc::new(FixedClock(UNIX_EPOCH))).await?;
        assert_eq!(repository.connection_capacity(), 1);
        assert_eq!(repository.descriptor().capabilities().len(), 0);
        repository.close().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    remove_database(&path);
    Ok(())
}

#[test]
fn begin_immediate_serializes_competing_write_units() -> Result<(), Box<dyn Error>> {
    let (path, url) = database();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = SqliteConfig::new(url)?.with_pool_size(2)?;
        SqliteMigrator::migrate(&config).await?;
        let repository =
            SqliteJobRepository::connect(config, Arc::new(FixedClock(UNIX_EPOCH))).await?;

        let first = repository.begin().await?;
        let mut second = Box::pin(repository.begin());
        let blocked = tokio::time::timeout(Duration::from_millis(100), &mut second).await;
        assert!(
            blocked.is_err(),
            "a second SQLite write unit must wait while BEGIN IMMEDIATE owns the writer lock"
        );

        first.rollback().await?;
        let second = tokio::time::timeout(Duration::from_secs(1), &mut second)
            .await
            .map_err(|_| RepositoryError::Unavailable)??;
        second.rollback().await?;
        repository.close().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    remove_database(&path);
    Ok(())
}

#[test]
fn facade_feature_keeps_sqlite_unbundled() -> Result<(), Box<dyn Error>> {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))?;
    assert!(
        manifest.contains(r#"sqlite = ["dep:sqlx", "sqlx/sqlite-unbundled"]"#),
        "the SQLite facade must remain on SQLx's unbundled path while RUSTSEC-2026-0459 is ignored"
    );
    Ok(())
}
