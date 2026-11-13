//! Core repository certification for the constrained `SQLx` `SQLite` adapter.

#![cfg(feature = "sqlite")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contract::run_core_repository_contract;
use oxide_batch::{
    Clock, JobRepository, RepositoryError, SqliteConfig, SqliteJobRepository, SqliteMigrator,
};

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
fn shared_core_repository_contract_passes_on_real_sqlite_files() -> Result<(), Box<dyn Error>> {
    let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let retained = Arc::clone(&paths);
    runtime.block_on(async move {
        run_core_repository_contract("sqlite", move || {
            let retained = Arc::clone(&retained);
            async move {
                let (path, url) = database();
                retained
                    .lock()
                    .map_err(|_| RepositoryError::Unavailable)?
                    .push(path);
                let config =
                    SqliteConfig::new(url).map_err(|_| RepositoryError::Unavailable)?;
                SqliteMigrator::migrate(&config).await?;
                SqliteJobRepository::connect(config, Arc::new(FixedClock(UNIX_EPOCH))).await
            }
        })
        .await
    })?;
    for path in paths.lock().map_err(|_| RepositoryError::Unavailable)?.iter() {
        remove_database(path);
    }
    Ok(())
}

#[test]
fn default_profile_is_single_connection_and_declares_no_optional_capabilities(
) -> Result<(), Box<dyn Error>> {
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
