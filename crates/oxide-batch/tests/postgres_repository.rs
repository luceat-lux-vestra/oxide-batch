//! `PostgreSQL` repository contracts.

#![cfg(feature = "postgres")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contract::run_repository_contract;
use oxide_batch::{
    CaCertificate, Clock, JobInstanceKey, JobName, JobParameters, JobRepository, PostgresConfig,
    PostgresConfigError, PostgresJobRepository, PostgresMigrator, RepositoryError, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

async fn remove_contract_rows(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance \
         WHERE job_name = 'repository_contract_job')",
        "DELETE FROM oxide_batch.ob_job_instance \
         WHERE job_name = 'repository_contract_job'",
        "DELETE FROM oxide_batch.ob_job_definition \
         WHERE job_name = 'repository_contract_job'",
    ] {
        sqlx::query(statement).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

#[test]
fn configuration_bounds_and_diagnostics_are_safe() -> Result<(), Box<dyn Error>> {
    let secret_url = "postgres://runtime:do-not-disclose@db.internal/metadata";
    let secret_ca = b"private-ca-contents".to_vec();
    let config = PostgresConfig::new(secret_url)?.with_tls_mode(TlsMode::VerifyFull {
        ca_certificate: Some(CaCertificate::new(secret_ca.clone())?),
    });
    let diagnostic = format!("{config:?}");
    assert!(!diagnostic.contains(secret_url));
    assert!(!diagnostic.contains("do-not-disclose"));
    assert!(!diagnostic.contains("private-ca-contents"));

    assert_eq!(
        PostgresConfig::new(secret_url)?.with_pool_size(0).err(),
        Some(PostgresConfigError::PoolSize)
    );
    assert_eq!(
        PostgresConfig::new(secret_url)?
            .with_lock_timeout(Duration::from_secs(31))
            .err(),
        Some(PostgresConfigError::LockExceedsStatement)
    );
    assert_eq!(
        PostgresConfig::new("postgres://runtime@localhost/db?sslmode=disable").err(),
        Some(PostgresConfigError::TlsOptionInConnectionString)
    );
    assert_eq!(
        CaCertificate::new(Vec::new()).err(),
        Some(PostgresConfigError::EmptyCaCertificate)
    );
    assert_eq!(PostgresMigrator::supported_schema_version(), 1);
    Ok(())
}

#[test]
fn shared_repository_contract_passes_on_postgres() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _runtime_guard = runtime.enter();
    run_repository_contract("postgres", || {
        runtime
            .block_on(async {
                remove_contract_rows(&url)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                PostgresJobRepository::connect(
                    plaintext_config(url.clone()).map_err(|_| RepositoryError::Unavailable)?,
                    Arc::new(FixedClock(UNIX_EPOCH)),
                )
                .await
            })
            .map_err(|_| RepositoryError::Unavailable)
    })?;
    runtime.block_on(remove_contract_rows(&url))?;
    Ok(())
}

#[test]
fn concurrent_identical_launches_create_one_active_execution() -> Result<(), Box<dyn Error>> {
    const CONTENDERS: usize = 8;

    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_contract_rows(&url).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(100))),
        )
        .await?;
        let barrier = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
        let key = JobInstanceKey::new(
            JobName::new("repository_contract_job")?,
            &JobParameters::new(),
        );
        let mut handles = Vec::with_capacity(CONTENDERS);
        for _ in 0..CONTENDERS {
            let repository = repository.clone();
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut unit = repository.begin().await?;
                barrier.wait().await;
                let instance = unit
                    .select_or_create_job_instance(&key)
                    .await?
                    .instance()
                    .clone();
                unit.create_job_execution(instance.id()).await?;
                unit.commit().await
            }));
        }
        let mut committed = 0;
        let mut active_rejections = 0;
        for handle in handles {
            match handle.await? {
                Ok(()) => committed += 1,
                Err(RepositoryError::ExecutionAlreadyActive { .. }) => active_rejections += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!(committed, 1);
        assert_eq!(active_rejections, CONTENDERS - 1);

        let mut inspection = repository.begin().await?;
        let instance = inspection
            .find_job_instance(&key)
            .await?
            .ok_or("canonical instance was not committed")?;
        assert_eq!(inspection.job_executions(instance.id()).await?.len(), 1);
        inspection.rollback().await?;
        repository.close().await?;
        remove_contract_rows(&url).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn migration_is_idempotent_when_migrator_fixture_is_available() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let config = plaintext_config(url)?;
    runtime.block_on(PostgresMigrator::migrate(&config))?;
    runtime.block_on(PostgresMigrator::migrate(&config))?;
    Ok(())
}

#[test]
fn newer_schema_is_rejected_without_guessing_compatibility() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query("UPDATE oxide_batch.ob_schema_version SET version = 2")
            .execute(&pool)
            .await?;
        let result = PostgresJobRepository::connect(
            plaintext_config(url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH)),
        )
        .await;
        sqlx::query("UPDATE oxide_batch.ob_schema_version SET version = 1")
            .execute(&pool)
            .await?;
        pool.close().await;
        let Err(error) = result else {
            return Err("newer schema was accepted".into());
        };
        assert_eq!(
            error,
            RepositoryError::NewerSchema {
                current: 2,
                supported: 1,
            }
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn disconnected_transaction_has_unknown_commit_and_pool_recovers() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(admin_url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_contract_rows(&runtime_url).await?;
        let repository = PostgresJobRepository::connect(
            plaintext_config(runtime_url.clone())?,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(100))),
        )
        .await?;
        let mut unit = repository.begin().await?;
        let key = JobInstanceKey::new(
            JobName::new("repository_contract_job")?,
            &JobParameters::new(),
        );
        unit.select_or_create_job_instance(&key).await?;

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        let terminated: bool = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'oxide-batch' \
             AND state = 'idle in transaction' \
             ORDER BY backend_start LIMIT 1",
        )
        .fetch_one(&admin)
        .await?;
        assert!(terminated);
        assert_eq!(
            unit.commit().await,
            Err(RepositoryError::CommitOutcomeUnknown)
        );

        let inspection = repository.begin().await?;
        inspection.rollback().await?;
        repository.close().await?;
        admin.close().await;
        remove_contract_rows(&runtime_url).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}
