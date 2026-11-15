//! Runnable-core repository certification source for the first-party SQL Server adapter.

#![cfg(feature = "sqlserver")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use contract::run_runnable_core_repository_contract;
use oxide_batch::{
    JobInstanceKey, JobName, JobParameters, JobRepository, RepositoryError, SqlServerConfig,
    SqlServerJobRepository, SqlServerMigrator, SystemClock,
};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tiberius::{AuthMethod, Client, Config as TiberiusConfig, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[derive(Clone)]
struct TestSettings {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    migrator_username: String,
    migrator_password: String,
    hostname_in_certificate: Option<String>,
    ca_bundle: Option<Vec<u8>>,
    wrong_ca_bundle: Option<Vec<u8>>,
    strict_tds80: bool,
}

impl TestSettings {
    fn from_environment() -> Result<Option<Self>, Box<dyn Error>> {
        let Some(host) = required("OXIDEBATCH_SQLSERVER_TEST_HOST") else {
            return Ok(None);
        };
        let Some(database) = required("OXIDEBATCH_SQLSERVER_TEST_DATABASE") else {
            return Ok(None);
        };
        let Some(username) = required("OXIDEBATCH_SQLSERVER_TEST_USERNAME") else {
            return Ok(None);
        };
        let Some(password) = required("OXIDEBATCH_SQLSERVER_TEST_PASSWORD") else {
            return Ok(None);
        };
        let Some(migrator_username) = required("OXIDEBATCH_SQLSERVER_MIGRATOR_USERNAME") else {
            return Ok(None);
        };
        let Some(migrator_password) = required("OXIDEBATCH_SQLSERVER_MIGRATOR_PASSWORD") else {
            return Ok(None);
        };
        let port = std::env::var("OXIDEBATCH_SQLSERVER_TEST_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(1433);
        let hostname_in_certificate = std::env::var("OXIDEBATCH_SQLSERVER_TEST_CERT_HOSTNAME").ok();
        let ca_bundle = std::env::var("OXIDEBATCH_SQLSERVER_TEST_CA_PEM")
            .ok()
            .map(String::into_bytes);
        let wrong_ca_bundle = std::env::var("OXIDEBATCH_SQLSERVER_TEST_WRONG_CA_PEM")
            .ok()
            .map(String::into_bytes);
        let strict_tds80 =
            std::env::var("OXIDEBATCH_SQLSERVER_TEST_TDS80_STRICT").as_deref() == Ok("1");
        Ok(Some(Self {
            host,
            port,
            database,
            username,
            password,
            migrator_username,
            migrator_password,
            hostname_in_certificate,
            ca_bundle,
            wrong_ca_bundle,
            strict_tds80,
        }))
    }

    fn repository_config(&self) -> Result<SqlServerConfig, Box<dyn Error>> {
        self.repository_config_with_credentials(&self.username, &self.password)
    }

    fn migrator_config(&self) -> Result<SqlServerConfig, Box<dyn Error>> {
        self.repository_config_with_credentials(&self.migrator_username, &self.migrator_password)
    }

    fn repository_config_with_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SqlServerConfig, Box<dyn Error>> {
        let mut config = SqlServerConfig::new(
            self.host.clone(),
            self.database.clone(),
            username.to_owned(),
            password.to_owned(),
        )?
        .with_port(self.port)?;
        if let Some(hostname) = &self.hostname_in_certificate {
            config = config.with_hostname_in_certificate(hostname.clone())?;
        }
        if let Some(bundle) = &self.ca_bundle {
            config = config.with_ca_bundle(bundle.clone())?;
        }
        if self.strict_tds80 {
            config = config.with_strict_tds80_encryption();
        }
        Ok(config)
    }

    fn driver_config(&self) -> TiberiusConfig {
        self.driver_config_with_credentials(&self.username, &self.password)
    }

    fn migrator_driver_config(&self) -> TiberiusConfig {
        self.driver_config_with_credentials(&self.migrator_username, &self.migrator_password)
    }

    fn driver_config_with_credentials(&self, username: &str, password: &str) -> TiberiusConfig {
        let mut config = TiberiusConfig::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.application_name("OxideBatch SQL Server contract cleanup");
        config.authentication(AuthMethod::sql_server(username, password));
        config.encryption(if self.strict_tds80 {
            EncryptionLevel::Strict
        } else {
            EncryptionLevel::Required
        });
        if let Some(hostname) = &self.hostname_in_certificate {
            config.hostname_in_certificate(hostname);
        }
        if let Some(bundle) = &self.ca_bundle {
            config.trust_cert_ca_bundle(bundle.clone());
        }
        config
    }
}

fn required(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

async fn connect_test_client(
    settings: &TestSettings,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>, Box<dyn Error>> {
    let config = settings.driver_config();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Box::pin(Client::connect(config, tcp.compat_write())).await?)
}

async fn connect_migrator_test_client(
    settings: &TestSettings,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>, Box<dyn Error>> {
    let config = settings.migrator_driver_config();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Box::pin(Client::connect(config, tcp.compat_write())).await?)
}

async fn execute_test_query(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) -> Result<(), tiberius::error::Error> {
    let stream = client.simple_query(sql).await?;
    stream.into_results().await?;
    Ok(())
}

async fn remove_contract_rows(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    client
        .simple_query(
            "UPDATE decision \
             SET reused_decision_id = NULL \
             FROM dbo.ob_flow_decision AS decision \
             JOIN dbo.ob_job_execution AS execution \
               ON execution.id = decision.job_execution_id \
             JOIN dbo.ob_job_instance AS instance \
               ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = N'repository_contract_job'; \
             DELETE decision \
             FROM dbo.ob_flow_decision AS decision \
             JOIN dbo.ob_job_execution AS execution \
               ON execution.id = decision.job_execution_id \
             JOIN dbo.ob_job_instance AS instance \
               ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = N'repository_contract_job'; \
             DELETE step \
             FROM dbo.ob_step_execution AS step \
             JOIN dbo.ob_job_execution AS execution \
               ON execution.id = step.job_execution_id \
             JOIN dbo.ob_job_instance AS instance \
               ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = N'repository_contract_job'; \
             DELETE execution \
             FROM dbo.ob_job_execution AS execution \
             JOIN dbo.ob_job_instance AS instance \
               ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = N'repository_contract_job'; \
             DELETE FROM dbo.ob_job_instance \
               WHERE job_name = N'repository_contract_job'; \
             DELETE FROM dbo.ob_core_job_definition \
               WHERE job_name = N'repository_contract_job';",
        )
        .await?
        .into_results()
        .await?;
    Ok(())
}

async fn remove_contention_rows(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    execute_test_query(
        &mut client,
        "DELETE FROM dbo.ob_job_instance \
         WHERE job_name IN ( \
           N'repository_contention_lock_timeout', \
           N'repository_contention_deadlock_a', \
           N'repository_contention_deadlock_b' \
         )",
    )
    .await?;
    Ok(())
}

#[test]
fn migration_is_idempotent_for_sql_server() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.migrator_config()?;
        SqlServerMigrator::migrate(&config).await?;
        SqlServerMigrator::migrate(&config).await?;
        assert_eq!(
            SqlServerMigrator::installed_schema_version(&config).await?,
            Some(SqlServerMigrator::supported_schema_version())
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn named_sql_server_lane_reports_the_expected_product_version() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let Some(expected) = required("OXIDEBATCH_SQLSERVER_EXPECTED_PRODUCT_VERSION") else {
        eprintln!("skipped: OXIDEBATCH_SQLSERVER_EXPECTED_PRODUCT_VERSION is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut client = Box::pin(connect_migrator_test_client(&settings)).await?;
        let row = client
            .simple_query(
                "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) \
                 AS product_version",
            )
            .await?
            .into_row()
            .await?
            .ok_or_else(|| std::io::Error::other("SQL Server did not return ProductVersion"))?;
        let actual = row
            .try_get::<&str, _>("product_version")?
            .ok_or_else(|| std::io::Error::other("SQL Server ProductVersion was NULL"))?;
        assert_eq!(actual, expected);
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn runnable_core_repository_contract_passes_on_sql_server() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(run_runnable_core_repository_contract(
            "sqlserver",
            |clock| {
                let settings = settings.clone();
                async move {
                    Box::pin(remove_contract_rows(&settings))
                        .await
                        .map_err(|_| RepositoryError::Unavailable)?;
                    SqlServerJobRepository::connect(
                        settings
                            .repository_config()
                            .map_err(|_| RepositoryError::Unavailable)?,
                        clock,
                    )
                    .await
                }
            },
        ))
        .await?;
        Box::pin(remove_contract_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn sql_server_descriptor_declares_zero_optional_capabilities() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        let config = settings.repository_config()?;
        let repository = SqlServerJobRepository::connect(config, Arc::new(SystemClock)).await?;
        assert_eq!(repository.descriptor().capabilities().len(), 0);
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn concurrent_launch_creates_one_sql_server_instance_and_attempt() -> Result<(), Box<dyn Error>> {
    const CONTENDERS: usize = 8;

    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_contract_rows(&settings)).await?;
        let config = settings.repository_config()?;
        let repository = SqlServerJobRepository::connect(config, Arc::new(SystemClock)).await?;
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
                Err(RepositoryError::ExecutionAlreadyActive { .. }) => {
                    active_rejections += 1;
                }
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!(committed, 1);
        assert_eq!(active_rejections, CONTENDERS - 1);

        let mut inspection = repository.begin().await?;
        let instance = inspection.find_job_instance(&key).await?.ok_or_else(|| {
            std::io::Error::other("canonical SQL Server instance was not committed")
        })?;
        assert_eq!(inspection.job_executions(instance.id()).await?.len(), 1);
        inspection.rollback().await?;
        Box::pin(remove_contract_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn lock_timeout_1222_maps_to_concurrent_modification() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_contention_rows(&settings)).await?;

        let key = JobInstanceKey::new(
            JobName::new("repository_contention_lock_timeout")?,
            &JobParameters::new(),
        );
        let repository = SqlServerJobRepository::connect(
            settings
                .repository_config()?
                .with_lock_timeout(Duration::from_millis(150))?,
            Arc::new(SystemClock),
        )
        .await?;

        let mut seed = repository.begin().await?;
        seed.select_or_create_job_instance(&key).await?;
        seed.commit().await?;

        let mut blocker = Box::pin(connect_test_client(&settings)).await?;
        execute_test_query(
            &mut blocker,
            "BEGIN TRAN; \
             SELECT id FROM dbo.ob_job_instance WITH (XLOCK, HOLDLOCK) \
             WHERE job_name = N'repository_contention_lock_timeout'",
        )
        .await?;

        let mut raw_probe = Box::pin(connect_test_client(&settings)).await?;
        let raw_error = execute_test_query(
            &mut raw_probe,
            "SET LOCK_TIMEOUT 150; \
             SELECT id FROM dbo.ob_job_instance WITH (UPDLOCK, HOLDLOCK) \
             WHERE job_name = N'repository_contention_lock_timeout'",
        )
        .await
        .expect_err("raw SQL Server lock-timeout probe unexpectedly succeeded");
        assert_eq!(raw_error.code(), Some(1222));

        let mut unit = repository.begin().await?;
        assert_eq!(
            unit.select_or_create_job_instance(&key).await.err(),
            Some(RepositoryError::ConcurrentModification)
        );
        drop(unit);

        execute_test_query(&mut blocker, "IF XACT_STATE() <> 0 ROLLBACK").await?;

        let mut fresh = repository.begin().await?;
        assert!(fresh.find_job_instance(&key).await?.is_some());
        fresh.rollback().await?;

        Box::pin(remove_contention_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn deadlock_1205_maps_to_concurrent_modification() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_contention_rows(&settings)).await?;

        let key_a = JobInstanceKey::new(
            JobName::new("repository_contention_deadlock_a")?,
            &JobParameters::new(),
        );
        let key_b = JobInstanceKey::new(
            JobName::new("repository_contention_deadlock_b")?,
            &JobParameters::new(),
        );
        let repository = SqlServerJobRepository::connect(
            settings
                .repository_config()?
                .with_lock_timeout(Duration::from_secs(30))?,
            Arc::new(SystemClock),
        )
        .await?;

        let mut seed = repository.begin().await?;
        let instance_a = seed
            .select_or_create_job_instance(&key_a)
            .await?
            .instance()
            .id()
            .get();
        let instance_b = seed
            .select_or_create_job_instance(&key_b)
            .await?
            .instance()
            .id()
            .get();
        seed.commit().await?;

        // First prove SQL Server 1205 with an exact two-row UPDATE cycle. Using
        // primary-key predicates avoids range/page ambiguity from the previous
        // SELECT/XLOCK choreography.
        let mut raw_a = Box::pin(connect_test_client(&settings)).await?;
        let mut raw_b = Box::pin(connect_test_client(&settings)).await?;
        let raw_a_first = format!(
            "SET DEADLOCK_PRIORITY HIGH; BEGIN TRAN; \
             UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_a}"
        );
        let raw_b_first = format!(
            "SET DEADLOCK_PRIORITY LOW; BEGIN TRAN; \
             UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_b}"
        );
        execute_test_query(&mut raw_a, &raw_a_first).await?;
        execute_test_query(&mut raw_b, &raw_b_first).await?;

        let raw_a_second = format!(
            "UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_b}"
        );
        let raw_b_second = format!(
            "UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_a}"
        );
        let (raw_a_wait, raw_b_wait) =
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::join!(
                    execute_test_query(&mut raw_a, &raw_a_second),
                    execute_test_query(&mut raw_b, &raw_b_second)
                )
            })
            .await
            .map_err(|_| {
                std::io::Error::other(
                    "phase=raw-1205 timed out before SQL Server selected a deadlock victim",
                )
            })?;

        assert!(raw_a_wait.is_ok());
        assert_eq!(
            raw_b_wait
                .expect_err("LOW-priority raw deadlock participant unexpectedly survived")
                .code(),
            Some(1205)
        );
        execute_test_query(&mut raw_a, "IF XACT_STATE() <> 0 ROLLBACK").await?;
        let _ = execute_test_query(&mut raw_b, "IF XACT_STATE() <> 0 ROLLBACK").await;

        // Then make the repository transaction the lower-priority victim. The
        // UoW keeps the adapter's UPDLOCK/HOLDLOCK on A; a HIGH-priority raw
        // transaction holds an exact UPDATE lock on B. Each side then requests
        // the other's row, producing a cycle whose repository side must surface
        // as the typed contention error.
        let mut unit = repository.begin().await?;
        unit.select_or_create_job_instance(&key_a).await?;

        let mut survivor = Box::pin(connect_test_client(&settings)).await?;
        let survivor_first = format!(
            "SET DEADLOCK_PRIORITY HIGH; BEGIN TRAN; \
             UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_b}"
        );
        execute_test_query(&mut survivor, &survivor_first).await?;

        let survivor_second = format!(
            "UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_a}"
        );
        let (survivor_wait, repository_wait) =
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::join!(
                    execute_test_query(&mut survivor, &survivor_second),
                    unit.select_or_create_job_instance(&key_b)
                )
            })
            .await
            .map_err(|_| {
                std::io::Error::other(
                    "phase=repository-victim-1205 timed out before SQL Server selected a deadlock victim",
                )
            })?;

        assert!(survivor_wait.is_ok());
        assert_eq!(
            repository_wait.err(),
            Some(RepositoryError::ConcurrentModification)
        );
        drop(unit);
        execute_test_query(&mut survivor, "IF XACT_STATE() <> 0 ROLLBACK").await?;

        let mut fresh = repository.begin().await?;
        assert!(fresh.find_job_instance(&key_a).await?.is_some());
        assert!(fresh.find_job_instance(&key_b).await?.is_some());
        fresh.rollback().await?;

        Box::pin(remove_contention_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn tls_rejects_wrong_hostname_and_untrusted_ca() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let Some(wrong_ca_bundle) = settings.wrong_ca_bundle.clone() else {
        eprintln!("skipped: SQL Server wrong-CA fixture is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;

        let wrong_hostname = settings
            .repository_config()?
            .with_hostname_in_certificate("wrong-hostname.oxide-batch.invalid")?;
        assert_eq!(
            SqlServerJobRepository::connect(wrong_hostname, Arc::new(SystemClock))
                .await
                .err(),
            Some(RepositoryError::Unavailable)
        );

        let wrong_ca = settings
            .repository_config()?
            .with_ca_bundle(wrong_ca_bundle)?;
        assert_eq!(
            SqlServerJobRepository::connect(wrong_ca, Arc::new(SystemClock))
                .await
                .err(),
            Some(RepositoryError::Unavailable)
        );

        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn runtime_principal_cannot_migrate_or_mutate_schema_version() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;

        let mut client = Box::pin(connect_test_client(&settings)).await?;
        let ddl = execute_test_query(
            &mut client,
            "CREATE TABLE dbo.ob_runtime_privilege_probe (id int NOT NULL)",
        )
        .await;
        if ddl.is_ok() {
            let _ =
                execute_test_query(&mut client, "DROP TABLE dbo.ob_runtime_privilege_probe").await;
            return Err(
                std::io::Error::other("runtime principal unexpectedly has DDL authority").into(),
            );
        }

        let schema_write = execute_test_query(
            &mut client,
            "UPDATE dbo.ob_schema_version SET applied_ms = applied_ms",
        )
        .await;
        if schema_write.is_ok() {
            return Err(std::io::Error::other(
                "runtime principal unexpectedly can mutate schema version",
            )
            .into());
        }

        let row = client
            .simple_query("SELECT MAX(version) AS version FROM dbo.ob_schema_version")
            .await?
            .into_row()
            .await?
            .ok_or_else(|| std::io::Error::other("schema version row was not visible"))?;
        assert_eq!(
            row.try_get::<i32, _>("version")?,
            Some(
                i32::try_from(SqlServerMigrator::supported_schema_version())
                    .map_err(|_| std::io::Error::other("schema version is outside INT range"))?
            )
        );

        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn newer_sql_server_schema_is_rejected_without_guessing_compatibility() -> Result<(), Box<dyn Error>>
{
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        let config = settings.repository_config()?;
        let supported = SqlServerMigrator::supported_schema_version();
        let newer = supported
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("schema version overflow"))?;
        let mut client = Box::pin(connect_migrator_test_client(&settings)).await?;
        let update = format!(
            "UPDATE dbo.ob_schema_version SET version = {newer} \
             WHERE version = {supported}"
        );
        client.simple_query(update).await?.into_results().await?;

        let result = SqlServerJobRepository::connect(config, Arc::new(SystemClock)).await;

        let restore = format!(
            "UPDATE dbo.ob_schema_version SET version = {supported} \
             WHERE version = {newer}"
        );
        client.simple_query(restore).await?.into_results().await?;

        assert_eq!(
            result.err(),
            Some(RepositoryError::NewerSchema {
                current: newer,
                supported,
            })
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
