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
    hostname_in_certificate: Option<String>,
    ca_bundle: Option<Vec<u8>>,
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
        let port = std::env::var("OXIDEBATCH_SQLSERVER_TEST_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(1433);
        let hostname_in_certificate = std::env::var("OXIDEBATCH_SQLSERVER_TEST_CERT_HOSTNAME").ok();
        let ca_bundle = std::env::var("OXIDEBATCH_SQLSERVER_TEST_CA_PEM")
            .ok()
            .map(String::into_bytes);
        Ok(Some(Self {
            host,
            port,
            database,
            username,
            password,
            hostname_in_certificate,
            ca_bundle,
        }))
    }

    fn repository_config(&self) -> Result<SqlServerConfig, Box<dyn Error>> {
        let mut config = SqlServerConfig::new(
            self.host.clone(),
            self.database.clone(),
            self.username.clone(),
            self.password.clone(),
        )?
        .with_port(self.port)?;
        if let Some(hostname) = &self.hostname_in_certificate {
            config = config.with_hostname_in_certificate(hostname.clone())?;
        }
        if let Some(bundle) = &self.ca_bundle {
            config = config.with_ca_bundle(bundle.clone())?;
        }
        Ok(config)
    }

    fn driver_config(&self) -> TiberiusConfig {
        let mut config = TiberiusConfig::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.application_name("OxideBatch SQL Server contract cleanup");
        config.authentication(AuthMethod::sql_server(&self.username, &self.password));
        config.encryption(EncryptionLevel::Strict);
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
        let config = settings.repository_config()?;
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
        let mut client = Box::pin(connect_test_client(&settings)).await?;
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
        let config = settings.repository_config()?;
        SqlServerMigrator::migrate(&config).await?;
        Box::pin(run_runnable_core_repository_contract("sqlserver", |clock| {
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
        }))
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
        let config = settings.repository_config()?;
        SqlServerMigrator::migrate(&config).await?;
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
        let config = settings.repository_config()?;
        SqlServerMigrator::migrate(&config).await?;
        Box::pin(remove_contract_rows(&settings)).await?;
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
        let config = settings.repository_config()?;
        SqlServerMigrator::migrate(&config).await?;
        let supported = SqlServerMigrator::supported_schema_version();
        let newer = supported
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("schema version overflow"))?;
        let mut client = Box::pin(connect_test_client(&settings)).await?;
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
