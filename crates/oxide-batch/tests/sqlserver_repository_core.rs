//! Runnable-core repository certification source for the first-party SQL Server adapter.

#![cfg(feature = "sqlserver")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use contract::run_runnable_core_repository_contract;
use oxide_batch::{
    BatchStatus, BoxFuture, ComponentRevision, DefinitionRevision, FailureCategory, FailureId,
    FailureSummary, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    JobInstanceKey, JobName, JobParameters, JobRepository, LifecycleTransition, NodeId,
    RepositoryError, SequentialIdGenerator, SqlServerConfig, SqlServerJobRepository,
    SqlServerMigrator, StepComponents, StepName, StepNode, StopSource, SystemClock, Tasklet,
    TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TerminalKind,
};
use std::error::Error;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
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

fn proxied_repository_config(
    settings: &TestSettings,
    port: u16,
) -> Result<SqlServerConfig, Box<dyn Error>> {
    let mut config = SqlServerConfig::new(
        "127.0.0.1",
        settings.database.clone(),
        settings.username.clone(),
        settings.password.clone(),
    )?
    .with_port(port)?
    .with_pool_size(1)?
    .with_command_timeout(Duration::from_secs(5))?;
    if let Some(hostname) = &settings.hostname_in_certificate {
        config = config.with_hostname_in_certificate(hostname.clone())?;
    }
    if let Some(bundle) = &settings.ca_bundle {
        config = config.with_ca_bundle(bundle.clone())?;
    }
    if settings.strict_tds80 {
        config = config.with_strict_tds80_encryption();
    }
    Ok(config)
}

struct CommitResponseDropProxy {
    address: std::net::SocketAddr,
    armed: Arc<AtomicBool>,
    fault_fired: Arc<AtomicBool>,
    response_dropped: Arc<AtomicBool>,
    accepted_connections: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl CommitResponseDropProxy {
    fn start(settings: &TestSettings) -> Result<Self, Box<dyn Error>> {
        let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let target_host = settings.host.clone();
        let target_port = settings.port;
        let armed = Arc::new(AtomicBool::new(false));
        let fault_fired = Arc::new(AtomicBool::new(false));
        let response_dropped = Arc::new(AtomicBool::new(false));
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let accept_armed = Arc::clone(&armed);
        let accept_fault_fired = Arc::clone(&fault_fired);
        let accept_response_dropped = Arc::clone(&response_dropped);
        let accept_count = Arc::clone(&accepted_connections);
        let accept_stop = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            while !accept_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((client, _)) => {
                        accept_count.fetch_add(1, Ordering::AcqRel);
                        let target_host = target_host.clone();
                        let armed = Arc::clone(&accept_armed);
                        let fault_fired = Arc::clone(&accept_fault_fired);
                        let response_dropped = Arc::clone(&accept_response_dropped);
                        let _ = thread::spawn(move || {
                            let _ = relay_sql_server_connection(
                                client,
                                &target_host,
                                target_port,
                                &armed,
                                &fault_fired,
                                &response_dropped,
                            );
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            address,
            armed,
            fault_fired,
            response_dropped,
            accepted_connections,
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    fn port(&self) -> u16 {
        self.address.port()
    }

    fn arm_next_client_request(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn fault_fired(&self) -> bool {
        self.fault_fired.load(Ordering::Acquire)
    }

    fn response_dropped(&self) -> bool {
        self.response_dropped.load(Ordering::Acquire)
    }

    fn accepted_connections(&self) -> usize {
        self.accepted_connections.load(Ordering::Acquire)
    }
}

impl Drop for CommitResponseDropProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

fn relay_sql_server_connection(
    client: StdTcpStream,
    target_host: &str,
    target_port: u16,
    armed: &Arc<AtomicBool>,
    fault_fired: &Arc<AtomicBool>,
    response_dropped: &Arc<AtomicBool>,
) -> std::io::Result<()> {
    client.set_nodelay(true)?;
    let server = StdTcpStream::connect((target_host, target_port))?;
    server.set_nodelay(true)?;

    let mut client_reader = client.try_clone()?;
    let mut client_writer = client;
    let mut server_reader = server.try_clone()?;
    let mut server_writer = server;
    let suppress_response = Arc::new(AtomicBool::new(false));
    let server_side_suppression = Arc::clone(&suppress_response);
    let server_side_response_dropped = Arc::clone(response_dropped);

    let response_relay = thread::spawn(move || -> std::io::Result<()> {
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = server_reader.read(&mut buffer)?;
            if count == 0 {
                let _ = client_writer.shutdown(Shutdown::Both);
                return Ok(());
            }
            if server_side_suppression.load(Ordering::Acquire) {
                server_side_response_dropped.store(true, Ordering::Release);
                let _ = client_writer.shutdown(Shutdown::Both);
                return Ok(());
            }
            client_writer.write_all(&buffer[..count])?;
            client_writer.flush()?;
        }
    });

    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = match client_reader.read(&mut buffer) {
            Ok(count) => count,
            Err(_) if response_dropped.load(Ordering::Acquire) => break,
            Err(error) => return Err(error),
        };
        if count == 0 {
            break;
        }

        let should_fault = armed.load(Ordering::Acquire)
            && fault_fired
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        if should_fault {
            // No response from an earlier command is outstanding when the
            // proxy is armed. Suppress the first server response after the
            // next complete client request is forwarded; that request is the
            // adapter's COMMIT.
            suppress_response.store(true, Ordering::Release);
        }

        server_writer.write_all(&buffer[..count])?;
        server_writer.flush()?;

        // Keep relaying every client fragment of the armed request. The
        // server-to-client half closes the client only after SQL Server emits
        // the corresponding response, so a fragmented TLS/TDS COMMIT cannot be
        // mistaken for a pre-COMMIT disconnect.
    }

    let _ = client_reader.shutdown(Shutdown::Both);
    let _ = server_writer.shutdown(Shutdown::Both);
    let _ = response_relay.join();
    Ok(())
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

async fn remove_crash_restart_rows(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    execute_test_query(
        &mut client,
        "DELETE execution \
         FROM dbo.ob_job_execution AS execution \
         JOIN dbo.ob_job_instance AS instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name IN ( \
           N'repository_crash_committed', N'repository_crash_uncommitted' \
         ); \
         DELETE FROM dbo.ob_job_instance \
         WHERE job_name IN ( \
           N'repository_crash_committed', N'repository_crash_uncommitted' \
         )",
    )
    .await?;
    Ok(())
}

async fn run_crash_restart_child(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let repository =
        SqlServerJobRepository::connect(settings.repository_config()?, Arc::new(SystemClock))
            .await?;
    let durable_key = JobInstanceKey::new(
        JobName::new("repository_crash_committed")?,
        &JobParameters::new(),
    );
    let undurable_key = JobInstanceKey::new(
        JobName::new("repository_crash_uncommitted")?,
        &JobParameters::new(),
    );

    // Persist a failed execution before the process is killed. A new process
    // must see the same durable instance and be allowed to create a retry.
    let mut committed = repository.begin().await?;
    let instance = committed
        .select_or_create_job_instance(&durable_key)
        .await?
        .instance()
        .clone();
    let attempt = committed.create_job_execution(instance.id()).await?;
    let created_at = attempt.metadata().timestamps().created_at();
    let started = committed
        .transition_job_execution(
            attempt.id(),
            attempt.version(),
            LifecycleTransition::new(BatchStatus::Started, created_at + Duration::from_secs(1)),
        )
        .await?;
    committed
        .transition_job_execution(
            started.id(),
            started.version(),
            LifecycleTransition::failed(
                created_at + Duration::from_secs(2),
                FailureSummary::new(FailureCategory::UserComponent, FailureId::new(674)?),
            ),
        )
        .await?;
    committed.commit().await?;

    // Hold a different, uncommitted transaction open until the parent sends
    // SIGKILL. No Rust destructor or RepositoryUnitOfWork::rollback is run.
    let mut pending = repository.begin().await?;
    pending
        .select_or_create_job_instance(&undurable_key)
        .await?;
    println!("OXIDEBATCH_SQLSERVER_CRASH_READY");
    std::io::stdout().flush()?;
    std::future::pending::<()>().await;
    drop(pending);
    Ok(())
}

async fn remove_commit_unknown_row(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    execute_test_query(
        &mut client,
        "DELETE FROM dbo.ob_job_instance \
         WHERE job_name = N'repository_commit_unknown'",
    )
    .await?;
    Ok(())
}

async fn remove_pool_resource_bound_row(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    execute_test_query(
        &mut client,
        "DELETE FROM dbo.ob_job_instance \
         WHERE job_name = N'repository_pool_resource_bound'",
    )
    .await?;
    Ok(())
}

const BACKUP_RESTORE_JOB: &str = "repository_backup_restore";
const BACKUP_RESTORE_NODE: &str = "backup_step";
const BACKUP_RESTORE_MODE: &str = "OXIDEBATCH_SQLSERVER_BACKUP_RESTORE_MODE";

struct BackupRestoreTasklet {
    fail: bool,
}

impl Tasklet for BackupRestoreTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            if self.fail {
                Err(TaskletError::new())
            } else {
                Ok(TaskletOutcome::Completed)
            }
        })
    }
}

fn backup_restore_job(fail: bool) -> Result<FlowJob, Box<dyn Error>> {
    let job_name = JobName::new(BACKUP_RESTORE_JOB)?;
    let node = NodeId::new(BACKUP_RESTORE_NODE)?;
    let step_name = StepName::new(BACKUP_RESTORE_NODE)?;
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            step_name.clone(),
            StepComponents::Tasklet(ComponentRevision::new("backup-tasklet-v1")?),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&job_name, DefinitionRevision::new("backup-v1")?)?;
    Ok(FlowJob::new(job_name, plan)?.with_tasklet_step(
        node,
        TaskletStep::new(step_name, Arc::new(BackupRestoreTasklet { fail })),
    )?)
}

async fn remove_backup_restore_rows(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let mut client = Box::pin(connect_test_client(settings)).await?;
    execute_test_query(
        &mut client,
        "UPDATE decision SET reused_decision_id = NULL \
         FROM dbo.ob_flow_decision AS decision \
         JOIN dbo.ob_job_execution AS execution \
           ON execution.id = decision.job_execution_id \
         JOIN dbo.ob_job_instance AS instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = N'repository_backup_restore'; \
         DELETE decision FROM dbo.ob_flow_decision AS decision \
         JOIN dbo.ob_job_execution AS execution \
           ON execution.id = decision.job_execution_id \
         JOIN dbo.ob_job_instance AS instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = N'repository_backup_restore'; \
         DELETE step FROM dbo.ob_step_execution AS step \
         JOIN dbo.ob_job_execution AS execution \
           ON execution.id = step.job_execution_id \
         JOIN dbo.ob_job_instance AS instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = N'repository_backup_restore'; \
         DELETE execution FROM dbo.ob_job_execution AS execution \
         JOIN dbo.ob_job_instance AS instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = N'repository_backup_restore'; \
         DELETE FROM dbo.ob_job_instance \
         WHERE job_name = N'repository_backup_restore'; \
         DELETE FROM dbo.ob_core_job_definition \
         WHERE job_name = N'repository_backup_restore'",
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
        .err()
        .ok_or_else(|| {
            std::io::Error::other("raw SQL Server lock-timeout probe unexpectedly succeeded")
        })?;
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
        let mut high_priority = Box::pin(connect_test_client(&settings)).await?;
        let mut low_priority = Box::pin(connect_test_client(&settings)).await?;
        let lock_high_row = format!(
            "SET DEADLOCK_PRIORITY HIGH; BEGIN TRAN; \
             UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_a}"
        );
        let lock_low_row = format!(
            "SET DEADLOCK_PRIORITY LOW; BEGIN TRAN; \
             UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_b}"
        );
        execute_test_query(&mut high_priority, &lock_high_row).await?;
        execute_test_query(&mut low_priority, &lock_low_row).await?;

        let request_low_row = format!(
            "UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_b}"
        );
        let request_high_row = format!(
            "UPDATE dbo.ob_job_instance SET created_ms = created_ms WHERE id = {instance_a}"
        );
        let (winner_outcome, victim_outcome) =
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::join!(
                    execute_test_query(&mut high_priority, &request_low_row),
                    execute_test_query(&mut low_priority, &request_high_row)
                )
            })
            .await
            .map_err(|_| {
                std::io::Error::other(
                    "phase=raw-1205 timed out before SQL Server selected a deadlock victim",
                )
            })?;

        assert!(winner_outcome.is_ok());
        let victim_error = victim_outcome.err().ok_or_else(|| {
            std::io::Error::other("LOW-priority raw deadlock participant unexpectedly survived")
        })?;
        assert_eq!(victim_error.code(), Some(1205));
        execute_test_query(&mut high_priority, "IF XACT_STATE() <> 0 ROLLBACK").await?;
        let _ =
            execute_test_query(&mut low_priority, "IF XACT_STATE() <> 0 ROLLBACK").await;

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
fn lost_commit_response_is_unknown_and_fresh_session_reconciles() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_commit_unknown_row(&settings)).await?;

        let proxy = CommitResponseDropProxy::start(&settings)?;
        let repository = SqlServerJobRepository::connect(
            proxied_repository_config(&settings, proxy.port())?,
            Arc::new(SystemClock),
        )
        .await?;
        let key = JobInstanceKey::new(
            JobName::new("repository_commit_unknown")?,
            &JobParameters::new(),
        );

        let mut unit = repository.begin().await?;
        unit.select_or_create_job_instance(&key).await?;

        // All prior request/response traffic is complete here. The next client
        // request on this checked-out connection is RepositoryUnitOfWork::commit.
        proxy.arm_next_client_request();
        assert_eq!(
            unit.commit().await,
            Err(RepositoryError::CommitOutcomeUnknown)
        );
        assert!(proxy.fault_fired());
        assert!(
            proxy.response_dropped(),
            "proxy did not observe and suppress the server response to COMMIT"
        );

        // The adapter must not reuse the suspect physical connection. A fresh
        // pool checkout goes through a second TCP connection and reconciles the
        // durable state instead of inferring the commit result from the error.
        let mut fresh = repository.begin().await?;
        let durable = fresh.find_job_instance(&key).await?;
        fresh.rollback().await?;
        assert!(
            durable.is_some(),
            "server produced a COMMIT response but fresh reconciliation did not find the durable row"
        );
        assert!(
            proxy.accepted_connections() >= 2,
            "fresh reconciliation reused the connection whose COMMIT response was lost"
        );

        drop(repository);
        drop(proxy);
        Box::pin(remove_commit_unknown_row(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn pool_capacity_bounds_concurrent_uows_and_replaces_suspect_connection()
-> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_pool_resource_bound_row(&settings)).await?;

        let proxy = CommitResponseDropProxy::start(&settings)?;
        let config = proxied_repository_config(&settings, proxy.port())?
            .with_pool_size(2)?
            .with_acquire_timeout(Duration::from_millis(250))?;
        let repository = SqlServerJobRepository::connect(config, Arc::new(SystemClock)).await?;
        assert_eq!(repository.connection_capacity(), 2);

        // Hold the entire declared pool. More concurrent UoWs must fail closed
        // at the acquire bound rather than creating an unbounded connection.
        let mut first = repository.begin().await?;
        let second = repository.begin().await?;
        let (overflow_a, overflow_b, overflow_c) =
            tokio::join!(repository.begin(), repository.begin(), repository.begin());
        assert_eq!(overflow_a.err(), Some(RepositoryError::Unavailable));
        assert_eq!(overflow_b.err(), Some(RepositoryError::Unavailable));
        assert_eq!(overflow_c.err(), Some(RepositoryError::Unavailable));

        let key = JobInstanceKey::new(
            JobName::new("repository_pool_resource_bound")?,
            &JobParameters::new(),
        );
        first.select_or_create_job_instance(&key).await?;
        let accepted_before_fault = proxy.accepted_connections();
        assert!(
            accepted_before_fault >= 2,
            "pool saturation did not establish two physical SQL Server connections"
        );

        // Make one of the two saturated connections suspect. The other stays
        // checked out, so successful reconciliation can only happen after the
        // broken physical session is discarded and a replacement is created.
        proxy.arm_next_client_request();
        assert_eq!(
            first.commit().await,
            Err(RepositoryError::CommitOutcomeUnknown)
        );
        assert!(proxy.fault_fired());
        assert!(proxy.response_dropped());

        let mut fresh = repository.begin().await?;
        let durable = fresh.find_job_instance(&key).await?;
        fresh.rollback().await?;
        assert!(
            durable.is_some(),
            "replacement connection did not reconcile the durable COMMIT"
        );
        assert!(
            proxy.accepted_connections() > accepted_before_fault,
            "pool reused the suspect physical connection instead of replacing it"
        );
        assert_eq!(repository.connection_capacity(), 2);

        second.rollback().await?;
        drop(repository);
        drop(proxy);
        Box::pin(remove_pool_resource_bound_row(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn process_kill_recovers_sql_server_state() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment()? else {
        eprintln!("skipped: SQL Server test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    if std::env::var("OXIDEBATCH_SQLSERVER_CRASH_CHILD").as_deref() == Ok("1") {
        return runtime.block_on(run_crash_restart_child(&settings));
    }

    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_crash_restart_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;

    // Run the identical integration-test binary in a separate OS process.
    // The child commits a FAILED attempt, then holds another transaction open.
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "process_kill_recovers_sql_server_state",
            "--nocapture",
        ])
        .env("OXIDEBATCH_SQLSERVER_CRASH_CHILD", "1")
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("child stdout pipe was unavailable"))?;
    let (send_ready, receive_ready) = std::sync::mpsc::channel();
    let reader = thread::spawn(move || {
        let ready = std::io::BufRead::lines(std::io::BufReader::new(stdout))
            .any(|line| line.is_ok_and(|line| line.contains("OXIDEBATCH_SQLSERVER_CRASH_READY")));
        let _ = send_ready.send(ready);
    });

    // A hard kill is materially different from dropping a UoW or gracefully
    // terminating Tokio. Always reap the child, including on handoff timeout.
    let ready = receive_ready.recv_timeout(Duration::from_secs(45));
    let kill_result = child.kill();
    let child_status = child.wait()?;
    reader
        .join()
        .map_err(|_| std::io::Error::other("child output reader panicked"))?;
    assert!(
        matches!(ready, Ok(true)),
        "child never confirmed durable commit plus open uncommitted transaction: {ready:?}"
    );
    kill_result?;
    assert!(
        child_status.code().is_none(),
        "child was not terminated by a signal: {child_status}"
    );

    runtime.block_on(async {
        // Connect through an entirely new process-side repository/pool. The
        // committed FAILED attempt must survive and the killed UoW must not.
        let repository =
            SqlServerJobRepository::connect(settings.repository_config()?, Arc::new(SystemClock))
                .await?;
        let durable_key = JobInstanceKey::new(
            JobName::new("repository_crash_committed")?,
            &JobParameters::new(),
        );
        let undurable_key = JobInstanceKey::new(
            JobName::new("repository_crash_uncommitted")?,
            &JobParameters::new(),
        );
        let mut inspection = repository.begin().await?;
        let instance = inspection
            .find_job_instance(&durable_key)
            .await?
            .ok_or_else(|| std::io::Error::other("committed instance vanished after SIGKILL"))?;
        let attempts = inspection.job_executions(instance.id()).await?;
        assert_eq!(attempts.len(), 1);
        assert_eq!(
            attempts[0].metadata().status(),
            oxide_batch::BatchStatus::Failed
        );
        assert!(
            inspection
                .find_job_instance(&undurable_key)
                .await?
                .is_none(),
            "uncommitted job instance survived the process kill"
        );
        inspection.rollback().await?;

        // A FAILED durable execution is restartable across the process
        // boundary: a new attempt must preserve instance identity.
        let mut restart = repository.begin().await?;
        let retry = restart.create_job_execution(instance.id()).await?;
        assert_ne!(retry.id(), attempts[0].id());
        restart.commit().await?;

        let mut verify = repository.begin().await?;
        assert_eq!(verify.job_executions(instance.id()).await?.len(), 2);
        verify.rollback().await?;
        Box::pin(remove_crash_restart_rows(&settings)).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn logical_backup_source_contains_restartable_state() -> Result<(), Box<dyn Error>> {
    if required(BACKUP_RESTORE_MODE).as_deref() != Some("seed") {
        eprintln!("skipped: SQL Server logical backup seed mode is not enabled");
        return Ok(());
    }
    let Some(settings) = TestSettings::from_environment()? else {
        return Err(
            std::io::Error::other("SQL Server backup seed environment is incomplete").into(),
        );
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        SqlServerMigrator::migrate(&settings.migrator_config()?).await?;
        Box::pin(remove_backup_restore_rows(&settings)).await?;

        let clock = SystemClock;
        let repository =
            SqlServerJobRepository::connect(settings.repository_config()?, Arc::new(SystemClock))
                .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let report = FlowLauncher::new(&repository, &clock, &ids)
            .launch(&backup_restore_job(true)?, &JobParameters::new(), &stop)
            .await?;
        assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));

        let mut inspection = repository.begin().await?;
        let key = JobInstanceKey::new(JobName::new(BACKUP_RESTORE_JOB)?, &JobParameters::new());
        let instance = inspection
            .find_job_instance(&key)
            .await?
            .ok_or_else(|| std::io::Error::other("backup seed instance was not durable"))?;
        let attempts = inspection.job_executions(instance.id()).await?;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].metadata().status(), BatchStatus::Failed);
        let latest = inspection
            .latest_flow_step(instance.id(), &NodeId::new(BACKUP_RESTORE_NODE)?)
            .await?
            .ok_or_else(|| std::io::Error::other("backup seed step was not durable"))?;
        assert_eq!(latest.execution().metadata().status(), BatchStatus::Failed);
        inspection.rollback().await?;

        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn logical_backup_restores_restartable_state() -> Result<(), Box<dyn Error>> {
    if required(BACKUP_RESTORE_MODE).as_deref() != Some("restore") {
        eprintln!("skipped: SQL Server logical restore mode is not enabled");
        return Ok(());
    }
    let Some(settings) = TestSettings::from_environment()? else {
        return Err(std::io::Error::other("SQL Server restore environment is incomplete").into());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let migrator = settings.migrator_config()?;
        assert_eq!(
            SqlServerMigrator::installed_schema_version(&migrator).await?,
            Some(SqlServerMigrator::supported_schema_version())
        );

        let clock = SystemClock;
        let repository =
            SqlServerJobRepository::connect(settings.repository_config()?, Arc::new(SystemClock))
                .await?;
        let key = JobInstanceKey::new(JobName::new(BACKUP_RESTORE_JOB)?, &JobParameters::new());
        let node = NodeId::new(BACKUP_RESTORE_NODE)?;

        let mut inspection = repository.begin().await?;
        let instance = inspection
            .find_job_instance(&key)
            .await?
            .ok_or_else(|| std::io::Error::other("restored job instance was missing"))?;
        let source_attempts = inspection.job_executions(instance.id()).await?;
        assert_eq!(source_attempts.len(), 1);
        assert_eq!(source_attempts[0].metadata().status(), BatchStatus::Failed);
        let source_step = inspection
            .latest_flow_step(instance.id(), &node)
            .await?
            .ok_or_else(|| std::io::Error::other("restored failed step was missing"))?;
        assert_eq!(
            source_step.execution().metadata().status(),
            BatchStatus::Failed
        );
        inspection.rollback().await?;

        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let (_source, stop) = StopSource::new();
        let restarted = FlowLauncher::new(&repository, &clock, &ids)
            .launch(&backup_restore_job(false)?, &JobParameters::new(), &stop)
            .await?;
        assert_eq!(restarted.outcome(), &FlowExecutionOutcome::Completed);
        assert_eq!(restarted.instance().id(), instance.id());
        assert_ne!(restarted.job_execution().id(), source_attempts[0].id());

        let mut verify = repository.begin().await?;
        let attempts = verify.job_executions(instance.id()).await?;
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].metadata().status(), BatchStatus::Failed);
        assert_eq!(attempts[1].metadata().status(), BatchStatus::Completed);
        assert_eq!(
            verify
                .flow_decisions(restarted.job_execution().id())
                .await?
                .len(),
            1
        );
        verify.rollback().await?;

        Box::pin(remove_backup_restore_rows(&settings)).await?;
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
