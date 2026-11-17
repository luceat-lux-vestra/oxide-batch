use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bb8::{ManageConnection, Pool, PooledConnection};
use serde_json::{Map, Value, json};
use tiberius::{
    AuthMethod, Client, Config as TiberiusConfig, EncryptionLevel, Query, Row as TdsRow,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::{
    BatchStatus, BoxFuture, Clock, DefinitionIdentity, DefinitionUpgrade, ExecutionCounts,
    ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitCode, ExitStatus,
    FailureCategory, FailureId, FailureSummary, FlowDecision, FlowDecisionId, FlowDecisionRequest,
    FlowDecisionSequence, FlowStepState, FlowTarget, FlowTransitionKind, IdentifierKind,
    JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobInstanceSelection,
    JobName, JobParameter, JobParameters, JobRepository, LifecycleError, LifecycleTransition,
    NodeId, ParameterName, ParameterRole, ParameterValue, ParameterValueKind, RecoveryDecision,
    RecoveryRequest, RecoveryResult, RepositoryCapability, RepositoryDescriptor, RepositoryError,
    RepositoryUnitOfWork, StartLimit, StepExecution, StepExecutionId, StepName, TerminalKind,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 2;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;
const DEFAULT_PORT: u16 = 1433;
const DEFAULT_POOL_SIZE: u32 = 10;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_mins(30);
const MIN_OPERATION_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_mins(10);
const MAX_CONNECTION_LIFETIME: Duration = Duration::from_hours(24);

const MIGRATION_1: &str = include_str!("../../migrations-sqlserver/0001_core_metadata.sql");
const MIGRATION_2: &str = include_str!("../../migrations-sqlserver/0002_runnable_core.sql");

type SqlServerTransport = Compat<TcpStream>;

/// Redacted first-party SQL Server repository configuration.
#[derive(Clone)]
pub struct SqlServerConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    hostname_in_certificate: Option<String>,
    ca_bundle: Option<Vec<u8>>,
    strict_tds80: bool,
    pool_size: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    command_timeout: Duration,
    lock_timeout: Duration,
    max_lifetime: Duration,
}

impl SqlServerConfig {
    /// Constructs SQL Server configuration using SQL authentication.
    ///
    /// Certificate validation is always enabled. System trust roots are used
    /// unless an additional CA bundle is supplied.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted configuration error when a required field is
    /// empty or a configured bound is outside the accepted range.
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, SqlServerConfigError> {
        let config = Self {
            host: host.into(),
            port: DEFAULT_PORT,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            hostname_in_certificate: None,
            ca_bundle: None,
            strict_tds80: false,
            pool_size: DEFAULT_POOL_SIZE,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
            max_lifetime: DEFAULT_MAX_LIFETIME,
        };
        config.validate()?;
        Ok(config)
    }

    /// Overrides the SQL Server TCP port.
    ///
    /// # Errors
    /// Returns a configuration error when the port is zero.
    pub fn with_port(mut self, port: u16) -> Result<Self, SqlServerConfigError> {
        self.port = port;
        self.validate()?;
        Ok(self)
    }

    /// Sets the bounded bb8 pool capacity.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 through 1024 connections.
    pub fn with_pool_size(mut self, pool_size: u32) -> Result<Self, SqlServerConfigError> {
        self.pool_size = pool_size;
        self.validate()?;
        Ok(self)
    }

    /// Adds an in-memory PEM/DER CA bundle to the validated trust roots.
    ///
    /// # Errors
    /// Returns a configuration error for an empty bundle.
    pub fn with_ca_bundle(mut self, bundle: Vec<u8>) -> Result<Self, SqlServerConfigError> {
        if bundle.is_empty() {
            return Err(SqlServerConfigError::EmptyCaBundle);
        }
        self.ca_bundle = Some(bundle);
        self.validate()?;
        Ok(self)
    }

    /// Overrides the hostname used for certificate-name verification.
    ///
    /// # Errors
    /// Returns a configuration error for an empty hostname.
    pub fn with_hostname_in_certificate(
        mut self,
        hostname: impl Into<String>,
    ) -> Result<Self, SqlServerConfigError> {
        let hostname = hostname.into();
        if hostname.is_empty() {
            return Err(SqlServerConfigError::EmptyHostnameInCertificate);
        }
        self.hostname_in_certificate = Some(hostname);
        self.validate()?;
        Ok(self)
    }

    /// Uses TDS 8.0 strict encryption instead of compatible required TLS.
    ///
    /// Certificate-chain and hostname validation remain mandatory in either mode.
    #[must_use]
    pub fn with_strict_tds80_encryption(mut self) -> Self {
        self.strict_tds80 = true;
        self
    }

    /// Sets the finite pool-acquire timeout.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 ms through 10 minutes.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Result<Self, SqlServerConfigError> {
        self.acquire_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the finite TDS/TLS handshake timeout.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 ms through 10 minutes.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Result<Self, SqlServerConfigError> {
        self.connect_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the finite Tiberius command-response timeout.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 ms through 10 minutes.
    pub fn with_command_timeout(mut self, timeout: Duration) -> Result<Self, SqlServerConfigError> {
        self.command_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets SQL Server's per-session lock wait bound.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 ms through 10 minutes.
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Result<Self, SqlServerConfigError> {
        self.lock_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum lifetime of a pooled physical connection.
    ///
    /// # Errors
    /// Returns a configuration error outside 1 second through 24 hours.
    pub fn with_max_lifetime(mut self, timeout: Duration) -> Result<Self, SqlServerConfigError> {
        self.max_lifetime = timeout;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), SqlServerConfigError> {
        if self.host.is_empty() {
            return Err(SqlServerConfigError::EmptyHost);
        }
        if self.port == 0 {
            return Err(SqlServerConfigError::Port);
        }
        if self.database.is_empty() {
            return Err(SqlServerConfigError::EmptyDatabase);
        }
        if self.username.is_empty() {
            return Err(SqlServerConfigError::EmptyUsername);
        }
        if self.password.is_empty() {
            return Err(SqlServerConfigError::EmptyPassword);
        }
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(SqlServerConfigError::PoolSize);
        }
        for timeout in [
            self.acquire_timeout,
            self.connect_timeout,
            self.command_timeout,
            self.lock_timeout,
        ] {
            if timeout < MIN_OPERATION_TIMEOUT || timeout > MAX_OPERATION_TIMEOUT {
                return Err(SqlServerConfigError::Timeout);
            }
        }
        if self.max_lifetime < Duration::from_secs(1) || self.max_lifetime > MAX_CONNECTION_LIFETIME
        {
            return Err(SqlServerConfigError::ConnectionLifetime);
        }
        if self
            .hostname_in_certificate
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(SqlServerConfigError::EmptyHostnameInCertificate);
        }
        if self.ca_bundle.as_deref().is_some_and(<[u8]>::is_empty) {
            return Err(SqlServerConfigError::EmptyCaBundle);
        }
        Ok(())
    }

    fn tiberius_config(&self) -> TiberiusConfig {
        let mut config = TiberiusConfig::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.application_name("OxideBatch");
        config.authentication(AuthMethod::sql_server(&self.username, &self.password));
        config.encryption(if self.strict_tds80 {
            EncryptionLevel::Strict
        } else {
            EncryptionLevel::Required
        });
        config.handshake_timeout(Some(self.connect_timeout));
        config.command_timeout(Some(self.command_timeout));
        if let Some(hostname) = &self.hostname_in_certificate {
            config.hostname_in_certificate(hostname);
        }
        if let Some(bundle) = &self.ca_bundle {
            config.trust_cert_ca_bundle(bundle.clone());
        }
        config
    }

    fn lock_timeout_millis(&self) -> Result<i64, RepositoryError> {
        i64::try_from(self.lock_timeout.as_millis()).map_err(|_| RepositoryError::Unavailable)
    }
}

impl fmt::Debug for SqlServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlServerConfig")
            .field("host", &"<redacted>")
            .field("port", &self.port)
            .field("database", &"<redacted>")
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .field("hostname_in_certificate", &"<redacted>")
            .field("ca_bundle", &self.ca_bundle.as_ref().map(|_| "<redacted>"))
            .field("strict_tds80", &self.strict_tds80)
            .field("pool_size", &self.pool_size)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("lock_timeout", &self.lock_timeout)
            .field("max_lifetime", &self.max_lifetime)
            .finish()
    }
}

/// Value-redacted SQL Server adapter configuration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SqlServerConfigError {
    /// Host was empty.
    EmptyHost,
    /// Port was zero.
    Port,
    /// Database name was empty.
    EmptyDatabase,
    /// SQL login name was empty.
    EmptyUsername,
    /// SQL login password was empty.
    EmptyPassword,
    /// Certificate-validation hostname was empty.
    EmptyHostnameInCertificate,
    /// Additional CA bundle was empty.
    EmptyCaBundle,
    /// Pool size was outside 1 through 1024.
    PoolSize,
    /// One acquire/connect/command/lock timeout was outside 1 ms through 10 minutes.
    Timeout,
    /// Pool connection lifetime was outside 1 second through 24 hours.
    ConnectionLifetime,
}

impl fmt::Display for SqlServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyHost => "SQL Server host is empty",
            Self::Port => "SQL Server port must be nonzero",
            Self::EmptyDatabase => "SQL Server database is empty",
            Self::EmptyUsername => "SQL Server username is empty",
            Self::EmptyPassword => "SQL Server password is empty",
            Self::EmptyHostnameInCertificate => "SQL Server certificate hostname is empty",
            Self::EmptyCaBundle => "SQL Server CA bundle is empty",
            Self::PoolSize => "SQL Server pool size must be from 1 to 1024",
            Self::Timeout => "SQL Server timeout must be from 1 ms through 10 minutes",
            Self::ConnectionLifetime => {
                "SQL Server connection lifetime must be from 1 second through 24 hours"
            }
        })
    }
}

impl Error for SqlServerConfigError {}

struct SqlServerConnection {
    client: Client<SqlServerTransport>,
    broken: bool,
}

impl fmt::Debug for SqlServerConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlServerConnection")
            .field("broken", &self.broken)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct SqlServerConnectionManager {
    config: SqlServerConfig,
}

impl fmt::Debug for SqlServerConnectionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlServerConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}

impl ManageConnection for SqlServerConnectionManager {
    type Connection = SqlServerConnection;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Box::pin(connect_driver(&self.config)).await
    }

    async fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        connection
            .client
            .simple_query("SELECT 1")
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    fn has_broken(&self, connection: &mut Self::Connection) -> bool {
        connection.broken
    }
}

async fn connect_driver(
    config: &SqlServerConfig,
) -> Result<SqlServerConnection, tiberius::error::Error> {
    let driver = config.tiberius_config();
    let tcp = tokio::time::timeout(
        config.connect_timeout,
        TcpStream::connect(driver.get_addr()),
    )
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "SQL Server TCP connection timed out",
        )
    })??;
    tcp.set_nodelay(true)?;
    let mut client = Box::pin(Client::connect(driver, tcp.compat_write())).await?;
    client
        .simple_query(
            "SET XACT_ABORT ON; \
             SET TRANSACTION ISOLATION LEVEL READ COMMITTED; \
             SET NOCOUNT OFF;",
        )
        .await?
        .into_results()
        .await?;
    Ok(SqlServerConnection {
        client,
        broken: false,
    })
}

#[derive(Clone)]
enum TdsParam {
    I32(i32),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
    OptI64(Option<i64>),
    OptText(Option<String>),
}

struct TdsQuery {
    sql: String,
    params: Vec<TdsParam>,
}

impl TdsQuery {
    fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    fn i32(mut self, value: i32) -> Self {
        self.params.push(TdsParam::I32(value));
        self
    }

    fn i64(mut self, value: i64) -> Self {
        self.params.push(TdsParam::I64(value));
        self
    }

    fn text(mut self, value: impl Into<String>) -> Self {
        self.params.push(TdsParam::Text(value.into()));
        self
    }

    fn bytes(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.params.push(TdsParam::Bytes(value.into()));
        self
    }

    fn opt_i64(mut self, value: Option<i64>) -> Self {
        self.params.push(TdsParam::OptI64(value));
        self
    }

    fn opt_text(mut self, value: Option<impl Into<String>>) -> Self {
        self.params.push(TdsParam::OptText(value.map(Into::into)));
        self
    }

    fn into_driver(self) -> Query<'static> {
        let mut query = Query::new(self.sql);
        for parameter in self.params {
            match parameter {
                TdsParam::I32(value) => query.bind(value),
                TdsParam::I64(value) => query.bind(value),
                TdsParam::Text(value) => query.bind(value),
                TdsParam::Bytes(value) => query.bind(value),
                TdsParam::OptI64(value) => query.bind(value),
                TdsParam::OptText(value) => query.bind(value),
            }
        }
        query
    }

    async fn execute_raw(
        self,
        connection: &mut SqlServerConnection,
    ) -> Result<u64, tiberius::error::Error> {
        Ok(self
            .into_driver()
            .execute(&mut connection.client)
            .await?
            .total())
    }

    async fn optional_raw(
        self,
        connection: &mut SqlServerConnection,
    ) -> Result<Option<TdsRow>, tiberius::error::Error> {
        self.into_driver()
            .query(&mut connection.client)
            .await?
            .into_row()
            .await
    }

    async fn all_raw(
        self,
        connection: &mut SqlServerConnection,
    ) -> Result<Vec<TdsRow>, tiberius::error::Error> {
        self.into_driver()
            .query(&mut connection.client)
            .await?
            .into_first_result()
            .await
    }

    async fn one(self, connection: &mut SqlServerConnection) -> Result<TdsRow, RepositoryError> {
        self.optional_raw(connection)
            .await
            .map_err(|error| map_driver_error(connection, &error))?
            .ok_or(RepositoryError::Unavailable)
    }

    async fn optional(
        self,
        connection: &mut SqlServerConnection,
    ) -> Result<Option<TdsRow>, RepositoryError> {
        self.optional_raw(connection)
            .await
            .map_err(|error| map_driver_error(connection, &error))
    }

    async fn all(
        self,
        connection: &mut SqlServerConnection,
    ) -> Result<Vec<TdsRow>, RepositoryError> {
        self.all_raw(connection)
            .await
            .map_err(|error| map_driver_error(connection, &error))
    }

    async fn execute(self, connection: &mut SqlServerConnection) -> Result<u64, RepositoryError> {
        self.execute_raw(connection)
            .await
            .map_err(|error| map_driver_error(connection, &error))
    }
}

fn driver_code(error: &tiberius::error::Error) -> Option<u32> {
    error.code()
}

fn is_duplicate_key(error: &tiberius::error::Error) -> bool {
    matches!(driver_code(error), Some(2601 | 2627))
}

fn is_contention(error: &tiberius::error::Error) -> bool {
    matches!(driver_code(error), Some(1205 | 1222))
}

fn map_driver_error(
    connection: &mut SqlServerConnection,
    error: &tiberius::error::Error,
) -> RepositoryError {
    if is_duplicate_key(error) || is_contention(error) {
        // XACT_ABORT ON makes constraint/locking failures transaction-terminal
        // for this adapter. Discard the physical session instead of allowing a
        // caller to continue or return it to the pool.
        connection.broken = true;
        RepositoryError::ConcurrentModification
    } else {
        if driver_code(error).is_none() {
            connection.broken = true;
        }
        RepositoryError::Unavailable
    }
}

fn map_pool_error<E>(_error: E) -> RepositoryError {
    RepositoryError::Unavailable
}

async fn execute_simple_query_raw(
    connection: &mut SqlServerConnection,
    sql: &str,
) -> Result<(), tiberius::error::Error> {
    let stream = connection.client.simple_query(sql).await?;
    stream.into_results().await?;
    Ok(())
}

/// Applies the immutable SQL Server runnable-core metadata migrations.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqlServerMigrator;

impl SqlServerMigrator {
    /// Returns the metadata schema version understood by this adapter.
    #[must_use]
    pub const fn supported_schema_version() -> u32 {
        SUPPORTED_SCHEMA_VERSION
    }

    /// Reads the installed SQL Server metadata schema version without mutating it.
    ///
    /// # Errors
    /// Returns a value-redacted repository failure when the database cannot be inspected.
    pub async fn installed_schema_version(
        config: &SqlServerConfig,
    ) -> Result<Option<u32>, RepositoryError> {
        let mut connection = Box::pin(connect_driver(config))
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        installed_schema_version(&mut connection).await
    }

    /// Applies every pending immutable migration under a SQL Server application lock.
    ///
    /// # Errors
    /// Returns a value-redacted repository failure, including for a newer unsupported schema.
    pub async fn migrate(config: &SqlServerConfig) -> Result<(), RepositoryError> {
        let mut connection = Box::pin(connect_driver(config))
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

        execute_simple_query_raw(
            &mut connection,
            "DECLARE @result int; \
             EXEC @result = sys.sp_getapplock \
               @Resource = N'OxideBatchSchemaMigration', \
               @LockMode = 'Exclusive', @LockOwner = 'Session', \
               @LockTimeout = 15000; \
             IF @result < 0 THROW 51000, 'migration lock unavailable', 1;",
        )
        .await
        .map_err(|error| map_driver_error(&mut connection, &error))?;

        execute_simple_query_raw(
            &mut connection,
            "IF OBJECT_ID(N'dbo.ob_schema_version', N'U') IS NULL \
             BEGIN \
               CREATE TABLE dbo.ob_schema_version ( \
                 version INT NOT NULL PRIMARY KEY, \
                 applied_ms BIGINT NOT NULL \
               ); \
             END;",
        )
        .await
        .map_err(|error| map_driver_error(&mut connection, &error))?;

        let current = installed_schema_version(&mut connection)
            .await?
            .unwrap_or(0);
        if current > SUPPORTED_SCHEMA_VERSION {
            return Err(RepositoryError::NewerSchema {
                current,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }

        for (version, sql) in [(1_u32, MIGRATION_1), (2_u32, MIGRATION_2)] {
            if version <= current {
                continue;
            }
            let applied_ms = system_time_millis(SystemTime::now())?;

            execute_simple_query_raw(&mut connection, "BEGIN TRAN")
                .await
                .map_err(|error| map_driver_error(&mut connection, &error))?;

            let migration_result = async {
                execute_simple_query_raw(&mut connection, sql).await?;
                let version = i32::try_from(version).map_err(|_| {
                    tiberius::error::Error::Conversion(
                        "migration version is outside SQL Server INT range".into(),
                    )
                })?;
                TdsQuery::new(
                    "INSERT INTO dbo.ob_schema_version(version, applied_ms) VALUES (@P1, @P2)",
                )
                .i32(version)
                .i64(applied_ms)
                .execute_raw(&mut connection)
                .await?;
                execute_simple_query_raw(&mut connection, "COMMIT").await?;
                Ok::<(), tiberius::error::Error>(())
            }
            .await;

            if let Err(error) = migration_result {
                let _ = execute_simple_query_raw(&mut connection, "IF XACT_STATE() <> 0 ROLLBACK")
                    .await;
                connection.broken = true;
                return Err(map_driver_error(&mut connection, &error));
            }
        }

        let installed = installed_schema_version(&mut connection)
            .await?
            .ok_or(RepositoryError::SchemaUninitialized)?;
        verify_schema_version(installed)
    }
}

async fn installed_schema_version(
    connection: &mut SqlServerConnection,
) -> Result<Option<u32>, RepositoryError> {
    let object = TdsQuery::new("SELECT OBJECT_ID(N'dbo.ob_schema_version', N'U') AS object_id")
        .one(connection)
        .await?;
    if read_optional_i32(&object, "object_id")?.is_none() {
        return Ok(None);
    }
    let row = TdsQuery::new("SELECT MAX(version) AS version FROM dbo.ob_schema_version")
        .one(connection)
        .await?;
    let Some(version) = read_optional_i32(&row, "version")? else {
        return Ok(None);
    };
    u32::try_from(version)
        .map(Some)
        .map_err(|_| RepositoryError::Unavailable)
}

fn verify_schema_version(current: u32) -> Result<(), RepositoryError> {
    match current.cmp(&SUPPORTED_SCHEMA_VERSION) {
        std::cmp::Ordering::Less => Err(RepositoryError::MigrationRequired {
            current,
            supported: SUPPORTED_SCHEMA_VERSION,
        }),
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(RepositoryError::NewerSchema {
            current,
            supported: SUPPORTED_SCHEMA_VERSION,
        }),
    }
}

/// First-party SQL Server implementation of the mandatory runnable-core repository contract.
#[derive(Clone)]
pub struct SqlServerJobRepository {
    pool: Pool<SqlServerConnectionManager>,
    clock: Arc<dyn Clock>,
    config: SqlServerConfig,
}

impl SqlServerJobRepository {
    /// Builds the bounded private pool and verifies the installed schema version.
    ///
    /// # Errors
    /// Returns a value-redacted repository failure for invalid configuration, connectivity, or schema state.
    pub async fn connect(
        config: SqlServerConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        config
            .validate()
            .map_err(|_| RepositoryError::Unavailable)?;
        let manager = SqlServerConnectionManager {
            config: config.clone(),
        };
        let pool = Pool::builder()
            .max_size(config.pool_size)
            .connection_timeout(config.acquire_timeout)
            .max_lifetime(Some(config.max_lifetime))
            .retry_connection(false)
            .test_on_check_out(true)
            .build(manager)
            .await
            .map_err(map_pool_error)?;

        {
            let mut connection = pool.get().await.map_err(map_pool_error)?;
            let current = installed_schema_version(&mut connection)
                .await?
                .ok_or(RepositoryError::SchemaUninitialized)?;
            verify_schema_version(current)?;
        }

        Ok(Self {
            pool,
            clock,
            config,
        })
    }

    async fn begin_connection(
        &self,
    ) -> Result<PooledConnection<'_, SqlServerConnectionManager>, RepositoryError> {
        let mut connection = self.pool.get().await.map_err(map_pool_error)?;
        let lock_timeout = self.config.lock_timeout_millis()?;
        let sql = format!("SET XACT_ABORT ON; SET LOCK_TIMEOUT {lock_timeout}; BEGIN TRAN;");
        if let Err(error) = execute_simple_query_raw(&mut connection, &sql).await {
            connection.broken = true;
            return Err(map_driver_error(&mut connection, &error));
        }
        Ok(connection)
    }
}

impl fmt::Debug for SqlServerJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.pool.state();
        formatter
            .debug_struct("SqlServerJobRepository")
            .field("pool_connections", &state.connections)
            .field("pool_idle", &state.idle_connections)
            .field("pool_capacity", &self.config.pool_size)
            .finish_non_exhaustive()
    }
}

impl JobRepository for SqlServerJobRepository {
    fn connection_capacity(&self) -> u32 {
        self.config.pool_size
    }

    fn descriptor(&self) -> RepositoryDescriptor {
        RepositoryDescriptor::new(SUPPORTED_SCHEMA_VERSION, [])
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let connection = self.begin_connection().await?;
            Ok(Box::new(SqlServerUnitOfWork {
                repository: self,
                connection: Some(connection),
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

struct SqlServerUnitOfWork<'repository> {
    repository: &'repository SqlServerJobRepository,
    connection: Option<PooledConnection<'repository, SqlServerConnectionManager>>,
}

impl<'repository> SqlServerUnitOfWork<'repository> {
    fn connection<'unit>(
        &'unit mut self,
    ) -> Result<&'unit mut PooledConnection<'repository, SqlServerConnectionManager>, RepositoryError>
    {
        self.connection.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn ensure_core_definition(
        &mut self,
        job_name: &JobName,
        definition: &DefinitionIdentity,
    ) -> Result<i64, RepositoryError> {
        let existing = {
            let connection = self.connection()?;
            TdsQuery::new(
                "SELECT id, manifest_digest                  FROM dbo.ob_core_job_definition WITH (UPDLOCK, HOLDLOCK)                  WHERE job_name = @P1 AND definition_revision = @P2",
            )
            .text(job_name.as_str())
            .text(definition.revision().as_str())
            .optional(connection)
            .await?
        };
        if let Some(row) = existing {
            let digest = read_bytes(&row, "manifest_digest")?;
            if digest.as_slice() != definition.manifest_digest() {
                return Err(RepositoryError::DefinitionDrift {
                    job_name: job_name.clone(),
                    revision: definition.revision().clone(),
                });
            }
            return read_i64(&row, "id");
        }

        let manifest = std::str::from_utf8(definition.canonical_manifest())
            .map_err(|_| RepositoryError::Unavailable)?;
        let created_ms = system_time_millis(self.repository.clock.now())?;
        let inserted = {
            let connection = self.connection()?;
            TdsQuery::new(
                "INSERT INTO dbo.ob_core_job_definition                  (job_name, definition_revision, manifest_format, manifest_digest, manifest, created_ms)                  OUTPUT INSERTED.id                  VALUES (@P1, @P2, @P3, @P4, @P5, @P6)",
            )
            .text(job_name.as_str())
            .text(definition.revision().as_str())
            .i32(i32::from(definition.manifest_format()))
            .bytes(definition.manifest_digest().to_vec())
            .text(manifest)
            .i64(created_ms)
            .optional_raw(connection)
            .await
        };
        match inserted {
            Ok(Some(row)) => read_i64(&row, "id"),
            Ok(None) => Err(RepositoryError::Unavailable),
            Err(error) => {
                let connection = self.connection()?;
                Err(map_driver_error(connection, &error))
            }
        }
    }

    async fn job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<JobExecution>, RepositoryError> {
        let id = db_identifier(id.get(), IdentifierKind::JobExecution)?;
        let row = {
            let connection = self.connection()?;
            TdsQuery::new(job_execution_select("WHERE execution.id = @P1"))
                .i64(id)
                .optional(connection)
                .await?
        };
        row.map(|row| decode_job_execution(&row)).transpose()
    }

    async fn step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> Result<Option<StepExecution>, RepositoryError> {
        let id = db_identifier(id.get(), IdentifierKind::StepExecution)?;
        let row = {
            let connection = self.connection()?;
            TdsQuery::new(step_execution_select("WHERE execution.id = @P1"))
                .i64(id)
                .optional(connection)
                .await?
        };
        row.map(|row| decode_step_execution(&row)).transpose()
    }

    async fn classify_job_cas(
        &mut self,
        id: JobExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        let id_value = match db_identifier(id.get(), IdentifierKind::JobExecution) {
            Ok(value) => value,
            Err(error) => return error,
        };
        let row = {
            let connection = match self.connection() {
                Ok(value) => value,
                Err(error) => return error,
            };
            TdsQuery::new(job_execution_select(
                "WITH (UPDLOCK, HOLDLOCK) WHERE execution.id = @P1",
            ))
            .i64(id_value)
            .optional(connection)
            .await
        };
        match row {
            Ok(Some(row)) => match decode_job_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::JobExecutionNotFound { id },
            Err(error) => error,
        }
    }

    async fn classify_step_cas(
        &mut self,
        id: StepExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        let id_value = match db_identifier(id.get(), IdentifierKind::StepExecution) {
            Ok(value) => value,
            Err(error) => return error,
        };
        let row = {
            let connection = match self.connection() {
                Ok(value) => value,
                Err(error) => return error,
            };
            TdsQuery::new(step_execution_select(
                "WITH (UPDLOCK, HOLDLOCK) WHERE execution.id = @P1",
            ))
            .i64(id_value)
            .optional(connection)
            .await
        };
        match row {
            Ok(Some(row)) => match decode_step_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::StepExecutionNotFound { id },
            Err(error) => error,
        }
    }
}

impl RepositoryUnitOfWork for SqlServerUnitOfWork<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        _job_name: &'a JobName,
        _upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::DefinitionRegistry,
            })
        })
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        Box::pin(async move {
            let existing = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id, job_name, identifying_parameters                      FROM dbo.ob_job_instance WITH (UPDLOCK, HOLDLOCK)                      WHERE job_name = @P1 AND instance_key = @P2",
                )
                .text(key.job_name().as_str())
                .bytes(key.digest().to_vec())
                .optional(connection)
                .await?
            };
            if let Some(row) = existing {
                return Ok(JobInstanceSelection::Existing(decode_job_instance(&row)?));
            }

            let parameters = serde_json::to_string(&encode_identifying_parameters(key)?)
                .map_err(|_| RepositoryError::Unavailable)?;
            let created_ms = system_time_millis(self.repository.clock.now())?;
            let inserted = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "INSERT INTO dbo.ob_job_instance                      (job_name, instance_key, identifying_parameters, created_ms)                      OUTPUT INSERTED.id                      VALUES (@P1, @P2, @P3, @P4)",
                )
                .text(key.job_name().as_str())
                .bytes(key.digest().to_vec())
                .text(parameters)
                .i64(created_ms)
                .optional_raw(connection)
                .await
            };
            match inserted {
                Ok(Some(row)) => {
                    let id = JobInstanceId::new(read_u64(&row, "id")?)?;
                    Ok(JobInstanceSelection::Created(JobInstance::new(
                        id,
                        key.clone(),
                    )))
                }
                Ok(None) => Err(RepositoryError::Unavailable),
                Err(error) => {
                    let connection = self.connection()?;
                    Err(map_driver_error(connection, &error))
                }
            }
        })
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let instance_id = db_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id FROM dbo.ob_job_instance WITH (UPDLOCK, HOLDLOCK) WHERE id = @P1",
                )
                .i64(instance_id)
                .optional(connection)
                .await?
            };
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }

            let latest = {
                let connection = self.connection()?;
                TdsQuery::new(job_execution_select(
                    "WHERE execution.job_instance_id = @P1 \
                     ORDER BY execution.attempt DESC OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY",
                ))
                .i64(instance_id)
                .optional(connection)
                .await?
            };
            let mut attempt = 1_u64;
            if let Some(row) = latest {
                let latest_execution = decode_job_execution(&row)?;
                attempt = read_u64(&row, "attempt")?
                    .checked_add(1)
                    .ok_or(RepositoryError::Unavailable)?;
                match latest_execution.metadata().status() {
                    BatchStatus::Stopped | BatchStatus::Failed => {}
                    BatchStatus::Completed => {
                        return Err(RepositoryError::CompletedInstance {
                            id: job_instance_id,
                        });
                    }
                    BatchStatus::Abandoned => {
                        return Err(RepositoryError::AbandonedInstance {
                            id: job_instance_id,
                        });
                    }
                    status => {
                        return Err(RepositoryError::ExecutionAlreadyActive {
                            instance_id: job_instance_id,
                            execution_id: latest_execution.id(),
                            status,
                        });
                    }
                }
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let inserted = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "INSERT INTO dbo.ob_job_execution \
                     (job_instance_id, attempt, status, exit_code, created_ms, updated_ms, version) \
                     OUTPUT INSERTED.id \
                     VALUES (@P1, @P2, N'STARTING', N'UNKNOWN', @P3, @P4, 0)",
                )
                .i64(instance_id)
                .i64(db_u64(attempt)?)
                .i64(created_ms)
                .i64(created_ms)
                .optional_raw(connection)
                .await
            };
            let row = match inserted {
                Ok(Some(row)) => row,
                Ok(None) => return Err(RepositoryError::Unavailable),
                Err(error) => {
                    let connection = self.connection()?;
                    return Err(map_driver_error(connection, &error));
                }
            };
            let id = JobExecutionId::new(read_u64(&row, "id")?)?;
            Ok(JobExecution::new(
                id,
                job_instance_id,
                starting_metadata(created_at)?,
            ))
        })
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let instance_id = db_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let instance = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id, job_name FROM dbo.ob_job_instance WITH (UPDLOCK, HOLDLOCK) \
                     WHERE id = @P1",
                )
                .i64(instance_id)
                .optional(connection)
                .await?
                .ok_or(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                })?
            };
            let job_name = JobName::new(read_string(&instance, "job_name")?)?;
            if let Some(actual) = definition.job_name()
                && actual != &job_name
            {
                return Err(RepositoryError::DefinitionJobMismatch {
                    expected: job_name,
                    actual: actual.clone(),
                });
            }
            let definition_id = self.ensure_core_definition(&job_name, definition).await?;

            let latest = {
                let connection = self.connection()?;
                TdsQuery::new(job_execution_select(
                    "WHERE execution.job_instance_id = @P1 \
                     ORDER BY execution.attempt DESC OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY",
                ))
                .i64(instance_id)
                .optional(connection)
                .await?
            };

            let mut attempt = 1_u64;
            if let Some(row) = latest {
                let latest_execution = decode_job_execution(&row)?;
                attempt = read_u64(&row, "attempt")?
                    .checked_add(1)
                    .ok_or(RepositoryError::Unavailable)?;
                match latest_execution.metadata().status() {
                    BatchStatus::Stopped | BatchStatus::Failed => {}
                    BatchStatus::Completed => {
                        return Err(RepositoryError::CompletedInstance {
                            id: job_instance_id,
                        });
                    }
                    BatchStatus::Abandoned => {
                        return Err(RepositoryError::AbandonedInstance {
                            id: job_instance_id,
                        });
                    }
                    status => {
                        return Err(RepositoryError::ExecutionAlreadyActive {
                            instance_id: job_instance_id,
                            execution_id: latest_execution.id(),
                            status,
                        });
                    }
                }

                let previous = {
                    let connection = self.connection()?;
                    TdsQuery::new(
                        "SELECT definition.manifest_digest \
                         FROM dbo.ob_job_execution execution \
                         LEFT JOIN dbo.ob_core_job_definition definition \
                           ON definition.id = execution.definition_id \
                         WHERE execution.id = @P1",
                    )
                    .i64(db_identifier(
                        latest_execution.id().get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .one(connection)
                    .await?
                };
                let previous_digest = read_optional_bytes(&previous, "manifest_digest")?.ok_or(
                    RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    },
                )?;
                if previous_digest.as_slice() != definition.manifest_digest() {
                    return Err(RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    });
                }
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let query = TdsQuery::new(
                "INSERT INTO dbo.ob_job_execution \
                 (job_instance_id, definition_id, attempt, status, exit_code, \
                  created_ms, updated_ms, version) \
                 OUTPUT INSERTED.id \
                 VALUES (@P1, @P2, @P3, N'STARTING', N'UNKNOWN', @P4, @P5, 0)",
            )
            .i64(instance_id)
            .i64(definition_id)
            .i64(db_u64(attempt)?)
            .i64(created_ms)
            .i64(created_ms);
            let inserted = {
                let connection = self.connection()?;
                query.optional_raw(connection).await
            };
            let row = match inserted {
                Ok(Some(row)) => row,
                Ok(None) => return Err(RepositoryError::Unavailable),
                Err(error) => {
                    let connection = self.connection()?;
                    return Err(map_driver_error(connection, &error));
                }
            };
            let id = JobExecutionId::new(read_u64(&row, "id")?)?;
            Ok(JobExecution::new(
                id,
                job_instance_id,
                starting_metadata(created_at)?,
            ))
        })
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let node_id =
                NodeId::new(step_name.as_str()).map_err(|_| RepositoryError::FlowStateCorrupt)?;
            self.create_flow_step_execution(
                job_execution_id,
                step_name,
                &node_id,
                StartLimit::UNRESTRICTED,
            )
            .await
        })
    }

    fn create_flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
        node_id: &'a NodeId,
        start_limit: StartLimit,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let job_id = db_identifier(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let job = {
                let connection = self.connection()?;
                TdsQuery::new("SELECT job_instance_id FROM dbo.ob_job_execution WHERE id = @P1")
                    .i64(job_id)
                    .optional(connection)
                    .await?
                    .ok_or(RepositoryError::JobExecutionNotFound {
                        id: job_execution_id,
                    })?
            };
            let instance_id = read_i64(&job, "job_instance_id")?;

            {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id FROM dbo.ob_job_instance WITH (UPDLOCK, HOLDLOCK) WHERE id = @P1",
                )
                .i64(instance_id)
                .one(connection)
                .await?;
            }

            let starts = {
                let connection = self.connection()?;
                let row = TdsQuery::new(
                    "SELECT COUNT_BIG(*) AS starts \
                     FROM dbo.ob_step_execution step \
                     JOIN dbo.ob_job_execution job ON job.id = step.job_execution_id \
                     WHERE job.job_instance_id = @P1 AND step.step_logical_id = @P2",
                )
                .i64(instance_id)
                .text(node_id.as_str())
                .one(connection)
                .await?;
                read_u64(&row, "starts")?
            };
            if starts >= u64::from(start_limit.get()) {
                return Err(RepositoryError::StartLimitExceeded {
                    instance_id: JobInstanceId::new(db_read_u64(instance_id)?)?,
                    node_id: node_id.clone(),
                    limit: start_limit,
                });
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let query = TdsQuery::new(
                "INSERT INTO dbo.ob_step_execution \
                 (job_execution_id, step_name, step_logical_id, status, exit_code, \
                  created_ms, updated_ms, version) \
                 OUTPUT INSERTED.id \
                 VALUES (@P1, @P2, @P3, N'STARTING', N'UNKNOWN', @P4, @P5, 0)",
            )
            .i64(job_id)
            .text(step_name.as_str())
            .text(node_id.as_str())
            .i64(created_ms)
            .i64(created_ms);
            let inserted = {
                let connection = self.connection()?;
                query.optional_raw(connection).await
            };
            let row = match inserted {
                Ok(Some(row)) => row,
                Ok(None) => return Err(RepositoryError::Unavailable),
                Err(error) => {
                    let connection = self.connection()?;
                    return Err(map_driver_error(connection, &error));
                }
            };
            let id = StepExecutionId::new(read_u64(&row, "id")?)?;
            Ok(StepExecution::new(
                id,
                job_execution_id,
                step_name.clone(),
                starting_metadata(created_at)?,
            ))
        })
    }

    fn transition_job_execution(
        &mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            let affected = {
                let connection = self.connection()?;
                update_job_execution(
                    connection,
                    &execution,
                    transition.transitioned_at(),
                    expected_version,
                )
                .await?
            };
            if affected != 1 {
                return Err(self.classify_job_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let updated_at = self.repository.clock.now();
            let affected = {
                let connection = self.connection()?;
                update_job_execution(connection, &execution, updated_at, expected_version).await?
            };
            if affected != 1 {
                return Err(self.classify_job_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn transition_step_execution(
        &mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            let affected = {
                let connection = self.connection()?;
                update_step_execution(
                    connection,
                    &execution,
                    transition.transitioned_at(),
                    expected_version,
                )
                .await?
            };
            if affected != 1 {
                return Err(self.classify_step_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let updated_at = self.repository.clock.now();
            let affected = {
                let connection = self.connection()?;
                update_step_execution(connection, &execution, updated_at, expected_version).await?
            };
            if affected != 1 {
                return Err(self.classify_step_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id, job_name, identifying_parameters \
                     FROM dbo.ob_job_instance WHERE job_name = @P1 AND instance_key = @P2",
                )
                .text(key.job_name().as_str())
                .bytes(key.digest().to_vec())
                .optional(connection)
                .await?
            };
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let id = db_identifier(id.get(), IdentifierKind::JobInstance)?;
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT id, job_name, identifying_parameters FROM dbo.ob_job_instance WHERE id = @P1",
                )
                .i64(id)
                .optional(connection)
                .await?
            };
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move { self.job_execution(id).await })
    }

    fn latest_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            let instance_id = db_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists = {
                let connection = self.connection()?;
                TdsQuery::new("SELECT id FROM dbo.ob_job_instance WHERE id = @P1")
                    .i64(instance_id)
                    .optional(connection)
                    .await?
            };
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(job_execution_select(
                    "WHERE execution.job_instance_id = @P1 ORDER BY execution.attempt DESC OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY",
                ))
                .i64(instance_id)
                .optional(connection)
                .await?
            };
            row.map(|row| decode_job_execution(&row)).transpose()
        })
    }

    fn job_execution_attempt(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<std::num::NonZeroU64, RepositoryError>> {
        Box::pin(async move {
            let row = {
                let connection = self.connection()?;
                TdsQuery::new("SELECT attempt FROM dbo.ob_job_execution WHERE id = @P1")
                    .i64(db_identifier(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .optional(connection)
                    .await?
            }
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: job_execution_id,
            })?;
            std::num::NonZeroU64::new(read_u64(&row, "attempt")?)
                .ok_or(RepositoryError::Unavailable)
        })
    }


    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move { self.step_execution(id).await })
    }

    fn flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            if self.job_execution(job_execution_id).await?.is_none() {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(step_execution_select(
                    "WHERE execution.job_execution_id = @P1 AND execution.step_logical_id = @P2",
                ))
                .i64(db_identifier(
                    job_execution_id.get(),
                    IdentifierKind::JobExecution,
                )?)
                .text(node_id.as_str())
                .optional(connection)
                .await?
            };
            row.map(|row| decode_step_execution(&row)).transpose()
        })
    }


    fn latest_flow_step<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<FlowStepState>, RepositoryError>> {
        Box::pin(async move {
            let instance_id = db_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists = {
                let connection = self.connection()?;
                TdsQuery::new("SELECT id FROM dbo.ob_job_instance WHERE id = @P1")
                    .i64(instance_id)
                    .optional(connection)
                    .await?
            };
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(step_execution_select(
                    "JOIN dbo.ob_job_execution job ON job.id = execution.job_execution_id \
                     WHERE job.job_instance_id = @P1 AND execution.step_logical_id = @P2 \
                     ORDER BY job.attempt DESC, execution.id DESC \
                     OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY",
                ))
                .i64(instance_id)
                .text(node_id.as_str())
                .optional(connection)
                .await?
            };
            row.map(|row| {
                Ok(FlowStepState::new(
                    node_id.clone(),
                    decode_step_execution(&row)?,
                    None,
                ))
            })
            .transpose()
        })
    }

    fn append_flow_decision<'a>(
        &'a mut self,
        request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async move {
            let execution_id = db_identifier(
                request.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )?;
            let definition = {
                let connection = self.connection()?;
                TdsQuery::new(
                    "SELECT definition.manifest_digest, definition.manifest \
                     FROM dbo.ob_job_execution execution \
                     JOIN dbo.ob_core_job_definition definition ON definition.id = execution.definition_id \
                     WHERE execution.id = @P1",
                )
                .i64(execution_id)
                .optional(connection)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound {
                    id: request.job_execution_id(),
                })?
            };
            let fingerprint = read_bytes(&definition, "manifest_digest")?;
            if fingerprint.as_slice() != request.plan_fingerprint() {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            let manifest_text = read_string(&definition, "manifest")?;
            let manifest: Value = serde_json::from_str(&manifest_text)
                .map_err(|_| RepositoryError::FlowStateCorrupt)?;
            if !crate::flow::decision_matches_manifest(&manifest, request) {
                return Err(RepositoryError::FlowStateCorrupt);
            }

            let existing = {
                let connection = self.connection()?;
                let row = TdsQuery::new(
                    "SELECT COUNT_BIG(*) AS count FROM dbo.ob_flow_decision WHERE job_execution_id = @P1",
                )
                .i64(execution_id)
                .one(connection)
                .await?;
                read_u64(&row, "count")?
            };
            let advanced = manifest
                .get("format")
                .and_then(Value::as_u64)
                .is_some_and(|format| {
                    format == u64::from(oxide_batch_core::MANIFEST_FORMAT_ADVANCED_FLOW)
                });
            if !advanced
                && request.sequence().get()
                    != existing
                        .checked_add(1)
                        .ok_or(RepositoryError::FlowStateCorrupt)?
            {
                return Err(RepositoryError::ConcurrentModification);
            }

            if let Some(step_id) = request.source_step_execution_id() {
                let connection = self.connection()?;
                let row = TdsQuery::new(
                    "SELECT COUNT_BIG(*) AS count \
                     FROM dbo.ob_step_execution source \
                     JOIN dbo.ob_job_execution source_job ON source_job.id = source.job_execution_id \
                     JOIN dbo.ob_job_execution target_job ON target_job.id = @P1 \
                     WHERE source.id = @P2 \
                       AND source_job.job_instance_id = target_job.job_instance_id \
                       AND source.step_logical_id = @P3",
                )
                .i64(execution_id)
                .i64(db_identifier(step_id.get(), IdentifierKind::StepExecution)?)
                .text(request.source_node_id().as_str())
                .one(connection)
                .await?;
                if read_i64(&row, "count")? != 1 {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            } else if !matches!(
                request.kind(),
                FlowTransitionKind::Decider
                    | FlowTransitionKind::SplitAggregate
                    | FlowTransitionKind::NestedJobExit
            ) {
                return Err(RepositoryError::FlowStateCorrupt);
            }

            if let Some(reused_id) = request.reused_decision_id() {
                let target_node = flow_target_node(request.target()).map(str::to_owned);
                let terminal = flow_terminal_code(request.target())?.map(str::to_owned);
                let connection = self.connection()?;
                let row = TdsQuery::new(
                    "SELECT COUNT_BIG(*) AS count \
                     FROM dbo.ob_flow_decision prior \
                     JOIN dbo.ob_job_execution prior_job ON prior_job.id = prior.job_execution_id \
                     JOIN dbo.ob_job_execution target_job ON target_job.id = @P1 \
                     WHERE prior.id = @P2 \
                       AND prior_job.job_instance_id = target_job.job_instance_id \
                       AND prior.source_node_id = @P3 \
                       AND prior.plan_fingerprint = @P4 \
                       AND prior.input_digest = @P5 \
                       AND prior.observed_outcome = @P6 \
                       AND ((prior.target_node_id = @P7) OR (prior.target_node_id IS NULL AND @P7 IS NULL)) \
                       AND ((prior.terminal_kind = @P8) OR (prior.terminal_kind IS NULL AND @P8 IS NULL))",
                )
                .i64(execution_id)
                .i64(db_identifier(reused_id.get(), IdentifierKind::FlowDecision)?)
                .text(request.source_node_id().as_str())
                .bytes(request.plan_fingerprint().to_vec())
                .bytes(request.input_digest().to_vec())
                .text(request.observed_outcome().as_str())
                .opt_text(target_node)
                .opt_text(terminal)
                .one(connection)
                .await?;
                if read_i64(&row, "count")? != 1 {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            }

            let query = TdsQuery::new(
                "INSERT INTO dbo.ob_flow_decision \
                 (job_execution_id, source_step_execution_id, reused_decision_id, sequence, \
                  source_node_id, observed_outcome, target_node_id, transition_kind, \
                  terminal_kind, plan_fingerprint, input_digest, decided_ms) \
                 OUTPUT INSERTED.id \
                 VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12)",
            )
            .i64(execution_id)
            .opt_i64(
                request
                    .source_step_execution_id()
                    .map(|id| db_identifier(id.get(), IdentifierKind::StepExecution))
                    .transpose()?,
            )
            .opt_i64(
                request
                    .reused_decision_id()
                    .map(|id| db_identifier(id.get(), IdentifierKind::FlowDecision))
                    .transpose()?,
            )
            .i64(db_u64(request.sequence().get())?)
            .text(request.source_node_id().as_str())
            .text(request.observed_outcome().as_str())
            .opt_text(flow_target_node(request.target()))
            .text(request.kind().durable_code())
            .opt_text(flow_terminal_code(request.target())?)
            .bytes(request.plan_fingerprint().to_vec())
            .bytes(request.input_digest().to_vec())
            .i64(system_time_millis(request.decided_at())?);

            let inserted = {
                let connection = self.connection()?;
                query.optional_raw(connection).await
            };
            let row = match inserted {
                Ok(Some(row)) => row,
                Ok(None) => return Err(RepositoryError::Unavailable),
                Err(error) => {
                    let connection = self.connection()?;
                    return Err(map_driver_error(connection, &error));
                }
            };
            let id = FlowDecisionId::new(read_u64(&row, "id")?)?;
            Ok(request.materialize(id))
        })
    }

    fn find_reusable_flow_decision<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
        plan_fingerprint: &'a [u8; 32],
        input_digest: &'a [u8; 32],
        kind: FlowTransitionKind,
    ) -> BoxFuture<'a, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let instance_id = db_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists = {
                let connection = self.connection()?;
                TdsQuery::new("SELECT id FROM dbo.ob_job_instance WHERE id = @P1")
                    .i64(instance_id)
                    .optional(connection)
                    .await?
            };
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(flow_decision_select(
                    "JOIN dbo.ob_job_execution flow_job ON flow_job.id = decision.job_execution_id \
                     WHERE flow_job.job_instance_id = @P1 AND decision.source_node_id = @P2 \
                       AND decision.plan_fingerprint = @P3 AND decision.input_digest = @P4 \
                       AND decision.transition_kind = @P5 \
                     ORDER BY flow_job.attempt DESC, decision.sequence DESC \
                     OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY",
                ))
                .i64(instance_id)
                .text(node_id.as_str())
                .bytes(plan_fingerprint.to_vec())
                .bytes(input_digest.to_vec())
                .text(kind.durable_code())
                .optional(connection)
                .await?
            };
            row.map(|row| decode_flow_decision(&row)).transpose()
        })
    }

    fn get_flow_decision(
        &mut self,
        id: FlowDecisionId,
    ) -> BoxFuture<'_, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let row = {
                let connection = self.connection()?;
                TdsQuery::new(flow_decision_select("WHERE decision.id = @P1"))
                    .i64(db_identifier(id.get(), IdentifierKind::FlowDecision)?)
                    .optional(connection)
                    .await?
            };
            row.map(|row| decode_flow_decision(&row)).transpose()
        })
    }


    fn recover_job_execution<'a>(
        &'a mut self,
        _id: JobExecutionId,
        _request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::ExecutionOwnership,
            })
        })
    }

    fn recovery_decision(
        &mut self,
        _id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::ExecutionOwnership,
            })
        })
    }

    fn commit<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if execute_simple_query_raw(&mut connection, "COMMIT")
                .await
                .is_err()
            {
                connection.broken = true;
                return Err(RepositoryError::CommitOutcomeUnknown);
            }
            Ok(())
        })
    }

    fn rollback<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if execute_simple_query_raw(&mut connection, "IF XACT_STATE() <> 0 ROLLBACK")
                .await
                .is_err()
            {
                connection.broken = true;
                return Err(RepositoryError::Unavailable);
            }
            Ok(())
        })
    }
}

impl Drop for SqlServerUnitOfWork<'_> {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.broken = true;
        }
    }
}

fn flow_decision_select(suffix: &str) -> String {
    format!(
        "SELECT decision.id, decision.job_execution_id, \
         decision.source_step_execution_id, decision.reused_decision_id, \
         decision.sequence, decision.source_node_id, decision.observed_outcome, \
         decision.target_node_id, decision.transition_kind, decision.terminal_kind, \
         decision.plan_fingerprint, decision.input_digest, decision.decided_ms \
         FROM dbo.ob_flow_decision decision {suffix}"
    )
}

fn flow_target_node(target: &FlowTarget) -> Option<&str> {
    match target {
        FlowTarget::Node(node) => Some(node.as_str()),
        FlowTarget::Terminal(_) => None,
    }
}

fn flow_terminal_code(target: &FlowTarget) -> Result<Option<&'static str>, RepositoryError> {
    Ok(match target {
        FlowTarget::Node(_) => None,
        FlowTarget::Terminal(TerminalKind::Complete) => Some("COMPLETE"),
        FlowTarget::Terminal(TerminalKind::Fail) => Some("FAIL"),
        FlowTarget::Terminal(TerminalKind::Stop) => Some("STOP"),
        FlowTarget::Terminal(_) => return Err(RepositoryError::Unavailable),
    })
}

fn decode_flow_decision(row: &TdsRow) -> Result<FlowDecision, RepositoryError> {
    let id = FlowDecisionId::new(read_u64(row, "id")?)?;
    let job_execution_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let sequence = FlowDecisionSequence::new(read_u64(row, "sequence")?)
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let source_node_id = NodeId::new(read_string(row, "source_node_id")?)
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let source_step_execution_id = read_optional_i64(row, "source_step_execution_id")?
        .map(db_read_u64)
        .transpose()?
        .map(StepExecutionId::new)
        .transpose()?;
    let reused_decision_id = read_optional_i64(row, "reused_decision_id")?
        .map(db_read_u64)
        .transpose()?
        .map(FlowDecisionId::new)
        .transpose()?;
    let kind = FlowTransitionKind::from_durable_code(&read_string(row, "transition_kind")?)
        .ok_or(RepositoryError::FlowStateCorrupt)?;
    let observed_outcome = ExitCode::new(read_string(row, "observed_outcome")?)?;
    let target_node = read_optional_string(row, "target_node_id")?;
    let terminal = read_optional_string(row, "terminal_kind")?;
    let target = match (target_node, terminal.as_deref()) {
        (Some(node), None) => {
            FlowTarget::Node(NodeId::new(node).map_err(|_| RepositoryError::FlowStateCorrupt)?)
        }
        (None, Some("COMPLETE")) => FlowTarget::Terminal(TerminalKind::Complete),
        (None, Some("FAIL")) => FlowTarget::Terminal(TerminalKind::Fail),
        (None, Some("STOP")) => FlowTarget::Terminal(TerminalKind::Stop),
        _ => return Err(RepositoryError::FlowStateCorrupt),
    };
    let fingerprint: [u8; 32] = read_bytes(row, "plan_fingerprint")?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let input_digest: [u8; 32] = read_bytes(row, "input_digest")?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    Ok(FlowDecision::new(
        id,
        job_execution_id,
        sequence,
        source_node_id,
        source_step_execution_id,
        kind,
        observed_outcome,
        target,
        fingerprint,
        input_digest,
        reused_decision_id,
        millis_system_time(read_i64(row, "decided_ms")?)?,
    ))
}

fn job_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, execution.attempt, \
         execution.status, execution.exit_code, \
         CAST(0 AS BIGINT) AS read_count, CAST(0 AS BIGINT) AS processed_count, \
         CAST(0 AS BIGINT) AS write_count, CAST(0 AS BIGINT) AS filter_count, \
         CAST(0 AS BIGINT) AS commit_count, CAST(0 AS BIGINT) AS rollback_count, \
         execution.failure_category, execution.failure_id, execution.created_ms, \
         execution.started_ms, execution.ended_ms, execution.version \
         FROM dbo.ob_job_execution execution {suffix}"
    )
}

fn step_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.status, execution.exit_code, execution.read_count, \
         execution.processed_count, execution.write_count, execution.filter_count, \
         execution.commit_count, execution.rollback_count, execution.failure_category, \
         execution.failure_id, execution.created_ms, execution.started_ms, \
         execution.ended_ms, execution.version \
         FROM dbo.ob_step_execution execution {suffix}"
    )
}

async fn update_job_execution(
    connection: &mut SqlServerConnection,
    execution: &JobExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let metadata = execution.metadata();
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    TdsQuery::new(
        "UPDATE dbo.ob_job_execution SET \
         status = @P1, exit_code = @P2, failure_category = @P3, failure_id = @P4, \
         started_ms = @P5, ended_ms = @P6, updated_ms = @P7, version = @P8 \
         WHERE id = @P9 AND version = @P10",
    )
    .text(metadata.status().to_string())
    .text(metadata.exit_status().code().as_str())
    .opt_text(failure.map(|value| value.category().durable_code()))
    .opt_i64(
        failure
            .map(|value| db_u64(value.failure_id().get()))
            .transpose()?,
    )
    .opt_i64(
        timestamps
            .started_at()
            .map(system_time_millis)
            .transpose()?,
    )
    .opt_i64(timestamps.ended_at().map(system_time_millis).transpose()?)
    .i64(system_time_millis(updated_at)?)
    .i64(db_u64(execution.version().get())?)
    .i64(db_identifier(
        execution.id().get(),
        IdentifierKind::JobExecution,
    )?)
    .i64(db_u64(expected.get())?)
    .execute(connection)
    .await
}

async fn update_step_execution(
    connection: &mut SqlServerConnection,
    execution: &StepExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let metadata = execution.metadata();
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let counts = metadata.counts();
    TdsQuery::new(
        "UPDATE dbo.ob_step_execution SET \
         status = @P1, exit_code = @P2, failure_category = @P3, failure_id = @P4, \
         started_ms = @P5, ended_ms = @P6, read_count = @P7, processed_count = @P8, \
         write_count = @P9, filter_count = @P10, commit_count = @P11, rollback_count = @P12, \
         updated_ms = @P13, version = @P14 WHERE id = @P15 AND version = @P16",
    )
    .text(metadata.status().to_string())
    .text(metadata.exit_status().code().as_str())
    .opt_text(failure.map(|value| value.category().durable_code()))
    .opt_i64(
        failure
            .map(|value| db_u64(value.failure_id().get()))
            .transpose()?,
    )
    .opt_i64(
        timestamps
            .started_at()
            .map(system_time_millis)
            .transpose()?,
    )
    .opt_i64(timestamps.ended_at().map(system_time_millis).transpose()?)
    .i64(db_u64(counts.read())?)
    .i64(db_u64(counts.processed())?)
    .i64(db_u64(counts.written())?)
    .i64(db_u64(counts.filtered())?)
    .i64(db_u64(counts.committed())?)
    .i64(db_u64(counts.rolled_back())?)
    .i64(system_time_millis(updated_at)?)
    .i64(db_u64(execution.version().get())?)
    .i64(db_identifier(
        execution.id().get(),
        IdentifierKind::StepExecution,
    )?)
    .i64(db_u64(expected.get())?)
    .execute(connection)
    .await
}

fn decode_job_instance(row: &TdsRow) -> Result<JobInstance, RepositoryError> {
    let id = JobInstanceId::new(read_u64(row, "id")?)?;
    let job_name = JobName::new(read_string(row, "job_name")?)?;
    let encoded = read_string(row, "identifying_parameters")?;
    let value: Value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::Unavailable)?;
    let parameters = decode_identifying_parameters(&value)?;
    Ok(JobInstance::new(
        id,
        JobInstanceKey::new(job_name, &parameters),
    ))
}

fn decode_job_execution(row: &TdsRow) -> Result<JobExecution, RepositoryError> {
    let id = JobExecutionId::new(read_u64(row, "id")?)?;
    let instance_id = JobInstanceId::new(read_u64(row, "job_instance_id")?)?;
    Ok(JobExecution::from_snapshot(
        id,
        instance_id,
        decode_execution_metadata(row)?,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_step_execution(row: &TdsRow) -> Result<StepExecution, RepositoryError> {
    let id = StepExecutionId::new(read_u64(row, "id")?)?;
    let job_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let name = StepName::new(read_string(row, "step_name")?)?;
    Ok(StepExecution::from_snapshot(
        id,
        job_id,
        name,
        decode_execution_metadata(row)?,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_execution_metadata(row: &TdsRow) -> Result<ExecutionMetadata, RepositoryError> {
    let status = decode_status(&read_string(row, "status")?)?;
    let exit_status = ExitStatus::new(ExitCode::new(read_string(row, "exit_code")?)?);
    let created = millis_system_time(read_i64(row, "created_ms")?)?;
    let started = read_optional_i64(row, "started_ms")?
        .map(millis_system_time)
        .transpose()?;
    let ended = read_optional_i64(row, "ended_ms")?
        .map(millis_system_time)
        .transpose()?;
    let category = read_optional_string(row, "failure_category")?;
    let failure_id = read_optional_i64(row, "failure_id")?
        .map(db_read_u64)
        .transpose()?;
    let failure = match (category, failure_id) {
        (None, None) => None,
        (Some(category), Some(id)) => Some(FailureSummary::new(
            FailureCategory::from_durable_code(&category).ok_or(RepositoryError::Unavailable)?,
            FailureId::new(id)?,
        )),
        _ => return Err(RepositoryError::Unavailable),
    };
    ExecutionMetadata::new(
        status,
        exit_status,
        ExecutionTimestamps::new(created, started, ended)?,
        ExecutionCounts::new(
            read_u64(row, "read_count")?,
            read_u64(row, "processed_count")?,
            read_u64(row, "write_count")?,
            read_u64(row, "filter_count")?,
            read_u64(row, "commit_count")?,
            read_u64(row, "rollback_count")?,
        ),
        failure,
    )
    .map_err(Into::into)
}

fn encode_identifying_parameters(key: &JobInstanceKey) -> Result<Value, RepositoryError> {
    let mut object = Map::new();
    for (name, kind) in key.identifying_fields() {
        let parameter = key
            .identifying_value(name)
            .ok_or(RepositoryError::Unavailable)?;
        let (kind_name, value) = match kind {
            ParameterValueKind::String => (
                "string",
                Value::String(
                    parameter
                        .as_str()
                        .ok_or(RepositoryError::Unavailable)?
                        .to_owned(),
                ),
            ),
            ParameterValueKind::I64 => (
                "i64",
                Value::Number(
                    parameter
                        .as_i64()
                        .ok_or(RepositoryError::Unavailable)?
                        .into(),
                ),
            ),
            ParameterValueKind::U64 => (
                "u64",
                Value::Number(
                    parameter
                        .as_u64()
                        .ok_or(RepositoryError::Unavailable)?
                        .into(),
                ),
            ),
            ParameterValueKind::Bool => (
                "bool",
                Value::Bool(parameter.as_bool().ok_or(RepositoryError::Unavailable)?),
            ),
            _ => return Err(RepositoryError::Unavailable),
        };
        object.insert(
            name.as_str().to_owned(),
            json!({"type": kind_name, "identifying": true, "value": value}),
        );
    }
    let value = Value::Object(object);
    if serde_json::to_vec(&value)
        .map_err(|_| RepositoryError::Unavailable)?
        .len()
        > MAX_INSTANCE_KEY_INPUT
    {
        return Err(RepositoryError::Unavailable);
    }
    Ok(value)
}

fn decode_identifying_parameters(value: &Value) -> Result<JobParameters, RepositoryError> {
    let object = value.as_object().ok_or(RepositoryError::Unavailable)?;
    let mut parameters = JobParameters::new();
    for (raw_name, envelope) in object {
        let envelope = envelope.as_object().ok_or(RepositoryError::Unavailable)?;
        if envelope.get("identifying") != Some(&Value::Bool(true)) {
            return Err(RepositoryError::Unavailable);
        }
        let kind = envelope
            .get("type")
            .and_then(Value::as_str)
            .ok_or(RepositoryError::Unavailable)?;
        let raw_value = envelope.get("value").ok_or(RepositoryError::Unavailable)?;
        let value = match kind {
            "string" => {
                ParameterValue::string(raw_value.as_str().ok_or(RepositoryError::Unavailable)?)?
            }
            "i64" => ParameterValue::from(raw_value.as_i64().ok_or(RepositoryError::Unavailable)?),
            "u64" => ParameterValue::from(raw_value.as_u64().ok_or(RepositoryError::Unavailable)?),
            "bool" => {
                ParameterValue::from(raw_value.as_bool().ok_or(RepositoryError::Unavailable)?)
            }
            _ => return Err(RepositoryError::Unavailable),
        };
        parameters.insert(
            ParameterName::new(raw_name.clone())?,
            JobParameter::new(value, ParameterRole::Identifying),
        )?;
    }
    Ok(parameters)
}

fn starting_metadata(created_at: SystemTime) -> Result<ExecutionMetadata, RepositoryError> {
    Ok(ExecutionMetadata::new(
        BatchStatus::Starting,
        ExitStatus::unknown(),
        ExecutionTimestamps::new(created_at, None, None)?,
        ExecutionCounts::default(),
        None,
    )?)
}

fn db_identifier(value: u64, kind: IdentifierKind) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::IdentifierOutOfRange { kind, value })
}

fn db_u64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn db_read_u64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn system_time_millis(value: SystemTime) -> Result<i64, RepositoryError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn millis_system_time(value: i64) -> Result<SystemTime, RepositoryError> {
    Ok(UNIX_EPOCH
        + Duration::from_millis(u64::try_from(value).map_err(|_| RepositoryError::Unavailable)?))
}

fn read_i64(row: &TdsRow, name: &str) -> Result<i64, RepositoryError> {
    row.try_get::<i64, _>(name)
        .map_err(|_| RepositoryError::Unavailable)?
        .ok_or(RepositoryError::Unavailable)
}

fn read_optional_i64(row: &TdsRow, name: &str) -> Result<Option<i64>, RepositoryError> {
    row.try_get::<i64, _>(name)
        .map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i32(row: &TdsRow, name: &str) -> Result<Option<i32>, RepositoryError> {
    row.try_get::<i32, _>(name)
        .map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &TdsRow, name: &str) -> Result<u64, RepositoryError> {
    db_read_u64(read_i64(row, name)?)
}

fn read_string(row: &TdsRow, name: &str) -> Result<String, RepositoryError> {
    row.try_get::<&str, _>(name)
        .map_err(|_| RepositoryError::Unavailable)?
        .map(str::to_owned)
        .ok_or(RepositoryError::Unavailable)
}

fn read_optional_string(row: &TdsRow, name: &str) -> Result<Option<String>, RepositoryError> {
    row.try_get::<&str, _>(name)
        .map_err(|_| RepositoryError::Unavailable)
        .map(|value| value.map(str::to_owned))
}

fn read_bytes(row: &TdsRow, name: &str) -> Result<Vec<u8>, RepositoryError> {
    row.try_get::<&[u8], _>(name)
        .map_err(|_| RepositoryError::Unavailable)?
        .map(<[u8]>::to_vec)
        .ok_or(RepositoryError::Unavailable)
}

fn read_optional_bytes(row: &TdsRow, name: &str) -> Result<Option<Vec<u8>>, RepositoryError> {
    row.try_get::<&[u8], _>(name)
        .map_err(|_| RepositoryError::Unavailable)
        .map(|value| value.map(<[u8]>::to_vec))
}

fn decode_status(value: &str) -> Result<BatchStatus, RepositoryError> {
    match value {
        "STARTING" => Ok(BatchStatus::Starting),
        "STARTED" => Ok(BatchStatus::Started),
        "STOPPING" => Ok(BatchStatus::Stopping),
        "STOPPED" => Ok(BatchStatus::Stopped),
        "FAILED" => Ok(BatchStatus::Failed),
        "COMPLETED" => Ok(BatchStatus::Completed),
        "ABANDONED" => Ok(BatchStatus::Abandoned),
        "UNKNOWN" => Ok(BatchStatus::Unknown),
        _ => Err(RepositoryError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_debug_redacts_connection_identity_and_secret_material() -> Result<(), Box<dyn Error>>
    {
        let config = SqlServerConfig::new("db.internal", "batch", "batch_user", "secret")?
            .with_hostname_in_certificate("sql.example.test")?
            .with_ca_bundle(b"test-ca".to_vec())?;
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("db.internal"));
        assert!(!rendered.contains("batch_user"));
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("sql.example.test"));
        assert!(!rendered.contains("test-ca"));
        Ok(())
    }

    #[test]
    fn config_rejects_operation_timeout_outside_the_frozen_range() -> Result<(), Box<dyn Error>> {
        let base = SqlServerConfig::new("db.internal", "batch", "batch_user", "secret")?;

        assert!(matches!(
            base.clone().with_command_timeout(Duration::ZERO),
            Err(SqlServerConfigError::Timeout)
        ));
        assert!(matches!(
            base.clone()
                .with_command_timeout(MAX_OPERATION_TIMEOUT + Duration::from_millis(1)),
            Err(SqlServerConfigError::Timeout)
        ));
        assert!(base.with_command_timeout(MAX_OPERATION_TIMEOUT).is_ok());
        Ok(())
    }

    #[test]
    fn schema_version_guard_requires_runnable_core_schema() {
        assert!(matches!(
            verify_schema_version(1),
            Err(RepositoryError::MigrationRequired {
                current: 1,
                supported: 2
            })
        ));
        assert!(verify_schema_version(2).is_ok());
        assert!(matches!(
            verify_schema_version(3),
            Err(RepositoryError::NewerSchema {
                current: 3,
                supported: 2
            })
        ));
    }
}
