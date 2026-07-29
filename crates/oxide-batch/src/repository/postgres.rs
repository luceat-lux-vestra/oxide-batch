use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow, PgSslMode};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Postgres, Row};

use super::{
    BoxFuture, Clock, JobInstanceSelection, JobRepository, RepositoryError, RepositoryUnitOfWork,
};
use crate::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExecutionVersion,
    ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary, IdentifierKind, JobExecution,
    JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobName, JobParameter,
    JobParameters, LifecycleError, LifecycleTransition, ParameterName, ParameterRole,
    ParameterValue, ParameterValueKind, StepExecution, StepExecutionId, StepName,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 1;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_SHORT_TIMEOUT: Duration = Duration::from_mins(5);
const MAX_STATEMENT_TIMEOUT: Duration = Duration::from_hours(24);
const MAX_CONNECTION_LIFETIME: Duration = Duration::from_hours(7 * 24);
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;
const MAX_CA_CERTIFICATE_BYTES: usize = 1024 * 1024;
const DEFAULT_DEFINITION_REVISION: &str = "__m1_repository_port_v1";
const DEFAULT_CONTEXT_SCHEMA: &str = "oxide_batch.empty.v1";

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Transport security for a `PostgreSQL` repository connection.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsMode {
    /// Validate the server certificate and hostname.
    VerifyFull {
        /// An optional bounded PEM CA bundle. System roots are used when absent.
        ca_certificate: Option<CaCertificate>,
    },
    /// Use an unencrypted connection for an explicitly isolated environment.
    Plaintext,
}

/// A bounded, value-redacted PEM certificate-authority bundle.
#[derive(Clone, Eq, PartialEq)]
pub struct CaCertificate(Vec<u8>);

impl CaCertificate {
    /// Validates an in-memory PEM CA bundle.
    ///
    /// # Errors
    ///
    /// Rejects an empty bundle or one larger than 1 MiB. Certificate parsing
    /// and trust validation occur when the TLS connection is established.
    pub fn new(pem: impl Into<Vec<u8>>) -> Result<Self, PostgresConfigError> {
        let pem = pem.into();
        if pem.is_empty() {
            return Err(PostgresConfigError::EmptyCaCertificate);
        }
        if pem.len() > MAX_CA_CERTIFICATE_BYTES {
            return Err(PostgresConfigError::CaCertificateTooLarge {
                max_bytes: MAX_CA_CERTIFICATE_BYTES,
            });
        }
        Ok(Self(pem))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for CaCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CaCertificate")
            .field("byte_length", &self.0.len())
            .field("contents", &"<redacted>")
            .finish()
    }
}

impl Default for TlsMode {
    fn default() -> Self {
        Self::VerifyFull {
            ca_certificate: None,
        }
    }
}

impl fmt::Debug for TlsMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VerifyFull { ca_certificate } => formatter
                .debug_struct("VerifyFull")
                .field(
                    "ca_certificate",
                    &ca_certificate.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
            Self::Plaintext => formatter.write_str("Plaintext"),
        }
    }
}

/// Facade-owned `PostgreSQL` pool, TLS, and timeout configuration.
///
/// `Debug` deliberately omits the connection string and certificate path.
#[derive(Clone)]
pub struct PostgresConfig {
    connection_string: String,
    tls_mode: TlsMode,
    pool_size: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
    idle_transaction_timeout: Duration,
    connection_idle_timeout: Duration,
    connection_max_lifetime: Duration,
    pool_close_timeout: Duration,
}

impl PostgresConfig {
    /// Builds production-default configuration for a `PostgreSQL` connection.
    ///
    /// The connection string is treated as a secret and is never included in
    /// facade diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresConfigError`] when the connection string is empty.
    pub fn new(connection_string: impl Into<String>) -> Result<Self, PostgresConfigError> {
        let config = Self {
            connection_string: connection_string.into(),
            tls_mode: TlsMode::default(),
            pool_size: 10,
            acquire_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(5),
            idle_transaction_timeout: Duration::from_mins(1),
            connection_idle_timeout: Duration::from_mins(10),
            connection_max_lifetime: Duration::from_mins(30),
            pool_close_timeout: Duration::from_secs(30),
        };
        config.validate()?;
        Ok(config)
    }

    /// Selects certificate validation or explicit plaintext transport.
    #[must_use]
    pub fn with_tls_mode(mut self, tls_mode: TlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    /// Sets the maximum number of connections.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 1024.
    pub fn with_pool_size(mut self, value: u32) -> Result<Self, PostgresConfigError> {
        self.pool_size = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets connection acquisition timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or above close timeout.
    pub fn with_acquire_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.acquire_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets TCP, TLS, authentication establishment timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes.
    pub fn with_connect_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.connect_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the ordinary server-side statement timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 24 hours or below lock timeout.
    pub fn with_statement_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.statement_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the server-side lock timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or above statement
    /// timeout.
    pub fn with_lock_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.lock_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets protection against idle open transactions.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 second through 24 hours.
    pub fn with_idle_transaction_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.idle_transaction_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets idle pooled-connection retirement.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 second through 24 hours.
    pub fn with_connection_idle_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.connection_idle_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum physical connection lifetime.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 minute through 7 days.
    pub fn with_connection_max_lifetime(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.connection_max_lifetime = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets cooperative pool shutdown timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or below acquire timeout.
    pub fn with_pool_close_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.pool_close_timeout = value;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), PostgresConfigError> {
        if self.connection_string.is_empty() {
            return Err(PostgresConfigError::EmptyConnectionString);
        }
        validate_connection_query(&self.connection_string)?;
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(PostgresConfigError::PoolSize);
        }
        validate_duration(
            self.acquire_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "acquire",
        )?;
        validate_duration(
            self.connect_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "connect",
        )?;
        validate_duration(
            self.statement_timeout,
            Duration::from_millis(1),
            MAX_STATEMENT_TIMEOUT,
            "statement",
        )?;
        validate_duration(
            self.lock_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "lock",
        )?;
        validate_duration(
            self.idle_transaction_timeout,
            Duration::from_secs(1),
            MAX_STATEMENT_TIMEOUT,
            "idle transaction",
        )?;
        validate_duration(
            self.connection_idle_timeout,
            Duration::from_secs(1),
            MAX_STATEMENT_TIMEOUT,
            "connection idle",
        )?;
        validate_duration(
            self.connection_max_lifetime,
            Duration::from_mins(1),
            MAX_CONNECTION_LIFETIME,
            "connection lifetime",
        )?;
        validate_duration(
            self.pool_close_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "pool close",
        )?;
        if self.lock_timeout > self.statement_timeout {
            return Err(PostgresConfigError::LockExceedsStatement);
        }
        if self.acquire_timeout > self.pool_close_timeout {
            return Err(PostgresConfigError::AcquireExceedsClose);
        }
        Ok(())
    }

    fn connect_options(&self) -> Result<PgConnectOptions, PostgresConfigError> {
        let mut options = PgConnectOptions::from_str(&self.connection_string)
            .map_err(|_| PostgresConfigError::InvalidConnectionString)?;
        options = options.application_name("oxide-batch");
        options = match &self.tls_mode {
            TlsMode::VerifyFull { ca_certificate } => {
                let options = options.ssl_mode(PgSslMode::VerifyFull);
                if let Some(certificate) = ca_certificate {
                    options.ssl_root_cert_from_pem(certificate.as_bytes().to_vec())
                } else {
                    options
                }
            }
            TlsMode::Plaintext => options.ssl_mode(PgSslMode::Disable),
        };
        Ok(options)
    }
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresConfig")
            .field("connection", &"<redacted>")
            .field(
                "tls_mode",
                &match self.tls_mode {
                    TlsMode::VerifyFull { .. } => "verify-full",
                    TlsMode::Plaintext => "plaintext",
                },
            )
            .field("pool_size", &self.pool_size)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("statement_timeout", &self.statement_timeout)
            .field("lock_timeout", &self.lock_timeout)
            .field("idle_transaction_timeout", &self.idle_transaction_timeout)
            .field("connection_idle_timeout", &self.connection_idle_timeout)
            .field("connection_max_lifetime", &self.connection_max_lifetime)
            .field("pool_close_timeout", &self.pool_close_timeout)
            .finish_non_exhaustive()
    }
}

/// Invalid facade-owned `PostgreSQL` configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresConfigError {
    /// The redacted connection string was empty.
    EmptyConnectionString,
    /// The redacted connection string was not a `PostgreSQL` connection string.
    InvalidConnectionString,
    /// Pool size was outside 1 through 1024.
    PoolSize,
    /// TLS material or mode was embedded in the connection string.
    TlsOptionInConnectionString,
    /// An explicitly supplied CA bundle was empty.
    EmptyCaCertificate,
    /// An explicitly supplied CA bundle exceeded its hard limit.
    CaCertificateTooLarge {
        /// Maximum accepted PEM bytes.
        max_bytes: usize,
    },
    /// A timeout was outside its documented finite bounds.
    Timeout {
        /// The safe timeout class.
        class: &'static str,
    },
    /// Lock timeout exceeded ordinary statement timeout.
    LockExceedsStatement,
    /// Acquire timeout exceeded cooperative pool-close timeout.
    AcquireExceedsClose,
}

impl fmt::Display for PostgresConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyConnectionString => {
                formatter.write_str("PostgreSQL connection string is empty")
            }
            Self::InvalidConnectionString => {
                formatter.write_str("PostgreSQL connection string is invalid")
            }
            Self::PoolSize => formatter.write_str("PostgreSQL pool size must be from 1 to 1024"),
            Self::TlsOptionInConnectionString => formatter
                .write_str("PostgreSQL TLS options must use facade-owned TLS configuration"),
            Self::EmptyCaCertificate => {
                formatter.write_str("PostgreSQL CA certificate bundle is empty")
            }
            Self::CaCertificateTooLarge { max_bytes } => write!(
                formatter,
                "PostgreSQL CA certificate bundle exceeds {max_bytes} bytes"
            ),
            Self::Timeout { class } => {
                write!(
                    formatter,
                    "PostgreSQL {class} timeout is outside its bounds"
                )
            }
            Self::LockExceedsStatement => {
                formatter.write_str("PostgreSQL lock timeout exceeds statement timeout")
            }
            Self::AcquireExceedsClose => {
                formatter.write_str("PostgreSQL acquire timeout exceeds pool close timeout")
            }
        }
    }
}

impl Error for PostgresConfigError {}

fn validate_duration(
    value: Duration,
    minimum: Duration,
    maximum: Duration,
    class: &'static str,
) -> Result<(), PostgresConfigError> {
    if value < minimum || value > maximum {
        return Err(PostgresConfigError::Timeout { class });
    }
    Ok(())
}

fn validate_connection_query(connection_string: &str) -> Result<(), PostgresConfigError> {
    let Some((_, query)) = connection_string.split_once('?') else {
        return Ok(());
    };
    for pair in query.split('&') {
        let key = pair
            .split_once('=')
            .map_or(pair, |(key, _)| key)
            .to_ascii_lowercase();
        if matches!(
            key.as_str(),
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
        ) {
            return Err(PostgresConfigError::TlsOptionInConnectionString);
        }
        let recognized = matches!(
            key.as_str(),
            "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || (key.starts_with("options[") && key.ends_with(']'));
        if !recognized {
            return Err(PostgresConfigError::InvalidConnectionString);
        }
    }
    Ok(())
}

/// Applies the immutable `OxideBatch` `PostgreSQL` migration set.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresMigrator;

impl PostgresMigrator {
    /// Returns the schema version installed by this crate.
    #[must_use]
    pub const fn supported_schema_version() -> u32 {
        SUPPORTED_SCHEMA_VERSION
    }

    /// Applies pending migrations under a database-scoped advisory lock.
    ///
    /// This operation is intended for a dedicated migrator identity. Runtime
    /// repository startup only verifies the schema and never migrates it.
    ///
    /// # Errors
    ///
    /// Returns a redacted configuration or repository failure. A newer schema
    /// is never changed.
    pub async fn migrate(config: &PostgresConfig) -> Result<(), RepositoryError> {
        config
            .validate()
            .map_err(|_| RepositoryError::Unavailable)?;
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection =
            tokio::time::timeout(config.connect_timeout, PgConnection::connect_with(&options))
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .map_err(|_| RepositoryError::Unavailable)?;
        let lock_timeout = duration_millis(config.lock_timeout)?;
        sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(lock_timeout.to_string())
            .execute(&mut connection)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        tokio::time::timeout(
            config.lock_timeout,
            sqlx::query(
                "SELECT pg_advisory_lock(hashtextextended('oxide_batch.schema.migrations', 0))",
            )
            .execute(&mut connection),
        )
        .await
        .map_err(|_| RepositoryError::Unavailable)?
        .map_err(|_| RepositoryError::Unavailable)?;

        let result = async {
            match read_schema_version(&mut connection).await {
                Ok(current) => verify_schema_version(current)?,
                Err(RepositoryError::SchemaUninitialized) => {
                    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch")
                        .execute(&mut connection)
                        .await
                        .map_err(|_| RepositoryError::Unavailable)?;
                }
                Err(error) => return Err(error),
            }
            sqlx::query("SET search_path TO oxide_batch, pg_catalog")
                .execute(&mut connection)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            MIGRATOR
                .run(&mut connection)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let installed = read_schema_version(&mut connection).await?;
            verify_schema_version(installed)
        }
        .await;

        let _unlock = sqlx::query(
            "SELECT pg_advisory_unlock(hashtextextended('oxide_batch.schema.migrations', 0))",
        )
        .execute(&mut connection)
        .await;
        result
    }
}

/// Durable `PostgreSQL` implementation of [`JobRepository`].
///
/// `PostgreSQL` identity columns issue instance and execution identifiers, so
/// allocations remain collision-free across process restarts.
#[derive(Clone)]
pub struct PostgresJobRepository {
    pool: PgPool,
    clock: Arc<dyn Clock>,
    config: PostgresConfig,
}

impl PostgresJobRepository {
    /// Opens and verifies a repository-owned `PostgreSQL` pool.
    ///
    /// # Errors
    ///
    /// Returns a redacted unavailable or schema-version failure. Call
    /// [`PostgresMigrator::migrate`] separately with a migrator identity.
    pub async fn connect(
        config: PostgresConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        config
            .validate()
            .map_err(|_| RepositoryError::Unavailable)?;
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let pool = tokio::time::timeout(
            config.connect_timeout,
            PgPoolOptions::new()
                .max_connections(config.pool_size)
                .acquire_timeout(config.acquire_timeout)
                .idle_timeout(Some(config.connection_idle_timeout))
                .max_lifetime(Some(config.connection_max_lifetime))
                .connect_with(options),
        )
        .await
        .map_err(|_| RepositoryError::Unavailable)?
        .map_err(|_| RepositoryError::Unavailable)?;
        let current = read_schema_version(&pool).await?;
        verify_schema_version(current)?;
        Ok(Self {
            pool,
            clock,
            config,
        })
    }

    /// Closes the repository pool within the configured deadline.
    ///
    /// # Errors
    ///
    /// Returns unavailable when active borrowers do not release the pool
    /// before the deadline.
    pub async fn close(&self) -> Result<(), RepositoryError> {
        tokio::time::timeout(self.config.pool_close_timeout, self.pool.close())
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }
}

impl fmt::Debug for PostgresJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresJobRepository")
            .field("pool_size", &self.pool.size())
            .field("pool_idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

impl JobRepository for PostgresJobRepository {
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let mut connection = self
                .pool
                .acquire()
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            if sqlx::query("BEGIN")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
                return Err(RepositoryError::Unavailable);
            }
            if let Err(error) = configure_transaction(&mut connection, &self.config).await {
                connection.close_on_drop();
                return Err(error);
            }
            Ok(Box::new(PostgresUnitOfWork {
                repository: self,
                connection: Some(connection),
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

struct PostgresUnitOfWork<'repository> {
    repository: &'repository PostgresJobRepository,
    connection: Option<PoolConnection<Postgres>>,
}

impl PostgresUnitOfWork<'_> {
    fn transaction(&mut self) -> Result<&mut PoolConnection<Postgres>, RepositoryError> {
        self.connection.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<JobExecution>, RepositoryError> {
        let id = database_id(id.get(), IdentifierKind::JobExecution)?;
        let row = sqlx::query(AssertSqlSafe(job_execution_select(
            "WHERE execution.id = $1",
        )))
        .bind(id)
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_job_execution(&row)).transpose()
    }

    async fn step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> Result<Option<StepExecution>, RepositoryError> {
        let id = database_id(id.get(), IdentifierKind::StepExecution)?;
        let row = sqlx::query(AssertSqlSafe(step_execution_select(
            "WHERE execution.id = $1",
        )))
        .bind(id)
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_step_execution(&row)).transpose()
    }

    async fn classify_job_cas(
        &mut self,
        id: JobExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.job_execution(id).await {
            Ok(Some(actual)) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected,
                actual: actual.version(),
            }),
            Ok(None) => RepositoryError::JobExecutionNotFound { id },
            Err(error) => error,
        }
    }

    async fn classify_step_cas(
        &mut self,
        id: StepExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.step_execution(id).await {
            Ok(Some(actual)) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected,
                actual: actual.version(),
            }),
            Ok(None) => RepositoryError::StepExecutionNotFound { id },
            Err(error) => error,
        }
    }
}

impl RepositoryUnitOfWork for PostgresUnitOfWork<'_> {
    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        Box::pin(async move {
            let encoded = encode_identifying_parameters(key)?;
            let instance_key = canonical_instance_digest(key)?;
            let created_ms = system_time_millis(self.repository.clock.now())?;
            let inserted = sqlx::query(
                "INSERT INTO oxide_batch.ob_job_instance \
                 (job_name, instance_key, identifying_parameters, created_at) \
                 VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000.0)) \
                 ON CONFLICT (job_name, instance_key) DO NOTHING \
                 RETURNING id, job_name, identifying_parameters",
            )
            .bind(key.job_name().as_str())
            .bind(&instance_key[..])
            .bind(Json(encoded))
            .bind(created_ms)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            if let Some(row) = inserted {
                return Ok(JobInstanceSelection::Created(decode_job_instance(&row)?));
            }

            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance \
                 WHERE job_name = $1 AND instance_key = $2",
            )
            .bind(key.job_name().as_str())
            .bind(&instance_key[..])
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            Ok(JobInstanceSelection::Existing(decode_job_instance(&row)?))
        })
    }

    #[allow(clippy::too_many_lines)]
    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let instance_database_id =
                database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let instance = sqlx::query(
                "SELECT job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE",
            )
            .bind(instance_database_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobInstanceNotFound {
                id: job_instance_id,
            })?;

            let latest = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = $1 ORDER BY execution.attempt DESC LIMIT 1",
            )))
            .bind(instance_database_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map(|row| decode_job_execution(&row))
            .transpose()?;

            if let Some(execution) = &latest {
                match execution.metadata().status() {
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
                            execution_id: execution.id(),
                            status,
                        });
                    }
                }
            }

            let job_name: String = instance
                .try_get("job_name")
                .map_err(|_| RepositoryError::Unavailable)?;
            let parameters: Json<Value> = instance
                .try_get("identifying_parameters")
                .map_err(|_| RepositoryError::Unavailable)?;
            let registered_at = self.repository.clock.now();
            let definition_id =
                ensure_default_definition(&mut **self.transaction()?, &job_name, registered_at)
                    .await?;
            let attempt: i32 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(attempt), 0) + 1 \
                 FROM oxide_batch.ob_job_execution WHERE job_instance_id = $1",
            )
            .bind(instance_database_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let restart_of = latest
                .as_ref()
                .map(|execution| database_id(execution.id().get(), IdentifierKind::JobExecution))
                .transpose()?;
            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let context = Json(json!({}));
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_job_execution \
                 (job_instance_id, definition_id, restart_of_execution_id, attempt, \
                  status, exit_code, parameters, context_format, context_schema, \
                  context_schema_version, context_payload, created_at, updated_at, version) \
                 VALUES ($1, $2, $3, $4, 'STARTING', 'UNKNOWN', $5, 1, $6, 1, $7, \
                  to_timestamp($8::double precision / 1000.0), \
                  to_timestamp($8::double precision / 1000.0), 0) \
                 RETURNING id",
            )
            .bind(instance_database_id)
            .bind(definition_id)
            .bind(restart_of)
            .bind(attempt)
            .bind(parameters)
            .bind(DEFAULT_CONTEXT_SCHEMA)
            .bind(context)
            .bind(created_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id_value =
                JobExecutionId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
            Ok(JobExecution::new(
                id_value,
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
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
            )
            .bind(job_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_step_execution \
                 (job_execution_id, step_name, status, exit_code, \
                  checkpoint_format, checkpoint_schema, checkpoint_schema_version, \
                  checkpoint_payload, context_format, context_schema, \
                  context_schema_version, context_payload, created_at, updated_at, version) \
                 VALUES ($1, $2, 'STARTING', 'UNKNOWN', 1, $3, 1, $4, 1, $3, 1, $4, \
                  to_timestamp($5::double precision / 1000.0), \
                  to_timestamp($5::double precision / 1000.0), 0) \
                 RETURNING id",
            )
            .bind(job_id)
            .bind(step_name.as_str())
            .bind(DEFAULT_CONTEXT_SCHEMA)
            .bind(Json(json!({})))
            .bind(created_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id_value =
                StepExecutionId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
            Ok(StepExecution::new(
                id_value,
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
            let affected = update_job_execution(
                &mut **self.transaction()?,
                &execution,
                transition.transitioned_at(),
                expected_version,
            )
            .await?;
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
            let affected = update_job_execution(
                &mut **self.transaction()?,
                &execution,
                updated_at,
                expected_version,
            )
            .await?;
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
            let affected = update_step_execution(
                &mut **self.transaction()?,
                &execution,
                transition.transitioned_at(),
                expected_version,
            )
            .await?;
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
            let affected = update_step_execution(
                &mut **self.transaction()?,
                &execution,
                updated_at,
                expected_version,
            )
            .await?;
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
            let digest = canonical_instance_digest(key)?;
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance \
                 WHERE job_name = $1 AND instance_key = $2",
            )
            .bind(key.job_name().as_str())
            .bind(&digest[..])
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let id = database_id(id.get(), IdentifierKind::JobInstance)?;
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move { self.job_execution(id).await })
    }

    fn job_executions(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Vec<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            let instance_id = database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_instance WHERE id = $1)",
            )
            .bind(instance_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = $1 ORDER BY execution.attempt",
            )))
            .bind(instance_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_job_execution).collect()
        })
    }

    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move { self.step_execution(id).await })
    }

    fn step_executions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
            )
            .bind(job_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.job_execution_id = $1 ORDER BY execution.id",
            )))
            .bind(job_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_step_execution).collect()
        })
    }

    fn commit<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
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
            if sqlx::query("ROLLBACK")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
                return Err(RepositoryError::Unavailable);
            }
            Ok(())
        })
    }
}

impl Drop for PostgresUnitOfWork<'_> {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

async fn configure_transaction(
    connection: &mut PoolConnection<Postgres>,
    config: &PostgresConfig,
) -> Result<(), RepositoryError> {
    for (name, value) in [
        (
            "statement_timeout",
            duration_millis(config.statement_timeout)?,
        ),
        ("lock_timeout", duration_millis(config.lock_timeout)?),
        (
            "idle_in_transaction_session_timeout",
            duration_millis(config.idle_transaction_timeout)?,
        ),
    ] {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(name)
            .bind(value.to_string())
            .execute(&mut **connection)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
    }
    Ok(())
}

async fn read_schema_version<'executor, E>(executor: E) -> Result<u32, RepositoryError>
where
    E: sqlx::Executor<'executor, Database = Postgres>,
{
    let version = sqlx::query_scalar::<_, i32>(
        "SELECT version FROM oxide_batch.ob_schema_version WHERE singleton = TRUE",
    )
    .fetch_optional(executor)
    .await
    .map_err(|error| {
        if error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref()
            .is_some_and(|code| code == "42P01" || code == "3F000")
        {
            RepositoryError::SchemaUninitialized
        } else {
            RepositoryError::Unavailable
        }
    })?
    .ok_or(RepositoryError::SchemaUninitialized)?;
    u32::try_from(version).map_err(|_| RepositoryError::Unavailable)
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

fn duration_millis(duration: Duration) -> Result<i64, RepositoryError> {
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn database_id(value: u64, kind: IdentifierKind) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::IdentifierOutOfRange { kind, value })
}

fn system_time_millis(value: SystemTime) -> Result<i64, RepositoryError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn millis_system_time(value: i64) -> Result<SystemTime, RepositoryError> {
    let value = u64::try_from(value).map_err(|_| RepositoryError::Unavailable)?;
    Ok(UNIX_EPOCH + Duration::from_millis(value))
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

fn canonical_instance_digest(key: &JobInstanceKey) -> Result<[u8; 32], RepositoryError> {
    let mut encoded = Vec::new();
    encoded.push(1);
    push_length_prefixed(&mut encoded, key.job_name().as_str().as_bytes())?;
    for (name, kind) in key.identifying_fields() {
        push_length_prefixed(&mut encoded, name.as_str().as_bytes())?;
        let value = key
            .identifying_value(name)
            .ok_or(RepositoryError::Unavailable)?;
        encoded.push(parameter_tag(kind));
        match kind {
            ParameterValueKind::String => push_length_prefixed(
                &mut encoded,
                value
                    .as_str()
                    .ok_or(RepositoryError::Unavailable)?
                    .as_bytes(),
            )?,
            ParameterValueKind::I64 => encoded.extend_from_slice(
                &value
                    .as_i64()
                    .ok_or(RepositoryError::Unavailable)?
                    .to_be_bytes(),
            ),
            ParameterValueKind::U64 => encoded.extend_from_slice(
                &value
                    .as_u64()
                    .ok_or(RepositoryError::Unavailable)?
                    .to_be_bytes(),
            ),
            ParameterValueKind::Bool => encoded.push(u8::from(
                value.as_bool().ok_or(RepositoryError::Unavailable)?,
            )),
        }
        if encoded.len() > MAX_INSTANCE_KEY_INPUT {
            return Err(RepositoryError::Unavailable);
        }
    }
    Ok(Sha256::digest(encoded).into())
}

fn push_length_prefixed(target: &mut Vec<u8>, value: &[u8]) -> Result<(), RepositoryError> {
    let length = u32::try_from(value.len()).map_err(|_| RepositoryError::Unavailable)?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
    if target.len() > MAX_INSTANCE_KEY_INPUT {
        return Err(RepositoryError::Unavailable);
    }
    Ok(())
}

const fn parameter_tag(kind: ParameterValueKind) -> u8 {
    match kind {
        ParameterValueKind::String => 1,
        ParameterValueKind::I64 => 2,
        ParameterValueKind::U64 => 3,
        ParameterValueKind::Bool => 4,
    }
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
        };
        object.insert(
            name.as_str().to_owned(),
            json!({"type": kind_name, "identifying": true, "value": value}),
        );
    }
    let value = Value::Object(object);
    let size = serde_json::to_vec(&value)
        .map_err(|_| RepositoryError::Unavailable)?
        .len();
    if size > MAX_INSTANCE_KEY_INPUT {
        return Err(RepositoryError::Unavailable);
    }
    Ok(value)
}

fn decode_job_instance(row: &PgRow) -> Result<JobInstance, RepositoryError> {
    let id = row
        .try_get::<i64, _>("id")
        .map_err(|_| RepositoryError::Unavailable)?;
    let job_name = row
        .try_get::<String, _>("job_name")
        .map_err(|_| RepositoryError::Unavailable)?;
    let parameters = row
        .try_get::<Json<Value>, _>("identifying_parameters")
        .map_err(|_| RepositoryError::Unavailable)?;
    let parameters = decode_identifying_parameters(&parameters.0)?;
    let id = JobInstanceId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
    let key = JobInstanceKey::new(JobName::new(job_name)?, &parameters);
    Ok(JobInstance::new(id, key))
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

async fn ensure_default_definition(
    transaction: &mut PgConnection,
    job_name: &str,
    registered_at: SystemTime,
) -> Result<i64, RepositoryError> {
    let manifest = json!({
        "format": 1,
        "repository_port": "m1",
        "revision": DEFAULT_DEFINITION_REVISION
    });
    let canonical = serde_json::to_vec(&manifest).map_err(|_| RepositoryError::Unavailable)?;
    let digest = Sha256::digest(canonical);
    let registered_ms = system_time_millis(registered_at)?;
    sqlx::query(
        "INSERT INTO oxide_batch.ob_job_definition \
         (job_name, definition_revision, manifest_format, manifest_digest, manifest, registered_at) \
         VALUES ($1, $2, 1, $3, $4, to_timestamp($5::double precision / 1000.0)) \
         ON CONFLICT (job_name, definition_revision) DO NOTHING",
    )
    .bind(job_name)
    .bind(DEFAULT_DEFINITION_REVISION)
    .bind(&digest[..])
    .bind(Json(manifest))
    .bind(registered_ms)
    .execute(&mut *transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    sqlx::query_scalar(
        "SELECT id FROM oxide_batch.ob_job_definition \
         WHERE job_name = $1 AND definition_revision = $2 AND manifest_digest = $3",
    )
    .bind(job_name)
    .bind(DEFAULT_DEFINITION_REVISION)
    .bind(&digest[..])
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?
    .ok_or(RepositoryError::Unavailable)
}

fn job_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, execution.status, \
         execution.exit_code, 0::bigint AS read_count, 0::bigint AS processed_count, \
         0::bigint AS write_count, 0::bigint AS filter_count, \
         0::bigint AS commit_count, 0::bigint AS rollback_count, \
         execution.failure_category, execution.failure_id, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.version FROM oxide_batch.ob_job_execution execution {suffix}"
    )
}

fn step_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.status, execution.exit_code, execution.read_count, \
         execution.processed_count, execution.write_count, execution.filter_count, \
         execution.commit_count, execution.rollback_count, execution.failure_category, \
         execution.failure_id, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.version FROM oxide_batch.ob_step_execution execution {suffix}"
    )
}

fn decode_job_execution(row: &PgRow) -> Result<JobExecution, RepositoryError> {
    let id = JobExecutionId::new(read_u64(row, "id")?)?;
    let instance_id = JobInstanceId::new(read_u64(row, "job_instance_id")?)?;
    let metadata = decode_execution_metadata(row)?;
    let version = ExecutionVersion::new(read_u64(row, "version")?);
    Ok(JobExecution::from_snapshot(
        id,
        instance_id,
        metadata,
        version,
    ))
}

fn decode_step_execution(row: &PgRow) -> Result<StepExecution, RepositoryError> {
    let id = StepExecutionId::new(read_u64(row, "id")?)?;
    let job_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let name = StepName::new(
        row.try_get::<String, _>("step_name")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let metadata = decode_execution_metadata(row)?;
    let version = ExecutionVersion::new(read_u64(row, "version")?);
    Ok(StepExecution::from_snapshot(
        id, job_id, name, metadata, version,
    ))
}

fn decode_execution_metadata(row: &PgRow) -> Result<ExecutionMetadata, RepositoryError> {
    let status = decode_status(
        &row.try_get::<String, _>("status")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let exit_status = ExitStatus::new(ExitCode::new(
        row.try_get::<String, _>("exit_code")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?);
    let timestamps = ExecutionTimestamps::new(
        millis_system_time(read_i64(row, "created_ms")?)?,
        read_optional_i64(row, "started_ms")?
            .map(millis_system_time)
            .transpose()?,
        read_optional_i64(row, "ended_ms")?
            .map(millis_system_time)
            .transpose()?,
    )?;
    let counts = ExecutionCounts::new(
        read_u64(row, "read_count")?,
        read_u64(row, "processed_count")?,
        read_u64(row, "write_count")?,
        read_u64(row, "filter_count")?,
        read_u64(row, "commit_count")?,
        read_u64(row, "rollback_count")?,
    );
    let category = row
        .try_get::<Option<String>, _>("failure_category")
        .map_err(|_| RepositoryError::Unavailable)?;
    let failure_id = read_optional_i64(row, "failure_id")?;
    let failure = match (category, failure_id) {
        (None, None) => None,
        (Some(category), Some(id)) => Some(FailureSummary::new(
            decode_failure_category(&category)?,
            FailureId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?,
        )),
        _ => return Err(RepositoryError::Unavailable),
    };
    ExecutionMetadata::new(status, exit_status, timestamps, counts, failure)
        .map_err(RepositoryError::from)
}

async fn update_job_execution(
    transaction: &mut PgConnection,
    execution: &JobExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    update_execution(
        transaction,
        "oxide_batch.ob_job_execution",
        execution.id().get(),
        IdentifierKind::JobExecution,
        execution.metadata(),
        execution.version(),
        updated_at,
        expected,
    )
    .await
}

async fn update_step_execution(
    transaction: &mut PgConnection,
    execution: &StepExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    update_execution(
        transaction,
        "oxide_batch.ob_step_execution",
        execution.id().get(),
        IdentifierKind::StepExecution,
        execution.metadata(),
        execution.version(),
        updated_at,
        expected,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn update_execution(
    transaction: &mut PgConnection,
    table: &'static str,
    id: u64,
    kind: IdentifierKind,
    metadata: &ExecutionMetadata,
    new_version: ExecutionVersion,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let query = format!(
        "UPDATE {table} SET status = $1, exit_code = $2, failure_category = $3, \
         failure_id = $4, started_at = CASE WHEN $5::bigint IS NULL THEN NULL \
         ELSE to_timestamp($5::double precision / 1000.0) END, \
         ended_at = CASE WHEN $6::bigint IS NULL THEN NULL \
         ELSE to_timestamp($6::double precision / 1000.0) END, \
         updated_at = to_timestamp($7::double precision / 1000.0), version = $8 \
         WHERE id = $9 AND version = $10"
    );
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let failure_category = failure.map(|value| encode_failure_category(value.category()));
    let failure_id = failure
        .map(|value| database_id(value.failure_id().get(), IdentifierKind::Failure))
        .transpose()?;
    let result = sqlx::query(AssertSqlSafe(query))
        .bind(metadata.status().to_string())
        .bind(metadata.exit_status().code().as_str())
        .bind(failure_category)
        .bind(failure_id)
        .bind(
            timestamps
                .started_at()
                .map(system_time_millis)
                .transpose()?,
        )
        .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
        .bind(system_time_millis(updated_at)?)
        .bind(database_version(new_version)?)
        .bind(database_id(id, kind)?)
        .bind(database_version(expected)?)
        .execute(transaction)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

fn database_version(version: ExecutionVersion) -> Result<i64, RepositoryError> {
    i64::try_from(version.get()).map_err(|_| RepositoryError::Unavailable)
}

fn read_i64(row: &PgRow, name: &str) -> Result<i64, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i64(row: &PgRow, name: &str) -> Result<Option<i64>, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &PgRow, name: &str) -> Result<u64, RepositoryError> {
    u64::try_from(read_i64(row, name)?).map_err(|_| RepositoryError::Unavailable)
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

const fn encode_failure_category(value: FailureCategory) -> &'static str {
    match value {
        FailureCategory::InvalidDefinition => "INVALID_DEFINITION",
        FailureCategory::DuplicateExecution => "DUPLICATE_EXECUTION",
        FailureCategory::IllegalTransition => "ILLEGAL_TRANSITION",
        FailureCategory::TransientInfrastructure => "TRANSIENT_INFRASTRUCTURE",
        FailureCategory::PermanentInfrastructure => "PERMANENT_INFRASTRUCTURE",
        FailureCategory::UserComponent => "USER_COMPONENT",
        FailureCategory::Cancelled => "CANCELLED",
        FailureCategory::Serialization => "SERIALIZATION",
        FailureCategory::Invariant => "INVARIANT",
    }
}

fn decode_failure_category(value: &str) -> Result<FailureCategory, RepositoryError> {
    match value {
        "INVALID_DEFINITION" => Ok(FailureCategory::InvalidDefinition),
        "DUPLICATE_EXECUTION" => Ok(FailureCategory::DuplicateExecution),
        "ILLEGAL_TRANSITION" => Ok(FailureCategory::IllegalTransition),
        "TRANSIENT_INFRASTRUCTURE" => Ok(FailureCategory::TransientInfrastructure),
        "PERMANENT_INFRASTRUCTURE" => Ok(FailureCategory::PermanentInfrastructure),
        "USER_COMPONENT" => Ok(FailureCategory::UserComponent),
        "CANCELLED" => Ok(FailureCategory::Cancelled),
        "SERIALIZATION" => Ok(FailureCategory::Serialization),
        "INVARIANT" => Ok(FailureCategory::Invariant),
        _ => Err(RepositoryError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_instance_key_matches_version_one_golden_vector() -> Result<(), Box<dyn Error>> {
        let parameters = JobParameters::try_from_iter([
            (
                ParameterName::new("region")?,
                JobParameter::new(ParameterValue::string("서울")?, ParameterRole::Identifying),
            ),
            (
                ParameterName::new("limit")?,
                JobParameter::new(
                    ParameterValue::from(1_u64 << 63),
                    ParameterRole::Identifying,
                ),
            ),
            (
                ParameterName::new("count")?,
                JobParameter::new(ParameterValue::from(-2_i64), ParameterRole::Identifying),
            ),
            (
                ParameterName::new("active")?,
                JobParameter::new(ParameterValue::from(true), ParameterRole::Identifying),
            ),
        ])?;
        let key = JobInstanceKey::new(JobName::new("golden_job")?, &parameters);
        assert_eq!(
            canonical_instance_digest(&key)?,
            [
                0x71, 0xf1, 0x2d, 0xb9, 0xe3, 0x88, 0x7d, 0xe2, 0xcf, 0x92, 0xe9, 0x3b, 0xb6, 0x3f,
                0xd4, 0xe9, 0xe7, 0xc5, 0x36, 0xdf, 0x8f, 0xa2, 0x02, 0x21, 0x24, 0x45, 0xd1, 0x8b,
                0xf2, 0xe4, 0x36, 0x04,
            ]
        );
        Ok(())
    }
}
